/// <reference types="@cloudflare/workers-types" />

import { Cause, Context, Crypto, Effect, Layer, Redacted, Schema, type Scope } from "effect";
import { type InteractiveBrowserPolicy } from "effect-agent/interactive-browser";
import { ProtectedBrowserError } from "effect-agent/protected-browser";
import {
  type Browser,
  type Page,
  type CDPSession,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { BrowserRunBinding, type BrowserRunAttachment } from "../internal/browser-binding.ts";
import {
  browserFailure,
  BrowserRunFailure,
  reportBrowserCause,
  reportedBrowserError,
} from "../internal/browser-failure.ts";
import { BrowserRunSessionLifecycle } from "../internal/browser-session-lifecycle.ts";
import { makeProtectedNativeTransport, ProtectedNativeSession } from "./native.ts";
import {
  BrowserRunProtectedTransport,
  ProtectedTransportError,
  type ProtectedBrowserTransport,
} from "./policy.ts";

/** Host-private exact page identity; never include it in model-visible results. */
export const ProtectedProviderIdentity = Schema.Struct({
  sessionId: Schema.Redacted(Schema.String.check(Schema.isUUID())),
  contextId: Schema.Redacted(Schema.NonEmptyString.check(Schema.isMaxLength(256))),
  targetId: Schema.Redacted(Schema.NonEmptyString.check(Schema.isMaxLength(256))),
});

export type ProtectedProviderIdentity = typeof ProtectedProviderIdentity.Type;

export interface ProtectedProviderSession {
  readonly identity: ProtectedProviderIdentity;
  readonly driver: ProtectedBrowserTransport;
  readonly command: (
    method: "Cloudflare.getLiveView" | "Cloudflare.handoff" | "Cloudflare.getHandoffState",
    parameters: Readonly<Record<string, string | number>>,
  ) => Effect.Effect<unknown, ProtectedBrowserError>;
  readonly detach: Effect.Effect<void, ProtectedBrowserError>;
}

export class BrowserRunProtectedBinding extends Context.Service<
  BrowserRunProtectedBinding,
  {
    readonly open: (
      policy: InteractiveBrowserPolicy,
      identity?: ProtectedProviderIdentity,
    ) => Effect.Effect<ProtectedProviderSession, ProtectedBrowserError, Scope.Scope>;
    readonly keepAlive: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<void, ProtectedBrowserError>;
  }
>()("@effect-agent/platform-cloudflare/BrowserRunProtectedBinding") {}

/**
 * Acquires through BROWSER with recording=false explicitly on the wire. The trusted provider's
 * documented opt-in semantics are the recording guarantee; no recording-status API exists.
 * Account operators remain trusted, and must not attach external observers to private passes.
 */
const protectedBindingLayer = (options: { readonly browser: Pick<BrowserRun, "fetch"> }) =>
  Layer.effect(BrowserRunProtectedBinding)(
    Effect.gen(function* () {
      const lifecycle = yield* BrowserRunSessionLifecycle;
      const binding = yield* BrowserRunBinding;
      const crypto = yield* Crypto.Crypto;

      const open = Effect.fn("BrowserRunProtectedTransport.open")(function* (
        policy: InteractiveBrowserPolicy,
        identity?: ProtectedProviderIdentity,
      ): Effect.fn.Return<ProtectedProviderSession, ProtectedBrowserError, Scope.Scope> {
        const failure = () =>
          new ProtectedBrowserError({
            reason: "provider",
            dispatch: "not-dispatched",
            milestone: "none",
            observation: "closed",
            cleanup: "unconfirmed",
          });

        let sessionId: Redacted.Redacted<string> | undefined = identity?.sessionId;
        let detached = false;
        let control: CDPSession | undefined;
        let providerIdentity: ProtectedProviderIdentity | undefined;
        let browser: Browser | undefined;
        let attachment: BrowserRunAttachment | undefined;
        let driver: ProtectedBrowserTransport | undefined;
        let invalid = false;
        // SDK acquisition may finish after interruption. Its late-reply callback must await cleanup,
        // retaining this pass's clock and private reporting scope.
        const runCleanup = Effect.runPromiseWith(yield* Effect.context<never>());

        const terminate = Effect.gen(function* () {
          invalid = true;
          driver?.invalidate();
          if (sessionId === undefined) return "unconfirmed" as const;

          // A failed resume has not acquired the host's retained provider. Release only
          // this connection; successful attachments and new allocations own termination.
          if (identity !== undefined && driver === undefined) {
            if (attachment !== undefined)
              yield* attachment.retire.pipe(
                Effect.interruptible,
                Effect.timeoutOrElse({
                  duration: "1 second",
                  orElse: () =>
                    Effect.fail(
                      new BrowserRunFailure({
                        operation: "protected.disconnect",
                        reason: "timeout",
                      }),
                    ),
                }),
                Effect.catch((error) =>
                  reportBrowserCause("protected.disconnect", Cause.fail(error)).pipe(
                    Effect.andThen(() => Effect.die(reportedBrowserError(error))),
                  ),
                ),
              );

            return "unconfirmed" as const;
          }

          const cleanup = yield* lifecycle.close(sessionId).pipe(
            Effect.as("confirmed" as const),
            Effect.catchCause((cause) =>
              reportBrowserCause("protected.close", cause).pipe(Effect.as("unconfirmed" as const)),
            ),
            Effect.interruptible,
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () =>
                reportBrowserCause(
                  "protected.close",
                  Cause.fail(
                    new BrowserRunFailure({ operation: "protected.close", reason: "timeout" }),
                  ),
                ).pipe(Effect.as("unconfirmed" as const)),
            }),
          );

          // Local disconnect is not remote-closure evidence. Do it even when confirmation fails.
          const connected = browser;

          if (connected !== undefined)
            yield* Effect.promise(() => connected.disconnect()).pipe(
              Effect.catchCause((cause) => reportBrowserCause("protected.disconnect", cause)),
              Effect.interruptible,
              Effect.timeoutOrElse({
                duration: "1 second",
                orElse: () =>
                  reportBrowserCause(
                    "protected.disconnect",
                    Cause.fail(
                      new BrowserRunFailure({
                        operation: "protected.disconnect",
                        reason: "timeout",
                      }),
                    ),
                  ),
              }),
            );

          return cleanup;
        });

        const close = yield* Effect.cached(terminate);

        yield* Effect.addFinalizer(() => (detached ? Effect.void : close));
        let stage = identity === undefined ? "protected.acquire" : "protected.connect";

        const acquired = yield* Effect.tryPromise({
          try: async (signal) => {
            try {
              sessionId =
                identity === undefined
                  ? Redacted.make(
                      await binding.acquire(
                        Math.min(600_000, Math.max(10_000, policy.maxElapsedMillis)),
                        "protected.acquire",
                      ),
                    )
                  : identity.sessionId;
              if (signal.aborted || invalid) {
                await runCleanup(terminate);
                throw new ProtectedTransportError({ reason: "stale-reference" });
              }
              // Resume only a host-persisted exact page. Never open a replacement page or context.
              stage = "protected.connect";
              attachment = binding.connect(Redacted.value(sessionId), stage, signal);
              browser = await attachment.browser;
              if (signal.aborted || invalid) {
                await runCleanup(terminate);
                throw new ProtectedTransportError({ reason: "stale-reference" });
              }
              let page: Page;

              if (identity === undefined) {
                stage = "protected.context";
                const context = await browser.createBrowserContext();

                stage = "protected.page";
                page = await context.newPage();
              } else {
                stage = "protected.resume";

                const context = browser
                  .browserContexts()
                  .find((candidate) => candidate.id === Redacted.value(identity.contextId));

                if (context === undefined)
                  throw new ProtectedTransportError({ reason: "stale-reference" });
                const pages = await context.pages();
                let found: Page | undefined;

                for (const candidate of pages) {
                  const client = await candidate.createCDPSession();
                  const info = await client.send("Target.getTargetInfo");

                  await client.detach();
                  if (info.targetInfo.targetId === Redacted.value(identity.targetId))
                    found = candidate;
                }
                if (found === undefined || pages.length !== 1)
                  throw new ProtectedTransportError({ reason: "stale-reference" });
                page = found;
              }
              stage = "protected.control";
              control = await page.createCDPSession();
              const info = await control.send("Target.getTargetInfo");

              stage = "protected.identity";
              providerIdentity = Schema.decodeSync(ProtectedProviderIdentity)({
                sessionId,
                contextId: Redacted.make(page.browserContext().id ?? ""),
                targetId: Redacted.make(info.targetInfo.targetId),
              });

              // Unrestricted passes need no attachment-local network enforcement. Leave their
              // service workers and requests alone, including across host-owned detach/resume.
              if (policy.network._tag !== "Unrestricted") {
                stage = "protected.interception";
                await page.setBypassServiceWorker(true);
                await page.setRequestInterception(true);
                page.on("request", (request) => {
                  let allowed = false;

                  try {
                    const url = new URL(request.url());

                    allowed =
                      policy.network._tag === "ExactHosts" &&
                      url.protocol === "https:" &&
                      !url.username &&
                      !url.password &&
                      policy.network.allowedHosts.includes(url.host);
                  } catch {
                    /* Refuse malformed destinations. */
                  }
                  void (
                    allowed && !invalid ? request.continue() : request.abort("blockedbyclient")
                  ).catch(async (cause) => {
                    invalid = true;
                    driver?.invalidate();
                    await runCleanup(
                      reportBrowserCause("protected.interception", Cause.fail(cause)),
                    );
                  });
                });
              }
              if (signal.aborted || invalid) {
                await runCleanup(terminate);
                throw new ProtectedTransportError({ reason: "stale-reference" });
              }

              return ProtectedNativeSession.of({
                browser,
                page,
                close,
                release: Effect.suspend(() => (detached ? Effect.void : close.pipe(Effect.asVoid))),
              });
            } catch (cause) {
              if (signal.aborted && !(cause instanceof ProtectedTransportError))
                await runCleanup(reportBrowserCause(stage, Cause.fail(cause)));
              throw cause;
            }
          },
          catch: (cause) => browserFailure(stage, cause),
        }).pipe(
          Effect.timeoutOrElse({
            duration: Math.min(policy.maxElapsedMillis, 30_000),
            orElse: () =>
              Effect.fail(new BrowserRunFailure({ operation: stage, reason: "timeout" })),
          }),
          Effect.catch((error) => {
            if (identity !== undefined && driver === undefined)
              return reportBrowserCause(stage, Cause.fail(error)).pipe(
                Effect.andThen(
                  Effect.fail(
                    reportedBrowserError(
                      new ProtectedBrowserError({
                        ...failure(),
                        reason: error.reason === "timeout" ? "timeout" : "provider",
                        cleanup: "not-requested",
                      }),
                    ),
                  ),
                ),
                // A disposal defect stays alongside the original failure. Only a pure typed
                // failure proves that this pre-handle attachment is safe to relinquish.
                Effect.ensuring(close),
              );

            return reportBrowserCause(stage, Cause.fail(error)).pipe(
              Effect.andThen(close),
              Effect.flatMap((cleanup) =>
                Effect.fail(
                  reportedBrowserError(
                    new ProtectedBrowserError({
                      ...failure(),
                      reason: error.reason === "timeout" ? "timeout" : "provider",
                      cleanup,
                    }),
                  ),
                ),
              ),
            );
          }),
        );

        driver = yield* makeProtectedNativeTransport(policy).pipe(
          Effect.provideService(ProtectedNativeSession, acquired),
          Effect.provideService(Crypto.Crypto, crypto),
        );

        const exact = providerIdentity;

        if (exact === undefined) return yield* failure();

        return {
          identity: exact,
          driver,
          command: (method, parameters) =>
            Effect.tryPromise({
              try: async () => {
                if (invalid || detached || control === undefined) throw undefined;

                // Cloudflare extends CDP with host-only methods absent from upstream ProtocolMapping.
                const send = Reflect.get(control, "send");

                return await Reflect.apply(send, control, [method, parameters]);
              },
              catch: (cause) => browserFailure("protected.command", cause),
            }).pipe(
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () =>
                  Effect.fail(
                    new BrowserRunFailure({ operation: "protected.command", reason: "timeout" }),
                  ),
              }),
              Effect.catch((error) =>
                reportBrowserCause("protected.command", Cause.fail(error)).pipe(
                  Effect.andThen(Effect.fail(reportedBrowserError(failure()))),
                ),
              ),
            ),
          detach: Effect.tryPromise({
            try: async () => {
              if (invalid || detached || browser === undefined) throw undefined;
              // Stop this attachment before another controller can attach. The browser remains live.
              await browser.disconnect();
              detached = true;
              driver?.invalidate();
            },
            catch: (cause) => browserFailure("protected.detach", cause),
          }).pipe(
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () =>
                Effect.fail(
                  new BrowserRunFailure({ operation: "protected.detach", reason: "timeout" }),
                ),
            }),
            Effect.catch((error) =>
              reportBrowserCause("protected.detach", Cause.fail(error)).pipe(
                Effect.andThen(Effect.fail(reportedBrowserError(failure()))),
              ),
            ),
          ),
        };
      }, Effect.withTracerEnabled(false));

      const keepAlive = Effect.fn("BrowserRunProtectedBinding.keepAlive")(function* (
        sessionId: Redacted.Redacted<string>,
      ) {
        const failure = (reason: ProtectedBrowserError["reason"]) =>
          new ProtectedBrowserError({
            reason,
            dispatch: "not-dispatched",
            milestone: "none",
            observation: "protected",
            cleanup: "not-requested",
          });

        const id = yield* Schema.decodeEffect(ProtectedProviderIdentity.fields.sessionId)(
          sessionId,
        ).pipe(Effect.mapError(() => failure("denied")));

        yield* binding.keepAlive(Redacted.value(id)).pipe(
          Effect.timeoutOrElse({
            duration: "10 seconds",
            orElse: () =>
              Effect.fail(
                new BrowserRunFailure({ operation: "protected.keepAlive", reason: "timeout" }),
              ),
          }),
          Effect.tapError((error) => reportBrowserCause("protected.keepAlive", Cause.fail(error))),
          Effect.mapError((error) =>
            reportedBrowserError(failure(error.reason === "timeout" ? "timeout" : "provider")),
          ),
        );
      }, Effect.withTracerEnabled(false));

      return { open, keepAlive };
    }),
  ).pipe(Layer.provide(BrowserRunBinding.layer(options.browser)));

/** One binding implementation serves scoped tools and host-managed protected handoffs. */
export const browserRunProtectedBindingLayer = (options: {
  readonly browser: Pick<BrowserRun, "fetch">;
}) =>
  Layer.effect(BrowserRunProtectedTransport)(
    Effect.gen(function* () {
      const binding = yield* BrowserRunProtectedBinding;

      return {
        open: (policy: InteractiveBrowserPolicy) =>
          binding.open(policy).pipe(Effect.map((session) => session.driver)),
      };
    }),
  ).pipe(Layer.provideMerge(protectedBindingLayer(options)));
