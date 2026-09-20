/// <reference types="@cloudflare/workers-types" />

import {
  Cause,
  Clock,
  Context,
  Effect,
  Exit,
  Layer,
  Redacted,
  Schema,
  Semaphore,
  Scope,
} from "effect";
import type {
  Frame,
  JSHandle,
  Page,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import {
  CredentialFillError,
  type BrowserCredentialAccess,
  type CredentialFillResult,
  type FillCredentialRequest,
} from "./BrowserCredentials.ts";
import {
  BrowserRunHandoffRequest,
  BrowserRunHandoffResult,
  BrowserRunHandoffState,
  BrowserRunLiveViewRequest,
  BrowserRunLiveViewResult,
} from "./InteractiveBrowser.ts";
import { BrowserRunBinding, type BrowserRunAttachment } from "./internal/browser-binding.ts";
import { BrowserSessionPage, fillCredential } from "./internal/browser-credentials.ts";
import { reportBrowserCause, reportedBrowserError } from "./internal/browser-failure.ts";
import {
  BrowserRunSessionLifecycle,
  type BrowserRunLifecycleOptions,
} from "./internal/browser-session-lifecycle.ts";

export {
  BrowserRunHandoffRequest,
  BrowserRunHandoffResult,
  BrowserRunHandoffState,
  BrowserRunLiveViewRequest,
  BrowserRunLiveViewResult,
};

const PositiveMillis = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);

const SessionId = Schema.Redacted(Schema.String.check(Schema.isUUID()));
const ProviderId = Schema.Redacted(Schema.NonEmptyString.check(Schema.isMaxLength(256)));

/** Private host record, not a model capability. Persist together with the owner's controller state. */
export class BrowserSessionReference extends Schema.Class<BrowserSessionReference>(
  "BrowserSessionReference",
)({
  version: Schema.Literal(1),
  sessionId: SessionId,
  contextId: ProviderId,
  targetId: ProviderId,
  expiresAt: PositiveMillis,
  commandTimeoutMillis: PositiveMillis,
}) {}

export class BrowserSessionOptions extends Schema.Class<BrowserSessionOptions>(
  "BrowserSessionOptions",
)({
  maxElapsedMillis: PositiveMillis,
  keepAliveMillis: Schema.optionalKey(PositiveMillis.check(Schema.isLessThanOrEqualTo(600_000))),
  commandTimeoutMillis: Schema.optionalKey(PositiveMillis),
}) {}

/** No SDK text, selectors, page data, or credential values are retained in public failures. */
export class BrowserSessionError extends Schema.TaggedError<BrowserSessionError>()(
  "BrowserSessionError",
  {
    reason: Schema.Literals([
      "invalid",
      "expired",
      "missing-page",
      "busy",
      "closed",
      "provider",
      "timeout",
      "cleanup",
    ]),
    dispatch: Schema.Literals(["not-dispatched", "possibly-dispatched"]),
    cleanup: Schema.Literals(["not-requested", "confirmed", "unconfirmed"]),
  },
) {}

/**
 * One local attachment to an application-owned browser. Scope release disconnects; it never
 * transfers ownership or closes a healthy remote session. The owner serializes attachments,
 * fences old attempts, authorizes human access, and calls BrowserSessions.close on expiry/stop.
 * Native callbacks are trusted host code: never retain SDK handles or start unawaited work.
 */
/**
 * Trusted host callback creating an isolated-realm predicate, scoped to one credential fill.
 * The predicate runs synchronously beside each native write, receiving its actual field and index.
 * Return false to reject a stale observation. Never construct this program from model/page text.
 * The session disposes the returned handle, including on failure or interruption.
 */
export type CredentialTargetGuard = (
  frame: Frame,
) => Promise<JSHandle<(field: unknown, index: number) => boolean>>;

export interface BrowserSession {
  readonly reference: BrowserSessionReference;
  /**
   * Check current authority under the lock before native dispatch. A settled SDK rejection leaves
   * the session available for inspection, but may have changed the website; never retry blindly.
   * Timeout/interruption fences outstanding SDK work and terminates the exact browser.
   */
  readonly run: <A, E, R>(
    authorize: Effect.Effect<void, E, R>,
    action: (page: Page) => Promise<A>,
  ) => Effect.Effect<A, E | BrowserSessionError, R>;
  /**
   * Resolves host-owned material under fresh credential grants. Never submits or retries a write.
   * Credential timeouts retain acknowledged writes and dispatch evidence alongside cleanup.
   */
  readonly fillCredential: (
    request: FillCredentialRequest,
    guard?: CredentialTargetGuard,
  ) => Effect.Effect<
    CredentialFillResult,
    CredentialFillError | BrowserSessionError,
    BrowserCredentialAccess
  >;
  readonly handoff: <E, R>(
    authorize: Effect.Effect<void, E, R>,
    request: BrowserRunHandoffRequest,
  ) => Effect.Effect<BrowserRunHandoffResult, E | BrowserSessionError, R>;
  readonly getLiveView: <E, R>(
    authorize: Effect.Effect<void, E, R>,
    request: BrowserRunLiveViewRequest,
  ) => Effect.Effect<BrowserRunLiveViewResult, E | BrowserSessionError, R>;
  readonly getHandoffState: <E, R>(
    authorize: Effect.Effect<void, E, R>,
  ) => Effect.Effect<BrowserRunHandoffState, E | BrowserSessionError, R>;
}

const failure = (
  reason: BrowserSessionError["reason"],
  dispatch: BrowserSessionError["dispatch"] = "not-dispatched",
  cleanup: BrowserSessionError["cleanup"] = "not-requested",
) => new BrowserSessionError({ reason, dispatch, cleanup });

const decode = <A>(schema: Schema.Codec<A>, input: unknown) =>
  Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" })(input).pipe(
    Effect.mapError(() => failure("invalid")),
  );

// Catch inside the Promise before workerd can report an unhandled foreign diagnostic.
const native = Effect.fnUntraced(function* <A>(
  operation: string,
  action: (signal: AbortSignal) => Promise<A>,
) {
  const runReport = Effect.runPromiseWith(yield* Effect.context<never>());

  return yield* Effect.tryPromise({
    try: async (signal) => {
      try {
        return { ok: true as const, value: await action(signal) };
      } catch (cause) {
        if (!signal.aborted || cause !== signal.reason)
          await runReport(reportBrowserCause(operation, Cause.fail(cause)));

        return { ok: false as const };
      }
    },
    catch: () => failure("provider"),
  }).pipe(
    Effect.flatMap((result) =>
      result.ok
        ? Effect.succeed(result.value)
        : Effect.fail(reportedBrowserError(failure("provider"))),
    ),
    Effect.withTracerEnabled(false),
  );
});

const targetId = (page: Page) =>
  native("session.target", async () => {
    const cdp = await page.createCDPSession();

    try {
      return (await cdp.send("Target.getTargetInfo")).targetInfo.targetId;
    } finally {
      await cdp.detach();
    }
  });

export interface BrowserSessionsOptions extends BrowserRunLifecycleOptions {
  readonly browser: Pick<BrowserRun, "fetch">;
}

/** Native Cloudflare sessions; no framework browser workflow, checkpoint transfer, or observation policy. */
export class BrowserSessions extends Context.Service<
  BrowserSessions,
  {
    /**
     * Allocate once and commit its private reference in retain. Failed/uncommitted creation closes
     * the allocation. After retain succeeds the application owns remote cleanup, independent of
     * attempt Scopes. A lost allocation reply can remain indeterminate until provider idle expiry.
     */
    readonly create: <E, R>(
      options: BrowserSessionOptions,
      retain: (reference: BrowserSessionReference) => Effect.Effect<void, E, R>,
    ) => Effect.Effect<BrowserSessionReference, E | BrowserSessionError, R>;
    /** Attach only the exact saved context/page. Never recreate missing/expired state. */
    readonly attach: (
      reference: BrowserSessionReference,
    ) => Effect.Effect<BrowserSession, BrowserSessionError, Scope.Scope>;
    /** Refresh provider inactivity only; the owner must still enforce its expiresAt. */
    readonly keepAlive: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<void, BrowserSessionError>;
    readonly close: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<void, BrowserSessionError>;
  }
>()("@effect-agent/platform-cloudflare/BrowserSessions") {
  static readonly layerNoDeps = Layer.effect(
    this,
    Effect.gen(function* () {
      const binding = yield* BrowserRunBinding;
      const lifecycle = yield* BrowserRunSessionLifecycle;

      const close = (sessionId: Redacted.Redacted<string>) =>
        decode(SessionId, sessionId).pipe(
          Effect.flatMap((id) =>
            lifecycle
              .close(id)
              .pipe(Effect.mapError(() => failure("cleanup", "not-dispatched", "unconfirmed"))),
          ),
        );

      const cleanup = (sessionId: Redacted.Redacted<string>) =>
        close(sessionId).pipe(
          Effect.as("confirmed" as const),
          Effect.catchCause((cause) =>
            reportBrowserCause("session.close", cause).pipe(Effect.as("unconfirmed" as const)),
          ),
        );

      const connect = Effect.fnUntraced(function* (sessionId: Redacted.Redacted<string>) {
        let attachment: BrowserRunAttachment | undefined;

        const retire = Effect.suspend(() =>
          attachment === undefined
            ? Effect.void
            : attachment.retire.pipe(
                Effect.timeoutOrElse({
                  duration: "1 second",
                  orElse: () => Effect.fail(failure("cleanup")),
                }),
                Effect.catchCause((cause) =>
                  reportBrowserCause("session.disconnect", cause).pipe(
                    Effect.andThen(Effect.die(failure("cleanup", "not-dispatched", "unconfirmed"))),
                  ),
                ),
              ),
        );

        yield* Effect.addFinalizer(() => retire);

        const browser = yield* native("session.connect", (signal) => {
          attachment = binding.connect(Redacted.value(sessionId), "session.connect", signal);

          return attachment.browser;
        });

        return { browser, retire };
      });

      const create = <E, R>(
        input: BrowserSessionOptions,
        retain: (reference: BrowserSessionReference) => Effect.Effect<void, E, R>,
      ) =>
        Effect.gen(function* () {
          const options = yield* decode(BrowserSessionOptions, input);
          const expiresAt = (yield* Clock.currentTimeMillis) + options.maxElapsedMillis;

          if (!Number.isSafeInteger(expiresAt)) return yield* failure("invalid");
          let sessionId: Redacted.Redacted<string> | undefined;
          let retained = false;
          const runCleanup = Effect.runPromiseWith(yield* Effect.context<never>());

          yield* Effect.addFinalizer(() =>
            sessionId === undefined || retained ? Effect.void : cleanup(sessionId),
          );
          sessionId = yield* native("session.acquire", async (signal) => {
            const id = Redacted.make(
              await binding.acquire(options.keepAliveMillis ?? 600_000, "session.acquire"),
            );

            if (signal.aborted) {
              await runCleanup(cleanup(id));
              signal.throwIfAborted();
            }

            return id;
          });
          const { browser } = yield* connect(sessionId);

          const page = yield* native("session.page", async () => {
            const context = await browser.createBrowserContext();

            return await context.newPage();
          });

          const reference = yield* decode(BrowserSessionReference, {
            version: 1,
            sessionId,
            contextId: Redacted.make(page.browserContext().id ?? ""),
            targetId: Redacted.make(yield* targetId(page)),
            expiresAt,
            commandTimeoutMillis: options.commandTimeoutMillis ?? 30_000,
          });

          yield* Effect.uninterruptibleMask((restore) =>
            restore(retain(reference)).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  retained = true;
                }),
              ),
            ),
          );

          return reference;
        }).pipe(
          Effect.scoped,
          Effect.timeoutOrElse({
            duration: "30 seconds",
            orElse: () => Effect.fail(failure("timeout")),
          }),
          Effect.withTracerEnabled(false),
        );

      const attachPage = Effect.fnUntraced(function* (input: BrowserSessionReference) {
        const reference = yield* decode(BrowserSessionReference, input);

        if (reference.expiresAt <= (yield* Clock.currentTimeMillis))
          return yield* failure("expired");
        const scope = yield* Effect.scope;
        const { browser, retire } = yield* connect(reference.sessionId);

        const context = browser
          .browserContexts()
          .find((value) => value.id === Redacted.value(reference.contextId));

        if (context === undefined) return yield* failure("missing-page");
        let page: Page | undefined;

        for (const candidate of yield* native("session.pages", () => context.pages())) {
          if ((yield* targetId(candidate)) === Redacted.value(reference.targetId)) {
            page = candidate;
            break;
          }
        }
        if (page === undefined) return yield* failure("missing-page");
        const currentPage = page;
        const lock = yield* Semaphore.make(1);
        let invalid = false;

        const terminate = Effect.suspend(() => {
          invalid = true;

          return retire.pipe(
            Effect.catchCause((cause) => reportBrowserCause("session.disconnect", cause)),
            Effect.andThen(cleanup(reference.sessionId)),
          );
        });

        const runEffect = <A, E, R>(
          action: (commandTimeoutMillis: number) => Effect.Effect<A, E, R>,
        ) =>
          lock
            .withPermitsIfAvailable(1)(
              Effect.gen(function* () {
                if (invalid || scope.state._tag === "Closed" || !browser.isConnected())
                  return yield* failure("closed");
                const remaining = reference.expiresAt - (yield* Clock.currentTimeMillis);

                if (remaining <= 0) {
                  return yield* failure("expired", "not-dispatched", yield* terminate);
                }

                return yield* action(Math.min(remaining, reference.commandTimeoutMillis)).pipe(
                  Effect.catchDefect((defect) =>
                    reportBrowserCause("session.command", Cause.die(defect)).pipe(
                      Effect.andThen(terminate),
                      Effect.flatMap((closed) =>
                        Effect.fail(failure("provider", "possibly-dispatched", closed)),
                      ),
                    ),
                  ),
                  Effect.onInterrupt(() => terminate),
                );
              }),
            )
            .pipe(
              Effect.flatMap(Effect.fromOption(() => failure("busy"))),
              Effect.withTracerEnabled(false),
            );

        const run = <A, E, R>(
          authorize: Effect.Effect<void, E, R>,
          action: (page: Page) => Promise<A>,
          terminateOnError = false,
        ) =>
          runEffect((commandTimeoutMillis) =>
            authorize.pipe(
              Effect.andThen(() =>
                native("session.command", () => action(currentPage)).pipe(
                  Effect.catch((error) =>
                    Effect.gen(function* () {
                      const closed = terminateOnError ? yield* terminate : "not-requested";

                      return yield* reportedBrowserError(
                        failure(error.reason, "possibly-dispatched", closed),
                      );
                    }),
                  ),
                ),
              ),
              Effect.timeoutOrElse({
                duration: commandTimeoutMillis,
                orElse: () =>
                  terminate.pipe(
                    Effect.flatMap((closed) =>
                      Effect.fail(failure("timeout", "possibly-dispatched", closed)),
                    ),
                  ),
              }),
            ),
          );

        const command = <A, E, R>(
          authorize: Effect.Effect<void, E, R>,
          method: string,
          parameters: Record<string, string | number>,
          schema: Schema.Codec<A>,
        ) =>
          run(
            authorize,
            async (page) => {
              const cdp = await page.createCDPSession();

              try {
                const result: unknown = await Reflect.apply(cdp.send, cdp, [method, parameters]);

                return Schema.decodeUnknownSync(schema)(result);
              } finally {
                await cdp.detach();
              }
            },
            method === "Cloudflare.handoff",
          );

        const session: BrowserSession = {
          reference,
          run,
          fillCredential: (request, guard) =>
            runEffect((commandTimeoutMillis) =>
              fillCredential(request, guard).pipe(
                Effect.provideService(BrowserSessionPage, {
                  page: currentPage,
                  commandTimeoutMillis,
                }),
                Effect.catchIf(
                  (error) => error.reason === "timeout" || error.dispatch === "possibly-dispatched",
                  (error) =>
                    terminate.pipe(
                      Effect.flatMap((cleanup) =>
                        Effect.fail(CredentialFillError.make({ ...error, cleanup })),
                      ),
                    ),
                ),
              ),
            ),
          handoff: (authorize, request) =>
            decode(BrowserRunHandoffRequest, request).pipe(
              Effect.flatMap((value) =>
                command(
                  authorize,
                  "Cloudflare.handoff",
                  { instructions: value.instructions, timeout: value.timeout },
                  Schema.Struct({
                    handoffId: Schema.NonEmptyString.check(Schema.isMaxLength(1024)),
                  }),
                ),
              ),
              Effect.map((value) =>
                BrowserRunHandoffResult.make({ handoffId: Redacted.make(value.handoffId) }),
              ),
            ),
          getLiveView: (authorize, request) =>
            decode(BrowserRunLiveViewRequest, request).pipe(
              Effect.flatMap((value) =>
                command(
                  authorize,
                  "Cloudflare.getLiveView",
                  { mode: value.mode, expiresInMs: value.expiresInMs },
                  Schema.Struct({ devtoolsFrontendUrl: Schema.String }),
                ),
              ),
              Effect.flatMap((value) =>
                decode(BrowserRunLiveViewResult, {
                  devtoolsFrontendUrl: Redacted.make(value.devtoolsFrontendUrl),
                }),
              ),
            ),
          getHandoffState: (authorize) =>
            command(
              authorize,
              "Cloudflare.getHandoffState",
              {},
              Schema.Struct({
                active: Schema.Boolean,
                handoffId: Schema.optionalKey(
                  Schema.NonEmptyString.check(Schema.isMaxLength(1024)),
                ),
                durationMs: Schema.optionalKey(Schema.Natural),
              }),
            ).pipe(
              Effect.map((value) =>
                BrowserRunHandoffState.make({
                  active: value.active,
                  ...(value.handoffId === undefined
                    ? {}
                    : { handoffId: Redacted.make(value.handoffId) }),
                  ...(value.durationMs === undefined ? {} : { durationMs: value.durationMs }),
                }),
              ),
            ),
        };

        return session;
      });

      const attach = Effect.fnUntraced(function* (input: BrowserSessionReference) {
        const reference = yield* decode(BrowserSessionReference, input);
        const scope = yield* Scope.fork(yield* Effect.scope);

        return yield* attachPage(reference).pipe(
          Effect.provideService(Scope.Scope, scope),
          Effect.timeoutOrElse({
            duration: reference.commandTimeoutMillis,
            orElse: () =>
              reportBrowserCause("session.connect", Cause.fail(failure("timeout"))).pipe(
                Effect.andThen(Effect.fail(reportedBrowserError(failure("timeout")))),
              ),
          }),
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
        );
      });

      return BrowserSessions.of({
        create,
        attach,
        close,
        keepAlive: (sessionId) =>
          decode(SessionId, sessionId).pipe(
            Effect.flatMap((id) =>
              binding.keepAlive(Redacted.value(id)).pipe(
                Effect.timeoutOrElse({
                  duration: "10 seconds",
                  orElse: () => Effect.fail(failure("timeout")),
                }),
                Effect.catch((error) =>
                  reportBrowserCause("session.keepAlive", Cause.fail(error)).pipe(
                    Effect.andThen(
                      Effect.fail(
                        reportedBrowserError(
                          failure(error.reason === "timeout" ? "timeout" : "provider"),
                        ),
                      ),
                    ),
                  ),
                ),
              ),
            ),
            Effect.withTracerEnabled(false),
          ),
      });
    }),
  );

  static layer(options: BrowserSessionsOptions) {
    return this.layerNoDeps.pipe(
      Layer.provide(BrowserRunBinding.layer(options.browser)),
      Layer.provide(BrowserRunSessionLifecycle.layer(options)),
    );
  }
}
