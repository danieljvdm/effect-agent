/// <reference types="@cloudflare/workers-types" />
import puppeteer, { type Browser } from "@cloudflare/puppeteer";
import { type InteractiveBrowserPolicy } from "@effect-agent/sandbox/InteractiveBrowser";
import { ProtectedBrowserError } from "@effect-agent/sandbox/ProtectedBrowser";
import { Clock, Context, Crypto, Effect, Layer, Redacted, Schema, type Scope } from "effect";

import { BrowserRunSessionLifecycle } from "../internal/browser-session-lifecycle.ts";
import { makeProtectedNativeTransport, ProtectedNativeSession } from "./native.ts";
import {
  BrowserRunProtectedTransport,
  ProtectedTransportError,
  type ProtectedBrowserTransport,
} from "./policy.ts";

/**
 * Acquires through BROWSER with recording=false explicitly on the wire. The trusted provider's
 * documented opt-in semantics are the recording guarantee; no recording-status API exists.
 * Account operators remain trusted, and must not attach external observers to private passes.
 */
export const browserRunProtectedBindingLayer = (options: {
  readonly browser: Pick<BrowserRun, "fetch">;
}) =>
  Layer.effect(BrowserRunProtectedTransport)(
    Effect.gen(function* () {
      const lifecycle = yield* BrowserRunSessionLifecycle;
      const crypto = yield* Crypto.Crypto;

      const open = Effect.fn("BrowserRunProtectedTransport.open")(function* (
        policy: InteractiveBrowserPolicy,
      ): Effect.fn.Return<ProtectedBrowserTransport, ProtectedBrowserError, Scope.Scope> {
        const failure = () =>
          new ProtectedBrowserError({
            reason: "provider",
            dispatch: "not-dispatched",
            milestone: "none",
            observation: "closed",
            cleanup: "unconfirmed",
          });

        let sessionId: Redacted.Redacted<string> | undefined;
        let browser: Browser | undefined;
        let driver: ProtectedBrowserTransport | undefined;
        let invalid = false;
        // SDK acquisition may finish after interruption. Its late-reply callback must await cleanup,
        // using this pass's clock rather than starting an Effect runtime with default services.
        const runCleanup = Effect.runPromiseWith(Context.make(Clock.Clock, yield* Clock.Clock));

        const terminate = Effect.gen(function* () {
          invalid = true;
          driver?.invalidate();
          if (sessionId === undefined) return "unconfirmed" as const;

          const cleanup = yield* lifecycle.close(sessionId).pipe(
            Effect.as("confirmed" as const),
            Effect.catchCause(() => Effect.succeed("unconfirmed" as const)),
            Effect.interruptible,
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () => Effect.succeed("unconfirmed" as const),
            }),
          );

          // Local disconnect is not remote-closure evidence. Do it even when confirmation fails.
          const connected = browser;

          if (connected !== undefined)
            yield* Effect.promise(() => connected.disconnect()).pipe(
              Effect.catchCause(() => Effect.void),
              Effect.interruptible,
              Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.void }),
            );

          return cleanup;
        });

        const close = yield* Effect.cached(terminate);

        yield* Effect.addFinalizer(() =>
          close.pipe(
            Effect.flatMap((state) =>
              state === "confirmed"
                ? Effect.void
                : Effect.logWarning("Protected browser exact-session closure unconfirmed"),
            ),
          ),
        );

        const acquired = yield* Effect.tryPromise({
          try: async (signal) => {
            let recordingDisabled = false;

            const binding = {
              fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
                const request = new Request(input, init);
                const url = new URL(request.url);

                if (url.pathname === "/v1/devtools/browser" && request.method === "POST") {
                  url.searchParams.set("recording", "false");
                  recordingDisabled = url.searchParams.get("recording") === "false";

                  return options.browser.fetch(new Request(url, request));
                }

                return options.browser.fetch(request);
              },
            };

            const acquired = await puppeteer.acquire(binding, {
              recording: false,
              keep_alive: Math.max(10_000, policy.maxElapsedMillis),
            });

            sessionId = Redacted.make(
              Schema.decodeUnknownSync(Schema.String.check(Schema.isUUID()))(acquired.sessionId),
            );
            if (!recordingDisabled || signal.aborted || invalid) {
              await runCleanup(terminate);
              throw new ProtectedTransportError({ reason: "stale-reference" });
            }
            // Initial attachment only. No recovery/reconnection path exists.
            browser = await puppeteer.connect(options.browser, acquired.sessionId);
            if (signal.aborted || invalid) {
              await runCleanup(terminate);
              throw new ProtectedTransportError({ reason: "stale-reference" });
            }
            const context = await browser.createBrowserContext();
            const page = await context.newPage();

            await page.setBypassServiceWorker(true);
            await page.setRequestInterception(true);
            page.on("request", (request) => {
              let allowed = policy.network._tag === "Unrestricted";

              try {
                const url = new URL(request.url());

                allowed ||=
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
              ).catch(() => {
                invalid = true;
                driver?.invalidate();
              });
            });
            if (signal.aborted || invalid) {
              await runCleanup(terminate);
              throw new ProtectedTransportError({ reason: "stale-reference" });
            }

            return ProtectedNativeSession.of({ browser, page, close });
          },
          catch: failure,
        }).pipe(
          Effect.timeoutOrElse({
            duration: Math.min(policy.maxElapsedMillis, 30_000),
            orElse: () =>
              Effect.fail(new ProtectedBrowserError({ ...failure(), reason: "timeout" })),
          }),
          Effect.catch((error) =>
            close.pipe(
              Effect.flatMap((cleanup) =>
                Effect.fail(new ProtectedBrowserError({ ...error, cleanup })),
              ),
            ),
          ),
        );

        driver = yield* makeProtectedNativeTransport(policy).pipe(
          Effect.provideService(ProtectedNativeSession, acquired),
          Effect.provideService(Crypto.Crypto, crypto),
        );

        return driver;
      }, Effect.withTracerEnabled(false));

      return { open };
    }),
  );
