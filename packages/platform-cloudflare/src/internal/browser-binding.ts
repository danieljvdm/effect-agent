/// <reference types="@cloudflare/workers-types" />
import { Cause, Context, Effect, Layer, Schema } from "effect";
import puppeteer, {
  type Browser,
  type ConnectionTransport,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import {
  browserFailure,
  BrowserRunFailure,
  reportBrowserCause,
  reportedBrowserError,
} from "./browser-failure.ts";

const Acquired = Schema.Struct({ sessionId: Schema.String.check(Schema.isUUID()) });

const VersionReply = Schema.fromJsonString(
  Schema.Struct({
    id: Schema.optionalKey(Schema.Int),
    result: Schema.optionalKey(Schema.Unknown),
    error: Schema.optionalKey(Schema.Unknown),
  }),
);

const Version = Schema.Struct({
  product: Schema.NonEmptyString,
  protocolVersion: Schema.NonEmptyString,
});

/** Native transport port shared by both scoped adapters; the Layer owns the foreign binding. */
export class BrowserRunBinding extends Context.Service<
  BrowserRunBinding,
  {
    readonly acquire: (
      keepAliveMillis: number,
      operation: "protected.acquire" | "interactive.acquire",
    ) => Promise<string>;
    readonly connect: (
      sessionId: string,
      operation: string,
      signal?: AbortSignal,
    ) => Promise<Browser>;
    readonly keepAlive: (sessionId: string) => Effect.Effect<void, BrowserRunFailure>;
  }
>()("@effect-agent/platform-cloudflare/internal/BrowserRunBinding") {
  static layer(browser: Pick<BrowserRun, "fetch">) {
    const upgrade = async (sessionId: string, operation: string, signal?: AbortSignal) => {
      const response = await browser.fetch(
        `https://browser-rendering.cloudflare.com/v1/devtools/browser/${sessionId}`,
        { headers: { Upgrade: "websocket" }, ...(signal === undefined ? {} : { signal }) },
      );

      if (response.status !== 101) {
        const failure = new BrowserRunFailure({
          operation,
          reason: "provider",
          status: response.status,
        });

        try {
          await response.body?.cancel();
        } catch {
          // Preserve the provider refusal when local response release also fails.
        }
        throw failure;
      }
      const socket = response.webSocket;

      if (socket === null) throw new BrowserRunFailure({ operation, reason: "malformed" });

      return socket;
    };

    return Layer.succeed(this)({
      // One allocation path, with recording explicitly disabled for private sessions.
      acquire: async (keepAliveMillis, operation) => {
        const response = await browser.fetch(
          `https://browser-rendering.cloudflare.com/v1/devtools/browser?keep_alive=${keepAliveMillis}&recording=false`,
          { method: "POST" },
        );

        if (response.status !== 200) {
          const failure = new BrowserRunFailure({
            operation,
            reason: "provider",
            status: response.status,
          });

          try {
            await response.body?.cancel();
          } catch {
            // Body cancellation cannot replace the acquisition refusal.
          }
          throw failure;
        }

        return Schema.decodeUnknownSync(Acquired)(await response.json()).sessionId;
      },
      connect: async (sessionId, operation, signal) => {
        let socket: WebSocket | undefined;
        let closedByAbort = false;

        try {
          const connected = await upgrade(sessionId, operation, signal);

          socket = connected;

          const transport: ConnectionTransport = {
            send: (message) => connected.send(message),
            close: () => connected.close(),
          };

          connected.addEventListener("message", (event) => {
            transport.onmessage?.(event.data);
          });
          connected.addEventListener("close", () => transport.onclose?.());
          connected.accept();

          const abort = () => {
            try {
              const wasOpen = connected.readyState === WebSocket.OPEN;

              connected.close();
              closedByAbort = wasOpen;
            } catch {
              // The local transport may already have closed; never terminate the provider here.
            }
          };

          signal?.addEventListener("abort", abort, { once: true });
          try {
            signal?.throwIfAborted();

            return await puppeteer.connect({ transport });
          } finally {
            signal?.removeEventListener("abort", abort);
          }
        } catch (cause) {
          try {
            socket?.close();
          } catch {
            // Preserve the connection failure when local release also fails.
          }
          const failure = browserFailure(operation, cause);

          // A local abort can reject SDK initialization as target-closed. Genuine late
          // provider failures still report, even when the caller's signal has aborted.
          const localCancellation =
            cause instanceof Error &&
            signal?.aborted === true &&
            (cause === signal.reason || (closedByAbort && cause.name === "TargetCloseError"));

          throw localCancellation ? reportedBrowserError(failure) : failure;
        }
      },
      keepAlive: Effect.fnUntraced(function* (sessionId: string) {
        const operation = "protected.keepAlive";
        const runReport = Effect.runPromiseWith(yield* Effect.context<never>());

        const release = (socket: WebSocket) =>
          Effect.sync(() => socket.close()).pipe(
            Effect.catchCause((cause) => reportBrowserCause(operation, cause)),
          );

        const socket = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: async (signal) => {
              let acquired: WebSocket | undefined;

              try {
                acquired = await upgrade(sessionId, operation, signal);
                acquired.accept();
                // The binding may resolve an upgrade after the Effect was interrupted.
                signal.throwIfAborted();

                return acquired;
              } catch (cause) {
                if (acquired !== undefined) await runReport(release(acquired));
                if (signal.aborted && cause !== signal.reason)
                  await runReport(reportBrowserCause(operation, Cause.fail(cause)));
                throw cause;
              }
            },
            catch: (cause) => browserFailure(operation, cause),
          }),
          release,
          { interruptible: true },
        );

        yield* Effect.callback<void, BrowserRunFailure>((resume) => {
          const finish = (result: Effect.Effect<void, BrowserRunFailure>) => {
            removeListeners();
            resume(result);
          };

          const fail = (reason: BrowserRunFailure["reason"]) =>
            finish(Effect.fail(new BrowserRunFailure({ operation, reason })));

          const onMessage = (event: MessageEvent) => {
            try {
              if (typeof event.data !== "string" || event.data.length > 16_384) {
                fail("malformed");

                return;
              }
              const reply = Schema.decodeSync(VersionReply)(event.data);

              if (reply.id !== 1) return;
              if (reply.error !== undefined) {
                fail("provider");

                return;
              }
              Schema.decodeUnknownSync(Version)(reply.result);
              finish(Effect.void);
            } catch {
              fail("malformed");
            }
          };

          const onClose = () => fail("provider");

          const removeListeners = () => {
            socket.removeEventListener("message", onMessage);
            socket.removeEventListener("close", onClose);
            socket.removeEventListener("error", onClose);
          };

          socket.addEventListener("message", onMessage);
          socket.addEventListener("close", onClose);
          socket.addEventListener("error", onClose);
          try {
            socket.send(JSON.stringify({ id: 1, method: "Browser.getVersion" }));
          } catch (cause) {
            finish(Effect.fail(browserFailure(operation, cause)));
          }

          return Effect.sync(removeListeners);
        });
      }, Effect.scoped),
    });
  }
}
