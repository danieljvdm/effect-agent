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

/** Private attachment ownership is available before asynchronous SDK initialization completes. */
export interface BrowserRunAttachment {
  readonly browser: Promise<Browser>;
  /** Fence SDK dispatch/callbacks and await local socket closure; never terminate the provider. */
  readonly retire: Effect.Effect<void, BrowserRunFailure>;
}

/** Native transport port shared by both scoped adapters; the Layer owns the foreign binding. */
export class BrowserRunBinding extends Context.Service<
  BrowserRunBinding,
  {
    readonly acquire: (
      keepAliveMillis: number,
      operation: "interactive.acquire" | "session.acquire",
    ) => Promise<string>;
    readonly connect: (
      sessionId: string,
      operation: string,
      signal?: AbortSignal,
    ) => BrowserRunAttachment;
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
      connect: (sessionId, operation, signal) => {
        let socket: WebSocket | undefined;
        let retired = false;
        let closedByAbort = false;
        let retirementFailure: BrowserRunFailure | undefined;

        const disconnectOperation =
          operation === "session.connect" ? "session.disconnect" : "interactive.disconnect";

        const transport: ConnectionTransport = {
          send: (message) => {
            if (retired || socket === undefined)
              throw new BrowserRunFailure({ operation, reason: "provider" });
            socket.send(message);
          },
          close: () => retireNow(),
        };

        const onMessage = (event: MessageEvent) => {
          if (!retired) transport.onmessage?.(event.data);
        };

        const fence = () => {
          if (retired) return;
          retired = true;
          const notify = transport.onclose;

          transport.onmessage = undefined;
          transport.onclose = undefined;
          // Notify the SDK before touching the socket: disposal must reject pending CDP calls
          // even when the raw transport cannot close. The send fence also covers queued work.
          try {
            notify?.();
          } finally {
            socket?.removeEventListener("message", onMessage);
          }
        };

        const retireNow = () => {
          try {
            fence();
          } catch (cause) {
            retirementFailure ??= browserFailure(disconnectOperation, cause);
          }
          try {
            // Manual-close runtimes also require a reply when the peer leaves us CLOSING.
            if (socket !== undefined && socket.readyState < WebSocket.CLOSED) socket.close();
          } catch (cause) {
            retirementFailure ??= browserFailure(disconnectOperation, cause);
          }
          if (retirementFailure !== undefined) throw retirementFailure;
        };

        const abort = () => {
          closedByAbort = socket?.readyState === WebSocket.OPEN;
          try {
            retireNow();
          } catch {
            // The attachment owner observes this separately from the initiating failure.
          }
        };

        signal?.addEventListener("abort", abort, { once: true });

        const initialized = (async () => {
          try {
            signal?.throwIfAborted();
            socket = await upgrade(sessionId, operation, signal);
            socket.accept();
            if (retired || signal?.aborted) {
              // No SDK command may escape a late upgrade. Preserve a late local-close
              // failure independently of the cancellation already returned to the owner.
              retireNow();
              signal?.throwIfAborted();
              throw new BrowserRunFailure({ operation, reason: "provider" });
            }
            socket.addEventListener("message", onMessage);
            socket.addEventListener(
              "close",
              () => {
                try {
                  retireNow();
                } catch (cause) {
                  retirementFailure ??= browserFailure(disconnectOperation, cause);
                }
              },
              { once: true },
            );

            // Attaching must not replace the viewport owned by the host or another viewer.
            return await puppeteer.connect({ transport, defaultViewport: null });
          } catch (cause) {
            try {
              retireNow();
            } catch {
              // Preserve both facts: initialization failed, and retirement may be unconfirmed.
            }
            const failure = browserFailure(operation, cause);

            const localCancellation =
              cause instanceof Error &&
              signal?.aborted === true &&
              (cause === signal.reason || (closedByAbort && cause.name === "TargetCloseError"));

            throw localCancellation ? reportedBrowserError(failure) : failure;
          } finally {
            signal?.removeEventListener("abort", abort);
          }
        })();

        return {
          browser: initialized,
          retire: Effect.callback<void, BrowserRunFailure>((resume) => {
            const connected = socket;
            const onClose = () => resume(Effect.void);

            try {
              retireNow();
              if (connected === undefined || connected.readyState === WebSocket.CLOSED)
                resume(Effect.void);
              else connected.addEventListener("close", onClose, { once: true });
            } catch (cause) {
              resume(Effect.fail(browserFailure(disconnectOperation, cause)));
            }

            return Effect.sync(() => connected?.removeEventListener("close", onClose));
          }),
        };
      },
      keepAlive: Effect.fnUntraced(function* (sessionId: string) {
        const operation = "session.keepAlive";
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
