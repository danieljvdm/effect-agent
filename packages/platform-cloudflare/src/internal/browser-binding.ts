/// <reference types="@cloudflare/workers-types" />
import { Context, Layer, Schema } from "effect";
import puppeteer, {
  type Browser,
  type ConnectionTransport,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { browserFailure, BrowserRunFailure } from "./browser-failure.ts";

const Acquired = Schema.Struct({ sessionId: Schema.String.check(Schema.isUUID()) });

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
  }
>()("@effect-agent/platform-cloudflare/internal/BrowserRunBinding") {
  static layer(browser: Pick<BrowserRun, "fetch">) {
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

        try {
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
              // Body cancellation cannot replace the provider refusal.
            }
            throw failure;
          }
          const connected = response.webSocket;

          if (connected === null) throw new BrowserRunFailure({ operation, reason: "malformed" });
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
              connected.close();
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
          throw browserFailure(operation, cause);
        }
      },
    });
  }
}
