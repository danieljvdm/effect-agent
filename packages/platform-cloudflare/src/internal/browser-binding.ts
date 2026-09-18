/// <reference types="@cloudflare/workers-types" />
import puppeteer, { type Browser } from "@cloudflare/puppeteer";
import { Context, Layer, Schema } from "effect";

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
    readonly connect: (sessionId: string, operation: string) => Promise<Browser>;
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
      // The SDK embeds bodies and session IDs in errors; retain status before that loss.
      connect: async (sessionId, operation) => {
        let failure: BrowserRunFailure | undefined;

        try {
          return await puppeteer.connect(
            {
              fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
                try {
                  const response = await browser.fetch(input, init);

                  if (response.status !== 101) {
                    failure = new BrowserRunFailure({
                      operation,
                      reason: "provider",
                      status: response.status,
                    });
                    await response.body?.cancel();
                    throw failure;
                  }

                  return response;
                } catch (cause) {
                  failure ??= browserFailure(operation, cause);
                  throw failure;
                }
              },
            },
            sessionId,
          );
        } catch (cause) {
          throw failure ?? browserFailure(operation, cause);
        }
      },
    });
  }
}
