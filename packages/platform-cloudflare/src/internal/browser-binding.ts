/// <reference types="@cloudflare/workers-types" />
import puppeteer from "@cloudflare/puppeteer";
import { Schema } from "effect";

import { browserFailure, BrowserRunFailure } from "./browser-failure.ts";

const Acquired = Schema.Struct({ sessionId: Schema.String.check(Schema.isUUID()) });

/** One acquisition path for ordinary and protected sessions, with explicit recording disabled. */
export const acquireBrowserSession = async (
  browser: Pick<BrowserRun, "fetch">,
  keepAliveMillis: number,
  operation: "protected.acquire" | "interactive.acquire",
): Promise<string> => {
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
};

/** The SDK embeds provider bodies and session IDs in errors; preserve status before that loss. */
export const connectBrowserSession = async (
  browser: Pick<BrowserRun, "fetch">,
  sessionId: string,
  operation: string,
) => {
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
};
