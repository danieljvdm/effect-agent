import { browserRestCrawlLayer } from "@effect-agent/platform-cloudflare/browser-rest-crawl";
import { PageCrawl, PageCrawlLimits, PageCrawlRequest } from "@effect-agent/sandbox";
import { phase7LiveProfileEnabled } from "@effect-agent/testing/fixtures/travel-planner";
import { Config, Console, Effect, Stream } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { describe, expect, it } from "vite-plus/test";

const ACCOUNT = "CLOUDFLARE_ACCOUNT_ID";
const TOKEN = "CLOUDFLARE_API_TOKEN";
const START_URL = "https://httpbin.org/html";
const START_HOST = "httpbin.org";
const PROOF_FACT = "Herman Melville - Moby-Dick";
const MAX_PAGES = 3;
const MAX_PAGE_BYTES = 32 * 1_024;
const enabled =
  phase7LiveProfileEnabled(process.env) && !!process.env[ACCOUNT] && !!process.env[TOKEN];

describe.skipIf(!enabled)("Browser Run REST crawl smoke (opt-in)", () => {
  it("streams at most three same-host Markdown records", { timeout: 150_000 }, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const accountId = yield* Config.nonEmptyString(ACCOUNT);
        const apiToken = yield* Config.redacted(TOKEN);
        const records = yield* Effect.gen(function* () {
          const crawl = yield* PageCrawl;
          return yield* crawl
            .crawl(
              PageCrawlRequest.make({
                startUrl: START_URL,
                purposes: ["search"],
                limits: PageCrawlLimits.make({
                  maxPages: MAX_PAGES,
                  maxDepth: 1,
                  maxPageBytes: MAX_PAGE_BYTES,
                  maxTotalBytes: MAX_PAGES * MAX_PAGE_BYTES,
                  deadlineMillis: 120_000,
                }),
              }),
            )
            .pipe(Stream.runCollect);
        }).pipe(Effect.scoped, Effect.provide(browserRestCrawlLayer({ accountId, apiToken })));

        expect(records.length).toBeGreaterThan(0);
        expect(records.length).toBeLessThanOrEqual(MAX_PAGES);
        for (const record of records) {
          expect(new URL(record.url).host).toBe(START_HOST);
          if (record.markdown !== undefined) {
            expect(new TextEncoder().encode(record.markdown).byteLength).toBeLessThanOrEqual(
              MAX_PAGE_BYTES,
            );
          }
        }
        const completed = records.filter(
          (record) => record.status === "completed" && record.markdown !== undefined,
        );
        expect(completed.length).toBeGreaterThan(0);
        expect(completed[0]?.markdown).toContain(PROOF_FACT);

        yield* Console.log(
          "\nCloudflare Browser Run crawl records:",
          JSON.stringify(
            records.map((record) => ({
              url: record.url,
              status: record.status,
              markdown: record.markdown?.slice(0, 2_048),
            })),
            null,
            2,
          ),
        );
      }).pipe(Effect.provide(FetchHttpClient.layer)),
    ),
  );
});
