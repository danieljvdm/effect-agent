import { browserRestCrawlLayer } from "@effect-agent/platform-cloudflare/browser-rest-crawl";
import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Redacted, Stream } from "effect";
import {
  PageCrawl,
  PageCrawlLimits,
  PageCrawlRequest,
  type PageCrawlError,
  type PageCrawlRecord,
} from "effect-agent/page-crawl";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

const JOB_ID = "crawl-job-1";
const TOKEN = "secret-rest-token";

interface RequestOverrides {
  readonly maxPages?: number;
  readonly maxDepth?: number;
  readonly maxPageBytes?: number;
  readonly maxTotalBytes?: number;
  readonly deadlineMillis?: number;
}

const crawlRequest = (overrides: RequestOverrides = {}) =>
  PageCrawlRequest.make({
    startUrl: "https://docs.example.com/start",
    purposes: ["search"],
    limits: PageCrawlLimits.make({
      maxPages: overrides.maxPages ?? 3,
      maxDepth: overrides.maxDepth ?? 2,
      maxPageBytes: overrides.maxPageBytes ?? 1_024,
      maxTotalBytes: overrides.maxTotalBytes ?? 3_072,
      deadlineMillis: overrides.deadlineMillis ?? 10_000,
    }),
  });

const jsonResponse = (value: unknown, status = 200, headers: HeadersInit = {}) =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });

const createResponse = () => jsonResponse({ success: true, result: JOB_ID });

const deleteResponse = () =>
  jsonResponse({
    success: true,
    result: { job_id: JOB_ID, message: "cancelled" },
  });

const resultResponse = (
  status:
    | "running"
    | "completed"
    | "errored"
    | "cancelled_by_user"
    | "cancelled_due_to_timeout"
    | "cancelled_due_to_limits",
  records: ReadonlyArray<unknown> = [],
  cursor?: unknown,
) =>
  jsonResponse({
    success: true,
    result: {
      id: JOB_ID,
      status,
      browserSecondsUsed: 1.5,
      total: records.length,
      finished: records.length,
      skipped: 0,
      records,
      ...(cursor === undefined ? {} : { cursor }),
    },
  });

const record = (
  path: string,
  status: PageCrawlRecord["status"] = "completed",
  markdown?: string,
) => ({
  url: `https://docs.example.com/${path}`,
  status,
  ...(markdown === undefined ? {} : { markdown }),
  metadata: {
    status: status === "errored" ? 500 : 200,
    url: `https://docs.example.com/${path}`,
    title: path,
  },
});

const runCrawl = (
  client: HttpClient.HttpClient,
  input = crawlRequest(),
): Effect.Effect<ReadonlyArray<PageCrawlRecord>, PageCrawlError> =>
  Effect.gen(function* () {
    const crawl = yield* PageCrawl;

    return yield* crawl.crawl(input).pipe(Stream.runCollect);
  }).pipe(
    Effect.scoped,
    Effect.provide(
      browserRestCrawlLayer({
        accountId: "account-id",
        apiToken: Redacted.make(TOKEN),
      }),
    ),
    Effect.provideService(HttpClient.HttpClient, client),
  );

describe("Browser Run REST PageCrawl adapter", () => {
  it.effect("cancels a running job once on interruption and early scope close", () =>
    Effect.gen(function* () {
      const runScenario = Effect.fn("BrowserRestCrawlTest.runExitScenario")(function* () {
        const polling = yield* Deferred.make<void>();
        let deletes = 0;

        const client = HttpClient.make((request) => {
          if (request.method === "POST") {
            return Effect.succeed(HttpClientResponse.fromWeb(request, createResponse()));
          }
          if (request.method === "DELETE") {
            deletes += 1;

            return Effect.succeed(HttpClientResponse.fromWeb(request, deleteResponse()));
          }

          return Deferred.succeed(polling, undefined).pipe(Effect.andThen(Effect.never));
        });

        const effect = runCrawl(client, crawlRequest());

        const fiber = yield* effect.pipe(Effect.forkChild);

        yield* Deferred.await(polling);
        yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isFailure(exit)).toBe(true);
        expect(deletes).toBe(1);
      });

      yield* runScenario();

      const polling = yield* Deferred.make<void>();
      let deletes = 0;

      const earlyCloseClient = HttpClient.make((request) => {
        if (request.method === "POST") {
          return Effect.succeed(HttpClientResponse.fromWeb(request, createResponse()));
        }
        if (request.method === "DELETE") {
          deletes += 1;

          return Effect.succeed(HttpClientResponse.fromWeb(request, deleteResponse()));
        }

        return Deferred.succeed(polling, undefined).pipe(Effect.andThen(Effect.never));
      });

      yield* Effect.gen(function* () {
        const crawl = yield* PageCrawl;
        const pull = yield* Stream.toPull(crawl.crawl(crawlRequest()));

        yield* Effect.forkChild(pull);
        yield* Deferred.await(polling);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          browserRestCrawlLayer({
            accountId: "account-id",
            apiToken: Redacted.make(TOKEN),
          }),
        ),
        Effect.provideService(HttpClient.HttpClient, earlyCloseClient),
      );
      expect(deletes).toBe(1);
    }),
  );

  it.effect("rejects a repeated cursor before issuing a third page request", () =>
    Effect.gen(function* () {
      let resultGets = 0;

      const client = HttpClient.make((request, url) => {
        const response =
          request.method === "POST"
            ? createResponse()
            : request.method === "DELETE"
              ? deleteResponse()
              : url.searchParams.has("limit")
                ? resultResponse("completed")
                : ((resultGets += 1),
                  resultGets === 1
                    ? resultResponse("completed", [record("one")], 1)
                    : resultResponse("completed", [record("two")], "1"));

        return Effect.succeed(HttpClientResponse.fromWeb(request, response));
      });

      const repeated = yield* runCrawl(client).pipe(Effect.flip);

      expect(repeated).toMatchObject({ _tag: "PageCrawlProtocolError" });
      expect(repeated.message).toContain("repeated");
      expect(resultGets).toBe(2);
    }),
  );

  it.effect("stops bounded JSON reads on the first violating chunk", () =>
    Effect.gen(function* () {
      let reads = 0;
      let cancelled = false;

      const body = new ReadableStream<Uint8Array>(
        {
          pull(controller) {
            reads += 1;
            controller.enqueue(new Uint8Array(32 * 1024));
          },
          cancel() {
            cancelled = true;
          },
        },
        { highWaterMark: 0 },
      );

      const client = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(body, { headers: { "content-type": "application/json" } }),
          ),
        ),
      );

      const error = yield* runCrawl(client).pipe(Effect.flip);

      expect(error).toMatchObject({ _tag: "PageCrawlProtocolError" });
      expect(error.message).toContain("65536 transport bytes");
      expect(reads).toBe(3);
      expect(cancelled).toBe(true);
    }),
  );
});
