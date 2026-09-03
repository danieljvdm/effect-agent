import {
  browserRestCrawlImplementation,
  browserRestCrawlLayer,
} from "@effect-agent/platform-cloudflare/BrowserRestCrawl";
import {
  PageCrawl,
  PageCrawlLimits,
  PageCrawlRequest,
  type PageCrawlError,
  type PageCrawlRecord,
} from "@effect-agent/sandbox/PageCrawl";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Logger,
  Redacted,
  Schema,
  Stream,
  type Layer,
} from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type LayerRequirements<Value> =
  Value extends Layer.Layer<infer _Output, infer _Error, infer Requirements> ? Requirements : never;
type RestCrawlRequiresHttpClient = Equal<
  LayerRequirements<ReturnType<typeof browserRestCrawlLayer>>,
  HttpClient.HttpClient
>;

const JOB_ID = "crawl-job-1";
const API_PATH = "https://api.cloudflare.com/client/v4/accounts/account-id/browser-rendering/crawl";
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

const bodyValue = (request: Parameters<Parameters<typeof HttpClient.make>[0]>[0]): unknown => {
  if (request.body._tag !== "Uint8Array") return undefined;

  return Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(
    new TextDecoder().decode(request.body.body),
  );
};

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

const resultClient = (
  records: ReadonlyArray<unknown>,
  cursor?: unknown,
  onRequest?: (method: string, url: URL) => void,
): HttpClient.HttpClient =>
  HttpClient.make((request, url) => {
    onRequest?.(request.method, url);

    const response =
      request.method === "POST"
        ? createResponse()
        : request.method === "DELETE"
          ? deleteResponse()
          : url.searchParams.has("limit")
            ? resultResponse("completed")
            : resultResponse("completed", records, cursor);

    return Effect.succeed(HttpClientResponse.fromWeb(request, response));
  });

describe("Browser Run REST PageCrawl adapter", () => {
  it("keeps its Node-safe HttpClient requirement visible", () => {
    const proof: RestCrawlRequiresHttpClient = true;

    expect(proof).toBe(true);
  });

  it.effect("creates once, polls with the TestClock, and lazily paginates bounded records", () =>
    Effect.gen(function* () {
      const calls: Array<{
        readonly method: string;
        readonly url: string;
        readonly authorization: string | undefined;
        readonly body: unknown;
      }> = [];

      const firstPoll = yield* Deferred.make<void>();
      let statusCalls = 0;

      const client = HttpClient.make((request, url) =>
        Effect.gen(function* () {
          calls.push({
            method: request.method,
            url: url.href,
            authorization: request.headers.authorization,
            body: bodyValue(request),
          });
          let response: Response;

          if (request.method === "POST") {
            response = createResponse();
          } else if (request.method === "DELETE") {
            response = deleteResponse();
          } else if (url.searchParams.has("limit")) {
            statusCalls += 1;
            if (statusCalls === 1) yield* Deferred.succeed(firstPoll, undefined);
            response = resultResponse(statusCalls === 1 ? "running" : "completed");
          } else if (url.searchParams.get("cursor") === "7") {
            response = resultResponse("completed", [record("three", "skipped")]);
          } else {
            response = resultResponse(
              "completed",
              [record("one", "completed", "# One"), record("two", "errored")],
              7,
            );
          }

          return HttpClientResponse.fromWeb(request, response);
        }),
      );

      const fiber = yield* runCrawl(client).pipe(Effect.forkChild);

      yield* Deferred.await(firstPoll);
      yield* TestClock.adjust("1 second");
      const records = yield* Fiber.join(fiber);

      expect(records.map((value) => value.status)).toEqual(["completed", "errored", "skipped"]);
      expect(calls.filter((call) => call.method === "POST")).toHaveLength(1);
      expect(calls.filter((call) => call.method === "DELETE")).toHaveLength(0);
      expect(calls.every((call) => call.url.startsWith(API_PATH))).toBe(true);
      expect(calls.every((call) => call.authorization === `Bearer ${TOKEN}`)).toBe(true);
      expect(calls[0]?.body).toEqual({
        url: "https://docs.example.com/start",
        crawlPurposes: ["search"],
        limit: 3,
        depth: 2,
        formats: ["markdown"],
        render: true,
        options: { includeExternalLinks: false, includeSubdomains: false },
      });
      expect(calls.map((call) => call.url)).toEqual([
        API_PATH,
        `${API_PATH}/${JOB_ID}?limit=1`,
        `${API_PATH}/${JOB_ID}?limit=1`,
        `${API_PATH}/${JOB_ID}`,
        `${API_PATH}/${JOB_ID}?cursor=7`,
      ]);
    }),
  );

  it.effect("accepts a within-limit poll record larger than the control response budget", () =>
    Effect.gen(function* () {
      const markdown = `# Large\n${"x".repeat(70 * 1_024)}`;

      const client = HttpClient.make((request) => {
        const response =
          request.method === "POST"
            ? createResponse()
            : resultResponse("completed", [record("large", "completed", markdown)]);

        return Effect.succeed(HttpClientResponse.fromWeb(request, response));
      });

      const records = yield* runCrawl(
        client,
        crawlRequest({
          maxPages: 1,
          maxPageBytes: 8 * 1_024 * 1_024,
          maxTotalBytes: 8 * 1_024 * 1_024,
        }),
      );

      expect(records).toHaveLength(1);
      expect(records[0]?.markdown).toBe(markdown);
    }),
  );

  it.effect("preserves every documented record status", () =>
    Effect.gen(function* () {
      const statuses = [
        "queued",
        "completed",
        "disallowed",
        "skipped",
        "errored",
        "cancelled",
      ] as const;

      const records = yield* runCrawl(
        resultClient(statuses.map((status, index) => record(String(index), status, "# page"))),
        crawlRequest({ maxPages: 6, maxTotalBytes: 6_144 }),
      );

      expect(records.map((value) => value.status)).toEqual(statuses);
    }),
  );

  it.effect("keeps the four non-success terminal statuses distinct and never cancels them", () =>
    Effect.gen(function* () {
      for (const status of [
        "errored",
        "cancelled_by_user",
        "cancelled_due_to_timeout",
        "cancelled_due_to_limits",
      ] as const) {
        let deletes = 0;

        const client = HttpClient.make((request) => {
          const response =
            request.method === "POST"
              ? createResponse()
              : request.method === "DELETE"
                ? ((deletes += 1), deleteResponse())
                : resultResponse(status);

          return Effect.succeed(HttpClientResponse.fromWeb(request, response));
        });

        const error = yield* runCrawl(client).pipe(Effect.flip);

        expect(error).toMatchObject({ _tag: "PageCrawlTerminalError", status });
        expect(deletes).toBe(0);
      }
    }),
  );

  it.effect("classifies create throttling and rejects a malformed job status", () =>
    Effect.gen(function* () {
      let createRateDeletes = 0;

      const createRateClient = HttpClient.make((request) => {
        if (request.method === "DELETE") createRateDeletes += 1;

        const response = jsonResponse(
          { success: false, errors: [{ message: "daily quota exhausted" }] },
          429,
          { "retry-after": "4" },
        );

        return Effect.succeed(HttpClientResponse.fromWeb(request, response));
      });

      const rate = yield* runCrawl(createRateClient).pipe(Effect.flip);

      expect(rate).toMatchObject({
        _tag: "PageCrawlRateLimitedError",
        reason: "quota",
        retryAfterMillis: 4_000,
      });
      expect(createRateDeletes).toBe(0);

      let malformedDeletes = 0;

      const malformedStatusClient = HttpClient.make((request) => {
        const response =
          request.method === "POST"
            ? createResponse()
            : request.method === "DELETE"
              ? ((malformedDeletes += 1), deleteResponse())
              : jsonResponse({
                  success: true,
                  result: {
                    id: JOB_ID,
                    status: "unknown",
                    records: [],
                  },
                });

        return Effect.succeed(HttpClientResponse.fromWeb(request, response));
      });

      const malformed = yield* runCrawl(malformedStatusClient).pipe(Effect.flip);

      expect(malformed).toMatchObject({ _tag: "PageCrawlProtocolError" });
      expect(malformedDeletes).toBe(1);
    }),
  );

  it.effect("warns on cancellation failure without changing the primary typed failure", () =>
    Effect.gen(function* () {
      for (const deleteStatus of [200, 500]) {
        let deletes = 0;
        const warnings: Array<string> = [];

        const logger = Logger.make<unknown, void>(({ logLevel, message }) => {
          if (logLevel !== "Warn") return;
          warnings.push(Array.isArray(message) ? message.join(" ") : String(message));
        });

        const client = HttpClient.make((request) => {
          const response =
            request.method === "POST"
              ? createResponse()
              : request.method === "DELETE"
                ? ((deletes += 1),
                  deleteStatus === 200
                    ? deleteResponse()
                    : jsonResponse({ success: false, errors: [{ message: TOKEN }] }, 500))
                : jsonResponse({ success: false, errors: [{ message: "rate limited" }] }, 429, {
                    "retry-after": "7",
                  });

          return Effect.succeed(HttpClientResponse.fromWeb(request, response));
        });

        const exit = yield* runCrawl(client).pipe(
          Effect.exit,
          Effect.provide(Logger.layer([logger])),
        );

        if (Exit.isSuccess(exit)) return yield* Effect.die("crawl unexpectedly succeeded");
        expect(deletes).toBe(1);
        expect(Cause.squash(exit.cause)).toMatchObject({
          _tag: "PageCrawlRateLimitedError",
          reason: "rate",
          retryAfterMillis: 7_000,
        });
        expect(Cause.hasDies(exit.cause)).toBe(false);
        expect(warnings).toEqual(
          deleteStatus === 500 ? ["Browser Run crawl cancellation failed"] : [],
        );
        expect(warnings.join("\n")).not.toContain(TOKEN);
      }
    }),
  );

  it.effect("does not cancel when interruption wins before a job ID exists", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      let deletes = 0;

      const client = HttpClient.make((request) =>
        request.method === "POST"
          ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
          : Effect.sync(() => {
              if (request.method === "DELETE") deletes += 1;

              return HttpClientResponse.fromWeb(request, deleteResponse());
            }),
      );

      const fiber = yield* runCrawl(client).pipe(Effect.forkChild);

      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);

      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
      expect(deletes).toBe(0);
    }),
  );

  it.effect("cancels once on a running-job defect, deadline, interruption, and early close", () =>
    Effect.gen(function* () {
      const runScenario = Effect.fn("BrowserRestCrawlTest.runExitScenario")(function* (
        mode: "defect" | "deadline" | "interruption",
      ) {
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
          if (mode === "defect") return Effect.die("poll defect");
          if (mode === "deadline") {
            return Deferred.succeed(polling, undefined).pipe(
              Effect.as(HttpClientResponse.fromWeb(request, resultResponse("running"))),
            );
          }

          return Deferred.succeed(polling, undefined).pipe(Effect.andThen(Effect.never));
        });

        const effect = runCrawl(
          client,
          mode === "deadline" ? crawlRequest({ deadlineMillis: 1_000 }) : crawlRequest(),
        );

        const fiber = yield* effect.pipe(Effect.forkChild);

        if (mode !== "defect") yield* Deferred.await(polling);
        if (mode === "deadline") yield* TestClock.adjust("1 second");
        if (mode === "interruption") yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isFailure(exit)).toBe(true);
        if (mode === "deadline" && Exit.isFailure(exit)) {
          expect(Cause.squash(exit.cause)).toMatchObject({
            _tag: "PageCrawlLimitError",
            limit: "deadline",
            maximum: 1_000,
            observed: 1_000,
          });
        }
        expect(deletes).toBe(1);
      });

      yield* runScenario("defect");
      yield* runScenario("deadline");
      yield* runScenario("interruption");

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

  it.effect("fails each caller limit at its first violating record", () =>
    Effect.gen(function* () {
      const cases = [
        {
          input: crawlRequest({ maxPages: 1 }),
          records: [record("one", "completed", "a"), record("two", "completed", "b")],
          limit: "pages",
          maximum: 1,
          observed: 2,
        },
        {
          input: crawlRequest({ maxPageBytes: 3 }),
          records: [record("one", "completed", "four")],
          limit: "page-bytes",
          maximum: 3,
          observed: 4,
        },
        {
          input: crawlRequest({ maxPageBytes: 10, maxTotalBytes: 5 }),
          records: [record("one", "completed", "abc"), record("two", "completed", "def")],
          limit: "total-bytes",
          maximum: 5,
          observed: 6,
        },
      ] as const;

      for (const scenario of cases) {
        const error = yield* runCrawl(resultClient(scenario.records), scenario.input).pipe(
          Effect.flip,
        );

        expect(error).toMatchObject({
          _tag: "PageCrawlLimitError",
          limit: scenario.limit,
          maximum: scenario.maximum,
          observed: scenario.observed,
        });
      }
    }),
  );

  it.effect("rejects off-host source and redirect metadata before emission", () =>
    Effect.gen(function* () {
      for (const hostile of [
        { url: "https://other.example/page", status: "completed", markdown: "# bad" },
        {
          ...record("one", "completed", "# bad"),
          metadata: { status: 200, url: "https://other.example/redirect" },
        },
      ]) {
        const error = yield* runCrawl(resultClient([hostile])).pipe(Effect.flip);

        expect(error).toMatchObject({ _tag: "PageCrawlProtocolError" });
        expect(error.message).toContain("off-host");
      }
    }),
  );

  it.effect("rejects malformed and repeated cursors before another request", () =>
    Effect.gen(function* () {
      for (const cursor of ["", "x".repeat(1_025), -1, Number.MAX_SAFE_INTEGER + 1]) {
        const error = yield* runCrawl(resultClient([record("one")], cursor)).pipe(Effect.flip);

        expect(error).toMatchObject({ _tag: "PageCrawlProtocolError" });
      }

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

  it.effect("does not fetch the next cursor page until the current page is consumed", () =>
    Effect.gen(function* () {
      const resultUrls: Array<string> = [];
      let deletes = 0;

      const client = resultClient(
        [record("one", "completed", "# one"), record("two", "completed", "# two")],
        "next",
        (method, url) => {
          if (method === "DELETE") deletes += 1;
          if (method === "GET" && !url.searchParams.has("limit")) resultUrls.push(url.href);
        },
      );

      const values = yield* Effect.gen(function* () {
        const crawl = yield* PageCrawl;

        return yield* crawl.crawl(crawlRequest()).pipe(Stream.take(1), Stream.runCollect);
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

      expect(values).toHaveLength(1);
      expect(resultUrls).toEqual([`${API_PATH}/${JOB_ID}`]);
      expect(deletes).toBe(0);
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

  it.effect(
    "keeps the token at the fixed-origin Authorization boundary and out of diagnostics",
    () =>
      Effect.gen(function* () {
        const seen: Array<{
          readonly url: string;
          readonly authorization: string | undefined;
          readonly body: string;
        }> = [];

        const logs: Array<string> = [];

        const logger = Logger.make<unknown, void>(({ cause, message }) => {
          logs.push(`${String(message)} ${String(cause)}`);
        });

        const client = HttpClient.make((request, url) => {
          seen.push({
            url: url.href,
            authorization: request.headers.authorization,
            body: JSON.stringify(bodyValue(request)),
          });

          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              jsonResponse({ success: false, errors: [{ message: TOKEN }] }, 400),
            ),
          );
        });

        const error = yield* runCrawl(client).pipe(
          Effect.flip,
          Effect.provide(Logger.layer([logger])),
        );

        expect(seen).toHaveLength(1);
        expect(seen[0]?.url).toBe(API_PATH);
        expect(seen[0]?.authorization).toBe(`Bearer ${TOKEN}`);
        expect(seen[0]?.url).not.toContain(TOKEN);
        expect(seen[0]?.body).not.toContain(TOKEN);
        expect(JSON.stringify(error)).not.toContain(TOKEN);
        expect(String(error.cause)).not.toContain(TOKEN);
        expect(logs.join("\n")).not.toContain(TOKEN);
      }),
  );

  it("loads as a Node-safe subpath", async () => {
    const module = await import("@effect-agent/platform-cloudflare/BrowserRestCrawl");

    expect(module.browserRestCrawlImplementation).toBe(browserRestCrawlImplementation);
  });
});
