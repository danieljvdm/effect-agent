import { makeReviewer, ReviewChange, ReviewRequest } from "@effect-agent/pr-review/review";
import {
  ReviewContextError,
  ReviewFileList,
  ReviewRepository,
  ReviewSource,
} from "@effect-agent/pr-review/review-repository";
import type { OpenAiSchema } from "@effect/ai-openai";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  ConfigProvider,
  Deferred,
  Effect,
  Encoding,
  Exit,
  Fiber,
  Logger,
  Option,
  Redacted,
  Schema,
  Stream,
} from "effect";
import type { AiError } from "effect/unstable/ai";
import { HttpClient, HttpClientError, HttpClientResponse } from "effect/unstable/http";

import { reviewActionProgram, reviewPublicationFailure } from "../src/action.ts";
import { makeReviewOpenAi } from "../src/review-openai.ts";
import { reviewMarker } from "../src/selection.ts";

// Exercise tight admission independently of the configurable production default.
const tightCostLimitMicrousd = 499_999;

const WireRequest = Schema.Struct({
  model: Schema.String,
  input: Schema.Array(Schema.Record(Schema.String, Schema.Json)),
  tools: Schema.optional(Schema.Array(Schema.Json)),
  tool_choice: Schema.optional(Schema.Json),
  reasoning: Schema.optional(Schema.Json),
  text: Schema.optional(Schema.Json),
  max_output_tokens: Schema.optional(Schema.Natural),
  service_tier: Schema.optional(Schema.String),
  store: Schema.optional(Schema.Boolean),
  stream: Schema.optional(Schema.Boolean),
  prompt_cache_key: Schema.optional(Schema.String),
  prompt_cache_options: Schema.optional(Schema.Json),
});

type WireRequest = typeof WireRequest.Type;
type HttpRequest = Parameters<typeof HttpClientResponse.fromWeb>[0];

const decodeWire = (request: HttpRequest) => {
  if (request.body._tag !== "Uint8Array") throw new Error("Expected encoded JSON request");

  return Schema.decodeSync(Schema.fromJsonString(WireRequest))(
    new TextDecoder().decode(request.body.body),
  );
};

const json = (request: HttpRequest, body: unknown, status = 200) =>
  HttpClientResponse.fromWeb(
    request,
    new globalThis.Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    }),
  );

const finding = {
  path: "src/value.ts",
  line: 2,
  category: "correctness",
  title: "Preserve the acknowledged value",
  body: "Returning zero loses the acknowledged value on the supported caller. Return the saved value.",
  priority: 1,
};

const read = {
  name: "read_file",
  parameters: { path: "src/value.ts", revision: "head", startLine: 1, lineCount: 3 },
};

const record = { name: "record_finding", parameters: finding };
const submit = { name: "submit_review", parameters: {} };

const request = ReviewRequest.make({
  title: "Preserve values",
  description: "",
  baseRevision: "base",
  headRevision: "head",
  changes: [
    ReviewChange.make({
      path: "src/value.ts",
      patch: "@@ -1,2 +1,2 @@\n export const value = 1;\n-return value;\n+return 0;",
    }),
  ],
  unreviewedPaths: [],
});

const repository = ReviewRepository.of({
  searchCode: () => Effect.fail(ReviewContextError.make({ message: "Source unavailable" })),
  readFile: (input) =>
    Effect.succeed(
      ReviewSource.make({ ...input, totalLines: 3, content: "private-source-fixture" }),
    ),
  findFiles: () =>
    Effect.succeed(ReviewFileList.make({ paths: ["src/value.ts"], truncated: false })),
});

const rawUsage = (input: number, output: number, read = 0, write = input - read) => ({
  input_tokens: input,
  input_tokens_details: { cached_tokens: read, cache_write_tokens: write },
  output_tokens: output,
  output_tokens_details: { reasoning_tokens: output - 10 },
  total_tokens: input + output,
});

const response = (usage: unknown, model = "gpt-6-sol", serviceTier = "default") => ({
  id: "resp_fixture",
  object: "response",
  model,
  created_at: 1_788_000_000,
  service_tier: serviceTier,
  output: [],
  usage,
});

const sse = (
  httpRequest: HttpRequest,
  call: number,
  calls: ReadonlyArray<{ readonly name: string; readonly parameters: Schema.Json }>,
  usage: unknown,
  finish: "completed" | "incomplete" = "completed",
  serviceTier = "default",
) => {
  const model = decodeWire(httpRequest).model;

  const reasoning = {
    type: "reasoning",
    id: `rs_${call}`,
    summary: [],
    encrypted_content: `opaque-fixture-${call}`,
  };

  const output = calls.map((tool, index) => ({
    type: "function_call",
    id: `fc_${call}_${index}`,
    call_id: `call_${call}_${index}`,
    name: tool.name,
    arguments: JSON.stringify(tool.parameters),
    status: "completed",
  }));

  const events = [
    { type: "response.created", response: response(null, model, serviceTier) },
    { type: "response.output_item.added", output_index: 0, item: reasoning },
    { type: "response.output_item.done", output_index: 0, item: reasoning },
    ...output.flatMap((item, index) => [
      { type: "response.output_item.added", output_index: index + 1, item },
      {
        type: "response.function_call_arguments.done",
        output_index: index + 1,
        item_id: item.id,
        arguments: item.arguments,
      },
      { type: "response.output_item.done", output_index: index + 1, item },
    ]),
    {
      type: `response.${finish}`,
      response: {
        ...response(usage, model, serviceTier),
        output: [reasoning, ...output],
        ...(finish === "incomplete" ? { incomplete_details: { reason: "max_output_tokens" } } : {}),
      },
    },
  ];

  return HttpClientResponse.fromWeb(
    httpRequest,
    new globalThis.Response(
      events
        .map(
          (event, sequence_number) => `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
        )
        .join(""),
      { headers: { "content-type": "text/event-stream" } },
    ),
  );
};

const makeNative = (http: HttpClient.HttpClient) =>
  OpenAiClient.make({ apiKey: Redacted.make("test-key-never-log") }).pipe(
    Effect.provideService(HttpClient.HttpClient, http),
  );

const model = OpenAiLanguageModel.model("gpt-6-sol", {
  max_output_tokens: 32_000,
  service_tier: "default",
  store: false,
  strictJsonSchema: true,
  reasoning: { effort: "xhigh" },
});

const payload: OpenAiSchema.CreateResponse = {
  model: "gpt-6-sol",
  input: [{ role: "user", content: "fixture" }],
  max_output_tokens: 32_000,
  service_tier: "default",
  store: false,
};

describe("review provider boundary", () => {
  it.effect.each([
    { requested: "fast", serviceTier: "default", streaming: true },
    { requested: "auto", serviceTier: "fast", streaming: false },
  ] as const)(
    "settles $requested requests at the reported $serviceTier tier (streaming=$streaming)",
    ({ serviceTier, streaming, requested }) =>
      Effect.gen(function* () {
        const sent: Array<WireRequest> = [];
        const model = "gpt-6-astra";
        const valid = true;

        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.sync(() => {
              if (url.pathname.endsWith("/input_tokens"))
                return json(httpRequest, { object: "response.input_tokens", input_tokens: 10_000 });
              sent.push(decodeWire(httpRequest));
              const usage = rawUsage(10_000, 500, 2_000, 1_000);

              return streaming
                ? sse(httpRequest, 1, [], usage, "completed", serviceTier)
                : json(httpRequest, response(usage, model, serviceTier));
            }),
          ),
        );

        const provider = yield* makeReviewOpenAi({
          model,
          serviceTier: requested,
          cacheKey: "fast-tier",
          costLimitMicrousd: 2_500_000,
        }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, native));

        const input = {
          ...payload,
          model,
          service_tier: requested === "auto" ? undefined : "fast",
        };

        const result = yield* (
          streaming
            ? provider.client
                .createResponseStream(input)
                .pipe(Effect.flatMap(([, stream]) => Stream.runDrain(stream)))
            : provider.client.createResponse(input)
        ).pipe(Effect.exit);

        expect(Exit.isSuccess(result)).toBe(valid);
        expect(sent).toHaveLength(1);
        if (requested === "auto") expect(sent[0]).not.toHaveProperty("service_tier");
        else expect(sent[0]?.service_tier).toBe("fast");
        expect(yield* provider.costControl.snapshot).toMatchObject({
          modelCalls: 1,
          usage: {
            estimatedCostMicrousd: serviceTier === "default" ? 109_500 : 219_000,
            reservedCostMicrousd: 0,
          },
        });
      }),
  );

  it.effect.each([40_000])(
    "shares one provider allowance across the parent and native children: %i microdollars",
    (costLimitMicrousd) =>
      Effect.gen(function* () {
        const sent: Array<WireRequest> = [];
        let parentCalls = 0;

        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.sync(() => {
              if (url.pathname.endsWith("/input_tokens"))
                return json(httpRequest, { object: "response.input_tokens", input_tokens: 1_000 });
              const wire = decodeWire(httpRequest);

              sent.push(wire);

              const child = wire.tools?.some(
                (tool) =>
                  typeof tool === "object" &&
                  tool !== null &&
                  !Array.isArray(tool) &&
                  "name" in tool &&
                  tool.name === "finish_research",
              );

              if (child) {
                const recorded = wire.input.some(
                  (item) => item.type === "function_call" && item.name === "record_finding",
                );

                const title = JSON.stringify(wire.input).includes("second scope")
                  ? "Second retained finding"
                  : "First retained finding";

                return sse(
                  httpRequest,
                  sent.length,
                  recorded
                    ? [
                        {
                          name: "finish_research",
                          parameters: { summary: "Done", incomplete: false },
                        },
                      ]
                    : [{ name: "record_finding", parameters: { ...finding, title } }],
                  rawUsage(1_000, 10),
                );
              }
              parentCalls += 1;

              return sse(
                httpRequest,
                sent.length,
                parentCalls === 1
                  ? ["first scope", "second scope"].map((question) => ({
                      name: "delegate_research",
                      parameters: { question, paths: ["src/value.ts"] },
                    }))
                  : [submit],
                rawUsage(1_000, 10),
              );
            }),
          ),
        );

        const provider = yield* makeReviewOpenAi({
          model: "gpt-6-astra",
          cacheKey: "native-research-budget",
          costLimitMicrousd,
        }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, native));

        const astra = (maxOutputTokens: number) =>
          OpenAiLanguageModel.model("gpt-6-astra", {
            max_output_tokens: maxOutputTokens,
            reasoning: { effort: "medium" },
            service_tier: "default",
            store: false,
            strictJsonSchema: true,
          });

        const result = yield* makeReviewer({
          model: astra(32_000),
          research: { model: astra(4_000), concurrency: 1 },
          costControl: provider.costControl,
        })
          .review(request)
          .pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
            Effect.provideService(ReviewRepository, repository),
          );

        expect(sent).toHaveLength(3);
        expect(result.usage.estimatedCostMicrousd).toBe(39_000);
        expect(result.usage.reservedCostMicrousd).toBe(0);
        expect(result.usage.estimatedCostMicrousd).toBeLessThanOrEqual(costLimitMicrousd);
        expect(result.report.findings.map(({ title }) => title)).toEqual([
          "First retained finding",
        ]);
        expect(result.incomplete).toBe(true);
        expect(result.exhausted).toBe("cost");
      }),
  );

  it.effect("continues PR #291's affordable research beyond eight turns", () =>
    Effect.gen(function* () {
      // First eight rows are observed usage; continuation is scripted, not a live quality claim.
      const usage = [
        rawUsage(9_128, 396, 0, 8_903),
        rawUsage(17_567, 234, 8_903, 8_439),
        rawUsage(24_532, 506, 17_342, 6_965),
        rawUsage(29_042, 179, 24_307, 4_510),
        rawUsage(30_127, 52, 28_817, 1_085),
        rawUsage(30_267, 45, 29_902, 140),
        rawUsage(30_737, 158, 30_042, 470),
        rawUsage(31_707, 55, 30_512, 970),
        rawUsage(33_219, 286, 31_475, 1_512),
        rawUsage(34_000, 200, 32_987, 788),
      ];

      const researchBatches = [4, 4, 4, 4, 1, 1, 4, 1, 1];
      const sent: Array<WireRequest> = [];

      const native = yield* makeNative(
        HttpClient.make((httpRequest, url) =>
          Effect.sync(() => {
            const current = usage[sent.length];

            if (current === undefined) throw new Error("Unexpected additional model request");
            if (url.pathname.endsWith("/input_tokens"))
              return json(httpRequest, {
                object: "response.input_tokens",
                input_tokens: current.input_tokens,
              });
            const wire = decodeWire(httpRequest);
            const batch = researchBatches[sent.length];

            sent.push(wire);

            return sse(
              httpRequest,
              sent.length,
              batch === undefined || typeof wire.tool_choice === "object"
                ? [submit]
                : Array.from({ length: batch }, () => read),
              current,
            );
          }),
        ),
      );

      const provider = yield* makeReviewOpenAi({
        costLimitMicrousd: tightCostLimitMicrousd,
        model: "gpt-6-sol",
        cacheKey: "pr-291-turns",
      }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, native));

      const result = yield* makeReviewer({ model, costControl: provider.costControl })
        .review(request)
        .pipe(
          Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
          Effect.provideService(ReviewRepository, repository),
        );

      expect(sent).toHaveLength(10);
      expect(result.exhausted).toBeUndefined();
      expect(result.incomplete).toBeUndefined();
      expect(result.usage.estimatedCostMicrousd).toBeLessThan(tightCostLimitMicrousd);
      expect(
        reviewPublicationFailure({
          blockingFindings: 0,
          unreviewedPaths: 0,
          unresolvedChangeRequests: 0,
          exhausted: result.exhausted,
          incomplete: result.incomplete,
        }),
      ).toBeUndefined();
    }),
  );

  it.effect.each(["complete", "cost-finding"] as const)(
    "recovers an automatic review after a rebase with outcome %s under the same cap",
    (outcome) =>
      Effect.gen(function* () {
        const complete = outcome.startsWith("complete");
        const fast = false;
        const inputTokens = 70_000;
        const hasFinding = outcome.endsWith("finding");
        const checkEnabled = fast || outcome.startsWith("cost");
        const checkWrites: Array<Schema.Json> = [];

        const published: Array<{
          readonly commit_id: string;
          readonly event: string;
          readonly body: string;
          readonly comments: ReadonlyArray<unknown>;
        }> = [];

        let modelCalls = 0;

        const sources = {
          base: "export const value = 1;\nreturn value;\n",
          head: "export const value = 1;\nreturn 0;\n",
        };

        const client = HttpClient.make((httpRequest, url) =>
          Effect.sync(() => {
            if (url.pathname.includes("/check-runs")) {
              if (httpRequest.body._tag !== "Uint8Array") throw new Error("Expected check JSON");
              checkWrites.push(
                Schema.decodeSync(Schema.fromJsonString(Schema.Json))(
                  new TextDecoder().decode(httpRequest.body.body),
                ),
              );

              return json(httpRequest, {
                id: 100,
                name: "Effect Agent review",
                head_sha: "head",
                external_id: "effect-agent-pr-review:v1:12",
              });
            }
            if (url.pathname === "/v1/responses/input_tokens")
              return json(httpRequest, {
                object: "response.input_tokens",
                input_tokens: inputTokens,
              });
            if (url.pathname === "/v1/responses") {
              modelCalls += 1;
              if (checkEnabled)
                expect(checkWrites).toEqual([
                  expect.objectContaining({ head_sha: "head", status: "in_progress" }),
                ]);

              return sse(
                httpRequest,
                modelCalls,
                complete ? [submit] : hasFinding && modelCalls === 1 ? [record, read] : [read],
                rawUsage(inputTokens, complete ? 100 : 32_000),
                "completed",
                "default",
              );
            }
            if (httpRequest.method === "GET" && url.pathname.endsWith("/pulls/12"))
              return json(httpRequest, {
                number: 12,
                title: "Value change",
                body: null,
                draft: false,
                html_url: "https://github.test/fixtures/example/pull/12",
                base: { sha: "base" },
                head: { sha: "head" },
              });
            if (httpRequest.method === "GET" && url.pathname.endsWith("/pulls/12/reviews"))
              return json(httpRequest, [
                {
                  id: 1,
                  body: reviewMarker(true),
                  commit_id: "reviewed-head",
                  submitted_at: "2026-08-25T00:00:00Z",
                  state: "COMMENTED",
                  user: { login: "github-actions[bot]", type: "Bot" },
                },
              ]);
            if (url.pathname.endsWith("/pulls/12/files"))
              return json(httpRequest, [
                {
                  filename: "src/value.ts",
                  status: "modified",
                  additions: 1,
                  deletions: 1,
                  patch: request.changes[0]?.patch,
                },
              ]);
            if (url.pathname.includes("/compare/"))
              return json(httpRequest, {
                merge_base_commit: {
                  sha: url.pathname.endsWith("...reviewed-head") ? "old-base" : "base",
                },
              });
            if (url.pathname === "/graphql")
              return json(httpRequest, {
                data: {
                  repository: {
                    object: {
                      oid: "base",
                      file: { path: "src/value.ts", oid: "base-blob", isGenerated: false },
                    },
                  },
                },
              });
            if (url.pathname.includes("/git/commits/")) {
              const revision = url.pathname.endsWith("/base")
                ? "base"
                : url.pathname.endsWith("/head")
                  ? "head"
                  : undefined;

              if (revision === undefined) throw new Error("Unexpected review revision");

              return json(httpRequest, { sha: revision, tree: { sha: `${revision}-tree` } });
            }
            if (url.pathname.includes("/git/trees/")) {
              const revision = url.pathname.endsWith("/base-tree") ? "base" : "head";

              return json(httpRequest, {
                sha: `${revision}-tree`,
                truncated: false,
                tree: [
                  {
                    path: "src/value.ts",
                    type: "blob",
                    mode: "100644",
                    sha: `${revision}-blob`,
                    size: sources[revision].length,
                  },
                ],
              });
            }
            if (url.pathname.includes("/git/blobs/")) {
              const revision = url.pathname.endsWith("/base-blob") ? "base" : "head";
              const text = sources[revision];

              return json(httpRequest, {
                sha: `${revision}-blob`,
                encoding: "base64",
                size: text.length,
                content: Encoding.encodeBase64(new TextEncoder().encode(text)),
              });
            }
            if (httpRequest.method === "POST" && url.pathname.endsWith("/pulls/12/reviews")) {
              if (httpRequest.body._tag !== "Uint8Array") throw new Error("Expected review JSON");
              published.push(
                Schema.decodeSync(
                  Schema.fromJsonString(
                    Schema.Struct({
                      commit_id: Schema.String,
                      event: Schema.String,
                      body: Schema.String,
                      comments: Schema.Array(Schema.Unknown),
                    }),
                  ),
                )(new TextDecoder().decode(httpRequest.body.body)),
              );

              return json(httpRequest, {
                html_url: "https://github.test/fixtures/example/pull/12#review",
              });
            }
            throw new Error(`Unexpected fixture request: ${httpRequest.method} ${url.pathname}`);
          }),
        );

        const exit = yield* reviewActionProgram.pipe(
          Effect.provideService(
            ConfigProvider.ConfigProvider,
            ConfigProvider.fromEnv({
              env: {
                GITHUB_REPOSITORY: "fixtures/example",
                GITHUB_TOKEN: "github-fixture",
                GITHUB_API_URL: "https://api.github.test",
                OPENAI_API_KEY: "openai-fixture",
                PR_REVIEW_PULL_REQUEST: "12",
                PR_REVIEW_MAX_COST_USD: "0.495",
                PR_REVIEW_MODEL: "gpt-6-sol",
                PR_REVIEW_PRIORITY: "default",
                ...(checkEnabled ? { PR_REVIEW_CHECK_NAME: "Effect Agent review" } : {}),
              },
            }),
          ),
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.provide(NodeServices.layer),
          Effect.exit,
        );

        expect(Exit.isSuccess(exit)).toBe(complete || checkEnabled);
        if (checkEnabled)
          expect(checkWrites).toEqual([
            expect.objectContaining({ head_sha: "head", status: "in_progress" }),
            expect.objectContaining({
              status: "completed",
              conclusion: complete ? "success" : "failure",
            }),
          ]);
        if (Exit.isFailure(exit)) {
          expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject({
            _tag: hasFinding ? "BlockingFindings" : "ReviewAttemptIncomplete",
          });
        }
        expect(modelCalls).toBe(1);
        expect(published).toHaveLength(1);
        expect(published[0]).toMatchObject({
          commit_id: "head",
          event: hasFinding ? "REQUEST_CHANGES" : "COMMENT",
        });
        expect(published[0]?.comments).toHaveLength(hasFinding ? 1 : 0);
        expect(published[0]?.body).toContain(reviewMarker(true, complete));
      }),
  );

  // 812410b862e1c64090a47b29bd09c5cccb6cebf1 omitted even public provider error codes.
  it.effect.each(["provider-error", "known-provider-error", "transport", "invalid-id"] as const)(
    "logs safe provider diagnostics for %s without retrying paid inference",
    (phase) =>
      Effect.gen(function* () {
        const logs: Array<unknown> = [];
        let sends = 0;
        const streaming = phase === "provider-error" || phase === "known-provider-error";

        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) => {
            if (url.pathname.endsWith("/input_tokens"))
              return Effect.succeed(
                json(httpRequest, { object: "response.input_tokens", input_tokens: 20_000 }),
              );
            sends += 1;
            if (phase === "transport")
              return Effect.fail(
                new HttpClientError.HttpClientError({
                  reason: new HttpClientError.TransportError({
                    request: httpRequest,
                    description: "private-transport-cause",
                  }),
                }),
              );

            return Effect.succeed(
              HttpClientResponse.fromWeb(
                httpRequest,
                new globalThis.Response(
                  streaming
                    ? `data: {"type":"error","code":"${phase === "known-provider-error" ? "server_error" : "private_code"}","message":"private-provider-message","param":"private-param"}\n\n`
                    : JSON.stringify({ error: { message: "private-provider-body" } }),
                  {
                    status: streaming ? 200 : 503,
                    headers: {
                      "content-type": streaming ? "text/event-stream" : "application/json",
                      "x-request-id":
                        phase === "invalid-id" ? "private correlation header" : "req_safe-507",
                      "x-private-header": "private-header-value",
                    },
                  },
                ),
              ),
            );
          }),
        );

        const provider = yield* makeReviewOpenAi({
          costLimitMicrousd: tightCostLimitMicrousd,
          model: "gpt-6-sol",
          cacheKey: "failure-diagnostics",
        }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, native));

        const operation: Effect.Effect<
          ReadonlyArray<OpenAiSchema.ResponseStreamEvent>,
          AiError.AiError
        > = streaming
          ? provider.client
              .createResponseStream(payload)
              .pipe(Effect.flatMap(([, stream]) => Stream.runCollect(stream)))
          : provider.client.createResponse(payload).pipe(Effect.as([]));

        const exit = yield* operation.pipe(
          Effect.exit,
          Effect.provide(
            Logger.layer([Logger.make<unknown, void>(({ message }) => logs.push(message))]),
          ),
        );

        // Native error events still reach the interpreter; diagnostics do not consume them.
        expect(Exit.isFailure(exit)).toBe(!streaming);
        if (streaming) {
          expect(Exit.isSuccess(exit) && exit.value).toEqual([
            expect.objectContaining({ type: "error", message: "private-provider-message" }),
          ]);
          expect(logs).toContainEqual([
            "Review provider error event",
            expect.objectContaining({
              providerErrorCode: phase === "known-provider-error" ? "server_error" : "unrecognized",
              eventType: "error",
              status: 200,
              requestId: "req_safe-507",
            }),
          ]);
        }
        const diagnostic = JSON.stringify(logs);

        for (const secret of [
          "private-",
          "private_code",
          "private correlation",
          "test-key-never-log",
          "api.openai.com",
        ])
          expect(diagnostic).not.toContain(secret);
        expect((yield* provider.costControl.snapshot).usage.reservedCostMicrousd).toBeGreaterThan(
          0,
        );
        yield* provider.client.createResponse(payload).pipe(Effect.flip);
        expect(sends).toBe(1);
      }),
  );

  it.effect.each(["stream-eof", "malformed-count"] as const)(
    "preserves an unmetered review after %s while leaving pre-dispatch failures typed",
    (failure) =>
      Effect.gen(function* () {
        let sends = 0;
        const requestStarted = yield* Deferred.make<void>();

        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.gen(function* () {
              if (url.pathname.endsWith("/input_tokens"))
                return json(httpRequest, {
                  object: "response.input_tokens",
                  input_tokens: failure === "malformed-count" ? -1 : 70_000,
                });
              sends += 1;
              yield* Deferred.succeed(requestStarted, undefined);

              return HttpClientResponse.fromWeb(
                httpRequest,
                new globalThis.Response("", {
                  headers: { "content-type": "text/event-stream" },
                }),
              );
            }),
          ),
        );

        const provider = yield* makeReviewOpenAi({
          costLimitMicrousd: tightCostLimitMicrousd,
          model: "gpt-6-sol",
          cacheKey: "unmetered-review",
        }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, native));

        const pending = yield* makeReviewer({ model, costControl: provider.costControl })
          .review(request)
          .pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
            Effect.provideService(ReviewRepository, repository),
            Effect.exit,
            Effect.forkChild,
          );

        const exit = yield* Fiber.join(pending);
        const dispatched = failure !== "malformed-count";

        expect(sends).toBe(dispatched ? 1 : 0);
        expect(Exit.isSuccess(exit)).toBe(dispatched);
        if (Exit.isSuccess(exit)) {
          expect(exit.value).toMatchObject({
            incomplete: true,
            turns: 1,
            report: { findings: [] },
            usage: { estimatedCostMicrousd: 0, reservedCostMicrousd: 495_000 },
          });
          expect(
            reviewPublicationFailure({
              blockingFindings: 0,
              unreviewedPaths: 0,
              unresolvedChangeRequests: 0,
              incomplete: exit.value.incomplete,
            }),
          ).toMatchObject({ _tag: "ReviewAttemptIncomplete" });
        } else {
          expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject({
            _tag: "AiError",
          });
          expect(yield* provider.costControl.snapshot).toMatchObject({
            modelCalls: 0,
            usage: { reservedCostMicrousd: 0 },
          });
        }
      }),
  );

  it.effect("reserves concurrent requests atomically and retains interrupted liability", () =>
    Effect.gen(function* () {
      const dispatched = yield* Deferred.make<void>();
      let sends = 0;

      const native = yield* makeNative(
        HttpClient.make((httpRequest, url) => {
          if (url.pathname.endsWith("/input_tokens"))
            return Effect.succeed(
              json(httpRequest, { object: "response.input_tokens", input_tokens: 70_000 }),
            );
          sends += 1;

          return Deferred.succeed(dispatched, undefined).pipe(Effect.andThen(Effect.never));
        }),
      );

      const provider = yield* makeReviewOpenAi({
        costLimitMicrousd: tightCostLimitMicrousd,
        model: "gpt-6-sol",
        cacheKey: "concurrent",
      }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, native));

      const first = yield* Effect.forkChild(provider.client.createResponse(payload));

      yield* Deferred.await(dispatched);
      yield* provider.client.createResponse(payload).pipe(Effect.flip);
      yield* Fiber.interrupt(first);
      const snapshot = yield* provider.costControl.snapshot;

      expect(sends).toBe(1);
      expect(snapshot.usage.reservedCostMicrousd).toBe(495_000);
      expect(
        (snapshot.usage.estimatedCostMicrousd ?? 0) + (snapshot.usage.reservedCostMicrousd ?? 0),
      ).toBeLessThanOrEqual(tightCostLimitMicrousd);
    }),
  );
});
