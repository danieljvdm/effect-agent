import {
  makeReviewer,
  ReviewChange,
  ReviewFileList,
  ReviewRepository,
  ReviewRequest,
  ReviewSource,
} from "@effect-agent/pr-review";
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
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientError, HttpClientResponse } from "effect/unstable/http";

import { reviewActionProgram, reviewPublicationFailure } from "../src/action.ts";
import {
  makeReviewOpenAi,
  REVIEW_COST_LIMIT_MICROUSD,
  reviewCostEstimator,
} from "../src/review-openai.ts";
import { reviewMarker, reviewPauseMarker } from "../src/selection.ts";

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
  return Schema.decodeUnknownSync(Schema.fromJsonString(WireRequest))(
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
const submit = { name: "submit_review", parameters: { findings: [] } };
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
const response = (usage: unknown) => ({
  id: "resp_fixture",
  object: "response",
  model: "gpt-5.6-sol",
  created_at: 1_788_000_000,
  service_tier: "default",
  output: [],
  usage,
});
const sse = (
  httpRequest: HttpRequest,
  call: number,
  calls: ReadonlyArray<{ readonly name: string; readonly parameters: Schema.Json }>,
  usage: unknown,
  finish: "completed" | "incomplete" = "completed",
) => {
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
    { type: "response.created", response: response(null) },
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
        ...response(usage),
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
const model = OpenAiLanguageModel.model("gpt-5.6-sol", {
  max_output_tokens: 32_000,
  service_tier: "default",
  store: false,
  strictJsonSchema: true,
  reasoning: { effort: "xhigh" },
});
const payload: OpenAiSchema.CreateResponse = {
  model: "gpt-5.6-sol",
  input: [{ role: "user", content: "fixture" }],
  max_output_tokens: 32_000,
  service_tier: "default",
  store: false,
};

describe("review provider boundary", () => {
  it.effect("continues research with the remaining allowance after PR #252's usage", () =>
    Effect.gen(function* () {
      const sent: Array<WireRequest> = [];
      // Production counts and usage for the first two calls; the third is scripted continuation.
      const usage = [
        rawUsage(39_532, 430, 0, 39_351),
        rawUsage(51_735, 1_647, 39_338, 12_163),
        rawUsage(60_000, 1_000, 51_500, 8_000),
      ];
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
            sent.push(decodeWire(httpRequest));
            return sse(
              httpRequest,
              sent.length,
              sent.length === 1 ? [read] : sent.length === 2 ? [record, read] : [submit],
              current,
            );
          }),
        ),
      );
      const provider = yield* makeReviewOpenAi({
        client: native,
        model: "gpt-5.6-sol",
        cacheKey: "affordable-research",
      });
      const result = yield* makeReviewer({ model, costControl: provider.costControl })
        .review(request)
        .pipe(
          Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
          Effect.provideService(ReviewRepository, repository),
        );

      expect(sent).toHaveLength(3);
      expect(sent.map((wire) => wire.max_output_tokens)).toEqual([32_000, 26_762, 19_174]);
      expect(JSON.stringify(sent[0]?.input.at(-1))).toContain("$0.999999");
      expect(JSON.stringify(sent[1]?.input.at(-1))).toContain("$0.793920");
      expect(JSON.stringify(sent[1]?.input.at(-1))).toContain("full cache-miss rate");
      expect(JSON.stringify(sent[1]?.input.at(-1))).not.toContain("prompt_cache_breakpoint");
      expect(JSON.stringify(sent[1]?.input)).not.toContain("turn 1/8");
      for (const wire of sent) {
        expect(wire.model).toBe("gpt-5.6-sol");
        expect(wire.reasoning).toEqual({ effort: "xhigh" });
        expect(wire.tools).toEqual(sent[0]?.tools);
        expect(wire.tool_choice).toEqual(sent[0]?.tool_choice);
      }
      expect(result.exhausted).toBeUndefined();
      expect(result.incomplete).toBeUndefined();
      expect(result.report.findings.map((item) => item.title)).toEqual([finding.title]);
      expect(result.usage.estimatedCostMicrousd).toBe(399_106);
      expect(result.usage.reservedCostMicrousd).toBe(0);
    }),
  );

  it.effect.each(["hit", "miss"] as const)(
    "continues after PR #258's cached usage while reserving a full cache %s",
    (cache) =>
      Effect.gen(function* () {
        const sent: Array<WireRequest> = [];
        // The first six usage reports and batch sizes are from the live review.
        // The remaining research and completion are scripted, not measured improvement.
        const usage = [
          rawUsage(10_100, 320, 0, 9_919),
          rawUsage(22_327, 1_349, 9_919, 12_225),
          rawUsage(33_358, 586, 22_144, 11_031),
          rawUsage(45_650, 485, 33_175, 12_292),
          rawUsage(57_077, 539, 45_467, 11_427),
          rawUsage(71_236, 394, 56_894, 14_159),
          cache === "hit" ? rawUsage(80_000, 800, 71_053, 8_764) : rawUsage(80_000, 800),
          rawUsage(81_000, 200, 79_817, 1_000),
        ];
        const researchBatches = [8, 9, 10, 10, 10, 10];
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
              const batchSize = researchBatches[sent.length];
              sent.push(decodeWire(httpRequest));
              return sse(
                httpRequest,
                sent.length,
                batchSize !== undefined
                  ? Array.from({ length: batchSize }, () => read)
                  : sent.length === 7
                    ? [record, read]
                    : [submit],
                current,
              );
            }),
          ),
        );
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6-sol",
          cacheKey: "cached-research",
        });
        const result = yield* makeReviewer({ model, costControl: provider.costControl })
          .review(request)
          .pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
            Effect.provideService(ReviewRepository, repository),
          );

        expect(sent).toHaveLength(cache === "hit" ? 8 : 7);
        expect(sent[6]?.max_output_tokens).toBe(4_992);
        for (const wire of sent) {
          expect(wire.tools).toEqual(sent[0]?.tools);
          expect(wire.tool_choice).toBe("required");
          expect(wire.model).toBe("gpt-5.6-sol");
          expect(wire.reasoning).toEqual({ effort: "xhigh" });
        }
        expect(result.report.findings.map((item) => item.title)).toEqual([finding.title]);
        expect(result.exhausted).toBe(cache === "hit" ? undefined : "cost");
        expect(result.incomplete).toBe(cache === "hit" ? undefined : true);
        expect(result.usage.estimatedCostMicrousd).toBe(cache === "hit" ? 630_783 : 916_150);
        expect(result.usage.reservedCostMicrousd).toBe(0);
        if (cache === "miss")
          expect(result.report.summary).toContain("remaining change has not been verified");
      }),
  );

  it.effect.each(["complete", "cost", "protocol"] as const)(
    "reviews large input in fresh batches under one spending ledger: %s",
    (outcome) =>
      Effect.gen(function* () {
        const changes = [
          ...request.changes,
          ...Array.from({ length: 10 }, (_, index) =>
            ReviewChange.make({
              path: `docs/page-${String(index)}.md`,
              patch: `@@ -0,0 +1 @@\n+${"x".repeat(70_000)}`,
            }),
          ),
        ];
        const sent: Array<WireRequest> = [];
        const input = outcome === "cost" ? 70_000 : 20_000;
        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.sync(() => {
              if (url.pathname.endsWith("/input_tokens"))
                return json(httpRequest, { object: "response.input_tokens", input_tokens: input });
              sent.push(decodeWire(httpRequest));
              const calls =
                sent.length === 1
                  ? [{ name: "submit_review", parameters: { findings: [finding] } }]
                  : outcome === "protocol"
                    ? [{ name: "unknown_tool", parameters: {} }]
                    : [submit];
              return sse(
                httpRequest,
                sent.length,
                calls,
                rawUsage(input, outcome === "cost" ? 32_000 : 100),
              );
            }),
          ),
        );
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6-sol",
          cacheKey: "large-review",
        });
        const result = yield* makeReviewer({ model, costControl: provider.costControl })
          .review(ReviewRequest.make({ ...request, changes }))
          .pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
            Effect.provideService(ReviewRepository, repository),
          );
        expect(result.report.findings.map((item) => item.title)).toEqual([finding.title]);
        expect(result.turns).toBe(sent.length);
        expect(result.usage).toEqual((yield* provider.costControl.snapshot).usage);
        expect(result.usage.estimatedCostMicrousd).toBeLessThan(1_000_000);
        if (outcome === "complete") {
          expect(sent).toHaveLength(4);
          expect(result.pendingPaths).toBeUndefined();
          expect(result.incomplete).toBeUndefined();
          expect(result.exhausted).toBeUndefined();
          for (const change of changes) {
            expect(
              sent.filter((wire) => JSON.stringify(wire.input).includes(change.path)),
            ).toHaveLength(1);
          }
        } else {
          expect(sent).toHaveLength(outcome === "cost" ? 1 : 2);
          expect(result.exhausted).toBe(outcome === "cost" ? "cost" : undefined);
          expect(result.incomplete).toBe(true);
          expect(result.pendingPaths).toEqual(
            changes.slice(outcome === "cost" ? 4 : 7).map((change) => change.path),
          );
        }
      }),
  );

  it.effect("preserves cached tool schemas through the required completion turn", () =>
    Effect.gen(function* () {
      const sent: Array<WireRequest> = [];
      const native = yield* makeNative(
        HttpClient.make((httpRequest, url) =>
          Effect.sync(() => {
            const input = 20_000 + sent.length * 1_000;
            if (url.pathname.endsWith("/input_tokens"))
              return json(httpRequest, { object: "response.input_tokens", input_tokens: input });
            sent.push(decodeWire(httpRequest));
            return sse(
              httpRequest,
              sent.length,
              sent.length === 9 ? [submit] : sent.length === 1 ? [record, read] : [read],
              rawUsage(input, 100, sent.length === 1 ? 0 : input - 1_000),
            );
          }),
        ),
      );
      const provider = yield* makeReviewOpenAi({
        client: native,
        model: "gpt-5.6-sol",
        cacheKey: "completion-prefix",
      });
      const result = yield* makeReviewer({ model, costControl: provider.costControl })
        .review(request)
        .pipe(
          Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
          Effect.provideService(ReviewRepository, repository),
        );

      expect(sent).toHaveLength(9);
      const research = sent[7];
      const completion = sent[8];
      if (research === undefined || completion === undefined)
        throw new Error("Missing research or completion request");
      expect(completion.tools).toEqual(research.tools);
      expect(completion.tool_choice).toEqual({ type: "function", name: "submit_review" });
      expect(research.tool_choice).toBe("required");
      expect(completion.input.slice(0, research.input.length - 1)).toEqual(
        research.input.slice(0, -1),
      );
      expect(result.exhausted).toBe("turns");
      expect(result.report.findings.map((item) => item.title)).toEqual([finding.title]);
      expect(result.usage.estimatedCostMicrousd).toBeLessThan(REVIEW_COST_LIMIT_MICROUSD);
    }),
  );

  it.effect.each([
    "addressed",
    "omitted",
    "incomplete",
    "new-blocker",
    "stale",
    "publish-failure",
    "dismiss-failure",
  ] as const)(
    "rechecks a blocker after two automatic attempts without widening the delta: %s",
    (mode) =>
      Effect.gen(function* () {
        let modelCalls = 0;
        let dismissed = false;
        const mutations: Array<string> = [];
        const published: Array<string> = [];
        const user = { login: "github-actions[bot]", type: "Bot" };
        const prior = {
          id: 2,
          body: `One blocking defect.\n${reviewMarker(true)}`,
          commit_id: "reviewed-head",
          submitted_at: "2026-08-25T01:00:00Z",
          state: "CHANGES_REQUESTED",
          user,
        };
        const evidence = "src/value.ts now returns the saved value instead of zero.";
        const client = HttpClient.make((httpRequest, url) =>
          Effect.sync(() => {
            if (url.pathname.endsWith("/input_tokens"))
              return json(httpRequest, { object: "response.input_tokens", input_tokens: 1_000 });
            if (url.pathname === "/v1/responses") {
              modelCalls += 1;
              const input = JSON.stringify(decodeWire(httpRequest).input);
              expect(input).toContain("reviewed-head");
              expect(input).toContain("incremental");
              expect(input).toContain("Returning zero loses the acknowledged value");
              return sse(
                httpRequest,
                modelCalls,
                [
                  {
                    name: "submit_review",
                    parameters: {
                      findings: mode === "new-blocker" ? [finding] : [],
                      incomplete: mode === "incomplete",
                      ...(mode === "omitted" ? {} : { resolutions: [{ id: "2", evidence }] }),
                    },
                  },
                ],
                rawUsage(1_000, 100),
              );
            }
            if (url.pathname.endsWith("/pulls/12"))
              return json(httpRequest, {
                number: 12,
                title: "Fix reviewed blocker",
                body: null,
                draft: false,
                html_url: "https://github.test/fixtures/example/pull/12",
                base: { sha: "base" },
                head: { sha: mode === "stale" && modelCalls > 0 ? "moved-head" : "head" },
              });
            if (url.pathname.endsWith("/reviews") && httpRequest.method === "GET")
              return json(httpRequest, [
                {
                  ...prior,
                  id: 1,
                  state: "COMMENTED",
                  commit_id: "initial-head",
                  submitted_at: "2026-08-25T00:00:00Z",
                },
                { ...prior, state: dismissed ? "DISMISSED" : "CHANGES_REQUESTED" },
                {
                  ...prior,
                  id: 3,
                  state: "COMMENTED",
                  body: reviewPauseMarker(2),
                  commit_id: "head",
                  submitted_at: "2026-08-25T02:00:00Z",
                },
              ]);
            if (url.pathname.endsWith("/reviews/2/comments"))
              return json(httpRequest, [
                {
                  pull_request_review_id: 2,
                  path: "src/value.ts",
                  body: finding.body,
                  user,
                },
              ]);
            if (url.pathname.endsWith("/reviews/2")) return json(httpRequest, prior);
            if (url.pathname.endsWith("/reviews/2/dismissals")) {
              expect(httpRequest.method).toBe("PUT");
              const encoded =
                httpRequest.body._tag === "Uint8Array"
                  ? new TextDecoder().decode(httpRequest.body.body)
                  : "";
              expect(encoded).toContain("Verified addressed at head");
              expect(encoded).toContain(evidence);
              mutations.push("dismiss");
              if (mode === "dismiss-failure") return json(httpRequest, {}, 403);
              dismissed = true;
              return json(httpRequest, { id: 2, state: "DISMISSED" });
            }
            if (url.pathname.endsWith("/files"))
              return json(httpRequest, [
                {
                  filename: "src/value.ts",
                  status: "modified",
                  additions: 1,
                  deletions: 1,
                  patch: "@@ -1,2 +1,2 @@\n export const value = 1;\n-return 0;\n+return value;",
                },
              ]);
            if (url.pathname.includes("/compare/"))
              return json(httpRequest, { merge_base_commit: { sha: "base" } });
            if (url.pathname.includes("/git/commits/")) {
              const revision = url.pathname.split("/").at(-1);
              expect(["reviewed-head", "head"]).toContain(revision);
              return json(httpRequest, { sha: revision, tree: { sha: `${revision}-tree` } });
            }
            if (url.pathname.includes("/git/trees/")) {
              const tree = url.pathname.split("/").at(-1);
              return json(httpRequest, {
                sha: tree,
                truncated: false,
                tree: [
                  {
                    path: "src/value.ts",
                    type: "blob",
                    mode: "100644",
                    sha: `${tree}-blob`,
                    size: 48,
                  },
                ],
              });
            }
            if (url.pathname.includes("/git/blobs/")) {
              const sha = url.pathname.split("/").at(-1);
              const source = `export const value = 1;\nreturn ${sha?.startsWith("reviewed-head") ? "0" : "value"};\n`;
              return json(httpRequest, {
                sha,
                size: source.length,
                encoding: "base64",
                content: Encoding.encodeBase64(source),
              });
            }
            if (url.pathname.endsWith("/reviews") && httpRequest.method === "POST") {
              mutations.push("publish");
              const encoded =
                httpRequest.body._tag === "Uint8Array"
                  ? new TextDecoder().decode(httpRequest.body.body)
                  : "";
              const body = Schema.decodeUnknownSync(
                Schema.fromJsonString(Schema.Struct({ body: Schema.String })),
              )(encoded);
              published.push(body.body);
              return json(
                httpRequest,
                { html_url: "https://github.test/fixtures/example/pull/12#review" },
                mode === "publish-failure" ? 500 : 200,
              );
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
                PR_REVIEW_AUTOMATIC_LIMIT: "5",
              },
            }),
          ),
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.provide(NodeServices.layer),
          Effect.exit,
        );
        expect(modelCalls).toBe(1);
        expect(Exit.isSuccess(exit)).toBe(mode === "addressed");
        expect(dismissed).toBe(mode === "addressed" || mode === "publish-failure");
        expect(mutations).toEqual(
          dismissed || mode === "dismiss-failure" ? ["dismiss", "publish"] : ["publish"],
        );
        if (mode === "addressed") {
          expect(published[0]).toContain("**Incremental**");
          expect(published[0]).toContain("✅ None");
          expect(published[0]).toContain("2 automatic reviews remain");
        } else if (mode === "omitted")
          expect(published[0]).toContain("1 earlier change request remains unresolved");
        else if (mode === "stale" || mode === "incomplete" || mode === "dismiss-failure")
          expect(published[0]).toContain(reviewMarker(true, false));
      }),
  );

  it.effect.each([
    "complete",
    "cost-empty",
    "cost-finding",
    "protocol-empty",
    "protocol-finding",
  ] as const)(
    "recovers an automatic review after a rebase with outcome %s under the same cap",
    (outcome) =>
      Effect.gen(function* () {
        const complete = outcome === "complete";
        const protocolFailure = outcome.startsWith("protocol");
        const hasFinding = outcome.endsWith("finding");
        const logs: Array<unknown> = [];
        // Accounted usage from PR #257, with a scripted invalid final response.
        const protocolUsage = [
          rawUsage(52_260, 542, 0, 52_079),
          rawUsage(73_026, 1_476, 52_079, 20_764),
          rawUsage(93_225, 770, 72_843, 20_199),
          rawUsage(68_587, 166, 0, 68_391),
        ];
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
            if (url.pathname === "/v1/responses/input_tokens")
              return json(httpRequest, {
                object: "response.input_tokens",
                input_tokens: protocolFailure ? protocolUsage[modelCalls]?.input_tokens : 70_000,
              });
            if (url.pathname === "/v1/responses") {
              modelCalls += 1;
              const input = Schema.decodeUnknownSync(
                Schema.Struct({ content: Schema.Array(Schema.Struct({ text: Schema.String })) }),
              )(decodeWire(httpRequest).input.find((item) => item.role === "user"));
              const text = input.content.map((part) => part.text).join("\n");
              expect(text).toContain('"baseRevision":"base"');
              expect(text).toContain('"scope":"full"');
              return sse(
                httpRequest,
                modelCalls,
                protocolFailure && modelCalls === 4
                  ? []
                  : complete
                    ? [submit]
                    : hasFinding && modelCalls === 1
                      ? [record, read]
                      : [read],
                protocolFailure
                  ? protocolUsage[modelCalls - 1]
                  : rawUsage(70_000, complete ? 100 : 32_000),
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
                Schema.decodeUnknownSync(
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
              },
            }),
          ),
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.provide([
            NodeServices.layer,
            Logger.layer([
              Logger.make<unknown, void>(({ message }) => {
                logs.push(message);
              }),
            ]),
          ]),
          Effect.exit,
        );
        expect(Exit.isSuccess(exit)).toBe(complete);
        if (Exit.isFailure(exit)) {
          expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject({
            _tag: hasFinding ? "BlockingFindings" : "ReviewAttemptIncomplete",
          });
        }
        expect(modelCalls).toBe(protocolFailure ? 4 : 1);
        expect(published).toHaveLength(1);
        expect(published[0]).toMatchObject({
          commit_id: "head",
          event: hasFinding ? "REQUEST_CHANGES" : "COMMENT",
        });
        expect(published[0]?.comments).toHaveLength(hasFinding ? 1 : 0);
        expect(published[0]?.body).toContain(reviewMarker(true, complete));
        expect(published[0]?.body).toContain("**Full diff**");
        expect(published[0]?.body).toContain("Automatic reviews are paused");
        if (!complete) {
          expect(published[0]?.body).not.toContain("**No actionable findings.**");
          expect(published[0]?.body).toContain("1 supplied");
        }
        expect(published[0]?.body).toContain("$0.999999 spending ceiling");
        if (protocolFailure) {
          expect(published[0]?.body).toContain("model protocol error");
          expect(published[0]?.body).toContain("4 model calls");
          expect(published[0]?.body).toContain("287,098 input");
          expect(published[0]?.body).toContain("$0.9192");
          expect(published[0]?.body).not.toContain("✅");
          expect(logs).toContainEqual([
            "Review model usage",
            expect.objectContaining({ modelCall: 4, functionCalls: 0, completionCalls: 0 }),
          ]);
          expect(JSON.stringify(logs)).not.toContain("openai-fixture");
          expect(JSON.stringify(logs)).not.toContain("github-fixture");
          expect(JSON.stringify(logs)).not.toContain("export const value");
        }
      }),
  );

  it.effect(
    "proves the missing implicit prefix and preserves explicit prefixes on the encoded wire",
    () =>
      Effect.gen(function* () {
        for (const guarded of [false, true]) {
          const sent: Array<WireRequest> = [];
          const counted: Array<WireRequest> = [];
          const logs: Array<unknown> = [];
          const native = yield* makeNative(
            HttpClient.make((httpRequest, url) =>
              Effect.sync(() => {
                const wire = decodeWire(httpRequest);
                if (url.pathname === "/v1/responses/input_tokens") {
                  counted.push(wire);
                  return json(httpRequest, {
                    object: "response.input_tokens",
                    input_tokens: 20_000 + sent.length * 1_000,
                  });
                }
                expect(url.pathname).toBe("/v1/responses");
                sent.push(wire);
                const call = sent.length;
                const tools =
                  call === 1
                    ? [read]
                    : call === 2
                      ? [
                          record,
                          { name: "find_files", parameters: { query: "value", revision: "head" } },
                        ]
                      : [submit];
                const usage =
                  call === 1
                    ? rawUsage(20_000, 1_000, 0, 19_800)
                    : rawUsage(19_000 + call * 1_000, 1_000, 18_000 + call * 1_000, 800);
                return sse(httpRequest, call, tools, usage);
              }),
            ),
          );
          const provider = yield* makeReviewOpenAi({
            client: native,
            model: "gpt-5.6-sol",
            cacheKey: "review-fixture",
          });
          const result = yield* makeReviewer({
            model,
            ...(guarded ? { costControl: provider.costControl } : {}),
            estimateCostMicrousd: reviewCostEstimator("gpt-5.6-sol"),
          })
            .review(request)
            .pipe(
              Effect.provideService(OpenAiClient.OpenAiClient, guarded ? provider.client : native),
              Effect.provideService(ReviewRepository, repository),
              Effect.provide(
                Logger.layer([
                  Logger.make<unknown, void>(({ message }) => {
                    logs.push(message);
                  }),
                ]),
              ),
            );
          expect(sent).toHaveLength(3);
          const [first, second, third] = sent;
          if (first === undefined || second === undefined || third === undefined)
            throw new Error("Missing model request");
          const input = Schema.decodeUnknownSync(
            Schema.Struct({ content: Schema.Array(Schema.Struct({ text: Schema.String })) }),
          )(first.input.find((item) => item.role === "user"));
          const inputText = input.content.map((part) => part.text).join("\n");
          expect(inputText).toContain(request.changes[0]?.patch);
          expect(inputText).not.toContain("__new hunk__");
          expect(first.tools).toEqual(second.tools);
          expect(second.tools).toEqual(third.tools);
          expect(first.reasoning).toEqual({ effort: "xhigh" });
          expect(first.reasoning).toEqual(third.reasoning);
          expect(first.input.at(-1)).not.toEqual(second.input.at(-1));
          expect(JSON.stringify(second.input)).not.toContain("turn 1/8");
          expect(second.input.slice(0, first.input.length - 1)).toEqual(first.input.slice(0, -1));
          expect(third.input.slice(0, second.input.length - 1)).toEqual(second.input.slice(0, -1));
          expect(JSON.stringify(third.input)).toContain("opaque-fixture-1");
          expect(JSON.stringify(third.input)).toContain("private-source-fixture");
          if (guarded) {
            expect(first.prompt_cache_options).toEqual({ mode: "explicit", ttl: "30m" });
            expect(first.prompt_cache_key).toBe(third.prompt_cache_key);
            expect(JSON.stringify(first.input.slice(0, -1))).toContain(
              '"prompt_cache_breakpoint":{"mode":"explicit"}',
            );
            expect(JSON.stringify(first.input.at(-1))).not.toContain("prompt_cache_breakpoint");
            expect(second.input.find((item) => item.type === "function_call_output")).toMatchObject(
              {
                output: [{ type: "input_text", prompt_cache_breakpoint: { mode: "explicit" } }],
              },
            );
            for (const [index, countedRequest] of counted.entries()) {
              expect(countedRequest.input).toEqual(sent[index]?.input);
              expect(countedRequest.tools).toEqual(sent[index]?.tools);
              expect(countedRequest.tool_choice).toEqual(sent[index]?.tool_choice);
              expect(countedRequest.reasoning).toEqual(sent[index]?.reasoning);
              expect(countedRequest.text).toEqual(sent[index]?.text);
            }
            expect(result.usage).toMatchObject({
              inputTokens: 63_000,
              cachedInputTokens: 41_000,
              cacheWriteInputTokens: 21_400,
              uncachedInputTokens: 600,
              outputTokens: 3_000,
              estimatedCostMicrousd: 185_800,
              reservedCostMicrousd: 0,
            });
            expect(JSON.stringify(logs)).toContain("cacheHitRatio");
            expect(JSON.stringify(logs)).not.toContain("private-source-fixture");
            expect(JSON.stringify(logs)).not.toContain("test-key-never-log");
          } else {
            expect(counted).toHaveLength(0);
            expect(first.prompt_cache_options).toBeUndefined();
            expect(JSON.stringify(first.input)).not.toContain("prompt_cache_breakpoint");
          }
          // A later empty submission cannot erase an already validated finding.
          expect(result.report.findings.map((item) => item.title)).toEqual([finding.title]);
          expect(result.exhausted).toBeUndefined();
        }
      }),
  );

  it.effect.each([false, true])(
    "keeps all-miss maximum-output reviews below $1, with recorded finding=%s",
    (recorded) =>
      Effect.gen(function* () {
        const sent: Array<WireRequest> = [];
        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.sync(() => {
              if (url.pathname.endsWith("/input_tokens"))
                return json(httpRequest, { object: "response.input_tokens", input_tokens: 70_000 });
              sent.push(decodeWire(httpRequest));
              return sse(
                httpRequest,
                sent.length,
                recorded ? [record, read] : [read],
                rawUsage(70_000, 32_000),
              );
            }),
          ),
        );
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6-sol",
          cacheKey: "misses",
        });
        const result = yield* makeReviewer({ model, costControl: provider.costControl })
          .review(request)
          .pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
            Effect.provideService(ReviewRepository, repository),
          );
        expect(sent).toHaveLength(1);
        expect(result.exhausted).toBe("cost");
        expect(result.report.findings).toHaveLength(recorded ? 1 : 0);
        expect(result.report.summary).toContain("remaining change has not been verified");
        expect(result.usage.estimatedCostMicrousd).toBe(990_000);
        expect(
          reviewPublicationFailure({
            blockingFindings: 0,
            unreviewedPaths: 0,
            unresolvedChangeRequests: 0,
            exhausted: result.exhausted,
          }),
        ).toMatchObject({ _tag: "ReviewAttemptIncomplete" });
      }),
  );

  it.effect.each(["submit", "research", "truncated"] as const)(
    "admits capped research and final responses without overspending: %s",
    (completion) =>
      Effect.gen(function* () {
        const sent: Array<WireRequest> = [];
        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.sync(() => {
              if (url.pathname.endsWith("/input_tokens"))
                return json(httpRequest, {
                  object: "response.input_tokens",
                  input_tokens: sent.length === 0 ? 70_000 : 72_000,
                });
              const wire = decodeWire(httpRequest);
              sent.push(wire);
              return sse(
                httpRequest,
                sent.length,
                sent.length === 1 ? [record, read] : completion === "research" ? [read] : [submit],
                rawUsage(
                  sent.length === 1 ? 70_000 : 72_000,
                  sent.length === 1 ? 1_000 : (wire.max_output_tokens ?? 0),
                ),
                sent.length === 2 && completion === "truncated" ? "incomplete" : "completed",
              );
            }),
          ),
        );
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6-sol",
          cacheKey: "final",
        });
        let reads = 0;
        const result = yield* makeReviewer({ model, costControl: provider.costControl })
          .review(request)
          .pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
            Effect.provideService(ReviewRepository, {
              ...repository,
              readFile: (input) =>
                Effect.suspend(() => {
                  reads += 1;
                  return repository.readFile(input);
                }),
            }),
          );
        expect(sent).toHaveLength(2);
        expect(sent[1]?.max_output_tokens).toBe(13_499);
        expect(sent[1]?.tool_choice).toEqual(sent[0]?.tool_choice);
        expect(sent[1]?.tools).toEqual(sent[0]?.tools);
        expect(result.exhausted).toBe(completion === "submit" ? undefined : "cost");
        const failure = reviewPublicationFailure({
          blockingFindings: 0,
          unreviewedPaths: 0,
          unresolvedChangeRequests: 0,
          exhausted: result.exhausted,
          incomplete: result.incomplete,
        });
        if (completion === "submit") expect(failure).toBeUndefined();
        else expect(failure).toMatchObject({ _tag: "ReviewAttemptIncomplete" });
        expect(result.report.findings).toHaveLength(1);
        expect(reads).toBe(completion === "research" ? 2 : 1);
        expect(result.usage.estimatedCostMicrousd).toBe(999_980);
        expect(result.usage.estimatedCostMicrousd).toBeLessThan(1_000_000);
        yield* provider.client.createResponse(payload).pipe(Effect.flip);
        expect(sent).toHaveLength(2);
      }),
  );

  it.effect.each(["gpt-5.6", "gpt-5.6-sol"])(
    "settles nonstreaming and streaming calls in one ledger when the alias resolves to %s",
    (responseModel) =>
      Effect.gen(function* () {
        let calls = 0;
        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.sync(() => {
              if (url.pathname.endsWith("/input_tokens"))
                return json(httpRequest, { object: "response.input_tokens", input_tokens: 20_000 });
              calls += 1;
              const usage = rawUsage(20_000, 1_000);
              return decodeWire(httpRequest).stream === true
                ? sse(httpRequest, calls, [submit], usage)
                : json(httpRequest, { ...response(usage), model: responseModel });
            }),
          ),
        );
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6",
          cacheKey: "alias",
        });
        const aliasPayload = { ...payload, model: "gpt-5.6" };
        yield* provider.client.createResponse(aliasPayload);
        const [, stream] = yield* provider.client.createResponseStream(aliasPayload);
        yield* Stream.runDrain(stream);
        const snapshot = yield* provider.costControl.snapshot;
        expect(calls).toBe(2);
        expect(snapshot).toMatchObject({
          stopped: false,
          modelCalls: 2,
          usage: {
            inputTokens: 40_000,
            cacheWriteInputTokens: 40_000,
            outputTokens: 2_000,
            estimatedCostMicrousd: 240_000,
            reservedCostMicrousd: 0,
          },
        });
      }),
  );

  it.effect.each([503, 429, "transport", "persistent-503", 400, 401, 501, "malformed"] as const)(
    "retries only transient preflight failures once: %s",
    (failure) =>
      Effect.gen(function* () {
        const counted: Array<WireRequest> = [];
        const sent: Array<WireRequest> = [];
        const logs: Array<unknown> = [];
        const recoverable = failure === 503 || failure === 429 || failure === "transport";
        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.gen(function* () {
              if (url.pathname.endsWith("/input_tokens")) {
                counted.push(decodeWire(httpRequest));
                if (counted.length === 1 || !recoverable) {
                  if (failure === "transport")
                    return yield* new HttpClientError.HttpClientError({
                      reason: new HttpClientError.TransportError({
                        request: httpRequest,
                        cause: "private-preflight-data",
                      }),
                    });
                  if (failure === "malformed") return json(httpRequest, { input_tokens: -1 });
                  return json(
                    httpRequest,
                    { error: "private-preflight-data" },
                    failure === "persistent-503" ? 503 : failure,
                  );
                }
                return json(httpRequest, { object: "response.input_tokens", input_tokens: 20_000 });
              }
              sent.push(decodeWire(httpRequest));
              return json(httpRequest, response(rawUsage(20_000, 1_000)));
            }),
          ),
        );
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6-sol",
          cacheKey: "preflight-retry",
        });
        const exit = yield* provider.client
          .createResponse(payload)
          .pipe(
            Effect.exit,
            Effect.provide(
              Logger.layer([Logger.make<unknown, void>(({ message }) => logs.push(message))]),
            ),
          );
        expect(Exit.isSuccess(exit)).toBe(recoverable);
        expect(counted).toHaveLength(recoverable || failure === "persistent-503" ? 2 : 1);
        expect(sent).toHaveLength(recoverable ? 1 : 0);
        if (recoverable) {
          expect(counted[0]).toEqual(counted[1]);
          expect(counted[1]?.input).toEqual(sent[0]?.input);
          expect(sent[0]?.max_output_tokens).toBe(32_000);
        } else {
          yield* provider.client.createResponse(payload).pipe(Effect.flip);
          expect(counted).toHaveLength(failure === "persistent-503" ? 2 : 1);
        }
        expect(yield* provider.costControl.snapshot).toMatchObject({
          modelCalls: recoverable ? 1 : 0,
          usage: { estimatedCostMicrousd: recoverable ? 120_000 : 0, reservedCostMicrousd: 0 },
        });
        const diagnostic = JSON.stringify(logs);
        expect(diagnostic).toContain('"phase":"input-token-count"');
        if (typeof failure === "number") expect(diagnostic).toContain(`"status":${failure}`);
        expect(diagnostic).not.toContain("private-preflight-data");
        expect(diagnostic).not.toContain("test-key-never-log");
        expect(diagnostic).not.toContain("api.openai.com");
      }),
  );

  it.effect.each(["recover", "exhaust", "interrupt"] as const)(
    "bounds and cancels preflight attempts without paid liability: %s",
    (mode) =>
      Effect.gen(function* () {
        const firstStarted = yield* Deferred.make<void>();
        const secondStarted = yield* Deferred.make<void>();
        let counts = 0;
        let finalized = 0;
        let sends = 0;
        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.gen(function* () {
              if (url.pathname.endsWith("/input_tokens")) {
                counts += 1;
                if (counts === 2 && mode === "recover") {
                  expect(finalized).toBe(1);
                  return json(httpRequest, {
                    object: "response.input_tokens",
                    input_tokens: 20_000,
                  });
                }
                yield* Deferred.succeed(counts === 1 ? firstStarted : secondStarted, undefined);
                return yield* Effect.never.pipe(Effect.ensuring(Effect.sync(() => finalized++)));
              }
              sends += 1;
              return json(httpRequest, response(rawUsage(20_000, 1_000)));
            }),
          ),
        );
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6-sol",
          cacheKey: "preflight-timeout",
        });
        const pending = yield* provider.client.createResponse(payload).pipe(Effect.forkChild);
        yield* Deferred.await(firstStarted);
        if (mode === "interrupt") {
          yield* Fiber.interrupt(pending);
        } else {
          yield* TestClock.adjust("10 seconds");
          if (mode === "exhaust") {
            yield* Deferred.await(secondStarted);
            expect(finalized).toBe(1);
            yield* TestClock.adjust("10 seconds");
          }
        }
        const exit = yield* Fiber.await(pending);
        expect(Exit.isSuccess(exit)).toBe(mode === "recover");
        expect(Exit.hasInterrupts(exit)).toBe(mode === "interrupt");
        expect(counts).toBe(mode === "interrupt" ? 1 : 2);
        expect(finalized).toBe(mode === "exhaust" ? 2 : 1);
        expect(sends).toBe(mode === "recover" ? 1 : 0);
        expect(yield* provider.costControl.snapshot).toMatchObject({
          modelCalls: mode === "recover" ? 1 : 0,
          usage: {
            estimatedCostMicrousd: mode === "recover" ? 120_000 : 0,
            reservedCostMicrousd: 0,
          },
        });
        if (mode !== "recover") {
          yield* provider.client.createResponse(payload).pipe(Effect.flip);
          expect(counts).toBe(mode === "interrupt" ? 1 : 2);
        }
      }),
  );

  it.effect.each(["http", "stream-eof", "timeout", "malformed-count"] as const)(
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
              if (failure === "timeout") return yield* Effect.never;
              return failure === "http"
                ? json(httpRequest, { error: { message: "private-provider-fixture" } }, 500)
                : HttpClientResponse.fromWeb(
                    httpRequest,
                    new globalThis.Response("", {
                      headers: { "content-type": "text/event-stream" },
                    }),
                  );
            }),
          ),
        );
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6-sol",
          cacheKey: "unmetered-review",
        });
        const pending = yield* makeReviewer({ model, costControl: provider.costControl })
          .review(request)
          .pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
            Effect.provideService(ReviewRepository, repository),
            Effect.exit,
            Effect.forkChild,
          );
        if (failure === "timeout") {
          yield* Deferred.await(requestStarted);
          yield* TestClock.adjust("5 minutes");
        }
        const exit = yield* Fiber.join(pending);
        const dispatched = failure !== "malformed-count";
        expect(sends).toBe(dispatched ? 1 : 0);
        expect(Exit.isSuccess(exit)).toBe(dispatched);
        if (Exit.isSuccess(exit)) {
          expect(exit.value).toMatchObject({
            incomplete: true,
            turns: 1,
            report: { findings: [] },
            usage: { estimatedCostMicrousd: 0, reservedCostMicrousd: 990_000 },
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

  it.effect.each(["http", "missing-usage", "malformed-count", "stream-eof"])(
    "fails closed on %s without refunding capped requests with unknown charges",
    (failure) =>
      Effect.gen(function* () {
        let sends = 0;
        const native = yield* makeNative(
          HttpClient.make((httpRequest, url) =>
            Effect.sync(() => {
              if (url.pathname.endsWith("/input_tokens"))
                return json(httpRequest, {
                  object: "response.input_tokens",
                  input_tokens: failure === "malformed-count" ? -1 : 100_000,
                });
              sends += 1;
              expect(decodeWire(httpRequest).max_output_tokens).toBe(24_999);
              if (failure === "http")
                return json(httpRequest, { error: { message: "private-source-fixture" } }, 500);
              if (failure === "stream-eof")
                return HttpClientResponse.fromWeb(
                  httpRequest,
                  new globalThis.Response("", { headers: { "content-type": "text/event-stream" } }),
                );
              return json(httpRequest, response(null));
            }),
          ),
        );
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6-sol",
          cacheKey: "failed",
        });
        if (failure === "stream-eof") {
          const [, stream] = yield* provider.client.createResponseStream(payload);
          yield* Stream.runDrain(stream);
        } else {
          yield* provider.client.createResponse(payload).pipe(Effect.flip);
        }
        const snapshot = yield* provider.costControl.snapshot;
        expect(snapshot.stopped).toBe(false);
        expect(snapshot.modelCalls).toBe(failure === "malformed-count" ? 0 : 1);
        expect(snapshot.usage.estimatedCostMicrousd).toBe(0);
        expect(snapshot.usage.reservedCostMicrousd).toBe(
          failure === "malformed-count" ? 0 : 999_980,
        );
        yield* provider.client.createResponse(payload).pipe(Effect.flip);
        expect(sends).toBe(failure === "malformed-count" ? 0 : 1);
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
        client: native,
        model: "gpt-5.6-sol",
        cacheKey: "concurrent",
      });
      const first = yield* Effect.forkChild(provider.client.createResponse(payload));
      yield* Deferred.await(dispatched);
      yield* provider.client.createResponse(payload).pipe(Effect.flip);
      yield* Fiber.interrupt(first);
      const snapshot = yield* provider.costControl.snapshot;
      expect(sends).toBe(1);
      expect(snapshot.usage.reservedCostMicrousd).toBe(990_000);
      expect(
        (snapshot.usage.estimatedCostMicrousd ?? 0) + (snapshot.usage.reservedCostMicrousd ?? 0),
      ).toBeLessThanOrEqual(REVIEW_COST_LIMIT_MICROUSD);
    }),
  );

  it.effect(
    "refuses unknown models, service tiers, provider tools, and stale pricing before inference",
    () =>
      Effect.gen(function* () {
        let calls = 0;
        const native = yield* makeNative(
          HttpClient.make(() => {
            calls += 1;
            return Effect.die("Must not dispatch");
          }),
        );
        for (const name of ["custom-model", "toString", "__proto__"]) {
          yield* makeReviewOpenAi({ client: native, model: name, cacheKey: "config" }).pipe(
            Effect.flip,
          );
        }
        for (const change of [
          { service_tier: "priority" },
          { store: true },
          { max_output_tokens: 0 },
          { tools: [{ type: "web_search" as const }] },
        ]) {
          const provider = yield* makeReviewOpenAi({
            client: native,
            model: "gpt-5.6-sol",
            cacheKey: "config",
          });
          yield* provider.client.createResponse({ ...payload, ...change }).pipe(Effect.flip);
        }
        yield* TestClock.setTime(1_795_305_600_000);
        const provider = yield* makeReviewOpenAi({
          client: native,
          model: "gpt-5.6-sol",
          cacheKey: "expired",
        });
        yield* provider.client.createResponse(payload).pipe(Effect.flip);
        expect(calls).toBe(0);
      }),
  );
});
