import {
  ReviewChange,
  ReviewFollowUp,
  ReviewOutcome,
  ReviewReport,
  ReviewRequest,
} from "@effect-agent/pr-review/review";
import { OpenAiClient } from "@effect/ai-openai";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  ConfigProvider,
  Deferred,
  Effect,
  FileSystem,
  Layer,
  Option,
  PlatformError,
  Redacted,
  Ref,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  decodeObservationLines,
  digestReviewRequest,
  EvalCase,
  EvalCaseId,
  EvalDefectId,
  EvalExpectedDefect,
  EvalEvidence,
  EvalSuite,
  EvalVariantConfiguration,
  EvalVariantId,
  makeCurrentOpenAiVariant,
  runEvalSuite,
  writeObservations,
  type EvalVariant,
} from "../src/index.ts";

const patch = `@@ -1 +1 @@
-export const read = (value?: string) => value?.length ?? 0;
+export const read = (value?: string) => value.length;`;

const request = ReviewRequest.make({
  title: "Remove optional handling",
  description: "",
  baseRevision: "base",
  headRevision: "head",
  changes: [ReviewChange.make({ path: "src/read.ts", patch })],
  unreviewedPaths: [],
});

const caseId = Schema.decodeSync(EvalCaseId)("optional-read");
const defectId = Schema.decodeSync(EvalDefectId)("undefined-dereference");

const makeSuite = Effect.fn("PrReviewEvalTest.makeSuite")(function* (
  input: ReviewRequest = request,
) {
  const inputDigest = yield* digestReviewRequest(input);

  return EvalSuite.make({
    version: 1,
    cases: [
      EvalCase.make({
        version: 1,
        id: caseId,
        kind: "known-defects",
        provenance: "Synthetic fixture for the eval runner contract.",
        inputDigest,
        request: input,
        expectedDefects: [
          EvalExpectedDefect.make({
            id: defectId,
            severity: "blocking",
            invariant: "The changed function dereferences an optional string.",
            evidence: [
              EvalEvidence.make({
                path: "src/read.ts",
                line: 1,
                description: "value.length executes when value is undefined.",
              }),
            ],
          }),
        ],
      }),
    ],
  });
});

const configuration = (id: string) =>
  EvalVariantConfiguration.make({
    id,
    reviewerProfile: "scripted-v1",
    provider: "openai",
    model: "scripted-eval",
    reasoningEffort: "medium",
    serviceTier: "default",
    compaction: "prune",
    contextTokenLimit: 128_000,
    maxOutputTokens: 8_000,
    strictJsonSchema: true,
    store: false,
    maxCostMicrousd: 2_500_000,
    budgetPolicy: "input-size-v1",
  });

const successfulOutcome = ReviewOutcome.make({
  report: ReviewReport.make({ summary: "No findings.", findings: [] }),
  turns: 1,
  usage: {
    inputTokens: 10,
    uncachedInputTokens: 10,
    cachedInputTokens: 0,
    cacheWriteInputTokens: 0,
    outputTokens: 2,
  },
});

describe("PR-review model eval", () => {
  it.effect("records actionable AI error categories without provider payloads or credentials", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();

      const variant = yield* makeCurrentOpenAiVariant({
        id: Schema.decodeSync(EvalVariantId)("provider-failure"),
      }).pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromEnv({ env: { PR_REVIEW_MODEL: "gpt-6-astra" } }),
        ),
      );

      expect(variant.configuration.serviceTier).toBe("default");

      const privateText = "private-source-and-provider-payload";

      const client = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify({ error: { type: "invalid_request_error", message: privateText } }),
              { status: 400, headers: { "content-type": "application/json" } },
            ),
          ),
        ),
      );

      const observations = yield* runEvalSuite(suite, [variant], {
        trials: 1,
        concurrency: 1,
        caseIds: [],
      }).pipe(
        Stream.runCollect,
        Effect.provide(
          OpenAiClient.layer({ apiKey: Redacted.make("private-api-key") }).pipe(
            Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
          ),
        ),
      );

      expect(observations[0]?.result).toEqual({
        _tag: "Failed",
        errorTag: "AiError/InvalidRequestError",
        message: "AI failure; retryable=false",
        estimatedCostMicrousd: 0,
      });
      expect(JSON.stringify(observations)).not.toContain(privateText);
      expect(JSON.stringify(observations)).not.toContain("private-api-key");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("flushes out-of-order completions and cancels active work after a write failure", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-write-failure-" });
      const output = `${directory}/observations.jsonl`;
      const blocked = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      const saved = yield* Deferred.make<void>();
      const calls = yield* Ref.make(0);
      const writes = yield* Ref.make(0);
      const opened = yield* Ref.make<Option.Option<FileSystem.File>>(Option.none());

      const variant: EvalVariant<Scope.Scope> = {
        configuration: configuration("write-failure"),
        review: () =>
          Effect.gen(function* () {
            const call = yield* Ref.updateAndGet(calls, (count) => count + 1);

            if (call === 1) {
              return yield* Effect.acquireRelease(Deferred.succeed(blocked, undefined), () =>
                Deferred.succeed(released, undefined),
              ).pipe(Effect.andThen(Effect.never));
            }
            yield* Deferred.await(call === 2 ? blocked : saved);

            return successfulOutcome;
          }),
      };

      const outputFailure = PlatformError.systemError({
        _tag: "Unknown",
        module: "FileSystem",
        method: "writeAll",
        description: "Injected output failure",
      });

      const result = yield* writeObservations(
        output,
        runEvalSuite(suite, [variant], { trials: 6, concurrency: 2, caseIds: [] }),
      ).pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          open: Effect.fn(function* (path, options) {
            const file = yield* fs.open(path, options);

            yield* Ref.set(opened, Option.some(file));

            return {
              ...file,
              writeAll: (bytes: Uint8Array) =>
                Ref.updateAndGet(writes, (count) => count + 1).pipe(
                  Effect.flatMap((count) =>
                    count === 2 ? Effect.fail(outputFailure) : file.writeAll(bytes),
                  ),
                ),
              sync: file.sync.pipe(Effect.tap(() => Deferred.succeed(saved, undefined))),
            };
          }),
        }),
        Effect.result,
      );

      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "EvalDataError", operation: "write observations" },
      });
      expect(yield* Deferred.isDone(released)).toBe(true);
      const decoded = yield* decodeObservationLines(yield* fs.readFileString(output));

      expect(decoded.map((observation) => observation.trial)).toEqual([2]);
      const handle = yield* Ref.get(opened);

      expect(Option.isSome(handle)).toBe(true);
      if (Option.isSome(handle)) {
        expect((yield* Effect.result(handle.value.stat))._tag).toBe("Failure");
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect.each([
    {
      patchCharacters: 10_000,
      serviceTier: "default",
      maxOutputTokens: 12_000,
      outputTokens: 12_000,
      estimatedCostMicrousd: 1_100_000,
    },
    {
      patchCharacters: 30_000,
      serviceTier: "default",
      maxOutputTokens: 14_000,
      outputTokens: 12_000,
      estimatedCostMicrousd: 1_100_000,
    },
    {
      patchCharacters: 30_000,
      serviceTier: "fast",
      maxOutputTokens: 2_000,
      outputTokens: 2_000,
      estimatedCostMicrousd: 1_200_000,
    },
  ])(
    "scales each trial's provider allowance at $serviceTier tier: $patchCharacters characters",
    ({ patchCharacters, serviceTier, maxOutputTokens, outputTokens, estimatedCostMicrousd }) =>
      Effect.gen(function* () {
        const suite = yield* makeSuite(
          ReviewRequest.make({
            ...request,
            changes: [
              ReviewChange.make({ path: "src/read.ts", patch: patch.padEnd(patchCharacters, " ") }),
            ],
          }),
        );

        const variant = yield* makeCurrentOpenAiVariant({ id: "capped-wire" }).pipe(
          Effect.provideService(
            ConfigProvider.ConfigProvider,
            ConfigProvider.fromEnv({
              env: {
                PR_REVIEW_MODEL: "gpt-6-astra",
                PR_REVIEW_MAX_COST_USD: "1.2",
                PR_REVIEW_EFFORT: "max",
                PR_REVIEW_PRIORITY: serviceTier,
                // Isolate provider admission from the separate working-context boundary.
                PR_REVIEW_COMPACTION: "prune",
                PR_REVIEW_CONTEXT_TOKENS: "128000",
              },
            }),
          ),
        );

        expect(variant.configuration.maxCostMicrousd).toBe(1_200_000);
        expect(variant.configuration.reasoningEffort).toBe("max");
        expect(variant.configuration.budgetPolicy).toBe("input-size-v1");
        const sent: Array<Schema.Json> = [];
        let counts = 0;

        const client = HttpClient.make((httpRequest) => {
          if (httpRequest.url.endsWith("/responses/input_tokens")) {
            counts += 1;

            return Effect.succeed(
              HttpClientResponse.fromWeb(
                httpRequest,
                new Response(
                  JSON.stringify({ object: "response.input_tokens", input_tokens: 40_000 }),
                  { headers: { "content-type": "application/json" } },
                ),
              ),
            );
          }
          if (httpRequest.body._tag !== "Uint8Array") return Effect.die("Expected JSON body");
          sent.push(
            Schema.decodeSync(Schema.fromJsonString(Schema.Json))(
              new TextDecoder().decode(httpRequest.body.body),
            ),
          );

          const item = {
            type: "function_call",
            id: "fc_read",
            call_id: "call_read",
            name: "read_file",
            arguments: JSON.stringify({
              path: "src/read.ts",
              revision: "head",
              startLine: 1,
              lineCount: 1,
            }),
            status: "completed",
          };

          const events = [
            { type: "response.output_item.added", output_index: 0, item },
            {
              type: "response.function_call_arguments.done",
              output_index: 0,
              item_id: item.id,
              arguments: item.arguments,
            },
            { type: "response.output_item.done", output_index: 0, item },
            {
              type: "response.completed",
              response: {
                id: "resp_fixture",
                object: "response",
                model: "gpt-6-astra",
                created_at: 1_788_000_000,
                service_tier: serviceTier,
                output: [item],
                usage: {
                  input_tokens: 40_000,
                  output_tokens: outputTokens,
                  total_tokens: 40_000 + outputTokens,
                  input_tokens_details: { cached_tokens: 0, cache_write_tokens: 40_000 },
                  output_tokens_details: { reasoning_tokens: outputTokens - 100 },
                },
              },
            },
          ];

          return Effect.succeed(
            HttpClientResponse.fromWeb(
              httpRequest,
              new Response(
                events
                  .map(
                    (event, sequence_number) =>
                      `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
                  )
                  .join(""),
                { headers: { "content-type": "text/event-stream" } },
              ),
            ),
          );
        });

        const observations = yield* runEvalSuite(suite, [variant], {
          trials: 2,
          concurrency: 2,
          caseIds: [],
        }).pipe(
          Stream.runCollect,
          Effect.provide(
            OpenAiClient.layer({ apiKey: Redacted.make("offline-key") }).pipe(
              Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
            ),
          ),
        );

        expect(counts).toBe(4);
        expect(sent).toHaveLength(2);
        for (const payload of sent) {
          expect(payload).toMatchObject({
            model: "gpt-6-astra",
            reasoning: { effort: "max" },
            store: false,
            service_tier: serviceTier,
            max_output_tokens: maxOutputTokens,
            prompt_cache_key: "pr-review:head",
            prompt_cache_options: { mode: "explicit", ttl: "30m" },
            tools: expect.arrayContaining([
              expect.objectContaining({ name: "read_file", strict: true }),
              expect.objectContaining({ name: "submit_review", strict: true }),
            ]),
          });
          expect(JSON.stringify(payload)).toContain(
            '"prompt_cache_breakpoint":{"mode":"explicit"}',
          );
        }
        for (const observation of observations) {
          expect(observation.variant.serviceTier).toBe(serviceTier);
          expect(observation.result).toMatchObject({
            _tag: "Succeeded",
            outcome: {
              exhausted: "cost",
              incomplete: true,
              turns: 1,
              usage: {
                estimatedCostMicrousd,
                reservedCostMicrousd: 0,
                cachedInputTokens: 0,
                cacheWriteInputTokens: 40_000,
              },
            },
          });
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("requires a complete, disjoint oracle for supplied follow-ups", () =>
    Effect.gen(function* () {
      const withFollowUps = ReviewRequest.make({
        ...request,
        followUps: ["review-1", "review-2", "review-3"].map((id) =>
          ReviewFollowUp.make({ id, description: `Check ${id} against the head revision.` }),
        ),
      });

      const suite = yield* makeSuite(withFollowUps);
      const encoded = Schema.encodeSync(EvalSuite)(suite);
      const first = encoded.cases[0];

      if (first === undefined) throw new Error("Missing eval case");

      const oracle = {
        ...first,
        expectedResolvedFollowUpIds: ["review-1"],
        expectedUnresolvedFollowUpIds: ["review-2", "review-3"],
      };

      const parsed = Schema.decodeSync(EvalSuite)({ ...encoded, cases: [oracle] });

      expect(Schema.encodeSync(EvalSuite)(parsed).cases[0]).toMatchObject({
        expectedResolvedFollowUpIds: ["review-1"],
        expectedUnresolvedFollowUpIds: ["review-2", "review-3"],
      });

      for (const invalidCase of [
        { ...first, expectedResolvedFollowUpIds: ["review-1"] },
        { ...oracle, expectedUnresolvedFollowUpIds: ["review-2"] },
        { ...oracle, expectedUnresolvedFollowUpIds: ["review-1", "review-2", "review-3"] },
        { ...oracle, expectedUnresolvedFollowUpIds: ["review-2", "unknown-review"] },
        { ...oracle, expectedResolvedFollowUpIds: ["review-1", "review-1"] },
      ]) {
        expect(
          Option.isNone(
            Schema.decodeOption(EvalSuite)({
              ...encoded,
              cases: [invalidCase],
            }),
          ),
        ).toBe(true);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects resolution expectations when the request has no follow-ups", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const encoded = Schema.encodeSync(EvalSuite)(suite);
      const evalCase = encoded.cases[0];

      if (evalCase === undefined) throw new Error("Missing eval case");

      expect(
        Option.isNone(
          Schema.decodeOption(EvalSuite)({
            ...encoded,
            cases: [
              {
                ...evalCase,
                expectedResolvedFollowUpIds: [],
                expectedUnresolvedFollowUpIds: [],
              },
            ],
          }),
        ),
      ).toBe(true);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
