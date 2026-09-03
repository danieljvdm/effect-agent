import {
  makeReviewer,
  ReviewChange,
  ReviewOutcome,
  ReviewReport,
  type ReviewRepository,
  ReviewRequest,
  ReviewDiagnostics,
  ReviewDiagnosticsSink,
} from "@effect-agent/pr-review";
import { ScriptedModel } from "@effect-agent/testing";
import { OpenAiClient } from "@effect/ai-openai";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import {
  type Crypto,
  Cause,
  Deferred,
  Console,
  Effect,
  Fiber,
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
import { TestClock } from "effect/testing";
import { Model } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  decodeObservationLines,
  digestReviewRequest,
  EvalCase,
  EvalCaseId,
  EvalDefectId,
  EvalExpectedDefect,
  EvalEvidence,
  EvalInputDigest,
  EvalReviewerFailure,
  EvalSuite,
  EvalVariantConfiguration,
  EvalVariantId,
  digestText,
  digestRepositorySnapshot,
  EvalRepositorySnapshot,
  EvalRepositoryFile,
  freezeEvalSuite,
  makeCurrentOpenAiVariant,
  makeQualityReport,
  runEvalSuite,
  validateEvalSuite,
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

const makeSuite = Effect.fn("PrReviewEvalTest.makeSuite")(function* () {
  const inputDigest = yield* digestReviewRequest(request);

  return EvalSuite.make({
    version: 1,
    cases: [
      EvalCase.make({
        version: 1,
        id: caseId,
        kind: "known-defects",
        provenance: "Synthetic fixture for the eval runner contract.",
        inputDigest,
        request,
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
    maxOutputTokens: 8_000,
    strictJsonSchema: true,
    store: false,
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

const failedVerificationDiagnostics = (inputDigest: EvalInputDigest) =>
  ReviewDiagnostics.make({
    strategy: "verified",
    requestDigest: inputDigest,
    discovery: "complete",
    verification: "failed",
    patchesSupplied: 1,
    candidates: [],
    activity: [],
    droppedActivityCount: 0,
    droppedCandidateCount: 0,
    stages: [],
  });

describe("PR-review model eval", () => {
  it.effect("retains bounded final diagnostics while a defect propagates", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const evalCase = suite.cases[0];

      if (evalCase === undefined) return yield* Effect.die("Missing fixture");
      const console = yield* Console.Console;
      const logged: Array<unknown> = [];
      let invocations = 0;

      const diagnostics = failedVerificationDiagnostics(evalCase.inputDigest);

      const variant: EvalVariant<ReviewDiagnosticsSink> = {
        configuration: configuration("defect"),
        review: () =>
          Effect.gen(function* () {
            const sink = yield* ReviewDiagnosticsSink;

            invocations += 1;
            yield* Effect.addFinalizer(() => sink.record(diagnostics));

            return yield* Effect.die("private defect payload");
          }).pipe(Effect.scoped),
      };

      const exit = yield* runEvalSuite(suite, [variant], {
        trials: 3,
        concurrency: 1,
        caseIds: [],
      }).pipe(
        Stream.runCollect,
        Effect.provideService(Console.Console, {
          ...console,
          error: (...values: ReadonlyArray<unknown>) => {
            logged.push(...values);
          },
        }),
        Effect.exit,
      );

      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") expect(Cause.hasDies(exit.cause)).toBe(true);
      expect(invocations).toBe(1);
      expect(logged).toHaveLength(1);
      expect(String(logged[0])).toContain('"type":"eval-trial-finalization"');
      expect(String(logged[0])).toContain('"stop":"defect"');
      expect(String(logged[0])).not.toContain("private defect payload");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
  it.effect("freezes paired order, isolates caches, and preserves a failed first trial", () =>
    Effect.gen(function* () {
      const original = (yield* makeSuite()).cases[0];

      if (original === undefined) return yield* Effect.die("Missing fixture");

      const snapshot = EvalRepositorySnapshot.make({
        version: 1,
        digest: original.inputDigest,
        files: [
          EvalRepositoryFile.make({
            path: "src/read.ts",
            revision: "base",
            content: "export const read = (value?: string) => value?.length ?? 0;",
          }),
          EvalRepositoryFile.make({
            path: "src/read.ts",
            revision: "head",
            content: "export const read = (value?: string) => value.length;",
          }),
        ],
      });

      const repository = EvalRepositorySnapshot.make({
        ...snapshot,
        digest: yield* digestRepositorySnapshot(snapshot),
      });

      const suite = EvalSuite.make({
        version: 1,
        cases: [
          EvalCase.make({
            ...original,
            id: Schema.decodeSync(EvalCaseId)("development"),
            repository,
            oracleVersion: 1,
            split: "development",
            relatedGroup: "development",
          }),
          EvalCase.make({
            ...original,
            id: Schema.decodeSync(EvalCaseId)("heldout"),
            repository,
            oracleVersion: 1,
            split: "heldout",
            relatedGroup: "heldout",
          }),
        ],
      });

      const baseline = yield* makeCurrentOpenAiVariant({
        id: "baseline",
        strategy: "baseline",
        reviewerRevision: "a".repeat(40),
      });

      const verified = yield* makeCurrentOpenAiVariant({
        id: "verified",
        strategy: "verified",
        reviewerRevision: "a".repeat(40),
      });

      const frozen = yield* freezeEvalSuite(
        suite,
        [baseline.configuration, verified.configuration],
        "paired-test",
      );

      const invoked: Array<string> = [];
      let first = true;

      const variants: Array<EvalVariant<never>> = [baseline, verified].map((variant) => ({
        configuration: variant.configuration,
        review: () =>
          Effect.suspend(() => {
            invoked.push(variant.configuration.id);
            if (first) {
              first = false;

              return Effect.fail(
                EvalReviewerFailure.make({
                  errorTag: "Unavailable",
                  message: "First trial failed",
                  estimatedCostMicrousd: 17,
                  reservedCostMicrousd: 900_000,
                }),
              );
            }

            return Effect.succeed(successfulOutcome);
          }),
      }));

      const observations = yield* runEvalSuite(frozen, variants, {
        trials: 3,
        concurrency: 1,
        caseIds: [],
      }).pipe(Stream.runCollect);

      expect(invoked).toEqual([
        "baseline",
        "verified",
        "verified",
        "baseline",
        "baseline",
        "verified",
        "verified",
        "baseline",
        "baseline",
        "verified",
        "verified",
        "baseline",
      ]);
      expect(observations[0]?.result).toMatchObject({
        _tag: "Failed",
        estimatedCostMicrousd: 17,
        reservedCostMicrousd: 900_000,
      });
      expect(
        observations
          .filter((row) => row.caseId === "development" && row.variant.id === "baseline")
          .map((row) => [row.trial, row.result._tag]),
      ).toEqual([
        [1, "Failed"],
        [2, "Succeeded"],
        [3, "Succeeded"],
      ]);
      expect(new Set(observations.map((row) => row.cacheNamespace)).size).toBe(12);
      expect(new Set(observations.map((row) => row.runId)).size).toBe(1);
      expect(frozen.comparison?.maximumCostMicrousd).toBe(11_999_988);

      const wrongSplit = EvalSuite.make({
        ...suite,
        cases: suite.cases.map((evalCase) =>
          EvalCase.make({ ...evalCase, relatedGroup: "same-revision" }),
        ),
      });

      expect(
        (yield* freezeEvalSuite(
          wrongSplit,
          [baseline.configuration, verified.configuration],
          "bad-split",
        ).pipe(Effect.result))._tag,
      ).toBe("Failure");

      const tooLarge = EvalSuite.make({
        version: 1,
        cases: Array.from({ length: 21 }, (_, index) =>
          EvalCase.make({
            ...suite.cases[index % 2]!,
            id: Schema.decodeSync(EvalCaseId)(`case-${index}`),
          }),
        ),
      });

      expect(
        (yield* freezeEvalSuite(
          tooLarge,
          [baseline.configuration, verified.configuration],
          "over-budget",
        ).pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(
        (yield* runEvalSuite(frozen, variants, { trials: 3, concurrency: 2, caseIds: [] }).pipe(
          Stream.runCollect,
          Effect.result,
        ))._tag,
      ).toBe("Failure");
      expect(invoked).toHaveLength(12);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
  it.effect("replays the real reviewer and round-trips its JSONL observation", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();

      const scripted = yield* Layer.build(
        ScriptedModel.layer([
          {
            _tag: "Stream",
            parts: [
              {
                type: "tool-call",
                id: "review",
                name: "submit_review",
                params: { findings: [] },
              },
              {
                type: "finish",
                reason: "tool-calls",
                usage: {
                  inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
                  outputTokens: { total: 4 },
                },
              },
            ],
            termination: { _tag: "Complete" },
          },
        ]),
      );

      const model = Model.make("scripted", "eval", Layer.succeedContext(scripted));
      const reviewer = makeReviewer({ model });

      const variant: EvalVariant<ReviewRepository | ReviewDiagnosticsSink | Crypto.Crypto> = {
        configuration: configuration("scripted"),
        review: (input) =>
          reviewer.review(input).pipe(
            Effect.mapError((error) =>
              EvalReviewerFailure.make({
                errorTag: error._tag,
                message: "Scripted reviewer failed",
              }),
            ),
          ),
      };

      const observations = yield* runEvalSuite(suite, [variant], {
        trials: 1,
        concurrency: 1,
        caseIds: [],
      }).pipe(Stream.runCollect);

      expect(observations).toHaveLength(1);
      expect(observations[0]?.runnerVersion).toBe("0.3.0");
      expect(observations[0]?.result._tag).toBe("Succeeded");
      if (observations[0]?.result._tag === "Succeeded") {
        expect(observations[0].result.outcome.report.findings).toEqual([]);
      }

      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-eval-" });
      const output = `${directory}/observations.jsonl`;

      expect(yield* writeObservations(output, Stream.fromIterable(observations))).toBe(1);
      const decoded = yield* decodeObservationLines(yield* fs.readFileString(output));

      expect(decoded).toEqual(observations);

      const historical = (yield* fs.readFileString(output)).replace(
        '"runnerVersion":"0.3.0"',
        '"runnerVersion":"0.0.9"',
      );

      expect((yield* decodeObservationLines(historical))[0]?.runnerVersion).toBe("0.0.9");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "retains a recorded finding after an AI failure without counting a successful trial",
    () =>
      Effect.gen(function* () {
        const suite = yield* makeSuite();

        const scripted = yield* Layer.build(
          ScriptedModel.layer([
            {
              _tag: "Stream",
              parts: [
                {
                  type: "tool-call",
                  id: "recorded",
                  name: "record_finding",
                  params: {
                    path: "src/read.ts",
                    line: 1,
                    category: "correctness",
                    title: "Handle undefined before reading the length",
                    body: "Passing undefined now throws at value.length. Preserve the optional handling.",
                    priority: 1,
                  },
                },
                {
                  type: "finish",
                  reason: "tool-calls",
                  usage: {
                    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
                    outputTokens: { total: 4 },
                  },
                },
              ],
              termination: { _tag: "Complete" },
            },
            {
              _tag: "Stream",
              parts: [],
              termination: { _tag: "Fail", description: "Provider disconnected after recording" },
            },
          ]),
        );

        const reviewer = makeReviewer({
          model: Model.make("scripted", "eval", Layer.succeedContext(scripted)),
          estimateCostMicrousd: () => Effect.succeed({ costMicrousd: 12 }),
        });

        const variant: EvalVariant<ReviewRepository | ReviewDiagnosticsSink | Crypto.Crypto> = {
          configuration: configuration("partial"),
          review: (input) =>
            reviewer.review(input).pipe(
              Effect.mapError((error) =>
                EvalReviewerFailure.make({
                  errorTag: error._tag,
                  message: "Scripted reviewer failed",
                }),
              ),
            ),
        };

        const observations = yield* runEvalSuite(suite, [variant], {
          trials: 1,
          concurrency: 1,
          caseIds: [],
        }).pipe(Stream.runCollect);

        expect(observations[0]?.result).toMatchObject({
          _tag: "Succeeded",
          outcome: { incomplete: true, report: { findings: [{ path: "src/read.ts", line: 1 }] } },
        });
        const report = yield* makeQualityReport(suite, observations, 1);

        expect(report.variants[0]?.resources).toMatchObject({
          attemptedTrials: 1,
          succeededTrials: 0,
          incompleteTrials: 1,
          failedTrials: 0,
          costedIncompleteTrials: 1,
          estimatedCostMicrousd: 12,
          inputTokens: 10,
          outputTokens: 4,
        });
        expect(report.unjudgedFindings).toHaveLength(1);
        expect(report.unjudgedFindings[0]?.finding.title).toBe(
          "Handle undefined before reading the length",
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("derives model, reasoning, and guidance identity inside the live variant", () =>
    Effect.gen(function* () {
      const variant = yield* makeCurrentOpenAiVariant({
        id: Schema.decodeSync(EvalVariantId)("candidate-guidance-v1"),
        guidance: "  Keep the public error channel typed.  ",
        model: "gpt-5.6-terra",
        reasoningEffort: "high",
      });

      type Review = ReturnType<typeof variant.review>;
      expectTypeOf<Effect.Error<Review>>().toEqualTypeOf<EvalReviewerFailure>();
      expectTypeOf<Effect.Services<Review>>().toEqualTypeOf<
        OpenAiClient.OpenAiClient | ReviewRepository | ReviewDiagnosticsSink | Crypto.Crypto
      >();

      const stream = runEvalSuite(yield* makeSuite(), [variant], {
        trials: 1,
        concurrency: 1,
        caseIds: [],
      });

      expectTypeOf<Stream.Services<typeof stream>>().toEqualTypeOf<
        OpenAiClient.OpenAiClient | Crypto.Crypto
      >();
      expect(variant.configuration.id).toBe("candidate-guidance-v1");
      expect(variant.configuration.reviewerProfile).toBe("diff-review-v5-capped");
      expect(variant.configuration.costLimitMicrousd).toBe(999_999);
      expect(variant.configuration).toMatchObject({
        model: "gpt-5.6-terra",
        reasoningEffort: "high",
        effective: { pricing: { input: 200, read: 20, write: 250, output: 1_200 } },
      });
      expect(variant.configuration.guidanceDigest).toBe(
        yield* digestText("Keep the public error channel typed."),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects an unpriced model before constructing a live variant", () =>
    Effect.gen(function* () {
      const failure = yield* makeCurrentOpenAiVariant({
        id: "unpriced-model",
        model: "unsupported-model",
      }).pipe(Effect.flip);

      expect(failure).toMatchObject({
        _tag: "EvalConfigurationError",
        message: "Missing pricing for the configured reviewer model",
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each(["preflight", "paid"] as const)(
    "retains %s failure accounting without provider payloads or credentials",
    (phase) =>
      Effect.gen(function* () {
        const suite = yield* makeSuite();

        const variant = yield* makeCurrentOpenAiVariant({
          id: Schema.decodeSync(EvalVariantId)("provider-failure"),
        });

        const privateText = "private-source-and-provider-payload";

        const client = HttpClient.make((request) => {
          const counted = phase === "paid" && request.url.endsWith("/responses/input_tokens");

          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify(
                  counted
                    ? { object: "response.input_tokens", input_tokens: 1_000 }
                    : { error: { type: "invalid_request_error", message: privateText } },
                ),
                { status: counted ? 200 : 400, headers: { "content-type": "application/json" } },
              ),
            ),
          );
        });

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

        expect(observations[0]?.result).toMatchObject(
          phase === "paid"
            ? {
                _tag: "Succeeded",
                outcome: {
                  incomplete: true,
                  usage: { estimatedCostMicrousd: 0, reservedCostMicrousd: 645_000 },
                },
              }
            : {
                _tag: "Failed",
                errorTag: "AiError/InvalidRequestError",
                message: "AI failure; retryable=false",
                estimatedCostMicrousd: 0,
                reservedCostMicrousd: 0,
                diagnostics: { discovery: "failed", candidates: [] },
              },
        );
        const report = yield* makeQualityReport(suite, observations, 1);

        expect(report.variants[0]?.resources).toMatchObject({
          estimatedCostMicrousd: 0,
          reservedCostMicrousd: phase === "paid" ? 645_000 : 0,
          reservationKnownTrials: 1,
          reservationUnknownTrials: 0,
        });
        expect(JSON.stringify(observations)).not.toContain(privateText);
        expect(JSON.stringify(observations)).not.toContain("private-api-key");
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "caps each live-variant trial independently through the Action's cached wire path",
    () =>
      Effect.gen(function* () {
        const suite = yield* makeSuite();
        const variant = yield* makeCurrentOpenAiVariant({ id: "capped-wire" });
        const sent: Array<Schema.Json> = [];
        let counts = 0;

        const client = HttpClient.make((httpRequest) => {
          if (httpRequest.url.endsWith("/responses/input_tokens")) {
            counts += 1;

            return Effect.succeed(
              HttpClientResponse.fromWeb(
                httpRequest,
                new Response(
                  JSON.stringify({ object: "response.input_tokens", input_tokens: 100_000 }),
                  { headers: { "content-type": "application/json" } },
                ),
              ),
            );
          }
          if (httpRequest.body._tag !== "Uint8Array") return Effect.die("Expected JSON body");
          sent.push(
            Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json))(
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
                model: "gpt-5.6-sol",
                created_at: 1_788_000_000,
                service_tier: "default",
                output: [item],
                usage: {
                  input_tokens: 100_000,
                  output_tokens: 12_000,
                  total_tokens: 112_000,
                  input_tokens_details: { cached_tokens: 0, cache_write_tokens: 100_000 },
                  output_tokens_details: { reasoning_tokens: 11_900 },
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
            model: "gpt-5.6-sol",
            reasoning: { effort: "xhigh" },
            store: false,
            service_tier: "default",
            max_output_tokens: 24_999,
            prompt_cache_key: expect.stringMatching(/^[a-f0-9]{64}$/),
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
          expect(observation.result).toMatchObject({
            _tag: "Succeeded",
            outcome: {
              exhausted: "cost",
              incomplete: true,
              turns: 1,
              usage: {
                estimatedCostMicrousd: 740_000,
                reservedCostMicrousd: 0,
                cachedInputTokens: 0,
                cacheWriteInputTokens: 100_000,
              },
            },
          });
        }

        const unadjudicated = EvalSuite.make({
          ...suite,
          cases: suite.cases.map((evalCase) =>
            EvalCase.make({
              ...evalCase,
              kind: "unadjudicated",
              expectedDefects: [],
            }),
          ),
        });

        expect(
          (yield* makeQualityReport(unadjudicated, observations, 2).pipe(Effect.result))._tag,
        ).toBe("Failure");

        const replayObservations = yield* runEvalSuite(
          unadjudicated,
          [
            {
              configuration: configuration("replay"),
              review: () => Effect.succeed(successfulOutcome),
            },
          ],
          { trials: 2, concurrency: 1, caseIds: [] },
        ).pipe(Stream.runCollect);

        const report = yield* makeQualityReport(unadjudicated, replayObservations, 2);

        expect(report.variants[0]?.cleanControls).toEqual({ passed: 0, total: 0 });
        expect(report.variants[0]?.blockerCases.total).toBe(0);
        expect(report.variants[0]?.cases[0]?.blockerStatus).toBe("not-applicable");
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("selects cases and records elapsed time from the Effect clock", () =>
    Effect.gen(function* () {
      const originalSuite = yield* makeSuite();
      const original = originalSuite.cases[0];

      expect(original).toBeDefined();
      if (original === undefined) return;

      const selectedId = Schema.decodeSync(EvalCaseId)("selected-case");

      const suite = EvalSuite.make({
        version: 1,
        cases: [original, EvalCase.make({ ...original, id: selectedId })],
      });

      const variant: EvalVariant<never> = {
        configuration: configuration("timed"),
        review: () => TestClock.adjust(5).pipe(Effect.as(successfulOutcome)),
      };

      const observations = yield* runEvalSuite(suite, [variant], {
        trials: 1,
        concurrency: 1,
        caseIds: [selectedId],
      }).pipe(Stream.runCollect);

      expect(observations).toHaveLength(1);
      expect(observations[0]?.caseId).toBe(selectedId);
      expect(observations[0]?.elapsedMillis).toBe(5);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "persists completed trials before interruption and closes model and file resources",
    () =>
      Effect.gen(function* () {
        const suite = yield* makeSuite();
        const evalCase = suite.cases[0];

        if (evalCase === undefined) return yield* Effect.die("Missing fixture");
        const console = yield* Console.Console;
        const logged: Array<unknown> = [];

        const diagnostics = failedVerificationDiagnostics(evalCase.inputDigest);

        const acquired = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const calls = yield* Ref.make(0);
        const opened = yield* Ref.make<Option.Option<FileSystem.File>>(Option.none());
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-interrupted-" });
        const output = `${directory}/observations.jsonl`;

        const variant: EvalVariant<ReviewDiagnosticsSink> = {
          configuration: configuration("interruptible"),
          review: () =>
            Effect.gen(function* () {
              if ((yield* Ref.updateAndGet(calls, (count) => count + 1)) === 1) {
                return successfulOutcome;
              }
              const sink = yield* ReviewDiagnosticsSink;

              return yield* Effect.acquireRelease(Deferred.succeed(acquired, undefined), () =>
                sink
                  .record(diagnostics)
                  .pipe(Effect.andThen(Deferred.succeed(released, undefined))),
              ).pipe(Effect.andThen(Effect.never));
            }).pipe(Effect.scoped),
        };

        const fiber = yield* writeObservations(
          output,
          runEvalSuite(suite, [variant], { trials: 3, concurrency: 1, caseIds: [] }),
        ).pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            open: (path, options) =>
              fs.open(path, options).pipe(Effect.tap((file) => Ref.set(opened, Option.some(file)))),
          }),
          Effect.provideService(Console.Console, {
            ...console,
            error: (...values: ReadonlyArray<unknown>) => {
              logged.push(...values);
            },
          }),
          Effect.forkChild,
        );

        yield* Deferred.await(acquired);
        const persisted = yield* fs.readFileString(output);
        const decoded = yield* decodeObservationLines(persisted);

        expect(decoded.map((observation) => observation.trial)).toEqual([1]);
        expect(decoded[0]?.result._tag).toBe("Succeeded");
        yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(exit._tag).toBe("Failure");
        if (exit._tag === "Failure") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
        expect(yield* Deferred.isDone(released)).toBe(true);
        expect(yield* fs.readFileString(output)).toBe(persisted);
        expect(yield* Ref.get(calls)).toBe(2);
        expect(logged).toHaveLength(1);
        expect(String(logged[0])).toContain('"stop":"interrupted"');
        expect(String(logged[0])).toContain('"verification":"failed"');
        const handle = yield* Ref.get(opened);

        expect(Option.isSome(handle)).toBe(true);
        if (Option.isSome(handle)) {
          expect((yield* Effect.result(handle.value.stat))._tag).toBe("Failure");
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses an existing output before starting any model call", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-existing-" });
      const output = `${directory}/observations.jsonl`;

      yield* fs.writeFileString(output, "existing evidence\n");
      const calls = yield* Ref.make(0);

      const variant: EvalVariant<never> = {
        configuration: configuration("existing"),
        review: () => Ref.update(calls, (count) => count + 1).pipe(Effect.as(successfulOutcome)),
      };

      const result = yield* writeObservations(
        output,
        runEvalSuite(suite, [variant], { trials: 1, concurrency: 1, caseIds: [] }),
      ).pipe(Effect.result);

      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "EvalDataError", operation: "open observations" },
      });
      expect(yield* Ref.get(calls)).toBe(0);
      expect(yield* fs.readFileString(output)).toBe("existing evidence\n");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
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

  it.effect("bounds concurrent trials and isolates typed-failure diagnostics", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const active = yield* Ref.make(0);
      const maximum = yield* Ref.make(0);
      const gate = yield* Deferred.make<void>();
      const failures = yield* Ref.make(0);
      const firstRecorded = yield* Deferred.make<void>();
      const secondRecorded = yield* Deferred.make<void>();
      const inputDigest = yield* digestReviewRequest(request);

      const successful: EvalVariant<never> = {
        configuration: configuration("successful"),
        review: () =>
          Effect.gen(function* () {
            const count = yield* Ref.updateAndGet(active, (value) => value + 1);

            yield* Ref.update(maximum, (value) => Math.max(value, count));
            if (count === 2) yield* Deferred.succeed(gate, undefined);
            yield* Deferred.await(gate);
            yield* Ref.update(active, (value) => value - 1);

            return successfulOutcome;
          }),
      };

      const failing: EvalVariant<ReviewDiagnosticsSink> = {
        configuration: configuration("failing"),
        review: () =>
          Effect.gen(function* () {
            const sink = yield* ReviewDiagnosticsSink;
            const call = yield* Ref.updateAndGet(failures, (count) => count + 1);

            const diagnostics = ReviewDiagnostics.make({
              ...failedVerificationDiagnostics(inputDigest),
              droppedActivityCount: call,
            });

            if (call === 2) yield* Deferred.await(firstRecorded);
            if (call !== 3) yield* sink.record(diagnostics);
            if (call === 1) {
              yield* Deferred.succeed(firstRecorded, undefined);
              yield* Deferred.await(secondRecorded);
            }
            if (call === 2) yield* Deferred.succeed(secondRecorded, undefined);

            return yield* EvalReviewerFailure.make({
              errorTag: "RateLimitError",
              message: "Provider declined the trial",
              estimatedCostMicrousd: 13,
              ...(call === 2
                ? {
                    diagnostics: ReviewDiagnostics.make({
                      ...diagnostics,
                      droppedActivityCount: 20,
                    }),
                  }
                : {}),
            });
          }),
      };

      const observations = yield* runEvalSuite(suite, [successful, failing], {
        trials: 4,
        concurrency: 2,
        caseIds: [],
      }).pipe(Stream.runCollect);

      expect(yield* Ref.get(maximum)).toBe(2);
      expect(observations).toHaveLength(8);
      expect(
        observations.filter((observation) => observation.result._tag === "Failed"),
      ).toHaveLength(4);
      expect(
        observations
          .filter((observation) => observation.result._tag === "Failed")
          .every(
            (observation) =>
              observation.result._tag === "Failed" &&
              observation.result.errorTag === "RateLimitError" &&
              observation.result.estimatedCostMicrousd === 13,
          ),
      ).toBe(true);
      expect(
        observations
          .filter((observation) => observation.result._tag === "Failed")
          .sort((left, right) => left.trial - right.trial)
          .map((observation) =>
            observation.result._tag === "Failed"
              ? observation.result.diagnostics?.droppedActivityCount
              : undefined,
          ),
      ).toEqual([1, 20, undefined, 4]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects corrupt corpus identity before a model can run", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const original = suite.cases[0];

      expect(original).toBeDefined();
      if (original === undefined) return;

      const wrongDigest = Schema.decodeSync(EvalInputDigest)("0".repeat(64));

      const corrupt = EvalSuite.make({
        version: 1,
        cases: [EvalCase.make({ ...original, inputDigest: wrongDigest })],
      });

      const result = yield* Effect.result(validateEvalSuite(corrupt));

      expect(result._tag).toBe("Failure");

      const encoded = Schema.encodeSync(EvalSuite)(suite);

      expect(Schema.decodeSync(EvalSuite)(encoded)).toEqual(suite);

      const changedDigest = yield* digestReviewRequest(
        ReviewRequest.make({ ...request, headRevision: "changed-head" }),
      );

      expect(changedDigest).not.toBe(original.inputDigest);
      const duplicateCases = { ...encoded, cases: [encoded.cases[0], encoded.cases[0]] };

      const duplicateDefects = {
        ...encoded,
        cases: [
          {
            ...encoded.cases[0],
            expectedDefects: [
              encoded.cases[0]?.expectedDefects[0],
              encoded.cases[0]?.expectedDefects[0],
            ],
          },
        ],
      };

      const cleanWithDefect = {
        ...encoded,
        cases: [{ ...encoded.cases[0], kind: "clean-control" }],
      };

      const unknownEvidencePath = {
        ...encoded,
        cases: [
          {
            ...encoded.cases[0],
            expectedDefects: [
              {
                ...encoded.cases[0]?.expectedDefects[0],
                evidence: [
                  {
                    ...encoded.cases[0]?.expectedDefects[0]?.evidence[0],
                    path: "src/not-admitted.ts",
                  },
                ],
              },
            ],
          },
        ],
      };

      const malformedRevision = {
        ...encoded,
        cases: [
          {
            ...encoded.cases[0],
            request: { ...encoded.cases[0]?.request, headRevision: "" },
          },
        ],
      };

      for (const invalid of [
        duplicateCases,
        duplicateDefects,
        cleanWithDefect,
        unknownEvidencePath,
        malformedRevision,
      ]) {
        expect(Option.isNone(Schema.decodeUnknownOption(EvalSuite)(invalid))).toBe(true);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
