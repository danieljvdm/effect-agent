import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Ref, Result, Stream, Struct } from "effect";
import {
  type AiError,
  LanguageModel,
  Model,
  type Prompt,
  type Response,
  Tool,
} from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";

import {
  isCommentableLine,
  makeReviewer,
  ReviewChange,
  ReviewContextError,
  ReviewCostSnapshot,
  ReviewFileList,
  ReviewFinding,
  type ReviewOutcome,
  ReviewRepository,
  ReviewRequest,
  ReviewSource,
  ReviewUsage,
  type ReviewVerificationError,
} from "../src/index.ts";

const patch = `@@ -1,3 +1,4 @@
 unchanged
-old
+new
 tail
+added`;

const request = ReviewRequest.make({
  title: "Change",
  description: "",
  baseRevision: "base",
  headRevision: "head",
  changes: [ReviewChange.make({ path: "src/index.ts", patch })],
  unreviewedPaths: [],
});

const usage = {
  inputTokens: { total: 10, uncached: 7, cacheRead: 2, cacheWrite: 1 },
  outputTokens: { total: 4 },
};

const response = (
  value: object,
  responseUsage: typeof usage = usage,
): Stream.Stream<Response.StreamPartEncoded> =>
  Stream.fromIterable([
    { type: "tool-call", id: "review", name: "submit_review", params: value },
    { type: "finish", reason: "tool-calls", usage: responseUsage },
  ]);

const scriptedModel = (
  respond: (
    prompt: Prompt.Prompt,
    tools: ReadonlyArray<Tool.Any>,
    toolChoice: LanguageModel.ToolChoice<string>,
  ) => Stream.Stream<Response.StreamPartEncoded, AiError.AiError>,
) =>
  Model.make(
    "scripted",
    "review",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt, tools, toolChoice }) => respond(prompt, tools, toolChoice),
      }),
    ),
  );

const emptyRepository = ReviewRepository.of({
  readFile: () => Effect.fail(ReviewContextError.make({ message: "Source unavailable" })),
  findFiles: () => Effect.succeed(ReviewFileList.make({ paths: [], truncated: false })),
});

const blocker = ReviewFinding.make({
  path: "src/index.ts",
  line: 2,
  severity: "blocking",
  category: "reliability",
  title: "Dropped acknowledgment",
  body: "A committed operation loses its acknowledgment; preserve the result until acknowledgment completes.",
});

const otherBlocker = ReviewFinding.make({
  ...blocker,
  category: "security",
  title: "Ownership check bypassed",
  body: "The cached return bypasses the owner check; check ownership before returning the record.",
});

const importantFinding = ReviewFinding.make({
  ...otherBlocker,
  severity: "important",
  title: "Nonblocking fallback error",
  body: "The fallback reports the wrong optional status; return the status produced by the supported fallback.",
});

const nitFinding = ReviewFinding.make({
  ...otherBlocker,
  severity: "nit",
  title: "Minor diagnostic mismatch",
  body: "The diagnostic names the wrong optional phase; use the phase that produced the message.",
});

const submittedFinding = (
  finding: ReviewFinding,
  priority: 0 | 1 | 2 | 3,
): Omit<ReviewFinding, "severity"> & { readonly priority: 0 | 1 | 2 | 3 } => ({
  ...Struct.omit(finding, ["severity"]),
  priority,
});

const sourceResults = (prompt: Prompt.Prompt) =>
  prompt.content
    .filter((message) => message.role === "tool")
    .flatMap((message) => message.content)
    .flatMap((part) => (part.type === "tool-result" && part.name === "read_file" ? [part] : []));

const reviewInput = (prompt: Prompt.Prompt): string =>
  prompt.content
    .flatMap((message) =>
      message.role === "user" && typeof message.content !== "string"
        ? message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
        : [],
    )
    .at(0) ?? "";

describe("review output boundary", () => {
  it.effect("preserves findings when the model explicitly reports unfinished coverage", () =>
    Effect.gen(function* () {
      const model = scriptedModel(() =>
        response({
          findings: [submittedFinding(blocker, 1)],
          incomplete: true,
        }),
      );
      const outcome = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));
      expect(outcome.report.findings).toEqual([blocker]);
      expect(outcome.incomplete).toBe(true);
      expect(outcome.exhausted).toBeUndefined();
      expect(outcome.report.summary).toContain("remaining change has not been verified");
    }),
  );

  it.effect("PRR-002 completes one native review and keeps independent same-line blockers", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const model = scriptedModel((prompt, tools, toolChoice) => {
        expect(toolChoice).toBe("required");
        expect(tools.map((tool) => tool.name)).toEqual([
          "read_file",
          "find_files",
          "record_finding",
          "submit_review",
        ]);
        const text = reviewInput(prompt);
        expect(text).toContain(patch);
        expect(text.split(patch)).toHaveLength(2);
        expect(text).not.toContain("__new hunk__");
        expect(text).not.toContain("__old hunk__");

        const completion = tools.find((tool) => tool.name === "submit_review");
        expect(completion).toBeDefined();
        if (completion !== undefined) {
          const schema = JSON.stringify(
            Tool.getJsonSchema(completion, { transformer: toCodecOpenAI }),
          );
          expect(schema).toContain('"findings"');
          expect(schema).toContain('"maxItems":24');
          expect(schema).toContain('"priority"');
          expect(schema).not.toContain('"severity"');
          expect(schema.indexOf('"body"')).toBeLessThan(schema.indexOf('"priority"'));
        }
        return Stream.unwrap(
          Ref.update(calls, (count) => count + 1).pipe(
            Effect.as(
              response({
                findings: [
                  submittedFinding(blocker, 0),
                  submittedFinding(otherBlocker, 1),
                  submittedFinding(importantFinding, 2),
                  submittedFinding(nitFinding, 3),
                ],
              }),
            ),
          ),
        );
      });

      const review = makeReviewer({
        model,
        guidance: "Preserve acknowledgments.",
        estimateCostMicrousd: () => Effect.succeed(123),
      }).review(request);
      expectTypeOf<Effect.Services<typeof review>>().toEqualTypeOf<ReviewRepository>();
      expectTypeOf<Effect.Error<typeof review>>().not.toBeAny();
      expectTypeOf<
        Extract<Effect.Error<typeof review>, AiError.AiError | ReviewVerificationError>
      >().toEqualTypeOf<AiError.AiError | ReviewVerificationError>();
      const outcome = yield* review.pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(yield* Ref.get(calls)).toBe(1);
      expect(outcome.report.findings).toEqual([
        blocker,
        otherBlocker,
        importantFinding,
        nitFinding,
      ]);
      expect(outcome).toMatchObject({
        turns: 1,
        usage: {
          inputTokens: 10,
          uncachedInputTokens: 7,
          cachedInputTokens: 2,
          cacheWriteInputTokens: 1,
          outputTokens: 4,
          estimatedCostMicrousd: 123,
        },
      });
      expect(outcome.exhausted).toBeUndefined();
    }),
  );

  it.effect(
    "PRR-002 reads immutable base and head source and recovers from a bounded failure",
    () =>
      Effect.gen(function* () {
        const reads = yield* Ref.make<
          ReadonlyArray<Parameters<typeof emptyRepository.readFile>[0]>
        >([]);
        const failedInputs = [
          {
            path: "src/missing.ts",
            revision: "base",
            startLine: 1,
            lineCount: 4,
          },
          {
            path: "src/index.ts",
            revision: "base",
            startLine: 99,
            lineCount: 4,
          },
          {
            path: "src/index.ts",
            revision: "head",
            startLine: 99,
            lineCount: 4,
          },
        ] as const;
        const base = {
          path: "src/index.ts",
          revision: "base",
          startLine: 1,
          lineCount: 4,
        } as const;
        const head = { ...base, revision: "head" } as const;
        const recoveryUsage = {
          inputTokens: { total: 40_000, uncached: 40_000, cacheRead: 0, cacheWrite: 0 },
          outputTokens: { total: 4 },
        };
        const model = scriptedModel((prompt) => {
          const results = sourceResults(prompt);
          if (results.length < failedInputs.length) {
            if (results.length > 0) {
              expect(results.at(-1)).toMatchObject({
                isFailure: true,
                result: { _tag: "ReviewContextError", message: "Source range unavailable" },
              });
            }
            return Stream.fromIterable([
              {
                type: "tool-call",
                id: `failed-${String(results.length)}`,
                name: "read_file",
                params: failedInputs[results.length] ?? base,
              },
              { type: "finish", reason: "tool-calls", usage: recoveryUsage },
            ]);
          }
          if (results.length === failedInputs.length) {
            expect(results.map((result) => result.isFailure)).toEqual([true, true, true]);
            return Stream.fromIterable([
              { type: "tool-call", id: "base", name: "read_file", params: base },
              { type: "finish", reason: "tool-calls", usage: recoveryUsage },
            ]);
          }
          if (results.length === failedInputs.length + 1) {
            expect(results.at(-1)).toMatchObject({
              isFailure: false,
              result: { revision: "base", content: "old" },
            });
            return Stream.fromIterable([
              { type: "tool-call", id: "head", name: "read_file", params: head },
              { type: "finish", reason: "tool-calls", usage: recoveryUsage },
            ]);
          }
          expect(results.at(-1)).toMatchObject({
            isFailure: false,
            result: { revision: "head", content: "new" },
          });
          return response({ findings: [] }, recoveryUsage);
        });
        const repository = ReviewRepository.of({
          ...emptyRepository,
          readFile: (input) =>
            Effect.gen(function* () {
              yield* Ref.update(reads, (current) => [...current, input]);
              if (input.path !== "src/index.ts" || input.startLine !== 1) {
                return yield* ReviewContextError.make({ message: "Source range unavailable" });
              }
              return ReviewSource.make({
                path: input.path,
                revision: input.revision,
                startLine: input.startLine,
                totalLines: 1,
                content: input.revision === "base" ? "old" : "new",
              });
            }),
        });

        const outcome = yield* makeReviewer({ model })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, repository));

        expect(yield* Ref.get(reads)).toEqual([...failedInputs, base, head]);
        expect(outcome).toMatchObject({
          turns: 6,
          report: { findings: [] },
          usage: {
            inputTokens: 240_000,
            uncachedInputTokens: 240_000,
            cachedInputTokens: 0,
            cacheWriteInputTokens: 0,
            outputTokens: 24,
          },
        });
      }),
  );

  it.effect(
    "reserves a completion turn and preserves findings and usage when research runs out",
    () =>
      Effect.gen(function* () {
        const reads = yield* Ref.make(0);
        const calls = yield* Ref.make(0);
        const model = scriptedModel((prompt, _tools, toolChoice) =>
          Stream.unwrap(
            Ref.updateAndGet(calls, (count) => count + 1).pipe(
              Effect.map((call) => {
                expect(JSON.stringify(prompt.content)).toContain("<run-status>");
                if (call <= 2) {
                  expect(toolChoice).toBe("required");
                  return Stream.fromIterable([
                    {
                      type: "tool-call",
                      id: `read-${String(call)}`,
                      name: "read_file",
                      params: {
                        path: "src/index.ts",
                        revision: "head",
                        startLine: 1,
                        lineCount: 1,
                      },
                    },
                    {
                      type: "finish",
                      reason: "tool-calls",
                      usage: {
                        inputTokens: {
                          total: 90_000,
                          uncached: 90_000,
                          cacheRead: 0,
                          cacheWrite: 0,
                        },
                        outputTokens: { total: 20_000 },
                      },
                    },
                  ]);
                }
                expect(call).toBe(3);
                expect(toolChoice).toEqual({ tool: "submit_review" });
                return response(
                  { findings: [submittedFinding(blocker, 1)] },
                  {
                    inputTokens: {
                      total: 90_000,
                      uncached: 10_000,
                      cacheRead: 80_000,
                      cacheWrite: 0,
                    },
                    outputTokens: { total: 1_000 },
                  },
                );
              }),
            ),
          ),
        );
        const outcome = yield* makeReviewer({
          model,
          estimateCostMicrousd: () => Effect.succeed(123),
        })
          .review(request)
          .pipe(
            Effect.provideService(ReviewRepository, {
              ...emptyRepository,
              readFile: () =>
                Ref.update(reads, (count) => count + 1).pipe(
                  Effect.as(
                    ReviewSource.make({
                      path: "src/index.ts",
                      revision: "head",
                      startLine: 1,
                      totalLines: 1,
                      content: "new",
                    }),
                  ),
                ),
            }),
          );
        expect(yield* Ref.get(reads)).toBe(2);
        expect(yield* Ref.get(calls)).toBe(3);
        expect(outcome).toMatchObject({
          turns: 3,
          exhausted: "tokens",
          report: { findings: [blocker] },
          usage: {
            inputTokens: 270_000,
            uncachedInputTokens: 190_000,
            cachedInputTokens: 80_000,
            outputTokens: 41_000,
            estimatedCostMicrousd: 369,
          },
        });
        expect(outcome.report.summary).toContain("remaining change has not been verified");
      }),
  );

  it.effect("PRR-002 rejects a finding that does not name a causative changed path", () =>
    Effect.gen(function* () {
      const model = scriptedModel(() =>
        response({
          findings: [
            submittedFinding(ReviewFinding.make({ ...blocker, path: "src/unchanged.ts" }), 0),
          ],
        }),
      );
      const result = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.result);
      expect(Result.isFailure(result) && result.failure._tag).toBe("ReviewVerificationError");
    }),
  );

  it.effect("PRR-002 demotes invalid anchors and removes only exact duplicates", () =>
    Effect.gen(function* () {
      const topLevel = ReviewFinding.make(Struct.omit(otherBlocker, ["line"]));
      const invalid = ReviewFinding.make({ ...otherBlocker, line: 999 });
      const model = scriptedModel(() =>
        response({
          findings: [
            submittedFinding(blocker, 0),
            submittedFinding(invalid, 1),
            submittedFinding(topLevel, 1),
            submittedFinding(invalid, 1),
            submittedFinding(blocker, 0),
          ],
        }),
      );
      const outcome = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));
      expect(outcome.report.findings).toEqual([blocker, topLevel]);
    }),
  );

  it.effect.each([24, 25])("PRR-002 enforces the native finding bound: %s", (count) =>
    Effect.gen(function* () {
      const findings = Array.from({ length: count }, (_, index) =>
        ReviewFinding.make({
          ...Struct.omit(blocker, ["line"]),
          title: `Independent cause ${String(index)}`,
        }),
      );
      const result = yield* makeReviewer({
        model: scriptedModel(() =>
          response({ findings: findings.map((finding) => submittedFinding(finding, 1)) }),
        ),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.result);
      if (count === 24) {
        expect(Result.isSuccess(result) && result.success.report.findings).toEqual(findings);
      } else {
        expect(Result.isFailure(result) && result.failure._tag).toBe("AiError");
      }
    }),
  );

  it.effect("PRR-002 rejects malformed native completion without retrying", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const model = scriptedModel(() =>
        Stream.unwrap(
          Ref.update(calls, (count) => count + 1).pipe(Effect.as(response({ summary: "safe" }))),
        ),
      );
      const result = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.result);
      expect(Result.isFailure(result) && result.failure._tag).toBe("AiError");
      expect(yield* Ref.get(calls)).toBe(1);
    }),
  );

  it.effect("preserves recorded findings when a later completion fails validation", () =>
    Effect.gen(function* () {
      let calls = 0;
      const model = scriptedModel(() => {
        calls += 1;
        return calls === 1
          ? Stream.fromIterable([
              {
                type: "tool-call",
                id: "saved",
                name: "record_finding",
                params: submittedFinding(blocker, 1),
              },
              { type: "finish", reason: "tool-calls", usage },
            ])
          : response({
              findings: [
                submittedFinding(
                  ReviewFinding.make({ ...otherBlocker, path: "src/unchanged.ts" }),
                  1,
                ),
              ],
            });
      });
      const outcome = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));
      expect(calls).toBe(2);
      expect(outcome.incomplete).toBe(true);
      expect(outcome.exhausted).toBeUndefined();
      expect(outcome.report.findings).toEqual([blocker]);
      expect(outcome.report.summary).toContain("remaining change has not been verified");
    }),
  );

  it.effect("PRR-002 retains headers, mode metadata, and complete deletion hunks", () =>
    Effect.gen(function* () {
      const deletionPatch = `diff --git a/src/deleted.ts b/src/deleted.ts
old mode 100644
new mode 100755
--- a/src/deleted.ts
+++ b/src/deleted.ts
@@ -5,2 +5,1 @@
 keep
-removed`;
      const modeOnlyPatch = `diff --git a/tool.sh b/tool.sh
old mode 100644
new mode 100755`;
      const formattedRequest = ReviewRequest.make({
        ...request,
        changes: [
          ReviewChange.make({ path: "src/deleted.ts", patch: deletionPatch }),
          ReviewChange.make({ path: "tool.sh", patch: modeOnlyPatch }),
        ],
      });
      const model = scriptedModel((prompt) => {
        const text = reviewInput(prompt);
        expect(text).toContain("old mode 100644");
        expect(text).toContain("new mode 100755");
        expect(text).toContain(deletionPatch);
        expect(text).toContain(modeOnlyPatch);
        return response({ findings: [] });
      });
      yield* makeReviewer({ model })
        .review(formattedRequest)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));
    }),
  );

  it.effect("PRR-002 preserves large literal patches without duplicating context", () =>
    Effect.gen(function* () {
      const manyContextLines = Array.from({ length: 7_000 }, () => " context").join("\n");
      const rawPatch = `@@ -1,7001 +1,7001 @@\n${manyContextLines}\n-old\n+new`;
      expect(rawPatch.length).toBeLessThan(80_000);
      const largeRequest = ReviewRequest.make({
        ...request,
        changes: [ReviewChange.make({ path: "src/index.ts", patch: rawPatch })],
      });
      const model = scriptedModel((prompt) => {
        const text = reviewInput(prompt);
        expect(text).not.toContain("__new hunk__");
        expect(text).toContain(rawPatch);
        expect(text.split(rawPatch)).toHaveLength(2);
        return response({ findings: [] });
      });
      yield* makeReviewer({ model })
        .review(largeRequest)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));
    }),
  );

  it.effect("PRR-002 keeps incremental all-clear wording conservative", () =>
    Effect.gen(function* () {
      const outcome = yield* makeReviewer({
        model: scriptedModel(() => response({ findings: [] })),
      })
        .review(ReviewRequest.make({ ...request, scope: "incremental" }))
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));
      expect(outcome.report.summary).toContain("does not resolve earlier findings");
      expect(outcome.report.summary).not.toContain("safe to merge");
    }),
  );

  it.effect("PRR-002 closes the single run on interruption", () =>
    Effect.gen(function* () {
      const finalized = yield* Ref.make(false);
      const started = yield* Deferred.make<void>();
      const model = scriptedModel(() =>
        Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
          Stream.flatMap(() => Stream.never),
          Stream.ensuring(Ref.set(finalized, true)),
        ),
      );
      const fiber = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.forkChild);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
      expect(yield* Ref.get(finalized)).toBe(true);
    }),
  );

  it.effect("PRR-003 shares exact source range bounds", () =>
    Effect.gen(function* () {
      const input = {
        path: "src/index.ts",
        revision: "head",
        startLine: 1,
        lineCount: 200,
      } as const;
      const source = yield* ReviewSource.fromText(input, "first\nlast\n");
      expect(source).toMatchObject({ totalLines: 2, content: "first\nlast" });
      const empty = yield* ReviewSource.fromText(input, "");
      expect(empty).toMatchObject({ totalLines: 0, content: "" });
      const finalBlank = yield* ReviewSource.fromText({ ...input, startLine: 2 }, "first\n\n");
      expect(finalBlank).toMatchObject({ totalLines: 2, startLine: 2, content: "" });
      for (const read of [
        ReviewSource.fromText({ ...input, startLine: 3 }, "first\nlast\n"),
        ReviewSource.fromText({ ...input, lineCount: 201 }, "first\nlast\n"),
        ReviewSource.fromText(input, "x".repeat(20_001)),
      ]) {
        const result = yield* Effect.result(read);
        expect(Result.isFailure(result) && result.failure._tag).toBe("ReviewContextError");
      }
    }),
  );

  it("PRR-004 accepts only RIGHT-side patch lines", () => {
    expect([0, 1, 2, 3, 4, 5].filter((line) => isCommentableLine(patch, line))).toEqual([
      1, 2, 3, 4,
    ]);
  });
});

type Equal<Left, Right> =
  (<T>() => T extends Left ? 1 : 2) extends <T>() => T extends Right ? 1 : 2 ? true : false;
type Assert<T extends true> = T;
type EffectError<Value> =
  Value extends Effect.Effect<unknown, infer Error, unknown> ? Error : never;
type EffectRequirements<Value> =
  Value extends Effect.Effect<unknown, unknown, infer Requirements> ? Requirements : never;

declare const typedModel: Model.Model<"typed", LanguageModel.LanguageModel, never>;
const typedReviews = (model: typeof typedModel) => ({
  unpriced: makeReviewer({ model }).review(request),
  priced: makeReviewer({
    model,
    estimateCostMicrousd: () => Effect.succeed(1),
  }).review(request),
  controlled: makeReviewer({
    model,
    costControl: {
      snapshot: Effect.succeed(
        ReviewCostSnapshot.make({
          stopped: false,
          modelCalls: 0,
          usage: ReviewUsage.make({
            inputTokens: 0,
            uncachedInputTokens: 0,
            cachedInputTokens: 0,
            cacheWriteInputTokens: 0,
            outputTokens: 0,
          }),
        }),
      ),
    },
  }).review(request),
});
type TypedReviews = ReturnType<typeof typedReviews>;
const pricingTypeProofs: readonly [
  Assert<Equal<EffectError<TypedReviews["controlled"]>, EffectError<TypedReviews["unpriced"]>>>,
  Assert<Equal<EffectRequirements<TypedReviews["controlled"]>, ReviewRepository>>,
  Assert<Equal<EffectError<TypedReviews["priced"]>, EffectError<TypedReviews["unpriced"]>>>,
  Assert<
    Equal<EffectRequirements<TypedReviews["priced"]>, EffectRequirements<TypedReviews["unpriced"]>>
  >,
  Assert<Equal<EffectRequirements<TypedReviews["priced"]>, ReviewRepository>>,
  Assert<Equal<Effect.Success<TypedReviews["priced"]>, ReviewOutcome>>,
  Assert<
    Equal<
      Extract<EffectError<TypedReviews["priced"]>, ReviewVerificationError>,
      ReviewVerificationError
    >
  >,
] = [true, true, true, true, true, true, true];
void pricingTypeProofs;
