import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Ref, Stream } from "effect";
import { LanguageModel, Model } from "effect/unstable/ai";

import {
  commentableLines,
  ReviewChange,
  ReviewFinding,
  ReviewReport,
  ReviewRequest,
  makeReviewer,
  sanitizeReviewReport,
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

describe("review output boundary", () => {
  it.effect("PRR-002 performs exactly one model call", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const model = Model.make(
        "scripted",
        "single-pass",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.update(calls, (value) => value + 1).pipe(
                  Effect.as(
                    Stream.fromIterable([
                      { type: "text-start" as const, id: "review" },
                      {
                        type: "text-delta" as const,
                        id: "review",
                        delta:
                          '{"summary":"One defect found.","findings":[{"path":"src/index.ts","line":2,"severity":"important","category":"reliability","title":"Dropped acknowledgment","body":"The completed operation can lose its acknowledgment."}]}',
                      },
                      { type: "text-end" as const, id: "review" },
                      {
                        type: "finish" as const,
                        reason: "stop" as const,
                        usage: {
                          inputTokens: {
                            total: 10,
                            uncached: 7,
                            cacheRead: 2,
                            cacheWrite: 1,
                          },
                          outputTokens: { total: 4 },
                        },
                      },
                    ]),
                  ),
                ),
              ),
          }),
        ),
      );

      const outcome = yield* makeReviewer({
        model,
        estimateCostMicrousd: () => Effect.succeed(123),
      }).review(request);
      expect(yield* Ref.get(calls)).toBe(1);
      expect(outcome.turns).toBe(1);
      expect(outcome.usage).toMatchObject({
        inputTokens: 10,
        uncachedInputTokens: 7,
        cachedInputTokens: 2,
        cacheWriteInputTokens: 1,
        outputTokens: 4,
        estimatedCostMicrousd: 123,
      });
      expect(outcome.report.findings[0]?.category).toBe("reliability");
    }),
  );

  it("PRR-004 accepts only RIGHT-side patch lines", () => {
    expect([...commentableLines(patch)]).toEqual([1, 2, 3, 4]);
  });

  it("PRR-004 drops unknown paths, demotes invalid anchors, and deduplicates", () => {
    const report = sanitizeReviewReport(
      request,
      ReviewReport.make({
        summary: "Two defects.",
        findings: [
          ReviewFinding.make({
            path: "src/index.ts",
            line: 2,
            severity: "important",
            category: "correctness",
            title: "Broken branch",
            body: "This branch returns the wrong value.",
          }),
          ReviewFinding.make({
            path: "src/index.ts",
            line: 99,
            severity: "nit",
            category: "maintainability",
            title: "Invalid anchor",
            body: "Keep this finding, but not its line.",
          }),
          ReviewFinding.make({
            path: "src/index.ts",
            line: 2,
            severity: "important",
            category: "correctness",
            title: "Broken branch",
            body: "Duplicate.",
          }),
          ReviewFinding.make({
            path: "not-in-the-diff.ts",
            severity: "blocking",
            category: "security",
            title: "Invented path",
            body: "Drop this.",
          }),
        ],
      }),
    );

    expect(report.findings).toHaveLength(2);
    expect(report.findings[0]?.line).toBe(2);
    expect(report.findings[0]?.category).toBe("correctness");
    expect(report.findings[1]?.line).toBeUndefined();
  });
});

// Compile-time E/R proof: adding host pricing does not add an error or service requirement.
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
});
type TypedReviews = ReturnType<typeof typedReviews>;
const pricingTypeProofs: readonly [
  Assert<Equal<EffectError<TypedReviews["priced"]>, EffectError<TypedReviews["unpriced"]>>>,
  Assert<
    Equal<EffectRequirements<TypedReviews["priced"]>, EffectRequirements<TypedReviews["unpriced"]>>
  >,
] = [true, true];
void pricingTypeProofs;
