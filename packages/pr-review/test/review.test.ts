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
                        delta: '{"summary":"No defects found.","findings":[]}',
                      },
                      { type: "text-end" as const, id: "review" },
                      {
                        type: "finish" as const,
                        reason: "stop" as const,
                        usage: {
                          inputTokens: { total: 10 },
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

      const outcome = yield* makeReviewer({ model }).review(request);
      expect(yield* Ref.get(calls)).toBe(1);
      expect(outcome.turns).toBe(1);
      expect(outcome.usage).toMatchObject({ inputTokens: 10, outputTokens: 4 });
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
