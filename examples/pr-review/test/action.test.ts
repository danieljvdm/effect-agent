import {
  ReviewChange,
  ReviewFinding,
  ReviewOutcome,
  ReviewReport,
  ReviewUsage,
  type ReviewSeverity,
} from "@effect-agent/pr-review";
import { describe, expect, it } from "@effect/vitest";
import { Config, ConfigProvider, Deferred, Effect, Fiber } from "effect";
import { Response } from "effect/unstable/ai";

import {
  MAX_REVIEW_SHARDS,
  estimateGpt56CostMicrousd,
  mergeReviewOutcomes,
  prepareReviewSurface,
  runReviewWave,
  shardReviewChanges,
  withActionInputs,
} from "../src/action.ts";
import type { ChangedFile } from "../src/github.ts";

const file = (path: string, patch: string | undefined): ChangedFile => ({
  path,
  status: "modified",
  additions: 1,
  deletions: 0,
  patch,
});

describe("GitHub diff admission", () => {
  it("PRR-003 bounds admitted files and reports the remainder", () => {
    const files = Array.from({ length: 102 }, (_, index) =>
      file(`src/file-${String(index)}.ts`, "@@ -0,0 +1 @@\n+export const value = 1;"),
    );
    const surface = prepareReviewSurface(files, []);

    expect(surface.changes).toHaveLength(100);
    expect(surface.unreviewedPaths).toEqual(["src/file-100.ts", "src/file-101.ts"]);
  });

  it("PRR-003 distinguishes ignored and unavailable files", () => {
    const surface = prepareReviewSurface(
      [file("bun.lock", "@@ -1 +1 @@\n-old\n+new"), file("assets/image.png", undefined)],
      ["**/*.lock"],
    );

    expect(surface.changes).toHaveLength(0);
    expect(surface.ignoredPaths).toEqual(["bun.lock"]);
    expect(surface.unreviewedPaths).toEqual(["assets/image.png"]);
  });
});

describe("Action configuration", () => {
  it.effect("reads a GitHub Action input without mutating the environment", () =>
    Effect.gen(function* () {
      const provider = withActionInputs(
        ConfigProvider.fromEnv({ env: { "INPUT_AUTOMATIC-REVIEW-LIMIT": "7" } }),
      );
      expect(yield* Config.string("PR_REVIEW_AUTOMATIC_LIMIT").parse(provider)).toBe("7");
    }),
  );

  it.effect("preserves the raw manual review command for validation", () =>
    Effect.gen(function* () {
      const provider = withActionInputs(
        ConfigProvider.fromEnv({ env: { INPUT_COMMAND: "@effect-agent review\r\n" } }),
      );
      expect(yield* Config.string("PR_REVIEW_COMMAND").parse(provider)).toBe(
        "@effect-agent review\r\n",
      );
    }),
  );

  it.effect("falls back to an Action input when the environment value is empty", () =>
    Effect.gen(function* () {
      const provider = withActionInputs(
        ConfigProvider.fromEnv({
          env: { OPENAI_API_KEY: "", "INPUT_OPENAI-API-KEY": "action-key" },
        }),
      );
      expect(yield* Config.nonEmptyString("OPENAI_API_KEY").parse(provider)).toBe("action-key");
    }),
  );
});

describe("GPT-5.6 cost estimation", () => {
  it("prices uncached, cached, cache-write, and output tokens at current family rates", () => {
    const usage = new Response.Usage({
      inputTokens: { total: 10_000, uncached: 8_000, cacheRead: 2_000, cacheWrite: 1_000 },
      outputTokens: { total: 500, text: 400, reasoning: 100 },
    });

    expect(estimateGpt56CostMicrousd("gpt-5.6-sol", usage)).toBe(43_800);
    expect(estimateGpt56CostMicrousd("gpt-5.6-terra", usage)).toBe(22_900);
    expect(estimateGpt56CostMicrousd("gpt-5.6-luna", usage)).toBe(2_290);
    expect(estimateGpt56CostMicrousd("custom-model", usage)).toBeUndefined();
  });
});

describe("bounded review wave", () => {
  it("PRR-005 partitions a large diff into at most four balanced shards", () => {
    const changes = [
      ["a.ts", 75_000],
      ["b.ts", 65_000],
      ["c.ts", 55_000],
      ["d.ts", 45_000],
      ["e.ts", 35_000],
      ["f.ts", 25_000],
    ].map(([path, chars]) =>
      ReviewChange.make({ path: String(path), patch: "x".repeat(Number(chars)) }),
    );

    const shards = shardReviewChanges(changes);
    expect(shards).toHaveLength(MAX_REVIEW_SHARDS);
    expect(shards.map((shard) => shard.map((change) => change.path))).toEqual([
      ["a.ts"],
      ["b.ts"],
      ["c.ts", "f.ts"],
      ["d.ts", "e.ts"],
    ]);
    expect(
      shardReviewChanges([
        ReviewChange.make({ path: "small-a.ts", patch: "x".repeat(10_000) }),
        ReviewChange.make({ path: "small-b.ts", patch: "x".repeat(10_000) }),
      ]),
    ).toHaveLength(1);
  });

  it.effect("PRR-005 starts independent shards concurrently", () =>
    Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const review = (started: Deferred.Deferred<void>, label: string) =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(label),
        );
      const fiber = yield* Effect.forkChild(
        runReviewWave([review(firstStarted, "first"), review(secondStarted, "second")]),
      );

      yield* Effect.all([Deferred.await(firstStarted), Deferred.await(secondStarted)], {
        concurrency: "unbounded",
      });
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(fiber)).toEqual(["first", "second"]);
    }),
  );

  it("PRR-005 merges usage and keeps the twelve highest-severity findings", () => {
    const outcome = (index: number, severity: ReviewSeverity): ReviewOutcome =>
      ReviewOutcome.make({
        turns: 1,
        usage: ReviewUsage.make({
          inputTokens: 100,
          uncachedInputTokens: 70,
          cachedInputTokens: 20,
          cacheWriteInputTokens: 10,
          outputTokens: 10,
          estimatedCostMicrousd: 25,
        }),
        report: ReviewReport.make({
          summary: `Summary ${String(index)}`,
          findings: Array.from({ length: 4 }, (_, findingIndex) =>
            ReviewFinding.make({
              path: `src/${String(index)}-${String(findingIndex)}.ts`,
              severity,
              category: "correctness",
              title: `Finding ${String(index)}-${String(findingIndex)}`,
              body: "Actionable defect.",
            }),
          ),
        }),
      });
    const merged = mergeReviewOutcomes([
      outcome(1, "nit"),
      outcome(2, "important"),
      outcome(3, "blocking"),
      outcome(4, "nit"),
    ]);

    expect(merged.inputTokens).toBe(400);
    expect(merged.uncachedInputTokens).toBe(280);
    expect(merged.cachedInputTokens).toBe(80);
    expect(merged.cacheWriteInputTokens).toBe(40);
    expect(merged.outputTokens).toBe(40);
    expect(merged.estimatedCostMicrousd).toBe(100);
    expect(merged.report.summary).toContain("Shard 4");
    expect(merged.report.findings).toHaveLength(12);
    expect(merged.report.findings.slice(0, 4).map((finding) => finding.severity)).toEqual([
      "blocking",
      "blocking",
      "blocking",
      "blocking",
    ]);
  });
});
