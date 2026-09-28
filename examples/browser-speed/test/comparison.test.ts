import { assert, it } from "@effect/vitest";
import { Effect } from "effect";

import type { RunInput } from "../src/contract.ts";
import { cohort, comparisons } from "../src/state.ts";
import { makeTrace } from "../src/telemetry.ts";

it.effect(
  "separates model and selector cohorts, counts failures, and excludes different workloads",
  () =>
    Effect.gen(function* () {
      const input: RunInput = {
        id: crypto.randomUUID(),
        scenario: "create",
        mode: "batched",
        prompt: "",
        screenshots: false,
        liveView: false,
      };

      const trace = yield* makeTrace(input, "gpt-6-luna");

      trace.update({ status: "passed", readyAt: 5, verifiedAt: 15 });
      const report = trace.snapshot();

      const { timing: _timing, readyAt: _ready, finishedAt: _finished, ...legacy } = report;

      const reports = [
        report,
        { ...report, verifiedAt: 35 },
        { ...report, status: "failed" as const, verifiedAt: null },
        { ...report, input: { ...input, grounding: "jev" as const }, verifiedAt: 65 },
        { ...report, model: "gpt-6-sol", verifiedAt: 25 },
        { ...legacy, verifiedAt: 1 },
        { ...report, readyAt: null, verifiedAt: null, status: "failed" as const },
        { ...report, input: { ...input, scenario: "custom" as const }, verifiedAt: 2 },
      ];

      const samples = reports.map((report) => ({ report, clientElapsedMillis: 100 }));

      assert.strictEqual(cohort(samples, report).length, 4);
      assert.deepStrictEqual(
        comparisons(samples, report).map(
          ({ model, grounding, count, started, preparationFailed, passed, median }) => ({
            model,
            grounding,
            count,
            started,
            preparationFailed,
            passed,
            median,
          }),
        ),
        [
          {
            model: "gpt-6-luna",
            grounding: "direct",
            count: 4,
            started: 3,
            preparationFailed: 1,
            passed: 2,
            median: 20,
          },
          {
            model: "gpt-6-luna",
            grounding: "jev",
            count: 1,
            started: 1,
            preparationFailed: 0,
            passed: 1,
            median: 60,
          },
          {
            model: "gpt-6-sol",
            grounding: "direct",
            count: 1,
            started: 1,
            preparationFailed: 0,
            passed: 1,
            median: 20,
          },
        ],
      );

      const fast = {
        ...report,
        input: { ...input, reasoning: "none" as const, serviceTier: "fast" as const },
        spans: [
          {
            id: 0,
            phase: "model" as const,
            name: "chat gpt-6-luna",
            start: 0,
            duration: 5,
            outcome: "success" as const,
            turn: 1,
            serviceTier: "fast",
          },
        ],
      };

      const settingsSamples = [
        fast,
        { ...fast, input: { ...fast.input, reasoning: "max" as const } },
        { ...fast, input: { ...fast.input, serviceTier: "default" as const } },
        { ...fast, spans: fast.spans.map((span) => ({ ...span, serviceTier: "default" })) },
        // 894818e5 split a pre-response failure out of its requested Fast cohort.
        { ...fast, status: "failed" as const, verifiedAt: null, spans: [] },
        report,
      ].map((report) => ({ report, clientElapsedMillis: 100 }));

      assert.strictEqual(
        cohort(settingsSamples, fast).length,
        3,
        "Requested settings own the denominator, including missing or different served tiers",
      );
      const settingsGroups = comparisons(settingsSamples, fast);

      assert.strictEqual(settingsGroups.length, 4);
      assert.deepStrictEqual(
        settingsGroups.map(({ started, passed, median, servedTier }) => ({
          started,
          passed,
          median,
          servedTier,
        }))[0],
        { started: 3, passed: 2, median: 10, servedTier: "default+fast+unknown" },
      );

      const engines = [
        fast,
        { ...fast, input: { ...fast.input, engine: "chromium" as const } },
        { ...fast, input: { ...fast.input, engine: "kitesurf" as const }, verifiedAt: 45 },
        { ...fast, browserVersion: "Chrome/1" },
        { ...fast, browserVersion: "Chrome/2" },
        { ...fast, browserVersion: "Chrome/1", browserRevision: "different-revision" },
        { ...fast, commandTimeoutMillis: 15_000 },
      ].map((report) => ({ report, clientElapsedMillis: 100 }));

      assert.strictEqual(cohort(engines, fast).length, 2);
      assert.deepStrictEqual(
        comparisons(engines, fast).map(({ engine, count, median }) => ({ engine, count, median })),
        [
          { engine: "chromium", count: 2, median: 10 },
          { engine: "kitesurf", count: 1, median: 40 },
          { engine: "chromium", count: 1, median: 10 },
          { engine: "chromium", count: 1, median: 10 },
          { engine: "chromium", count: 1, median: 10 },
          { engine: "chromium", count: 1, median: 10 },
        ],
      );
    }),
);
