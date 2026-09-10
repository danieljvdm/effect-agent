import {
  ModelCallUsage,
  RunTotals,
  emptyRunTotals,
  unknownRunTotals,
  sumRunTotals,
  runTotalsFromSummary,
  RunUsageSummary,
  summarizeModelUsage,
  UsageAggregationError,
} from "@effect-agent/core/Usage";
import { Effect, Schema } from "effect";
import { expect, expectTypeOf, it } from "vite-plus/test";

const legacy = {
  provider: "test",
  model: "binding-alias",
  serviceTier: "priority",
  pricingVersion: "v1",
  inputTokens: { total: 3, uncached: 1, cacheRead: 1, cacheWrite: 1 },
  outputTokens: { total: 2, text: 1, reasoning: 1 },
  costMicrousd: 5,
};

it("decodes legacy usage conservatively and separates actual models sharing a binding", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const old = Schema.decodeUnknownSync(ModelCallUsage)(legacy);

      expect(old.response).toBeUndefined();
      const oldSummary = yield* summarizeModelUsage([old]);

      expect(oldSummary.pricingStatus).toBe("unknown");

      const current = ["actual-a", "actual-b"].map((model) =>
        ModelCallUsage.make({
          ...legacy,
          response: { model },
          purpose: "turn",
          usageStatus: "complete",
          pricingStatus: "estimated",
        }),
      );

      const summary = yield* summarizeModelUsage(current);

      expect(summary.modelCalls).toBe(2);
      expect(summary.costMicrousd).toBe(10);
      expect(summary.byModel.map((group) => group.responseModel)).toEqual(["actual-a", "actual-b"]);
      expect(
        Schema.decodeUnknownExit(RunUsageSummary)(Schema.encodeSync(RunUsageSummary)(summary))._tag,
      ).toBe("Success");
    }),
  ));

it("resumes usage aggregation at every split without retaining calls or mutating the seed", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const complete = (model: string) =>
        ModelCallUsage.make({
          ...legacy,
          response: { model },
          usageStatus: "complete",
          pricingStatus: "estimated",
        });

      const calls = [
        complete("actual-a"),
        complete("actual-b"),
        ModelCallUsage.make(legacy),
        complete("actual-a"),
        ModelCallUsage.make({ ...legacy, serviceTier: "flex", usageStatus: "partial" }),
      ];

      const full = yield* summarizeModelUsage(calls);

      expect(full).toMatchObject({
        modelCalls: 5,
        inputTokens: { total: 15, uncached: 5, cacheRead: 5, cacheWrite: 5 },
        outputTokens: { total: 10, text: 5, reasoning: 5 },
        costMicrousd: 25,
        usageStatus: "partial",
        pricingStatus: "partial",
      });
      expect(full.byModel.map((group) => group.modelCalls)).toEqual([2, 1, 1, 1]);

      for (let split = 0; split <= calls.length; split += 1) {
        const seed = yield* summarizeModelUsage(calls.slice(0, split));
        const encoded = Schema.encodeSync(RunUsageSummary)(seed);

        expectTypeOf(summarizeModelUsage([], seed)).toEqualTypeOf<
          Effect.Effect<RunUsageSummary, UsageAggregationError>
        >();
        expect(yield* summarizeModelUsage(calls.slice(split), seed)).toEqual(full);
        expect(Schema.encodeSync(RunUsageSummary)(seed)).toEqual(encoded);
      }

      let incremental = yield* summarizeModelUsage([]);

      for (const call of calls) incremental = yield* summarizeModelUsage([call], incremental);
      expect(incremental).toEqual(full);
    }),
  ));

it("combines completeness conservatively while treating empty summaries as neutral", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const unknown = ModelCallUsage.make(legacy);

      const complete = ModelCallUsage.make({
        ...legacy,
        usageStatus: "complete",
        pricingStatus: "estimated",
      });

      const cases = [
        { prefix: [], suffix: [complete], expected: "complete" },
        { prefix: [complete], suffix: [], expected: "complete" },
        { prefix: [unknown], suffix: [unknown], expected: "unknown" },
        { prefix: [complete], suffix: [complete], expected: "complete" },
        { prefix: [unknown], suffix: [complete], expected: "partial" },
        { prefix: [complete], suffix: [unknown], expected: "partial" },
        { prefix: [unknown, complete], suffix: [complete], expected: "partial" },
      ];

      for (const { prefix, suffix, expected } of cases) {
        const seed = yield* summarizeModelUsage(prefix);
        const summary = yield* summarizeModelUsage(suffix, seed);

        expect(summary.usageStatus).toBe(expected);
        expect(summary.pricingStatus).toBe(expected);
      }
      const empty = yield* summarizeModelUsage([]);

      expect(yield* summarizeModelUsage([], empty)).toEqual(empty);

      const seed = RunUsageSummary.make({
        ...(yield* summarizeModelUsage([unknown])),
        unobservedModelCalls: 2,
      });

      expect((yield* summarizeModelUsage([complete], seed)).unobservedModelCalls).toBe(2);
    }),
  ));

it("rejects invalid seeds and seeded usage overflow through the typed error channel", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const call = (inputTokens: number, costMicrousd: number) =>
        ModelCallUsage.make({
          ...legacy,
          inputTokens: { total: inputTokens, uncached: inputTokens, cacheRead: 0, cacheWrite: 0 },
          outputTokens: { total: 0, text: 0, reasoning: 0 },
          costMicrousd,
        });

      const valid = yield* summarizeModelUsage([call(1, 1)]);
      const invalid = { ...valid, modelCalls: 2 };
      const invalidResult = yield* summarizeModelUsage([], invalid).pipe(Effect.flip);

      expect(invalidResult).toBeInstanceOf(UsageAggregationError);
      expect(invalidResult.field).toBe("seed");

      const cases = [
        { first: call(Number.MAX_SAFE_INTEGER, 0), next: call(1, 0), field: "inputTokens.total" },
        { first: call(0, Number.MAX_SAFE_INTEGER), next: call(0, 1), field: "costMicrousd" },
      ];

      for (const { first, next, field } of cases) {
        const seed = yield* summarizeModelUsage([first]);
        const error = yield* summarizeModelUsage([next], seed).pipe(Effect.flip);

        expect(error).toBeInstanceOf(UsageAggregationError);
        expect(error.field).toBe(field);
      }
      const zero = call(0, 0);
      const group = (yield* summarizeModelUsage([zero])).byModel[0];

      if (group === undefined) return yield* Effect.die("Expected one usage group");

      const callLimit = RunUsageSummary.make({
        modelCalls: Number.MAX_SAFE_INTEGER,
        inputTokens: zero.inputTokens,
        outputTokens: zero.outputTokens,
        costMicrousd: 0,
        byModel: [{ ...group, modelCalls: Number.MAX_SAFE_INTEGER }],
      });

      expect((yield* summarizeModelUsage([zero], callLimit).pipe(Effect.flip)).field).toBe(
        "modelCalls",
      );
    }),
  ));

it("combines disjoint Run totals without converting unknown or legacy evidence into free work", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const known = RunTotals.make({
        ...emptyRunTotals(),
        modelCalls: 2,
        inputTokens: 12,
        outputTokens: 3,
        costMicrousd: 17,
      });

      const legacy = Schema.decodeUnknownSync(RunTotals)({
        modelCalls: 1,
        inputTokens: 0,
        outputTokens: 0,
        costMicrousd: 0,
      });

      expect(yield* sumRunTotals([emptyRunTotals(), known])).toEqual(known);
      expect(yield* sumRunTotals([legacy])).toMatchObject({
        usageStatus: "unknown",
        pricingStatus: "unknown",
      });
      const combined = yield* sumRunTotals([known, unknownRunTotals(), legacy]);

      expect(combined).toMatchObject({
        modelCalls: 3,
        inputTokens: 12,
        outputTokens: 3,
        costMicrousd: 17,
        usageStatus: "partial",
        pricingStatus: "partial",
      });
      expect(yield* sumRunTotals([legacy, unknownRunTotals(), known])).toEqual(combined);
      expect(Schema.decodeUnknownSync(RunTotals)(Schema.encodeSync(RunTotals)(combined))).toEqual(
        combined,
      );
      expectTypeOf(sumRunTotals([known])).toEqualTypeOf<
        Effect.Effect<RunTotals, UsageAggregationError>
      >();

      const overflow = yield* sumRunTotals([
        RunTotals.make({ ...known, inputTokens: Number.MAX_SAFE_INTEGER }),
        known,
      ]).pipe(Effect.flip);

      expect(overflow).toMatchObject({ _tag: "UsageAggregationError", field: "inputTokens" });

      const summary = yield* summarizeModelUsage([
        ModelCallUsage.make({
          provider: "test",
          model: "model",
          inputTokens: { total: 0, uncached: 0, cacheRead: 0, cacheWrite: 0 },
          outputTokens: { total: 0, text: 0, reasoning: 0 },
          costMicrousd: 0,
          usageStatus: "unknown",
          pricingStatus: "unknown",
        }),
      ]);

      expect(runTotalsFromSummary(summary)).toMatchObject({
        modelCalls: 1,
        usageStatus: "unknown",
        pricingStatus: "unknown",
      });
    }),
  ));
