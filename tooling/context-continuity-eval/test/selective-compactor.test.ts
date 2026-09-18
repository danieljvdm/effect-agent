import type { DecisionModel } from "@effect-agent/ai-decision";
import { Effect } from "effect";
import { expect, expectTypeOf, it } from "vite-plus/test";

import { runProbe, scriptedDecisionLayer } from "../src/selective-spike.ts";

it("compares the same history through the native engine and keeps exact evidence selectively", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const age = yield* runProbe("age");
      const selective = yield* runProbe("selective");

      expect(age).toMatchObject({
        receiptAvailable: false,
        newestAvailable: true,
        retainedHistoryIntact: true,
        clearedResults: ["receipt", "noise"],
      });
      expect(selective).toMatchObject({
        receiptAvailable: true,
        newestAvailable: true,
        retainedHistoryIntact: true,
        clearedResults: ["noise"],
      });
      expect(selective.selectorUsage).toHaveLength(1);
      expect(selective.selectorUsage[0]).toMatchObject({
        purpose: "compaction",
        inputTokens: { total: 200 },
        outputTokens: { total: 2 },
      });
    }).pipe(Effect.provide(scriptedDecisionLayer)),
  );
});

it("keeps the probe's decision-provider requirement visible", () => {
  expectTypeOf<
    Effect.Services<ReturnType<typeof runProbe>>
  >().toEqualTypeOf<DecisionModel.DecisionModel>();
});
