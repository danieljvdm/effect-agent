import { ModelCallUsage, RunUsageSummary, summarizeModelUsage } from "@effect-agent/core/Usage";
import { Effect, Schema } from "effect";
import { expect, it } from "vite-plus/test";

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
