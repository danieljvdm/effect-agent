import { assert, expectTypeOf, it } from "@effect/vitest";
import { Effect } from "effect";
import type { AiError } from "effect/unstable/ai";

import { routeProbabilities } from "../src/route-probabilities.ts";

expectTypeOf<
  Effect.Error<ReturnType<typeof routeProbabilities>>
>().toEqualTypeOf<AiError.AiError>();
expectTypeOf<Effect.Services<ReturnType<typeof routeProbabilities>>>().toEqualTypeOf<never>();

const request = {
  model: "jev-latest",
  state: {},
  questions: {
    route_0: {
      type: "choice" as const,
      instructions: "Next hop?",
      criteria: { earth: "Earth", mars: "Mars" },
    },
  },
};

const response = (probabilities: Record<string, number>) => ({
  model: "jev-latest",
  answers: {
    route_0: { type: "choice" as const, choice: "earth", confidence: 0.8, probabilities },
  },
  usage: { input_tokens: 10, output_tokens: 2 },
});

it.effect(
  "preserves the selected link and raw probabilities while correcting narrow two-decimal rounding",
  () =>
    Effect.gen(function* () {
      const raw = response({ earth: 0.64, mars: 0.35 });
      const result = yield* routeProbabilities(request, raw);
      const answer = result.response.answers.route_0;

      assert.strictEqual(answer?.type, "choice");
      if (answer?.type === "choice") {
        assert.strictEqual(answer.choice, "earth");
        assert.strictEqual(answer.confidence, 0.8);
        assert.closeTo(
          Object.values(answer.probabilities).reduce((sum, value) => sum + value, 0),
          1,
          1e-12,
        );
        assert.closeTo(
          (answer.probabilities.earth ?? 0) / (answer.probabilities.mars ?? 1),
          0.64 / 0.35,
          1e-12,
        );
      }
      assert.deepStrictEqual(result.distributions, [
        { question: "route_0", ref: "earth", reportedProbability: 0.64, reportedMass: 0.99 },
      ]);
      assert.deepStrictEqual(raw.answers.route_0.probabilities, { earth: 0.64, mars: 0.35 });
      const exact = response({ earth: 0.625, mars: 0.375 });

      assert.deepStrictEqual((yield* routeProbabilities(request, exact)).response, exact);
    }),
);

it.effect(
  "rejects missing labels, invalid values and mass errors outside the supported rounding contract",
  () =>
    Effect.gen(function* () {
      const invalid: Array<Record<string, number>> = [
        { earth: 1 },
        { earth: 1, unrelated: 0 },
        { earth: 0.64, mars: 0.33 },
        { earth: 0.645, mars: 0.35 },
        { earth: 0, mars: 0 },
        { earth: 1.1, mars: -0.1 },
        { earth: Number.NaN, mars: 0 },
      ];

      for (const probabilities of invalid) {
        const result = yield* routeProbabilities(request, response(probabilities)).pipe(
          Effect.result,
        );

        assert.strictEqual(result._tag, "Failure");
      }
    }),
);
