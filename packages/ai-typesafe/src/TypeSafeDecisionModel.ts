import { DecisionModel, type DecisionSchema } from "@effect-agent/ai-decision";
import { Effect, Layer, Schema } from "effect";

import { choiceProbabilitySum } from "./internal/schema.ts";
import { TypeSafeClient } from "./TypeSafeClient.ts";
import * as TypeSafeSchema from "./TypeSafeSchema.ts";

/**
 * TypeSafe's distribution statistics, under result.providerMetadata.typesafe.
 * Noul questions have no confidence entry.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ProviderMetadata = Schema.Struct({
  confidence: Schema.Record(Schema.String, TypeSafeSchema.Probability),
});

/**
 * Supply a TypeSafe model as a provider-neutral decision model. The client retains HTTP policy;
 * this adapter translates probability questions to noul and preserves returned evidence/usage.
 * Capture TypeSafeClient at Layer construction. This Layer does not supply LanguageModel.
 */
export const model = (
  model: string,
): Layer.Layer<DecisionModel.DecisionModel, never, TypeSafeClient> =>
  Layer.effect(
    DecisionModel.DecisionModel,
    Effect.gen(function* () {
      const client = yield* TypeSafeClient;

      return yield* DecisionModel.make({
        choiceProbabilitySum,
        evaluate: Effect.fnUntraced(function* (request) {
          const questions: TypeSafeSchema.Questions = Object.fromEntries(
            Object.entries(request.questions).map(([id, question]) => [
              id,
              question.type === "probability" ? { ...question, type: "noul" } : question,
            ]),
          );

          const response = yield* client.evaluate({ model, state: request.state, questions });

          const answers: Array<readonly [string, DecisionSchema.Answer]> = [];

          const confidence: Array<readonly [string, number]> = [];

          for (const [id, answer] of Object.entries(response.answers)) {
            if (answer === undefined) continue;
            if (answer.type === "noul") {
              answers.push([id, { type: "probability", probability: answer.noul }]);
            } else {
              const { confidence: statistic, ...evidence } = answer;

              answers.push([id, evidence]);
              confidence.push([id, statistic]);
            }
          }

          return {
            provider: "typesafe",
            model: response.model,
            answers: Object.fromEntries(answers),
            usage: {
              inputTokens: response.usage.input_tokens,
              outputTokens: response.usage.output_tokens,
            },
            providerMetadata: {
              typesafe: {
                confidence: Object.fromEntries(confidence),
              },
            },
          };
        }),
      });
    }),
  );
