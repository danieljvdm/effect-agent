import { TypeSafeClient, TypeSafeDecisionModel, type TypeSafeSchema } from "@effect/ai-typesafe";
import { Effect, Layer, Redacted } from "effect";
import { AiError } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import type { Span } from "./contract.ts";
import { Trace } from "./telemetry.ts";

/** Jev has returned two-decimal distributions totaling 0.99. Tolerate at most
 * two percentage points of rounding, only on the route-only path (no threshold).
 * Keep raw values in telemetry; never alter the provider's chosen link or confidence.
 * Native DecisionModel still validates the resulting distribution and choice.
 */
export const routeProbabilities = Effect.fnUntraced(function* (
  request: typeof TypeSafeSchema.SystemOneRequest.Encoded,
  response: typeof TypeSafeSchema.SystemOneResponse.Type,
) {
  const answers = { ...response.answers };
  const distributions: NonNullable<Span["decisionDistributions"]>[number][] = [];

  for (const [question, definition] of Object.entries(request.questions)) {
    const answer = answers[question];

    if (definition.type !== "choice" || answer?.type !== "choice") continue;
    const keys = Object.keys(definition.criteria);
    const values = Object.values(answer.probabilities);
    const mass = values.reduce((sum, value) => sum + value, 0);

    const valid =
      keys.length === values.length &&
      keys.every((key) => Object.hasOwn(answer.probabilities, key)) &&
      values.every((value) => Number.isFinite(value) && value >= 0 && value <= 1);

    const rounded = values.every((value) => Math.abs(value * 100 - Math.round(value * 100)) < 1e-6);

    if (
      !valid ||
      !Number.isFinite(mass) ||
      mass <= 0 ||
      (Math.abs(mass - 1) > 1e-6 && (!rounded || Math.abs(mass - 1) > 0.020000001))
    )
      return yield* new AiError.AiError({
        module: "WikipediaRoute",
        method: "routeProbabilities",
        reason: new AiError.InvalidOutputError({
          description: `Jev returned an invalid route distribution for ${question} (reported mass ${mass.toFixed(4)}). Only two-decimal rounding within 0.02 is supported.`,
        }),
      });
    distributions.push({
      question,
      ref: answer.choice,
      reportedProbability: answer.probabilities[answer.choice] ?? 0,
      reportedMass: mass,
    });
    if (Math.abs(mass - 1) > 1e-6)
      answers[question] = {
        ...answer,
        probabilities: Object.fromEntries(
          Object.entries(answer.probabilities).map(([key, probability]) => [
            key,
            probability / mass,
          ]),
        ),
      };
  }

  return { response: { ...response, answers }, distributions };
});

export const routeDecisionLayer = (apiKey: string) =>
  TypeSafeDecisionModel.layer({ model: "jev-latest" }).pipe(
    Layer.provide(
      Layer.effect(
        TypeSafeClient.TypeSafeClient,
        Effect.gen(function* () {
          const trace = yield* Trace;
          const client = yield* TypeSafeClient.make({ apiKey: Redacted.make(apiKey) });

          return TypeSafeClient.TypeSafeClient.of({
            ...client,
            systemOne: (request) =>
              client.systemOne(request).pipe(
                Effect.flatMap((response) => routeProbabilities(request, response)),
                Effect.tap(({ distributions }) =>
                  Effect.sync(() =>
                    trace.annotateDecision({ decisionDistributions: distributions }),
                  ),
                ),
                Effect.map(({ response }) => response),
              ),
          });
        }),
      ),
    ),
    Layer.provide(FetchHttpClient.layer),
  );
