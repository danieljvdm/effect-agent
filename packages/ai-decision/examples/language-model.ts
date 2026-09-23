import { LanguageModelDecisionModel } from "@effect-agent/ai-decision";
import { Effect, Schema } from "effect";
import { Decision, DecisionModel } from "effect/unstable/ai";

export const Sentiment = Decision.make({
  input: Schema.String,
  decisions: {
    tone: Decision.classify({
      instructions: "Classify the sentiment.",
      criteria: {
        positive: "Expresses satisfaction",
        negative: "Expresses dissatisfaction",
      },
    }),
  },
});

// Supply any native LanguageModel with structured output to run this Effect.
export const program = DecisionModel.decide(Sentiment, {
  input: "This is excellent!",
}).pipe(
  Effect.map(({ answers }) => answers.tone.label),
  Effect.provide(LanguageModelDecisionModel.layer),
);
