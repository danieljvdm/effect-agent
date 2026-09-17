import { DecisionModel, DecisionQuery, DecisionSet } from "@effect-agent/ai-decision";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { Config, Effect, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

class Received extends Schema.TaggedClass<Received>()("Received", { message: Schema.String }) {}
class Review extends Schema.TaggedClass<Review>()("Review", { message: Schema.String }) {}
class Routed extends Schema.TaggedClass<Routed>()("Routed", {
  department: Schema.Literals(["billing", "technical"]),
  priority: Schema.Literals(["normal", "high"]),
}) {}
export const TicketState = Schema.Union([Received, Review, Routed]);

export const TicketAssessment = DecisionSet.make({
  input: Schema.Struct({ message: Schema.String }),
  questions: {
    department: DecisionQuery.choice({
      instructions: "Which team should handle this ticket?",
      options: { billing: "Payments and refunds", technical: "Bugs and outages" },
    }),
    severity: DecisionQuery.score({
      instructions: "How much work is blocked?",
      levels: ["None", "Some work", "All work"],
    }),
    urgent: DecisionQuery.probability({ instructions: "Does this need immediate attention?" }),
  },
});

// The model supplies evidence. Application code owns the state transition.
export const advance = Effect.fn("ticket.advance")(function* (state: typeof TicketState.Type) {
  if (state._tag !== "Received") return state;
  const model = yield* DecisionModel.DecisionModel;

  const { answers } = yield* model.evaluate(TicketAssessment, { message: state.message });

  // Illustrative application thresholds; evaluate them against your own cases.
  if (answers.department.probabilities[answers.department.choice] < 0.8)
    return new Review({ message: state.message });

  return new Routed({
    department: answers.department.choice,
    priority:
      answers.severity.score >= 1.5 || answers.urgent.probability >= 0.8 ? "high" : "normal",
  });
});

export const DecisionLive = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(
    TypeSafeClient.layerConfig({ apiKey: Config.Redacted("TYPESAFE_API_KEY") }).pipe(
      Layer.provide(FetchHttpClient.layer),
    ),
  ),
);

export const program = advance(
  new Received({ message: "Our production deployment is blocked." }),
).pipe(Effect.provide(DecisionLive));
