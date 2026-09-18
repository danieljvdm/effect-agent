import { TypeSafeClient, TypeSafeDecisionModel } from "@effect/ai-typesafe";
import { Effect, Layer, Schema } from "effect";
import { Decision, DecisionModel } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

class Received extends Schema.TaggedClass<Received>()("Received", { message: Schema.String }) {}
class Review extends Schema.TaggedClass<Review>()("Review", { message: Schema.String }) {}
class Routed extends Schema.TaggedClass<Routed>()("Routed", {
  department: Schema.Literals(["billing", "technical"]),
  priority: Schema.Literals(["normal", "high"]),
}) {}
export const TicketState = Schema.Union([Received, Review, Routed]);

export const TicketAssessment = Decision.make({
  input: Schema.Struct({ message: Schema.String }),
  decisions: {
    department: Decision.classify({
      instructions: "Which team should handle this ticket?",
      criteria: { billing: "Payments and refunds", technical: "Bugs and outages" },
    }),
    severity: Decision.rate({
      instructions: "How much work is blocked?",
      criteria: ["None", "Some work", "All work"],
    }),
    urgent: Decision.probability({
      instructions: "Does this need immediate attention?",
      criteria: { false: "Can wait", true: "Needs action now" },
    }),
  },
});

// The model supplies evidence. Application code owns the state transition.
export const advance = Effect.fn("ticket.advance")(function* (state: typeof TicketState.Type) {
  if (state._tag !== "Received") return state;

  const { answers } = yield* DecisionModel.decide(TicketAssessment, {
    input: { message: state.message },
  });

  // Illustrative application thresholds; evaluate them against your own cases.
  if (answers.department.probabilities[answers.department.label] < 0.8)
    return new Review({ message: state.message });

  return new Routed({
    department: answers.department.label,
    priority:
      answers.severity.rating >= 1.5 || answers.urgent.probability >= 0.8 ? "high" : "normal",
  });
});

export const DecisionLive = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layerConfig().pipe(Layer.provide(FetchHttpClient.layer))),
);

export const program = advance(
  new Received({ message: "Our production deployment is blocked." }),
).pipe(Effect.provide(DecisionLive));
