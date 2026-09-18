import { Effect, Schema, Stream } from "effect";
import { AiError, Decision, DecisionModel, Tool, Toolkit } from "effect/unstable/ai";

const Assessment = Decision.make({
  input: Schema.Struct({ message: Schema.String }),
  decisions: {
    department: Decision.classify({
      instructions: "Which team should handle the ticket?",
      criteria: { billing: "Payments and refunds", technical: "Bugs and outages" },
    }),
  },
});

export const ClassifyTicket = Tool.make("classify_ticket", {
  description: "Classify a support ticket by department.",
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.Struct({
    department: Schema.Literals(["billing", "technical"]),
    probability: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  }),
  failure: AiError.AiError,
  dependencies: [DecisionModel.DecisionModel],
});

export const TicketTools = Toolkit.make(ClassifyTicket);

export const TicketToolsLive = TicketTools.toLayer({
  classify_ticket: Effect.fnUntraced(function* ({ message }) {
    const { answers } = yield* DecisionModel.decide(Assessment, { input: { message } });

    return {
      department: answers.department.label,
      probability: answers.department.probabilities[answers.department.label],
    };
  }),
});

// A LanguageModel can use the same native toolkit. This call exercises the tool
// directly and still requires DecisionModel at execution time.
export const classifyTicket = Effect.gen(function* () {
  const tools = yield* TicketTools;

  const result = yield* tools.handle("classify_ticket", {
    message: "Please refund my duplicate charge.",
  });

  return yield* Stream.runCollect(result);
}).pipe(Effect.provide(TicketToolsLive));
