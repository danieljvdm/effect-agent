import { TypeSafeClient, TypeSafeSchema } from "@effect-agent/ai-typesafe";
import { Effect, Schema, Stream } from "effect";
import { AiError, Tool, Toolkit } from "effect/unstable/ai";

export const ClassifyTicket = Tool.make("classify_ticket", {
  description: "Classify a support ticket by department.",
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.Struct({
    department: Schema.Literals(["billing", "technical"]),
    confidence: TypeSafeSchema.Probability,
  }),
  failure: AiError.AiError,
  dependencies: [TypeSafeClient.TypeSafeClient],
});

export const TicketTools = Toolkit.make(ClassifyTicket);

export const TicketToolsLive = TicketTools.toLayer({
  classify_ticket: Effect.fnUntraced(function* ({ message }) {
    const client = yield* TypeSafeClient.TypeSafeClient;

    const { answers } = yield* client.evaluate({
      model: "jev-latest",
      state: message,
      questions: {
        department: {
          type: "choice",
          instructions: "Which team should handle the ticket?",
          criteria: { billing: "Payments and refunds", technical: "Bugs and outages" },
        },
      },
    });

    return { department: answers.department.choice, confidence: answers.department.confidence };
  }),
});

// A LanguageModel can use the same native toolkit. This call exercises the tool
// directly and still requires TypeSafeClient at execution time.
export const classifyTicket = Effect.gen(function* () {
  const tools = yield* TicketTools;

  const result = yield* tools.handle("classify_ticket", {
    message: "Please refund my duplicate charge.",
  });

  return yield* Stream.runCollect(result);
}).pipe(Effect.provide(TicketToolsLive));
