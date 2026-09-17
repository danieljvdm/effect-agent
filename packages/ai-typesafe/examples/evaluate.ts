import type { TypeSafeSchema } from "@effect-agent/ai-typesafe";
import { TypeSafeClient } from "@effect-agent/ai-typesafe";
import { Effect, Layer, Schedule } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

export const questions = {
  department: {
    type: "choice",
    instructions: "Which team should handle the ticket?",
    criteria: { billing: "Payments and refunds", technical: "Bugs and outages" },
  },
  frustration: {
    type: "score",
    instructions: "How frustrated is the customer?",
    criteria: ["Calm", "Frustrated", "Very angry"],
  },
  urgent: {
    type: "noul",
    instructions: "Does the customer need immediate help?",
  },
} satisfies TypeSafeSchema.Questions;

export const evaluateTicket = Effect.gen(function* () {
  const client = yield* TypeSafeClient.TypeSafeClient;

  return yield* client.evaluate({
    model: "jev-latest",
    state: { message: "I was charged twice. Please refund the duplicate." },
    questions,
  });
});

export const ClientLive = TypeSafeClient.layer.pipe(
  Layer.provide(TypeSafeClient.Config.layer),
  Layer.provide(FetchHttpClient.layer),
);

// Retry at most twice. The timeout covers the entire operation, including backoff.
export const program = evaluateTicket.pipe(
  Effect.retry({
    times: 2,
    schedule: Schedule.exponential("250 millis"),
    while: (error) => error.isRetryable,
  }),
  Effect.timeout("30 seconds"),
  Effect.provide(ClientLive),
);
