import { Agent, AgentPolicy } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import { AnthropicLanguageModel } from "@effect/ai-anthropic";
import { Effect, Schema } from "effect";

import { AppLive } from "./setup";
import { TravelTools } from "./tools";

export const TravelPlanner = Agent.make("travel-planner", {
  input: Schema.Struct({ city: Schema.String, days: Schema.Int.check(Schema.isGreaterThan(0)) }),
  output: Schema.Struct({ itinerary: Schema.Array(Schema.String) }),
  instructions: ({ city, days }) =>
    `Find activities with search_activities, then plan ${days} days in ${city}.`,
  toolkit: TravelTools,
  policy: AgentPolicy.make({
    maxTurns: 6,
    maxToolCalls: 10,
    maxDuration: "2 minutes",
    toolConcurrency: 1,
  }),
});

export const plan = AgentRuntime.run(TravelPlanner, { city: "Lisbon", days: 2 }).pipe(
  Effect.provide(AnthropicLanguageModel.model("claude-sonnet-5")),
  Effect.provide(AppLive),
);
