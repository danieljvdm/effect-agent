import { Schema } from "effect";
import { Agent } from "effect-agent";
import { Toolkit } from "effect/unstable/ai";

import { Research } from "./delegation.ts";

export const Coordinator = Agent.make("trip-coordinator", {
  input: Schema.Struct({ city: Schema.String }),
  output: Schema.Struct({ itinerary: Schema.Array(Schema.String) }),
  instructions:
    "Call delegate_research_activities for the requested city with a focus on food and walking. " +
    "Build an itinerary from the returned output.activities. If budgetExhausted is true, use only confirmed findings.",
  toolkit: Toolkit.make(Research.tool),
  policy: {
    maxTurns: 6,
    maxToolCalls: 2,
    maxDuration: "2 minutes",
    toolConcurrency: 2,
  },
});
