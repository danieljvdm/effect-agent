import { Agent } from "@effect-agent/core";
import { Schema } from "effect";

import { TravelTools } from "./tools.ts";

export const Researcher = Agent.make("activity-researcher", {
  input: Schema.Struct({ city: Schema.String, focus: Schema.String }),
  output: Schema.Struct({
    activities: Schema.Array(Schema.String),
    researchNotes: Schema.String,
  }),
  instructions: ({ city, focus }) =>
    `Use search_activities to find activities in ${city}. Focus on ${focus}. ` +
    "Return matching activities and notes explaining your selection.",
  toolkit: TravelTools,
  policy: {
    maxToolCalls: 8,
    toolConcurrency: 1,
  },
});
