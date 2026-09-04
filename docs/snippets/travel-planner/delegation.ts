import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentPolicy } from "@effect-agent/capabilities/Subagent";
import { Effect, Schema } from "effect";

import { Researcher } from "./researcher.ts";

export class ResearchFailed extends Schema.TaggedError<ResearchFailed>()("ResearchFailed", {
  reason: Schema.String,
}) {}

export const Research = Subagent.define("delegate_research_activities", {
  description: "Delegate activity research for one city and focus. Returns a shortlist.",
  target: Researcher,
  success: Schema.Struct({
    activities: Schema.Array(Schema.String),
    partial: Schema.Boolean,
  }),
  failure: ResearchFailed,
  failureMode: "error",
  projectResult: (output, { budgetExhausted }) =>
    Effect.succeed({
      activities: output.activities,
      partial: budgetExhausted,
      // researchNotes stays in the child's thread.
    }),
  policy: SubagentPolicy.make({
    maxChildren: 2,
    maxConcurrency: 2,
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: "30 seconds",
    maxResultBytes: 4_096,
  }),
});
