import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { Schema } from "effect";

import { CoordinatorInput } from "./background-input.ts";
import { Researcher } from "./researcher.ts";

export const ResearchBackground = Subagent.background(Researcher, {
  start: true,
  followUp: true,
  reportToParent: true,
});

export const BackgroundCoordinator = Agent.make("background-trip-coordinator", {
  input: CoordinatorInput,
  output: Schema.String,
  toolkit: ResearchBackground.toolkit,
  instructions:
    "Help the user plan a trip. Start activity research in the background when needed. " +
    "Keep discussing their preferences while research runs. Send changed preferences " +
    "to the existing worker with follow_up. When WorkerCompletion arrives, explain " +
    "the findings and flag partial results. On failure or cancellation, help choose a next step. " +
    "Do not start another search just because a research report arrived.",
  policy: { maxTurns: 6, maxToolCalls: 4, maxDuration: "2 minutes", toolConcurrency: 2 },
});
