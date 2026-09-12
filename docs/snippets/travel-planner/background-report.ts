import * as Subagent from "@effect-agent/capabilities/Subagent";
import { Effect } from "effect";

import { CoordinatorInput } from "./background-input.ts";
import { Research } from "./delegation.ts";

export const researchReport = Subagent.reporting(Research, {
  input: CoordinatorInput,
  prepare: (report) =>
    Effect.succeed(
      report.outcome === "completed"
        ? {
            _tag: "ResearchFinished",
            activities: report.result.activities,
            partial: report.result.partial,
          }
        : { _tag: "ResearchFailed", reason: report.failure.classification },
    ),
});
