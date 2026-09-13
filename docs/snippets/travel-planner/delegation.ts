import { Subagent } from "effect-agent";

import { Researcher } from "./researcher.ts";

export const Research = Subagent.make("delegate_research_activities", { target: Researcher });
