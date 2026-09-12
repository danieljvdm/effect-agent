import { Schema } from "effect";
import { Agent, Subagent } from "effect-agent";
import { Toolkit } from "effect/unstable/ai";

export const Summarize = Subagent.make("summarize", {
  description: "Summarize a support case in three sentences.",
  target: Agent.make("summarizer", {
    input: Schema.String,
    output: Schema.String,
    instructions: "Summarize the case. Include the issue and next action.",
    toolkit: Toolkit.empty,
  }),
});

export const Support = Agent.make("support", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Use summarize to prepare a concise case summary, then answer the user.",
  toolkit: Toolkit.make(Summarize.tool),
});
