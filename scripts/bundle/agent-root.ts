import { Schema } from "effect";
import { Agent } from "effect-agent";
import { Toolkit } from "effect/unstable/ai";

export const agent = Agent.make("bundle-probe", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer the question.",
  toolkit: Toolkit.empty,
});
