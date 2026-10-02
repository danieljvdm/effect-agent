import { Agent } from "@yielded/agent";
import { Schema } from "effect";
import { Toolkit } from "effect/ai";

export const agent = Agent.make("bundle-probe", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer the question.",
  toolkit: Toolkit.empty,
});
