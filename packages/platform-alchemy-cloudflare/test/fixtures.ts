import { ScriptedModel } from "@effect-agent/testing/scripted-model";
import { Schema } from "effect";
import { Agent } from "effect-agent";
import { DefinitionDigestInput } from "effect-agent/records";
import { Model, Toolkit } from "effect/unstable/ai";

export const planner = Agent.make("alchemy-planner", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer the question as JSON.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 2, maxToolCalls: 1, maxDuration: "30 seconds" },
});

export const definitions = DefinitionDigestInput.make({
  agent: { id: planner.id, revision: 1 },
  model: { provider: "scripted", name: "alchemy-planner" },
  tools: [],
});

export const model = Model.make(
  "scripted",
  "alchemy-planner",
  ScriptedModel.layer([
    {
      _tag: "Stream",
      parts: [
        { type: "text-start", id: "answer" },
        { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
        { type: "text-end", id: "answer" },
        { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
      ],
      termination: { _tag: "Complete" },
    },
  ]),
);

export const initializationFinalizers: Array<string> = [];
export const eventFinalizers: Array<string> = [];
export const constructorCounts = new Map<string, number>();
