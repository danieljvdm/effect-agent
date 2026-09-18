// #region catalog
import { AutoModel } from "@effect-agent/ai-decision";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Schema } from "effect";
import { Agent, AgentRuntime, Identifiers, InMemory } from "effect-agent";
import { Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

// ---cut---
export const ThreadModels = AutoModel.make({
  version: "profiles-v1",
  models: {
    routine: {
      model: OpenAiLanguageModel.model("gpt-5.6-luna", { reasoning: { effort: "medium" } }),
      description: "Low cost. Extraction, summaries, and well-specified tasks with clear steps.",
    },
    complex: {
      model: OpenAiLanguageModel.model("gpt-6-astra", { reasoning: { effort: "medium" } }),
      description: "Higher cost. Difficult reasoning, subtle bugs, and ambiguous requirements.",
    },
  },
});
// #endregion catalog

const Assistant = Agent.make("assistant", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: (task) => task,
  toolkit: Toolkit.empty,
});

// The host loads the record saved at thread creation. Restoration performs no selection.
export const continueThread = Effect.fn("continueThread")(function* (
  threadId: Identifiers.ThreadId,
  storedSelection: unknown,
  task: string,
) {
  const selected = yield* ThreadModels.restore(threadId, storedSelection);

  return yield* AgentRuntime.run(Agent.withModel(Assistant, selected.model), task, { threadId });
});

const DecisionLive = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layer),
  Layer.provide(TypeSafeClient.Config.layer),
  Layer.provide(FetchHttpClient.layer),
);

const OpenAiLive = OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(
  Layer.provide(FetchHttpClient.layer),
);

// This example retains the selection and conversation in memory. A durable host
// atomically saves selected.record with its thread metadata before starting work.
export const program = Effect.gen(function* () {
  const threadId = Identifiers.ThreadId.make("auto-example");
  const task = "Summarize the tradeoffs of taking a train or bus from Lisbon to Porto.";
  const selected = yield* ThreadModels.select({ threadId, state: { task, tools: [] } });

  yield* continueThread(threadId, selected.record, task);

  return yield* continueThread(threadId, selected.record, "Which would you choose for comfort?");
}).pipe(Effect.provide(Layer.mergeAll(DecisionLive, OpenAiLive, InMemory.layer)));
