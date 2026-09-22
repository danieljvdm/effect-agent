// #region catalog
import { AutoModel } from "@effect-agent/ai-decision";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect/ai-typesafe";
import { Config, Effect, Layer, Schema } from "effect";
import { Agent, AgentRuntime, Identifiers, InMemory, Subagent } from "effect-agent";
import { Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

// ---cut---
export const ThreadModels = AutoModel.make({
  version: "profiles-v1",
  models: {
    routine: {
      model: OpenAiLanguageModel.model("gpt-6-luna", { reasoning: { effort: "medium" } }),
      description: "Low cost. Extraction, summaries, and well-specified tasks with clear steps.",
    },
    complex: {
      model: OpenAiLanguageModel.model("gpt-6-astra", { reasoning: { effort: "medium" } }),
      description: "Higher cost. Difficult reasoning, subtle bugs, and ambiguous requirements.",
    },
  },
});
// #endregion catalog

export const Research = Subagent.make("research", {
  description: "Research a specific question before answering.",
  target: Agent.make("researcher", {
    input: Schema.Struct({ question: Schema.String }),
    output: Schema.String,
    instructions: "Research the question and explain the tradeoffs.",
    toolkit: Toolkit.empty,
  }),
});

export const Assistant = Agent.make("assistant", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Delegate questions to research when useful, then answer the user.",
  toolkit: Toolkit.make(Research.tool),
});

// Each spawn selects from its own delegated task on its first turn.
export const ResearchLive = Subagent.layer(Research);

const DecisionLive = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layerConfig()),
  Layer.provide(FetchHttpClient.layer),
);

const OpenAiLive = OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(
  Layer.provide(FetchHttpClient.layer),
);

// Provide one selection store around the parent and its subagent handlers.
// Durable hosts supply a SelectionStore that commits records across restarts.
const Live = ResearchLive.pipe(
  Layer.provideMerge(ThreadModels),
  Layer.provideMerge(
    Layer.mergeAll(DecisionLive, OpenAiLive, InMemory.layer, AutoModel.layerMemory()),
  ),
);

// #region runs
export const program = Effect.gen(function* () {
  const threadId = Identifiers.ThreadId.make("auto-example");

  // AutoModel selects before this thread's first model call.
  yield* AgentRuntime.run(Assistant, "Compare taking a train or bus from Lisbon to Porto.", {
    threadId,
  });

  // The same thread keeps its original model on follow-ups.
  return yield* AgentRuntime.run(Assistant, "Which would you choose for comfort?", {
    threadId,
  });
}).pipe(Effect.provide(Live));
// #endregion runs
