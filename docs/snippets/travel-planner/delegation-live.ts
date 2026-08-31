import { SubagentReservationsMemoryLive, SubagentRuntime } from "@effect-agent/capabilities";
import { Agent, IdGenerator } from "@effect-agent/core";
import { AgentRuntime, ThreadHistory } from "@effect-agent/engine";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { Coordinator } from "./coordinator.ts";
import { Research, ResearchFailed } from "./delegation.ts";
import { Researcher } from "./researcher.ts";
import { TravelToolsLive } from "./tools.ts";

const ChildBinding = Agent.withModel(Researcher, OpenAiLanguageModel.model("gpt-4.1-mini"));

const ResearchLive = SubagentRuntime.layer(Research, ChildBinding, {
  mapChildFailure: (error) => ResearchFailed.make({ reason: error._tag }),
}).pipe(Layer.provide(TravelToolsLive));

export const program = AgentRuntime.run(Coordinator, { city: "Lisbon" }).pipe(
  Effect.provide(ResearchLive),
  Effect.provide(OpenAiLanguageModel.model("gpt-4.1-mini")),
  Effect.provide(SubagentReservationsMemoryLive),
  Effect.provide(ThreadHistory.layerTransient),
  Effect.provide(IdGenerator.layer),
  Effect.provide(OpenAiClient.layerConfig({ apiKey: Config.redacted("OPENAI_API_KEY") })),
  Effect.provide(FetchHttpClient.layer),
);
