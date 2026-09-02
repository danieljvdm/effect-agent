import { SubagentReservationsMemoryLive, SubagentRuntime } from "@effect-agent/capabilities";
import { IdGenerator } from "@effect-agent/core";
import {
  AgentRuntime,
  ThreadHistory,
  RunContextPreparationPassthrough,
} from "@effect-agent/engine";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { Coordinator } from "./coordinator.ts";
import { Research, ResearchFailed } from "./delegation.ts";
import { TravelToolsLive } from "./tools.ts";

const ResearchLive = SubagentRuntime.layer(Research, OpenAiLanguageModel.model("gpt-4.1-mini"), {
  mapChildFailure: (error) => ResearchFailed.make({ reason: error._tag }),
}).pipe(Layer.provide(TravelToolsLive));

export const program = AgentRuntime.run(Coordinator, { city: "Lisbon" }).pipe(
  Effect.provide(ResearchLive),
  Effect.provide(OpenAiLanguageModel.model("gpt-4.1-mini")),
  Effect.provide(SubagentReservationsMemoryLive),
  Effect.provide(ThreadHistory.layerTransient),
  Effect.provide(RunContextPreparationPassthrough),
  Effect.provide(IdGenerator.layer),
  Effect.provide(OpenAiClient.layerConfig({ apiKey: Config.redacted("OPENAI_API_KEY") })),
  Effect.provide(FetchHttpClient.layer),
);
