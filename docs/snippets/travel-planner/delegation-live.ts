import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer } from "effect";
import { AgentRuntime, InMemory, Subagent } from "effect-agent";
import { FetchHttpClient } from "effect/unstable/http";

import { Coordinator } from "./coordinator.ts";
import { Research } from "./delegation.ts";
import { TravelToolsLive } from "./tools.ts";

const ModelLive = OpenAiLanguageModel.model("gpt-4.1-mini");

const ProviderLive = OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(
  Layer.provide(FetchHttpClient.layer),
);

const ResearchLive = Subagent.layer(Research).pipe(Layer.provide(TravelToolsLive));

const AppLive = ResearchLive.pipe(
  Layer.provideMerge(ModelLive),
  Layer.provideMerge(InMemory.layer),
  Layer.provide(ProviderLive),
);

export const program = AgentRuntime.run(Coordinator, { city: "Lisbon" }).pipe(
  Effect.provide(AppLive),
);
