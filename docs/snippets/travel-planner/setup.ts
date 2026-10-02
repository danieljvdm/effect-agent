import { AnthropicClient } from "@effect/ai-anthropic";
import { InMemory } from "@yielded/agent";
import { Config, Layer } from "effect";
import { FetchHttpClient } from "effect/http";

import { TravelToolsLive } from "./tools";

const AnthropicLive = AnthropicClient.layerConfig({
  apiKey: Config.Redacted("ANTHROPIC_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

export const AppLive = Layer.mergeAll(TravelToolsLive, InMemory.layer, AnthropicLive);
