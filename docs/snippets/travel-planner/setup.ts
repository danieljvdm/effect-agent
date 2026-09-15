import { AnthropicClient } from "@effect/ai-anthropic";
import { Config, Layer } from "effect";
import { Ephemeral } from "effect-agent";
import { FetchHttpClient } from "effect/unstable/http";

import { TravelToolsLive } from "./tools";

const AnthropicLive = AnthropicClient.layerConfig({
  apiKey: Config.Redacted("ANTHROPIC_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

export const AppLive = Layer.mergeAll(TravelToolsLive, Ephemeral.layer, AnthropicLive);
