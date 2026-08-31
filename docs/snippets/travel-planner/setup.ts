import { IdGenerator } from "@effect-agent/core";
import { ThreadHistory } from "@effect-agent/engine";
import { AnthropicClient } from "@effect/ai-anthropic";
import { Config, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { TravelToolsLive } from "./tools";

const AnthropicLive = AnthropicClient.layerConfig({
  apiKey: Config.redacted("ANTHROPIC_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

export const AppLive = Layer.mergeAll(
  TravelToolsLive,
  IdGenerator.layer,
  ThreadHistory.layerTransient,
  AnthropicLive,
);
