import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { RunContextPreparationPassthrough } from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
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
  RunContextPreparationPassthrough,
  AnthropicLive,
);
