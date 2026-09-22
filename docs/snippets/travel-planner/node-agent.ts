import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Layer, Schema } from "effect";
import { Agent } from "effect-agent";
import { Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

export const planner = Agent.make("trip-planner", {
  input: Schema.String,
  output: Schema.Struct({ itinerary: Schema.Array(Schema.String) }),
  instructions: "Plan a trip with one itinerary entry per day.",
  toolkit: Toolkit.empty,
});

export const ModelLive = OpenAiLanguageModel.model("gpt-6-luna");

export const OpenAiLive = OpenAiClient.layerConfig({
  apiKey: Config.Redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

export const definitions = { agent: "v1", model: "gpt-6-luna", tools: "v1" };
