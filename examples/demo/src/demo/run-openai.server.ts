import "@tanstack/react-start/server-only";

import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Schema, Stream } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { Agent, type RunEvent } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import {
  phase0Trip,
  TravelPlan,
  TravelPlanner,
  TravelPlannerRuntimeLayer,
} from "@effect-agent/testing";
import { type OpenAiDemoRunRequest, type OpenAiDemoRunSuccess } from "./contracts";

export const OPENAI_DEMO_MODEL = "gpt-5.6-luna" as const;

class DemoRunProtocolError extends Schema.TaggedErrorClass<DemoRunProtocolError>()(
  "DemoRunProtocolError",
  {
    message: Schema.String,
  },
) {}

const OpenAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

const LiveRuntimeLayer = Layer.mergeAll(TravelPlannerRuntimeLayer, OpenAiClientLayer);

const OpenAiTravelPlanner = Agent.withModel(
  TravelPlanner,
  OpenAiLanguageModel.model(OPENAI_DEMO_MODEL, {
    max_output_tokens: 1_600,
    store: false,
    strictJsonSchema: true,
    text: { verbosity: "low" },
  }),
);

const completedOutput = Effect.fn("Demo.completedOutput")(function* (
  events: ReadonlyArray<RunEvent>,
) {
  const completed = events.find((event) => event._tag === "RunCompleted");
  if (completed === undefined) {
    return yield* new DemoRunProtocolError({
      message: "The live run ended without a RunCompleted event.",
    });
  }
  return yield* Schema.decodeUnknownEffect(TravelPlan)(completed.output);
});

/**
 * Executes one scoped OpenAI-backed Travel Planner run. The API key remains an
 * Effect Config requirement resolved only inside the Start server process.
 */
export const runOpenAiDemoOnServer = Effect.fn("Demo.runOpenAiDemoOnServer")(function* (
  input: OpenAiDemoRunRequest,
) {
  const events = yield* AgentRuntime.stream(OpenAiTravelPlanner, {
    ...phase0Trip,
    request: input.request,
  }).pipe(Stream.runCollect, Effect.provide(LiveRuntimeLayer), Effect.scoped);
  const output = yield* completedOutput(events);
  return {
    _tag: "OpenAiDemoRunSuccess",
    model: OPENAI_DEMO_MODEL,
    events,
    output,
  } satisfies OpenAiDemoRunSuccess;
});
