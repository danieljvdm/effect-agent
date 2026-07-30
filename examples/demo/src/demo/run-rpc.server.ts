import "@tanstack/react-start/server-only";

import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Layer, Stream } from "effect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import { FetchHttpClient } from "effect/unstable/http";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";

import { Agent } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import { phase0Trip, TravelPlanner, TravelPlannerRuntimeLayer } from "@effect-agent/testing";
import { DemoRunRpcFailure, DemoRunRpcs } from "./run-rpc";

export const OPENAI_DEMO_MODEL = "gpt-5.6-luna" as const;

const OpenAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

const OpenAiTravelPlanner = Agent.withModel(
  TravelPlanner,
  OpenAiLanguageModel.model(OPENAI_DEMO_MODEL, {
    max_output_tokens: 1_600,
    store: false,
    strictJsonSchema: true,
    text: { verbosity: "low" },
  }),
);

const errorTag = (error: unknown): string =>
  typeof error === "object" &&
  error !== null &&
  "_tag" in error &&
  typeof error._tag === "string" &&
  error._tag.length > 0
    ? error._tag
    : "OpenAiRunError";

const safeErrorMessage = (error: unknown): string => {
  const message =
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof error.message === "string"
      ? error.message
      : "The live model run failed.";
  return message.replace(/\bsk-[A-Za-z0-9_-]+\b/g, "[redacted]").slice(0, 600);
};

const toRpcFailure = (error: unknown): DemoRunRpcFailure =>
  new DemoRunRpcFailure({
    errorTag: errorTag(error),
    message: safeErrorMessage(error),
  });

/** Implements the shared Run RPC with the server-only OpenAI Binding. */
export const DemoRunRpcHandlers = DemoRunRpcs.toLayer({
  StreamDemoRun: ({ request }) =>
    AgentRuntime.stream(OpenAiTravelPlanner, {
      ...phase0Trip,
      request,
    }).pipe(Stream.mapError(toRpcFailure)),
});

/** Complete server Layer for the credentialed HTTP/NDJSON RPC endpoint. */
export const DemoRunRpcServerLayer = RpcServer.layerHttp({
  group: DemoRunRpcs,
  path: "/api/rpc",
  protocol: "http",
  concurrency: 8,
}).pipe(
  Layer.provide(DemoRunRpcHandlers),
  Layer.provide(TravelPlannerRuntimeLayer),
  Layer.provide(OpenAiClientLayer),
  Layer.provide(RpcSerialization.layerNdjson),
);

/**
 * Long-lived Fetch handler for the Start route. Its Scope owns the RPC server
 * and each response stream transfers its request Scope until completion.
 */
export const demoRunRpcWebHandler = HttpRouter.toWebHandler(DemoRunRpcServerLayer, {
  disableLogger: true,
});

if (import.meta.hot) {
  import.meta.hot.dispose(() => {
    void demoRunRpcWebHandler.dispose();
  });
}
