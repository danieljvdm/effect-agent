import "@tanstack/react-start/server-only";

import { OpenAiClient } from "@effect/ai-openai";
import { Config, Layer, Stream } from "effect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import { FetchHttpClient } from "effect/unstable/http";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";

import { AgentRuntime } from "@effect-agent/engine";
import { LiveChatRuntimeLayer } from "./general-chat";
import { OpenAiChatAgent } from "./openai-profile";
import { DemoRunRpcFailure, DemoRunRpcs } from "./run-rpc";

const OpenAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

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
  StreamDemoRun: ({ message }) =>
    AgentRuntime.stream(OpenAiChatAgent, { message }).pipe(Stream.mapError(toRpcFailure)),
});

/** Complete server Layer for the credentialed HTTP/NDJSON RPC endpoint. */
export const DemoRunRpcServerLayer = RpcServer.layerHttp({
  group: DemoRunRpcs,
  path: "/api/rpc",
  protocol: "http",
  concurrency: 8,
}).pipe(
  Layer.provide(DemoRunRpcHandlers),
  Layer.provide(LiveChatRuntimeLayer),
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
