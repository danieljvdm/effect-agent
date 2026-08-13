import "@tanstack/react-start/server-only";

import { OpenAiClient } from "@effect/ai-openai";
import { Config, Effect, Layer, Stream } from "effect";
import { Prompt } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";

import { AgentRuntime } from "@effect-agent/engine";
import { type DemoChatHistoryMessage } from "./contracts";
import { decodeErrorDetails } from "./error-details";
import {
  FixtureChatRuntimeLayer,
  LiveChatRuntimeLayer,
  makeFixtureChatAgent,
} from "./general-chat";
import { DemoRunFailure } from "./operational-contracts";
import { DemoInteractiveRuntime, DemoInteractiveRuntimeLive } from "./operational-runtime.server";
import { OpenAiChatAgent } from "./openai-profile";
import { DemoRunRpcs } from "./run-rpc";

const OpenAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

const LiveChatServerLayer = Layer.merge(LiveChatRuntimeLayer, OpenAiClientLayer);

/** Converts schema-decoded browser history into the engine's official prior Prompt. */
export const chatHistoryPrompt = (history: ReadonlyArray<DemoChatHistoryMessage>): Prompt.Prompt =>
  Prompt.fromMessages(
    history.map((message) =>
      Prompt.makeMessage(message.role, {
        content: [Prompt.makePart("text", { text: message.content })],
      }),
    ),
  );

const toRunFailure = (error: unknown): DemoRunFailure => {
  const details = decodeErrorDetails(error);
  return DemoRunFailure.make({
    errorTag: details._tag ?? "DemoChatRunError",
    message: (details.message ?? "The chat Run failed.")
      .replace(/\bsk-[A-Za-z0-9_-]+\b/g, "[redacted]")
      .slice(0, 600),
  });
};

/** Thin RPC adapters over the server-scoped interactive Phase 2 runtime. */
export const DemoRunRpcHandlers = DemoRunRpcs.toLayer({
  StreamChatRun: ({ history, message, mode }) =>
    mode === "openai"
      ? AgentRuntime.stream(
          OpenAiChatAgent,
          { message },
          { history: chatHistoryPrompt(history) },
        ).pipe(Stream.provide(LiveChatServerLayer), Stream.mapError(toRunFailure))
      : AgentRuntime.stream(
          makeFixtureChatAgent(message),
          { message },
          { history: chatHistoryPrompt(history) },
        ).pipe(Stream.provide(FixtureChatRuntimeLayer), Stream.mapError(toRunFailure)),
  StreamOperationalRun: (request) =>
    DemoInteractiveRuntime.pipe(
      Effect.map((runtime) => runtime.start(request)),
      Stream.unwrap,
    ),
  StreamLiveTravelChatRun: (request) =>
    DemoInteractiveRuntime.pipe(
      Effect.map((runtime) => runtime.startLiveTravel(request)),
      Stream.unwrap,
      Stream.provide(OpenAiClientLayer),
      Stream.mapError(toRunFailure),
    ),
  QueueRunCommand: (request) =>
    DemoInteractiveRuntime.pipe(Effect.flatMap((runtime) => runtime.queueCommand(request))),
  ResolveRunApproval: (request) =>
    DemoInteractiveRuntime.pipe(Effect.flatMap((runtime) => runtime.resolveApproval(request))),
});

/** Complete server Layer for the credentialed HTTP/NDJSON RPC endpoint. */
export const DemoRunRpcServerLayer = RpcServer.layerHttp({
  group: DemoRunRpcs,
  path: "/api/rpc",
  protocol: "http",
  concurrency: 8,
}).pipe(
  Layer.provide(DemoRunRpcHandlers),
  Layer.provide(DemoInteractiveRuntimeLive),
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
