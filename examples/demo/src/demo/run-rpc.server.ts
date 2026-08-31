import "@tanstack/react-start/server-only";
import { AgentRuntime, withTerminalDefectEvent } from "@effect-agent/engine";
import { OpenAiClient } from "@effect/ai-openai";
import { Config, Effect, Layer, Stream } from "effect";
import { Prompt } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";

import { type DemoChatHistoryMessage } from "./contracts";
import { toDemoRunFailure } from "./error-details";
import {
  FixtureChatRuntimeLayer,
  LiveChatRuntimeLayer,
  makeFixtureChatAgent,
} from "./general-chat";
import { OpenAiChatAgent } from "./openai-profile";
import { DemoInteractiveRuntime, DemoInteractiveRuntimeLive } from "./operational-runtime.server";
import { DemoRunRpcs } from "./run-rpc";

const OpenAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

const LiveChatServerLayer = Layer.merge(LiveChatRuntimeLayer, OpenAiClientLayer);

/** Explicit request context from schema-decoded browser history; the server does not retain it. */
export const chatHistoryPrompt = (history: ReadonlyArray<DemoChatHistoryMessage>): Prompt.Prompt =>
  Prompt.fromMessages(
    history.map((message) =>
      Prompt.makeMessage(message.role, {
        content: [Prompt.makePart("text", { text: message.content })],
      }),
    ),
  );

/** Thin RPC adapters over the server-scoped interactive Phase 2 runtime. */
export const DemoRunRpcHandlers = DemoRunRpcs.toLayer({
  // The engine keeps defects as defects (P7 §7(h)); `withTerminalDefectEvent` is the
  // documented boundary opt-in that appends one bounded terminal RunFailed{Defect} event
  // before the cause is rethrown, so the browser always observes a terminal event. It
  // replaces nothing about `toDemoRunFailure`, which still maps the TYPED wire failure.
  StreamChatRun: ({ history, message, mode }) =>
    mode === "openai"
      ? AgentRuntime.stream(
          OpenAiChatAgent,
          { message },
          { history: chatHistoryPrompt(history) },
        ).pipe(
          withTerminalDefectEvent,
          Stream.provide(LiveChatServerLayer),
          Stream.mapError(toDemoRunFailure),
        )
      : AgentRuntime.stream(
          makeFixtureChatAgent(message),
          { message },
          { history: chatHistoryPrompt(history) },
        ).pipe(
          withTerminalDefectEvent,
          Stream.provide(FixtureChatRuntimeLayer),
          Stream.mapError(toDemoRunFailure),
        ),
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
      // The shared mapper passes an already-typed DemoRunFailure through, so
      // the runtime's specific errorTag is never re-wrapped on the wire.
      Stream.mapError(toDemoRunFailure),
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
