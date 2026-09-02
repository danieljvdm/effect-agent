import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Stream } from "effect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as RpcTest from "effect/unstable/rpc/RpcTest";

import { DemoInteractiveRuntimeLive } from "./operational-runtime.server";
import { DemoRunRpcs } from "./run-rpc";
import { chatHistoryPrompt, DemoRunRpcHandlers, DemoRunRpcServerLayer } from "./run-rpc.server";

describe("Phase 2 demo RPC server", () => {
  it.effect("builds without a provider credential or network dependency", () =>
    Effect.gen(function* () {
      yield* Layer.build(DemoRunRpcServerLayer.pipe(Layer.provide(HttpRouter.layer))).pipe(
        Effect.scoped,
      );
      expect(true).toBe(true);
    }),
  );

  it.effect("runs deterministic chat without resolving an OpenAI credential", () =>
    Effect.gen(function* () {
      const handlers = DemoRunRpcHandlers.pipe(Layer.provide(DemoInteractiveRuntimeLive));

      const events = yield* Effect.gen(function* () {
        const client = yield* RpcTest.makeClient(DemoRunRpcs);

        return yield* client
          .StreamChatRun({
            mode: "deterministic",
            message: "What does this Effect Agent demo prove?",
            history: [
              {
                role: "user",
                content: "Tell me about cities in Europe.",
              },
              {
                role: "assistant",
                content: "Edinburgh and Copenhagen are strong choices.",
              },
            ],
          })
          .pipe(Stream.runCollect);
      }).pipe(Effect.provide(handlers), Effect.scoped);

      expect(events.some((event) => event._tag === "ToolCallSucceeded")).toBe(true);
      expect(events.at(-1)?._tag).toBe("RunCompleted");
    }),
  );

  it("preserves prior user and assistant turns as official Prompt history", () => {
    const history = chatHistoryPrompt([
      {
        role: "user",
        content: "Which European cities are best in August?",
      },
      {
        role: "assistant",
        content: "Edinburgh, Copenhagen, and Lisbon.",
      },
    ]);

    expect(history.content.map((message) => message.role)).toEqual(["user", "assistant"]);
    expect(JSON.stringify(history.content)).toContain("Edinburgh, Copenhagen, and Lisbon.");
  });
});
