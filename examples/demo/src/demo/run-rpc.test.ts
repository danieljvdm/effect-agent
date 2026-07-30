import { describe, expect, it } from "vite-plus/test";

import { Deferred, Effect, Fiber, Stream } from "effect";
import * as RpcTest from "effect/unstable/rpc/RpcTest";

import { AgentRuntime } from "@effect-agent/engine";
import { FixtureChatRuntimeLayer, makeFixtureChatAgent } from "./general-chat";
import { DemoRunRpcFailure, DemoRunRpcs } from "./run-rpc";

const fixtureMessage = "What are the best cities to visit in Europe in August?";

const toRpcFailure = (error: unknown): DemoRunRpcFailure =>
  new DemoRunRpcFailure({
    errorTag:
      typeof error === "object" &&
      error !== null &&
      "_tag" in error &&
      typeof error._tag === "string"
        ? error._tag
        : "DemoRunError",
    message:
      typeof error === "object" &&
      error !== null &&
      "message" in error &&
      typeof error.message === "string"
        ? error.message
        : "The deterministic Run failed.",
  });

const DeterministicHandlers = DemoRunRpcs.toLayer({
  StreamDemoRun: ({ message }) =>
    AgentRuntime.stream(makeFixtureChatAgent(message), { message }).pipe(
      Stream.mapError(toRpcFailure),
      Stream.provide(FixtureChatRuntimeLayer),
    ),
});

describe("demo streaming RPC", () => {
  it("streams the deterministic semantic trace through the shared RPC client", async () => {
    const events = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* RpcTest.makeClient(DemoRunRpcs);
        return yield* client.StreamDemoRun({ message: fixtureMessage }).pipe(Stream.runCollect);
      }).pipe(Effect.provide(DeterministicHandlers), Effect.scoped),
    );

    expect(events[0]?._tag).toBe("RunStarted");
    expect(events.some((event) => event._tag === "TextDelta")).toBe(true);
    expect(events.some((event) => event._tag === "ToolCallSucceeded")).toBe(true);
    expect(events.at(-1)?._tag).toBe("RunCompleted");
  });

  it("interrupts the server handler when its client stream is interrupted", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const finalized = yield* Deferred.make<void>();
        const handlers = DemoRunRpcs.toLayer({
          StreamDemoRun: () =>
            Stream.never.pipe(
              Stream.onStart(Deferred.succeed(started, undefined)),
              Stream.ensuring(Deferred.succeed(finalized, undefined)),
            ),
        });

        yield* Effect.gen(function* () {
          const client = yield* RpcTest.makeClient(DemoRunRpcs);
          const fiber = yield* client
            .StreamDemoRun({ message: fixtureMessage })
            .pipe(Stream.runDrain, Effect.forkChild);
          yield* Deferred.await(started);
          yield* Fiber.interrupt(fiber);
          yield* Deferred.await(finalized);
        }).pipe(Effect.provide(handlers), Effect.scoped);
      }),
    );
  });
});
