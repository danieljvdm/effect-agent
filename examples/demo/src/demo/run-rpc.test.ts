import { describe, expect, it } from "@effect/vitest";

import { Deferred, Effect, Fiber, Stream } from "effect";
import * as RpcTest from "effect/unstable/rpc/RpcTest";

import { AgentRuntime } from "@effect-agent/engine";
import { decodeErrorDetails } from "./error-details";
import { FixtureChatRuntimeLayer, makeFixtureChatAgent } from "./general-chat";
import { DemoRunRpcFailure, DemoRunRpcs } from "./run-rpc";

const fixtureMessage = "What are the best cities to visit in Europe in August?";

const toRpcFailure = (error: unknown): DemoRunRpcFailure => {
  const details = decodeErrorDetails(error);
  return DemoRunRpcFailure.make({
    errorTag: details._tag ?? "DemoRunError",
    message: details.message ?? "The deterministic Run failed.",
  });
};

const DeterministicHandlers = DemoRunRpcs.toLayer({
  StreamDemoRun: ({ message }) =>
    AgentRuntime.stream(makeFixtureChatAgent(message), { message }).pipe(
      Stream.mapError(toRpcFailure),
      Stream.provide(FixtureChatRuntimeLayer),
    ),
});

describe("demo streaming RPC", () => {
  it.effect("streams the deterministic semantic trace through the shared RPC client", () =>
    Effect.gen(function* () {
      const events = yield* Effect.gen(function* () {
        const client = yield* RpcTest.makeClient(DemoRunRpcs);
        return yield* client.StreamDemoRun({ message: fixtureMessage }).pipe(Stream.runCollect);
      }).pipe(Effect.provide(DeterministicHandlers), Effect.scoped);

      expect(events[0]?._tag).toBe("RunStarted");
      expect(events.some((event) => event._tag === "TextDelta")).toBe(true);
      expect(events.some((event) => event._tag === "ToolCallSucceeded")).toBe(true);
      expect(events.at(-1)?._tag).toBe("RunCompleted");
    }),
  );

  it.effect("interrupts the server handler when its client stream is interrupted", () =>
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
