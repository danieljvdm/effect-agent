import { describe, expect, it } from "@effect/vitest";

import { Deferred, Effect, Fiber, Schema, Stream } from "effect";
import * as RpcTest from "effect/unstable/rpc/RpcTest";

import { DemoControlAccepted, DemoRunHandle, DemoRunOpened } from "./operational-contracts";
import { DemoRunRpcs } from "./run-rpc";

const handle = Schema.decodeSync(DemoRunHandle)("demo-handle-rpc");
const opened = Schema.decodeSync(DemoRunOpened)({
  _tag: "DemoRunOpened",
  handle,
  emittedAt: "2026-07-30T12:00:00.000Z",
  runId: "demo-run-rpc",
  conversationId: "demo-conversation-rpc",
  scenario: "guided",
  executionClass: "ephemeral",
  schedulerConcurrency: 3,
});

const accepted = DemoControlAccepted.make({ accepted: true });

describe("interactive Phase 2 RPC", () => {
  it.effect("streams evidence and accepts separate command and approval calls", () =>
    Effect.gen(function* () {
      const handlers = DemoRunRpcs.toLayer({
        StreamChatRun: () => Stream.empty,
        StreamOperationalRun: () => Stream.succeed(opened),
        StreamLiveTravelChatRun: () => Stream.succeed(opened),
        QueueRunCommand: () => Effect.succeed(accepted),
        ResolveRunApproval: () => Effect.succeed(accepted),
      });

      yield* Effect.gen(function* () {
        const client = yield* RpcTest.makeClient(DemoRunRpcs);
        const events = yield* client
          .StreamOperationalRun({ scenario: "guided" })
          .pipe(Stream.runCollect);
        const liveEvents = yield* client
          .StreamLiveTravelChatRun({
            message: "Plan the fixture London trip.",
            scenario: "guided",
          })
          .pipe(Stream.runCollect);
        const queued = yield* client.QueueRunCommand({
          handle,
          kind: "follow-up",
          content: "Prefer a quiet room.",
        });
        const resolved = yield* client.ResolveRunApproval({
          handle,
          requestId: "approval-rpc",
          choice: "deny",
        });

        expect(events).toEqual([opened]);
        expect(liveEvents).toEqual([opened]);
        expect(queued.accepted).toBe(true);
        expect(resolved.accepted).toBe(true);
      }).pipe(Effect.provide(handlers), Effect.scoped);
    }),
  );

  it.effect("finalizes the server stream when the client interrupts", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const finalized = yield* Deferred.make<void>();
      const handlers = DemoRunRpcs.toLayer({
        StreamChatRun: () => Stream.empty,
        StreamOperationalRun: () =>
          Stream.never.pipe(
            Stream.onStart(Deferred.succeed(started, undefined)),
            Stream.ensuring(Deferred.succeed(finalized, undefined)),
          ),
        StreamLiveTravelChatRun: () => Stream.empty,
        QueueRunCommand: () => Effect.succeed(accepted),
        ResolveRunApproval: () => Effect.succeed(accepted),
      });

      yield* Effect.gen(function* () {
        const client = yield* RpcTest.makeClient(DemoRunRpcs);
        const fiber = yield* client
          .StreamOperationalRun({ scenario: "guided" })
          .pipe(Stream.runDrain, Effect.forkChild);
        yield* Deferred.await(started);
        yield* Fiber.interrupt(fiber);
        yield* Deferred.await(finalized);
      }).pipe(Effect.provide(handlers), Effect.scoped);
    }),
  );
});
