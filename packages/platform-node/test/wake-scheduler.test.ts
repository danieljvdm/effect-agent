import { ledgerLayer } from "@effect-agent/storage-sqlite/sqlite-submission-ledger";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Clock,
  Deferred,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Queue,
  Schema,
  Stream,
} from "effect";
import { SubmissionLedger, SubmissionSnapshot } from "effect-agent/submission-ledger";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { TestClock } from "effect/testing";

import { NodeWakeSchedulerConfig, nodeWakeSchedulerLayer } from "../src/NodeWakeScheduler.ts";

const snapshot = (index: number) =>
  Schema.decodeSync(SubmissionSnapshot)({
    submissionId: `submission-${index}`,
    threadId: `thread-${index}`,
    queueSequence: index + 1,
    principal: "scheduler-test",
    idempotencyKey: `key-${index}`,
    agentId: "scheduler-agent",
    agentDigests: { agent: "a".repeat(64), model: "a".repeat(64), tools: "a".repeat(64) },
    deploymentId: "scheduler-deployment",
    inputPayload: null,
    inputDigest: "a".repeat(64),
    receiptId: `receipt-${index}`,
    state: "ready",
    createdAt: "2026-09-01T00:00:00.000Z",
  });

// Retain the complete SQLite port; only its scan is controlled for scheduler lifecycle evidence.
const schedulerLayer = (scan: SubmissionLedger["Service"]["scanNonterminal"]) =>
  nodeWakeSchedulerLayer.pipe(
    Layer.provide(
      Layer.effect(
        SubmissionLedger,
        Effect.map(SubmissionLedger, (ledger) => ({ ...ledger, scanNonterminal: scan })),
      ).pipe(
        Layer.provide(
          Layer.unwrap(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem;
              const directory = yield* fs.makeTempDirectoryScoped({ prefix: "wake-scheduler-" });

              return ledgerLayer({ filename: `${directory}/ledger.sqlite` });
            }),
          ).pipe(Layer.provide(NodeFileSystem.layer)),
        ),
      ),
    ),
    Layer.provide(NodeWakeSchedulerConfig.layer({ scanInterval: Duration.seconds(1) })),
  );

const withScheduler = <A, E, R>(
  scan: SubmissionLedger["Service"]["scanNonterminal"],
  body: (nextSleep: Effect.Effect<void>) => Effect.Effect<A, E, R | WakeScheduler>,
) =>
  Effect.gen(function* () {
    const clock = yield* Clock.Clock;
    const sleeps = yield* Queue.unbounded<void>();

    const observedClock: Clock.Clock = {
      ...clock,
      sleep: (duration) =>
        Queue.offer(sleeps, undefined).pipe(Effect.andThen(clock.sleep(duration))),
    };

    return yield* body(Queue.take(sleeps)).pipe(
      Effect.provide(
        schedulerLayer(scan).pipe(Layer.provide(Layer.succeed(Clock.Clock, observedClock))),
      ),
    );
  }).pipe(Effect.scoped);

it.effect("retains every lane in a large scan without blocking faster subscribers", () => {
  const rows = Array.from({ length: 1_050 }, (_, index) => snapshot(index));

  const scan = Stream.suspend(() => {
    return Stream.fromIterable(rows);
  });

  return withScheduler(scan, (nextSleep) =>
    Effect.gen(function* () {
      const wake = yield* WakeScheduler;
      const parked = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let first = true;

      const slow = yield* wake.wakes.pipe(
        Stream.tap(() => {
          if (!first) return Effect.void;
          first = false;

          return Deferred.succeed(parked, undefined).pipe(Effect.andThen(Deferred.await(release)));
        }),
        Stream.take(rows.length * 2),
        Stream.runCollect,
        Effect.forkChild,
      );

      const fast = yield* Stream.runCollect(Stream.take(wake.wakes, rows.length * 4)).pipe(
        Effect.forkChild,
      );

      yield* nextSleep;
      yield* TestClock.adjust("1 second");
      yield* Deferred.await(parked);
      for (let i = 0; i < 3; i++) {
        yield* nextSleep;
        yield* TestClock.adjust("1 second");
      }
      const expected = rows.map((row) => row.threadId);

      expect(yield* Fiber.join(fast)).toEqual(Array.from({ length: 4 }, () => expected).flat());
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(slow)).toEqual([...expected, ...expected]);
    }),
  );
});
