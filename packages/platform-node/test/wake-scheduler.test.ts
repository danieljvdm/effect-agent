import { ledgerLayer } from "@effect-agent/storage-sqlite/SqliteSubmissionLedger";
import {
  LedgerError,
  SubmissionLedger,
  SubmissionSnapshot,
} from "@effect-agent/thread/SubmissionLedger";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Queue,
  Schema,
  Stream,
} from "effect";
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

it.effect("shares one complete ledger scan per cadence across four subscribers", () => {
  let scans = 0;
  const row = snapshot(0);

  const scan = Stream.suspend(() => {
    scans++;

    return Stream.make(row, row);
  });

  return withScheduler(scan, (nextSleep) =>
    Effect.gen(function* () {
      const wake = yield* WakeScheduler;

      const consumers = yield* Effect.forEach([0, 1, 2, 3], () =>
        Stream.runCollect(Stream.take(wake.wakes, 2)).pipe(Effect.forkChild),
      );

      yield* nextSleep;
      yield* TestClock.adjust("1 second");
      expect(scans).toBe(1);
      yield* nextSleep;
      yield* TestClock.adjust("1 second");
      expect(scans).toBe(2);
      for (const consumer of consumers) {
        expect(yield* Fiber.join(consumer)).toEqual([row.threadId, row.threadId]);
      }
    }),
  );
});

it.effect("retains every lane in a large scan without blocking faster subscribers", () => {
  const rows = Array.from({ length: 1_050 }, (_, index) => snapshot(index));
  let scans = 0;

  const scan = Stream.suspend(() => {
    scans++;

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
      expect(scans).toBe(4);
      const expected = rows.map((row) => row.threadId);

      expect(yield* Fiber.join(fast)).toEqual(Array.from({ length: 4 }, () => expected).flat());
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(slow)).toEqual([...expected, ...expected]);
    }),
  );
});

it.effect("starts lazily, stops after the last subscriber, and restarts on later demand", () => {
  let scans = 0;
  let finalized = 0;

  const scan = Stream.fromEffect(
    Effect.gen(function* () {
      scans++;

      return yield* Effect.never.pipe(
        Effect.ensuring(
          Effect.sync(() => {
            finalized++;
          }),
        ),
      );
    }),
  );

  return withScheduler(scan, (nextSleep) =>
    Effect.gen(function* () {
      const wake = yield* WakeScheduler;

      yield* TestClock.adjust("2 seconds");
      expect(scans).toBe(0);
      const first = yield* Stream.runDrain(wake.wakes).pipe(Effect.forkChild);
      const second = yield* Stream.runDrain(wake.wakes).pipe(Effect.forkChild);

      yield* nextSleep;
      yield* TestClock.adjust("1 second");
      expect(scans).toBe(1);
      yield* Fiber.interrupt(first);
      expect(finalized).toBe(0);
      yield* Fiber.interrupt(second);
      expect(finalized).toBe(1);
      yield* TestClock.adjust("2 seconds");
      expect(scans).toBe(1);
      const restarted = yield* Stream.runDrain(wake.wakes).pipe(Effect.forkChild);

      yield* nextSleep;
      yield* TestClock.adjust("1 second");
      expect(scans).toBe(2);
      yield* Fiber.interrupt(restarted);
      expect(finalized).toBe(2);
    }),
  );
});

it.effect("retries typed scan failures and propagates scan defects to every subscriber", () => {
  let scans = 0;
  const defect = new Error("fallback scan defect");
  const row = snapshot(0);

  const scan = Stream.suspend(() => {
    scans++;
    if (scans === 1) return Stream.fail(LedgerError.make({ operation: "scan", message: "retry" }));
    if (scans === 2) return Stream.succeed(row);

    return Stream.die(defect);
  });

  return withScheduler(scan, (nextSleep) =>
    Effect.gen(function* () {
      const wake = yield* WakeScheduler;
      const observed: Array<string> = [];

      const consumers = yield* Effect.forEach([0, 1], () =>
        wake.wakes.pipe(
          Stream.tap((threadId) =>
            Effect.sync(() => {
              observed.push(threadId);
            }),
          ),
          Stream.runDrain,
          Effect.exit,
          Effect.forkChild,
        ),
      );

      yield* nextSleep;
      yield* TestClock.adjust("1 second");
      expect(observed).toEqual([]);
      yield* nextSleep;
      yield* TestClock.adjust("1 second");
      expect(observed).toEqual([row.threadId, row.threadId]);
      yield* nextSleep;
      yield* TestClock.adjust("1 second");
      for (const consumer of consumers) {
        const exit = yield* Fiber.join(consumer);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) expect(Cause.hasDies(exit.cause)).toBe(true);
      }
      expect(scans).toBe(3);
    }),
  );
});
