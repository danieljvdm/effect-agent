import { it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Schema, Stream } from "effect";
import { ThreadId } from "effect-agent/identifiers";
import { CanonicalRecordEnvelope, CanonicalSequence } from "effect-agent/records";
import { ThreadRead, ThreadStore, ThreadTail } from "effect-agent/thread-store";
import { expect } from "vite-plus/test";

import { workerHistory } from "../src/server/worker-history.ts";

const threadId = Schema.decodeSync(ThreadId)("worker:history");

const record = (sequence: number) =>
  Schema.decodeSync(CanonicalRecordEnvelope)({
    threadId,
    batchId: "batch",
    sequence,
    offset: `offset-${sequence}`,
    record: {
      recordId: `record-${sequence}`,
      family: "thread",
      schemaVersion: 1,
      createdAt: "2026-09-14T00:00:00.000Z",
      deploymentId: "test",
      payload: {
        _tag: "RunStarted",
        runId: `run-${sequence}`,
        policyAccountingVersion: 1,
        maxDurationMillis: 1000,
      },
    },
  });

const fixture = (
  tailSequence: number,
  mode: "normal" | "gap" | "foreign" | "short" | "defect" = "normal",
) => {
  const requests: ThreadRead[] = [];
  let consumed = 0;
  let closed = false;

  const store = ThreadStore.of({
    materialize: () => Effect.die("Reads cannot materialize a thread"),
    append: () => Effect.die("Reads cannot append"),
    export: () => Effect.die("Reads cannot export"),
    observe: () => Stream.die("Reads cannot observe"),
    readIdentity: () => Effect.die("History reads cannot look up thread identity"),
    inspectTail: () =>
      Effect.succeed(
        Schema.decodeSync(ThreadTail)({
          threadId,
          tailSequence,
          tailDigest: "0".repeat(64),
          producerEpoch: 1,
        }),
      ),
    read: (request) => {
      if ("selection" in request) return Stream.die("History reads require a sequence window");
      requests.push(request);

      return Stream.suspend(() =>
        mode === "defect"
          ? Stream.die("Broken adapter")
          : Stream.fromIteratorSucceed(
              (function* () {
                const start = request.afterSequence ?? 0;

                for (let sequence = start + 1; sequence <= start + request.limit; sequence++) {
                  if (mode === "short" && sequence === tailSequence) return;
                  consumed++;
                  const entry = record(mode === "gap" ? sequence + 1 : sequence);

                  yield mode === "foreign"
                    ? { ...entry, threadId: Schema.decodeSync(ThreadId)("other-worker") }
                    : entry;
                }
              })(),
            ),
      ).pipe(
        Stream.ensuring(
          Effect.sync(() => {
            closed = true;
          }),
        ),
      );
    },
  });

  return { store, requests, consumed: () => consumed, closed: () => closed };
};

it.effect.each([0, 3, 101, 10_000])(
  "reads only the final window of a %i-record history and closes its stream",
  (tail) =>
    Effect.gen(function* () {
      const test = fixture(tail);

      const entries = yield* workerHistory(threadId).pipe(
        Effect.provideService(ThreadStore, test.store),
      );

      expect(entries.map((entry) => entry.sequence)).toEqual(
        Array.from(
          { length: Math.min(tail, 100) },
          (_, index) => Math.max(0, tail - 100) + index + 1,
        ),
      );
      expect(test.requests).toEqual(
        tail === 0
          ? []
          : [
              ThreadRead.make({
                threadId,
                afterSequence: Schema.decodeSync(CanonicalSequence)(Math.max(0, tail - 100)),
                limit: Math.min(tail, 100),
              }),
            ],
      );
      expect(test.consumed()).toBe(Math.min(tail, 100));
      expect(test.closed()).toBe(tail > 0);
    }),
);

it.effect.each(["gap", "foreign", "short", "defect"] as const)(
  "fails closed for %s history and releases the reader",
  (mode) =>
    Effect.gen(function* () {
      const test = fixture(101, mode);

      const exit = yield* Effect.exit(
        workerHistory(threadId).pipe(Effect.provideService(ThreadStore, test.store)),
      );

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) expect(Cause.hasDies(exit.cause)).toBe(mode === "defect");
      expect(test.closed()).toBe(true);
    }),
);

it.effect("interrupts a stalled history read and runs its finalizer without writing", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    let finalized = false;
    const test = fixture(101);

    const reading = yield* workerHistory(threadId).pipe(
      Effect.provideService(ThreadStore, {
        ...test.store,
        read: () =>
          Stream.fromEffect(
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          ).pipe(
            Stream.ensuring(
              Effect.sync(() => {
                finalized = true;
              }),
            ),
          ),
      }),
      Effect.forkChild,
    );

    yield* Deferred.await(entered);
    yield* Fiber.interrupt(reading);
    expect(finalized).toBe(true);
  }),
);
