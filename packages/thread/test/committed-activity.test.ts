import { ThreadId } from "@effect-agent/core";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Clock,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";

import type { PreparedActivity } from "../src/index.ts";
import {
  ActivityClaim,
  ActivityMutationFailure,
  ActivityPassLimits,
  ActivityProcessingError,
  ActivityProcessorKey,
  ActivityProcessorStore,
  BatchId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  DeploymentId,
  EMPTY_TAIL_DIGEST,
  ObservationOffset,
  ProducerEpoch,
  RecordEnvelope,
  RecordId,
  ThreadStore,
  ThreadTail,
  UserInputRecorded,
  processCommittedActivity,
} from "../src/index.ts";

const threadId = Schema.decodeSync(ThreadId)("dan-chad");
const sequence = Schema.decodeSync(CanonicalSequence);

const key = ActivityProcessorKey.make({
  processorId: "discussion",
  processorVersion: "1",
  threadId,
});

const limits = ActivityPassLimits.make({
  maxRecords: 2,
  pageSize: 1,
  timeoutMillis: 1_000,
  leaseMillis: 2_000,
});

const record = (number: number, text = `statement ${number}`, offset = `offset:${number}`) =>
  CanonicalRecordEnvelope.make({
    threadId,
    batchId: Schema.decodeSync(BatchId)(`batch:${number}`),
    sequence: sequence(number),
    offset: Schema.decodeSync(ObservationOffset)(offset),
    record: RecordEnvelope.make({
      recordId: Schema.decodeSync(RecordId)(`record:${number}`),
      family: "thread",
      schemaVersion: 1,
      createdAt: DateTime.makeUnsafe(10),
      deploymentId: Schema.decodeSync(DeploymentId)("test"),
      payload: UserInputRecorded.make({ kind: "user", input: text }),
    }),
  });

class ExtractionUnavailable extends Schema.TaggedError<ExtractionUnavailable>()(
  "ExtractionUnavailable",
  {},
) {}

/** A workflow probe, not a storage implementation. SQLite tests own transaction/fencing semantics. */
const probe = (initial: ReadonlyArray<CanonicalRecordEnvelope> = [record(1)]) => {
  const state = {
    records: [...initial],
    through: sequence(0),
    pending: null as PreparedActivity | null,
    epoch: 0,
    prepared: [] as Array<PreparedActivity>,
    released: 0,
    pages: [] as Array<number>,
    prepareLostAck: false,
    advanceLostAck: false,
  };

  const store = ActivityProcessorStore.of({
    inspect: () => Effect.die("The processor must use its claim, not a separate progress read"),
    claim: (request) =>
      Effect.gen(function* () {
        state.epoch += 1;

        return ActivityClaim.make({
          ...request,
          epoch: state.epoch,
          throughSequence: state.through,
          pending: state.pending,
          leaseExpiresAt: (yield* Clock.currentTimeMillis) + request.leaseMillis,
        });
      }),
    prepare: ({ work }) =>
      Effect.gen(function* () {
        state.pending = work;
        state.prepared.push(work);
        if (state.prepareLostAck) {
          state.prepareLostAck = false;

          return yield* ActivityMutationFailure.make({ point: "activity:prepare:after" });
        }

        return work;
      }),
    advance: ({ claim }) =>
      Effect.gen(function* () {
        state.through = sequence(claim.throughSequence + 1);
        state.pending = null;
        if (state.advanceLostAck) {
          state.advanceLostAck = false;

          return yield* ActivityMutationFailure.make({ point: "activity:advance:after" });
        }

        return ActivityClaim.make({ ...claim, throughSequence: state.through, pending: null });
      }),
    release: () =>
      Effect.sync(() => {
        state.released += 1;
      }),
  });

  const threads = ThreadStore.of({
    materialize: () => Effect.die("Ingestion must not materialize Threads"),
    append: () => Effect.die("Ingestion must not append canonical records"),
    export: () => Effect.die("Ingestion must use bounded reads"),
    observe: () => Stream.die("Ingestion must not start an unbounded observation"),
    inspectTail: () =>
      Effect.succeed(
        ThreadTail.make({
          threadId,
          tailSequence: sequence(state.records.length),
          tailDigest: EMPTY_TAIL_DIGEST,
          producerEpoch: Schema.decodeSync(ProducerEpoch)(0),
        }),
      ),
    read: (request) => {
      state.pages.push(request.limit);

      return Stream.fromIterable(
        state.records.slice(
          request.afterSequence ?? 0,
          (request.afterSequence ?? 0) + request.limit,
        ),
      );
    },
  });

  const layer = Layer.mergeAll(
    Layer.succeed(ActivityProcessorStore, store),
    Layer.succeed(ThreadStore, threads),
    NodeCrypto.layer,
  );

  return { state, store, threads, layer };
};

describe("finite committed activity processing", () => {
  it.effect("captures a bounded prefix and leaves later commits for another pass", () => {
    const p = probe([record(1), record(2), record(3)]);
    const applied: Array<number> = [];

    const options = {
      key,
      owner: "worker",
      limits,
      extract: (value: CanonicalRecordEnvelope) =>
        Effect.sync(() => {
          if (value.sequence === 1) p.state.records.push(record(4));

          return value.record.payload._tag;
        }),
      apply: (work: PreparedActivity) =>
        Effect.sync(() => {
          applied.push(work.sequence);
        }),
    };

    return Effect.gen(function* () {
      const first = yield* processCommittedActivity(options);

      expect(first).toMatchObject({
        capturedTail: 3,
        throughSequence: 2,
        processed: 2,
        pendingRecords: 1,
      });
      expect(applied).toEqual([1, 2]);
      const second = yield* processCommittedActivity(options);

      expect(second).toMatchObject({
        capturedTail: 4,
        throughSequence: 4,
        processed: 2,
        pendingRecords: 0,
      });
      expect(p.state.pages).toEqual([1, 1, 1, 1]);
      expect(applied).toEqual([1, 2, 3, 4]);
      expect(p.state.released).toBe(2);
    }).pipe(Effect.provide(p.layer));
  });

  it.effect(
    "reuses pinned output after lost prepare acknowledgement even when the observation cursor changes",
    () => {
      const p = probe();

      p.state.prepareLostAck = true;
      let extractions = 0;
      const applied: Array<PreparedActivity> = [];

      const options = {
        key,
        owner: "worker",
        limits,
        extract: () =>
          Effect.sync(() => {
            extractions += 1;

            return `extraction ${extractions}`;
          }),
        apply: (work: PreparedActivity) =>
          Effect.sync(() => {
            applied.push(work);
          }),
      };

      return Effect.gen(function* () {
        expect(yield* processCommittedActivity(options).pipe(Effect.flip)).toEqual(
          ActivityMutationFailure.make({ point: "activity:prepare:after" }),
        );
        expect(p.state.through).toBe(0);
        p.state.records = [record(1, "statement 1", "restored-cursor")];
        yield* processCommittedActivity(options);
        expect(extractions).toBe(1);
        expect(applied).toEqual(p.state.prepared);
        expect(applied[0].output).toBe("extraction 1");
      }).pipe(Effect.provide(p.layer));
    },
  );

  it.effect(
    "replays applied work before progress and resumes beyond an acknowledged-lost advance",
    () => {
      const p = probe();
      let extractions = 0;
      let attempts = 0;
      const receipts = new Map<string, PreparedActivity>();

      const options = {
        key,
        owner: "worker",
        limits,
        extract: () =>
          Effect.sync(() => {
            extractions += 1;

            return "pinned";
          }),
        apply: (work: PreparedActivity) =>
          Effect.gen(function* () {
            attempts += 1;
            receipts.set(work.workId, work);
            if (attempts === 1) return yield* new ExtractionUnavailable();
          }),
      };

      return Effect.gen(function* () {
        expect(yield* processCommittedActivity(options).pipe(Effect.flip)).toEqual(
          new ExtractionUnavailable(),
        );
        expect(p.state.through).toBe(0);
        p.state.advanceLostAck = true;
        expect(yield* processCommittedActivity(options).pipe(Effect.flip)).toEqual(
          ActivityMutationFailure.make({ point: "activity:advance:after" }),
        );
        const resumed = yield* processCommittedActivity(options);

        expect(resumed.processed).toBe(0);
        expect(resumed.throughSequence).toBe(1);
        expect(extractions).toBe(1);
        expect(attempts).toBe(2);
        expect(receipts.size).toBe(1);
      }).pipe(Effect.provide(p.layer));
    },
  );

  it.effect(
    "rejects gaps, truncated tails, and changed content before applying pinned output",
    () =>
      Effect.gen(function* () {
        const gap = probe([record(2)]);
        let applied = 0;

        const options = {
          key,
          owner: "worker",
          limits,
          extract: () => Effect.succeed(null),
          apply: () =>
            Effect.sync(() => {
              applied += 1;
            }),
        };

        const gapError = yield* processCommittedActivity(options).pipe(
          Effect.provide(gap.layer),
          Effect.flip,
        );

        expect(gapError).toMatchObject({
          _tag: "ActivityProcessingError",
          reason: "noncontiguous",
        });
        const changed = probe();

        changed.state.prepareLostAck = true;
        yield* processCommittedActivity(options).pipe(Effect.provide(changed.layer), Effect.exit);
        const pending = changed.state.pending;

        changed.state.records = [];
        expect(
          yield* processCommittedActivity(options).pipe(Effect.provide(changed.layer), Effect.flip),
        ).toMatchObject({ _tag: "ActivityProcessingError", reason: "noncontiguous" });
        expect(changed.state.pending).toEqual(pending);
        expect(changed.state.through).toBe(0);
        expect(changed.state.released).toBe(2);
        expect(changed.state.pages).toEqual([1]);
        changed.state.records = [record(1, "mutated canonical source")];

        const changedError = yield* processCommittedActivity(options).pipe(
          Effect.provide(changed.layer),
          Effect.flip,
        );

        expect(changedError).toMatchObject({
          _tag: "ActivityProcessingError",
          reason: "noncontiguous",
        });
        expect(applied).toBe(0);
        expect(changed.state.through).toBe(0);
      }),
  );

  it.effect(
    "keeps expected failures and defects visible and rejects malformed callback output",
    () =>
      Effect.gen(function* () {
        for (const scenario of [
          {
            extract: () => Effect.fail(new ExtractionUnavailable()),
            expected: new ExtractionUnavailable(),
          },
          { extract: () => Effect.die("extract defect"), expected: "extract defect" },
          {
            extract: () => Effect.succeed(Number.NaN),
            expected: ActivityProcessingError.make({
              reason: "invalid-input",
              message: "Malformed activity output",
            }),
          },
        ]) {
          const p = probe();

          const exit = yield* processCommittedActivity({
            key,
            owner: "worker",
            limits,
            extract: scenario.extract,
            apply: () => Effect.die("No invalid output may apply"),
          }).pipe(Effect.provide(p.layer), Effect.exit);

          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toEqual(scenario.expected);
          expect(p.state.prepared).toHaveLength(0);
          expect(p.state.through).toBe(0);
          expect(p.state.released).toBe(1);
        }
      }),
  );

  it.effect("finalizes scoped callbacks on timeout and interruption without advancing", () =>
    Effect.gen(function* () {
      for (const boundary of ["extract", "apply"] as const) {
        for (const mode of ["timeout", "interrupt"] as const) {
          const p = probe();
          const entered = yield* Deferred.make<void>();
          let released = 0;

          const waiting = Effect.gen(function* () {
            yield* Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
              Effect.sync(() => {
                released += 1;
              }),
            );

            return yield* Effect.never;
          });

          const fiber = yield* processCommittedActivity({
            key,
            owner: "worker",
            limits,
            extract: () => (boundary === "extract" ? waiting : Effect.succeed(null)),
            apply: () =>
              boundary === "apply" ? waiting : Effect.die("No interrupted extraction may apply"),
          }).pipe(Effect.provide(p.layer), Effect.forkChild);

          yield* Deferred.await(entered);
          if (mode === "timeout") yield* TestClock.adjust(1_000);
          else yield* Fiber.interrupt(fiber);
          const exit = yield* Fiber.await(fiber);

          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            if (mode === "interrupt") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
            else
              expect(Cause.squash(exit.cause)).toMatchObject({
                _tag: "ActivityProcessingError",
                reason: "timeout",
              });
          }
          expect(released).toBe(1);
          expect(p.state.released).toBe(1);
          expect(p.state.through).toBe(0);
          expect(p.state.pending !== null).toBe(boundary === "apply");
        }
      }
    }),
  );

  it.effect("bounds a hanging release independently of the pass timeout", () =>
    Effect.gen(function* () {
      const p = probe();
      const entered = yield* Deferred.make<void>();
      let releaseFinalized = false;

      const service = ActivityProcessorStore.of({
        ...p.store,
        release: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Effect.sync(() => {
                releaseFinalized = true;
              }),
            ),
          ),
      });

      const fiber = yield* processCommittedActivity({
        key,
        owner: "worker",
        limits,
        extract: () => Effect.succeed(null),
        apply: () => Effect.void,
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(ActivityProcessorStore, service),
            Layer.succeed(ThreadStore, p.threads),
            NodeCrypto.layer,
          ),
        ),
        Effect.forkChild,
      );

      yield* Deferred.await(entered);
      yield* TestClock.adjust(500);
      expect((yield* Fiber.join(fiber)).throughSequence).toBe(1);
      expect(releaseFinalized).toBe(true);
    }),
  );
});
