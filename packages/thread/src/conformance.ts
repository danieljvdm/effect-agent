import { ThreadId, RunId, SubmissionId } from "@effect-agent/core";
import { Effect, DateTime, Option, Schema, Stream } from "effect";

import { EMPTY_TAIL_DIGEST } from "./digest.ts";
import type { Digest } from "./records.ts";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  DeploymentId,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  UserInputRecorded,
} from "./records.ts";
import {
  type AppendResult,
  AppendConflict,
  CheckpointRejected,
  ThreadCheckpoint,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadObservation,
  ThreadRead,
  ThreadStore,
  ThreadTailRequest,
  FencedAppendRequest,
  FenceRejected,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  type ThreadStoreFailure,
} from "./store.ts";

/** A ThreadStore contract invariant that an adapter under test violated. */
export class ThreadStoreConformanceViolation extends Schema.TaggedError<ThreadStoreConformanceViolation>()(
  "ThreadStoreConformanceViolation",
  {
    caseName: Schema.String,
    message: Schema.String,
  },
) {}

export type ThreadStoreConformanceFailure =
  | ThreadStoreFailure
  | CheckpointRejected
  | ThreadStoreConformanceViolation;

/**
 * One adapter-neutral ThreadStore contract case. Each case owns disjoint
 * Thread identities, so a suite may run every case against one shared store
 * instance or against a fresh store per case.
 */
export interface ThreadStoreConformanceCase {
  readonly name: string;
  readonly run: Effect.Effect<void, ThreadStoreConformanceFailure, ThreadStore>;
}

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeRecordId = Schema.decodeSync(RecordId);
const decodeBatchId = Schema.decodeSync(BatchId);
const decodeSequence = Schema.decodeSync(CanonicalSequence);
const decodeEpoch = Schema.decodeSync(ProducerEpoch);

const CONFORMANCE_DEPLOYMENT = Schema.decodeSync(DeploymentId)("deployment-conformance");
const CONFORMANCE_PRODUCER = Schema.decodeSync(ProducerId)("producer-conformance");
const CONFORMANCE_SUBMISSION = Schema.decodeSync(SubmissionId)("submission-conformance");
const CONFORMANCE_RUN = Schema.decodeSync(RunId)("run-conformance");
const CONFORMANCE_CREATED_AT = DateTime.toUtc(DateTime.makeUnsafe(1));

const ZERO_SEQUENCE = decodeSequence(0);
const EPOCH_ONE = decodeEpoch(1);
const EPOCH_TWO = decodeEpoch(2);
const EMPTY_TAIL = { lastSequence: ZERO_SEQUENCE, tailDigest: EMPTY_TAIL_DIGEST } as const;
const isAppendConflict = Schema.is(AppendConflict);
const isFenceRejected = Schema.is(FenceRejected);
const isCheckpointRejected = Schema.is(CheckpointRejected);
const isThreadNotMaterialized = Schema.is(ThreadNotMaterialized);

const record = (recordId: string, input: string): RecordEnvelope =>
  RecordEnvelope.make({
    recordId: decodeRecordId(recordId),
    family: "thread",
    schemaVersion: 1,
    createdAt: CONFORMANCE_CREATED_AT,
    deploymentId: CONFORMANCE_DEPLOYMENT,
    payload: UserInputRecorded.make({
      submissionId: CONFORMANCE_SUBMISSION,
      kind: "user",
      runId: CONFORMANCE_RUN,
      input,
    }),
  });

const batch = (
  batchId: string,
  records: readonly [RecordEnvelope, ...Array<RecordEnvelope>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: decodeBatchId(batchId),
    producerId: CONFORMANCE_PRODUCER,
    records,
  });

const materialize = Effect.fn("ThreadStoreConformance.materialize")(function* (
  threadId: ThreadId,
  producerEpoch: ProducerEpoch,
) {
  const store = yield* ThreadStore;
  yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch }));
});

const append = Effect.fn("ThreadStoreConformance.append")(function* (
  threadId: ThreadId,
  canonicalBatch: CanonicalBatch,
  tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = EMPTY_TAIL,
  producerEpoch: ProducerEpoch = EPOCH_ONE,
) {
  const store = yield* ThreadStore;
  return yield* store.append(
    FencedAppendRequest.make({
      threadId,
      batch: canonicalBatch,
      expectedTailSequence: tail.lastSequence,
      expectedTailDigest: tail.tailDigest,
      producerEpoch,
    }),
  );
});

const readAll = Effect.fn("ThreadStoreConformance.readAll")(function* (threadId: ThreadId) {
  const store = yield* ThreadStore;
  return yield* store.read(ThreadRead.make({ threadId, limit: 1_024 })).pipe(Stream.runCollect);
});

const checkpointAt = (
  threadId: ThreadId,
  throughSequence: CanonicalSequence,
  tailDigest: Digest,
): ThreadCheckpoint =>
  ThreadCheckpoint.make({
    schemaVersion: 1,
    threadId,
    throughSequence,
    tailDigest,
    engineVersion: "conformance",
    agentDefinitionDigest: tailDigest,
    modelDigest: tailDigest,
    toolDigest: tailDigest,
    state: { conformance: true },
    createdAt: CONFORMANCE_CREATED_AT,
  });

const conformanceCase = (
  name: string,
  build: (assert: {
    readonly ensure: (
      condition: boolean,
      message: string,
    ) => Effect.Effect<void, ThreadStoreConformanceViolation>;
    readonly expectFailure: <A, E, R>(
      description: string,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<E, ThreadStoreConformanceViolation, R>;
  }) => Effect.Effect<void, ThreadStoreConformanceFailure, ThreadStore>,
): ThreadStoreConformanceCase => ({
  name,
  run: build({
    ensure: (condition, message) =>
      condition
        ? Effect.void
        : Effect.fail(ThreadStoreConformanceViolation.make({ caseName: name, message })),
    expectFailure: (description, effect) =>
      Effect.flip(effect).pipe(
        Effect.mapError(() =>
          ThreadStoreConformanceViolation.make({
            caseName: name,
            message: `Expected failure but the operation succeeded: ${description}`,
          }),
        ),
      ),
  }).pipe(Effect.withSpan(`ThreadStoreConformance.${name}`)),
});

const atomicBatchVisibility = conformanceCase(
  "commits batches atomically with no partial reader visibility",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId("conformance-atomic");
      const store = yield* ThreadStore;
      yield* materialize(threadId, EPOCH_ONE);

      const committed = yield* append(
        threadId,
        batch("atomic-batch-1", [
          record("atomic-record-1", "first"),
          record("atomic-record-2", "second"),
          record("atomic-record-3", "third"),
        ]),
      );
      yield* ensure(
        committed.firstSequence === 1 && committed.lastSequence === 3,
        "A three-record batch must commit sequences 1 through 3",
      );

      const read = yield* readAll(threadId);
      yield* ensure(read.length === 3, "Readers must observe the full committed batch");
      yield* ensure(
        read.every((envelope, index) => envelope.sequence === index + 1),
        "Committed records must be gap-free in declaration order",
      );

      const observed = yield* store
        .observe(ThreadObservation.make({ threadId }))
        .pipe(Stream.take(3), Stream.runCollect);
      yield* ensure(
        observed.map((envelope) => envelope.record.recordId).join(",") ===
          read.map((envelope) => envelope.record.recordId).join(","),
        "Observers must see exactly the committed batch records in order",
      );

      const duplicate = record("atomic-record-4", "duplicate");
      const intraBatch = yield* expectFailure(
        "a batch containing one record ID twice",
        append(threadId, batch("atomic-batch-2", [duplicate, duplicate]), committed),
      );
      yield* ensure(
        isAppendConflict(intraBatch) && intraBatch.reason === "record-identity",
        "A duplicated record ID inside one batch must conflict with reason record-identity",
      );

      const crossBatch = yield* expectFailure(
        "a batch reusing an already-committed record ID",
        append(threadId, batch("atomic-batch-3", [record("atomic-record-1", "reused")]), committed),
      );
      yield* ensure(
        isAppendConflict(crossBatch) && crossBatch.reason === "record-identity",
        "A record ID reused across batches must conflict with reason record-identity",
      );

      const exported = yield* store.export(ThreadExportRequest.make({ threadId }));
      yield* ensure(
        exported.records.length === 3 &&
          exported.tailSequence === committed.lastSequence &&
          exported.tailDigest === committed.tailDigest,
        "A conflicting batch must leave no partial prefix behind",
      );
    }),
);

const idempotentReplay = conformanceCase(
  "replays an identical batch idempotently and rejects altered content",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId("conformance-replay");
      yield* materialize(threadId, EPOCH_ONE);

      const original = batch("replay-batch-1", [
        record("replay-record-1", "Lisbon"),
        record("replay-record-2", "Porto"),
      ]);
      const first = yield* append(threadId, original);
      yield* ensure(!first.replayed, "The first append of a batch must not report replayed");

      const replayed = yield* append(threadId, original);
      yield* ensure(
        replayed.replayed &&
          replayed.firstSequence === first.firstSequence &&
          replayed.lastSequence === first.lastSequence &&
          replayed.tailDigest === first.tailDigest,
        "Replaying an identical batch must return the original result with replayed set",
      );

      const altered = yield* expectFailure(
        "replaying a batch ID with different canonical content",
        append(threadId, batch("replay-batch-1", [record("replay-record-3", "Faro")])),
      );
      yield* ensure(
        isAppendConflict(altered) && altered.reason === "batch-digest",
        "A batch ID replayed with different content must conflict with reason batch-digest",
      );

      const read = yield* readAll(threadId);
      yield* ensure(read.length === 2, "Replay and conflict must not duplicate committed records");
    }),
);

const tailConflict = conformanceCase(
  "rejects stale expected tails and reports the actual tail",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId("conformance-tail");
      yield* materialize(threadId, EPOCH_ONE);
      const first = yield* append(
        threadId,
        batch("tail-batch-1", [record("tail-record-1", "first")]),
      );

      const staleSequence = yield* expectFailure(
        "an append declaring the pre-append tail sequence",
        append(threadId, batch("tail-batch-2", [record("tail-record-2", "second")])),
      );
      yield* ensure(
        isAppendConflict(staleSequence) && staleSequence.reason === "tail",
        "A stale expected tail sequence must conflict with reason tail",
      );
      yield* ensure(
        isAppendConflict(staleSequence) &&
          staleSequence.actualTailSequence === first.lastSequence &&
          staleSequence.actualTailDigest === first.tailDigest,
        "A tail conflict must carry the actual committed tail as a resume hint",
      );

      const staleDigest = yield* expectFailure(
        "an append declaring the right sequence with the wrong digest",
        append(threadId, batch("tail-batch-2", [record("tail-record-2", "second")]), {
          lastSequence: first.lastSequence,
          tailDigest: EMPTY_TAIL_DIGEST,
        }),
      );
      yield* ensure(
        isAppendConflict(staleDigest) && staleDigest.reason === "tail",
        "A stale expected tail digest must conflict with reason tail",
      );

      const recovered = yield* append(
        threadId,
        batch("tail-batch-2", [record("tail-record-2", "second")]),
        first,
      );
      yield* ensure(
        recovered.firstSequence === first.lastSequence + 1,
        "An append declaring the true tail must commit after a tail conflict",
      );
    }),
);

const producerFencing = conformanceCase(
  "fences stale producer epochs while allowing epoch takeover",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId("conformance-fencing");
      yield* materialize(threadId, EPOCH_ONE);
      const first = yield* append(
        threadId,
        batch("fence-batch-1", [record("fence-record-1", "first")]),
      );

      yield* materialize(threadId, EPOCH_TWO);

      const staleAppend = yield* expectFailure(
        "an append using the pre-takeover producer epoch",
        append(
          threadId,
          batch("fence-batch-2", [record("fence-record-2", "second")]),
          first,
          EPOCH_ONE,
        ),
      );
      yield* ensure(
        isFenceRejected(staleAppend) &&
          staleAppend.actualEpoch === EPOCH_TWO &&
          staleAppend.attemptedEpoch === EPOCH_ONE,
        "A stale producer epoch must be fenced with both epochs reported",
      );

      const staleMaterialize = yield* expectFailure(
        "re-materializing with the pre-takeover producer epoch",
        materialize(threadId, EPOCH_ONE),
      );
      yield* ensure(
        isFenceRejected(staleMaterialize),
        "A stale producer epoch must not re-register through materialization",
      );

      const takeover = yield* append(
        threadId,
        batch("fence-batch-2", [record("fence-record-2", "second")]),
        first,
        EPOCH_TWO,
      );
      yield* ensure(
        !takeover.replayed && takeover.firstSequence === first.lastSequence + 1,
        "The takeover epoch must append after the committed tail",
      );
    }),
);

const offsetResumability = conformanceCase(
  "resumes observation from a saved offset with exactly the suffix",
  ({ ensure }) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId("conformance-offsets");
      const store = yield* ThreadStore;
      yield* materialize(threadId, EPOCH_ONE);
      const first = yield* append(
        threadId,
        batch("offset-batch-1", [
          record("offset-record-1", "first"),
          record("offset-record-2", "second"),
        ]),
      );
      const second = yield* append(
        threadId,
        batch("offset-batch-2", [record("offset-record-3", "third")]),
        first,
      );
      yield* append(
        threadId,
        batch("offset-batch-3", [record("offset-record-4", "fourth")]),
        second,
      );

      const committed = yield* readAll(threadId);
      yield* ensure(committed.length === 4, "All committed records must be readable");
      const savedOffset = committed[1]?.offset;
      yield* ensure(savedOffset !== undefined, "Committed records must carry observation offsets");
      if (savedOffset === undefined) return;

      const resumed = yield* store
        .observe(ThreadObservation.make({ threadId, afterOffset: savedOffset }))
        .pipe(Stream.take(2), Stream.runCollect);
      yield* ensure(
        resumed.map((envelope) => envelope.record.recordId).join(",") ===
          committed
            .slice(2)
            .map((envelope) => envelope.record.recordId)
            .join(","),
        "Observation from a saved offset must deliver exactly the suffix in order",
      );

      const fromStart = yield* store
        .observe(ThreadObservation.make({ threadId }))
        .pipe(Stream.take(4), Stream.runCollect);
      yield* ensure(
        fromStart.map((envelope) => envelope.record.recordId).join(",") ===
          committed.map((envelope) => envelope.record.recordId).join(","),
        "Observation without an offset must deliver the full log in order",
      );
    }),
);

const checkpointBoundaries = conformanceCase(
  "enforces checkpoint save and load boundary rules",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId("conformance-checkpoints");
      const store = yield* ThreadStore;
      const checkpoints = store.checkpoints;
      if (checkpoints === undefined) {
        return yield* ThreadStoreConformanceViolation.make({
          caseName: "enforces checkpoint save and load boundary rules",
          message: "The optional checkpoint suite requires checkpoint support",
        });
      }
      const missing = [
        yield* expectFailure(
          "saving a checkpoint for an unmaterialized Thread",
          checkpoints.save(
            SaveCheckpointRequest.make({
              checkpoint: checkpointAt(threadId, ZERO_SEQUENCE, EMPTY_TAIL_DIGEST),
            }),
          ),
        ),
        yield* expectFailure(
          "loading a checkpoint for an unmaterialized Thread",
          checkpoints.load(LoadCheckpointRequest.make({ threadId })),
        ),
      ];
      yield* ensure(
        missing.every(isThreadNotMaterialized),
        "Checkpoints require a materialized Thread",
      );
      yield* materialize(threadId, EPOCH_ONE);
      const first = yield* append(
        threadId,
        batch("checkpoint-batch-1", [record("checkpoint-record-1", "Kyoto")]),
      );

      const aheadOfTail = yield* expectFailure(
        "a checkpoint claiming a sequence beyond the committed tail",
        checkpoints.save(
          SaveCheckpointRequest.make({
            checkpoint: checkpointAt(
              threadId,
              decodeSequence(first.lastSequence + 1),
              first.tailDigest,
            ),
          }),
        ),
      );
      yield* ensure(
        isCheckpointRejected(aheadOfTail) && aheadOfTail.reason === "ahead-of-tail",
        "A checkpoint ahead of the tail must be rejected with reason ahead-of-tail",
      );

      const digestMismatch = yield* expectFailure(
        "a checkpoint whose digest does not match the canonical chain",
        checkpoints.save(
          SaveCheckpointRequest.make({
            checkpoint: checkpointAt(threadId, first.lastSequence, EMPTY_TAIL_DIGEST),
          }),
        ),
      );
      yield* ensure(
        isCheckpointRejected(digestMismatch) && digestMismatch.reason === "digest-mismatch",
        "A checkpoint with a mismatched digest must be rejected with reason digest-mismatch",
      );
      yield* ensure(
        Option.isNone(yield* checkpoints.load(LoadCheckpointRequest.make({ threadId }))),
        "Rejected checkpoints must not become loadable",
      );

      const valid = checkpointAt(threadId, first.lastSequence, first.tailDigest);
      yield* checkpoints.save(SaveCheckpointRequest.make({ checkpoint: valid }));
      yield* checkpoints.save(SaveCheckpointRequest.make({ checkpoint: valid }));

      const loaded = yield* checkpoints.load(LoadCheckpointRequest.make({ threadId }));
      yield* ensure(
        Option.isSome(loaded) &&
          loaded.value.throughSequence === first.lastSequence &&
          loaded.value.tailDigest === first.tailDigest,
        "A valid checkpoint must load bound to its canonical sequence and digest",
      );

      const beforeCheckpoint = yield* checkpoints.load(
        LoadCheckpointRequest.make({ threadId, atOrBeforeSequence: ZERO_SEQUENCE }),
      );
      yield* ensure(
        Option.isNone(beforeCheckpoint),
        "Checkpoint lookup must respect the atOrBeforeSequence bound",
      );

      yield* append(
        threadId,
        batch("checkpoint-batch-2", [record("checkpoint-record-2", "Nara")]),
        first,
      );
      const afterAppend = yield* checkpoints.load(LoadCheckpointRequest.make({ threadId }));
      yield* ensure(
        Option.isSome(afterAppend) && afterAppend.value.throughSequence === first.lastSequence,
        "An older checkpoint must remain loadable after later appends",
      );
    }),
);

const tailInspection = conformanceCase(
  "inspects the canonical tail so a resuming producer can compose its next append",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId("conformance-tail-inspection");
      const store = yield* ThreadStore;

      const missing = yield* expectFailure(
        "inspecting the tail of an unmaterialized Thread",
        store.inspectTail(ThreadTailRequest.make({ threadId })),
      );
      yield* ensure(
        isThreadNotMaterialized(missing) && missing.threadId === threadId,
        "Tail inspection of an unmaterialized Thread must fail as not materialized",
      );

      yield* materialize(threadId, EPOCH_ONE);
      const empty = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
      yield* ensure(
        empty.tailSequence === ZERO_SEQUENCE &&
          empty.tailDigest === EMPTY_TAIL_DIGEST &&
          empty.producerEpoch === EPOCH_ONE,
        "An empty Thread must report the zero tail and its registered epoch",
      );

      const first = yield* append(
        threadId,
        batch("inspect-batch-1", [record("inspect-record-1", "first")]),
      );
      const afterAppend = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
      yield* ensure(
        afterAppend.tailSequence === first.lastSequence &&
          afterAppend.tailDigest === first.tailDigest,
        "Tail inspection must match the latest AppendResult",
      );

      const resumed = yield* append(
        threadId,
        batch("inspect-batch-2", [record("inspect-record-2", "second")]),
        { lastSequence: afterAppend.tailSequence, tailDigest: afterAppend.tailDigest },
        afterAppend.producerEpoch,
      );
      yield* ensure(
        resumed.firstSequence === afterAppend.tailSequence + 1,
        "An append composed from the inspected tail must commit without exporting the log",
      );

      yield* materialize(threadId, EPOCH_TWO);
      const afterTakeover = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
      yield* ensure(
        afterTakeover.producerEpoch === EPOCH_TWO &&
          afterTakeover.tailSequence === resumed.lastSequence,
        "Tail inspection must reflect epoch takeover without disturbing the tail",
      );
    }),
);

const notMaterializedOperations = conformanceCase(
  "fails every read and write against an unmaterialized Thread",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId("conformance-not-materialized");
      const store = yield* ThreadStore;

      const failures = [
        yield* expectFailure(
          "appending to an unmaterialized Thread",
          append(threadId, batch("missing-batch-1", [record("missing-record-1", "first")])),
        ),
        yield* expectFailure("reading an unmaterialized Thread", readAll(threadId)),
        yield* expectFailure(
          "observing an unmaterialized Thread",
          store
            .observe(ThreadObservation.make({ threadId }))
            .pipe(Stream.take(1), Stream.runCollect),
        ),
        yield* expectFailure(
          "exporting an unmaterialized Thread",
          store.export(ThreadExportRequest.make({ threadId })),
        ),
        yield* expectFailure(
          "inspecting the tail of an unmaterialized Thread",
          store.inspectTail(ThreadTailRequest.make({ threadId })),
        ),
      ];
      yield* Effect.forEach(
        failures,
        (failure) =>
          ensure(
            isThreadNotMaterialized(failure) && failure.threadId === threadId,
            "Operations against an unmaterialized Thread must fail as not materialized",
          ),
        { discard: true },
      );
    }),
);

/**
 * The shared, adapter-parameterized ThreadStore contract suite (STORE-010). Every
 * durable adapter test suite must execute each case against its own store provisioning.
 */
export const threadStoreConformanceCases: ReadonlyArray<ThreadStoreConformanceCase> = [
  atomicBatchVisibility,
  idempotentReplay,
  tailConflict,
  producerFencing,
  offsetResumability,
  tailInspection,
  notMaterializedOperations,
];

/** Additional contract cases only for adapters advertising `ThreadStore.checkpoints`. */
export const threadCheckpointConformanceCases: ReadonlyArray<ThreadStoreConformanceCase> = [
  checkpointBoundaries,
];
