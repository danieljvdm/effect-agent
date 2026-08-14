import { ConversationId, RunId, SubmissionId } from "@effect-agent/core";
import { Effect, DateTime, Option, Schema, Stream } from "effect";

import { EMPTY_TAIL_DIGEST } from "./digest.ts";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  DeploymentId,
  Digest,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  UserInputRecorded,
} from "./records.ts";
import {
  AppendConflict,
  AppendResult,
  CheckpointRejected,
  ConversationCheckpoint,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationNotMaterialized,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  ConversationTailRequest,
  FencedAppendRequest,
  FenceRejected,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  type ConversationStoreFailure,
} from "./store.ts";

/** A ConversationStore contract invariant that an adapter under test violated. */
export class ConversationStoreConformanceViolation extends Schema.TaggedErrorClass<ConversationStoreConformanceViolation>()(
  "ConversationStoreConformanceViolation",
  {
    caseName: Schema.String,
    message: Schema.String,
  },
) {}

export type ConversationStoreConformanceFailure =
  | ConversationStoreFailure
  | ConversationStoreConformanceViolation;

/**
 * One adapter-neutral ConversationStore contract case. Each case owns disjoint
 * Conversation identities, so a suite may run every case against one shared store
 * instance or against a fresh store per case.
 */
export interface ConversationStoreConformanceCase {
  readonly name: string;
  readonly run: Effect.Effect<void, ConversationStoreConformanceFailure, ConversationStore>;
}

const decodeConversationId = Schema.decodeSync(ConversationId);
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

const record = (recordId: string, input: string): RecordEnvelope =>
  RecordEnvelope.make({
    recordId: decodeRecordId(recordId),
    family: "conversation",
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

const materialize = Effect.fn("ConversationStoreConformance.materialize")(function* (
  conversationId: ConversationId,
  producerEpoch: ProducerEpoch,
) {
  const store = yield* ConversationStore;
  yield* store.materialize(ConversationMaterialization.make({ conversationId, producerEpoch }));
});

const append = Effect.fn("ConversationStoreConformance.append")(function* (
  conversationId: ConversationId,
  canonicalBatch: CanonicalBatch,
  tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = EMPTY_TAIL,
  producerEpoch: ProducerEpoch = EPOCH_ONE,
) {
  const store = yield* ConversationStore;
  return yield* store.append(
    FencedAppendRequest.make({
      conversationId,
      batch: canonicalBatch,
      expectedTailSequence: tail.lastSequence,
      expectedTailDigest: tail.tailDigest,
      producerEpoch,
    }),
  );
});

const readAll = Effect.fn("ConversationStoreConformance.readAll")(function* (
  conversationId: ConversationId,
) {
  const store = yield* ConversationStore;
  return yield* store
    .read(ConversationRead.make({ conversationId, limit: 1_024 }))
    .pipe(Stream.runCollect);
});

const checkpointAt = (
  conversationId: ConversationId,
  throughSequence: CanonicalSequence,
  tailDigest: Digest,
): ConversationCheckpoint =>
  ConversationCheckpoint.make({
    schemaVersion: 1,
    conversationId,
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
    ) => Effect.Effect<void, ConversationStoreConformanceViolation>;
    readonly expectFailure: <A, E, R>(
      description: string,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<E, ConversationStoreConformanceViolation, R>;
  }) => Effect.Effect<void, ConversationStoreConformanceFailure, ConversationStore>,
): ConversationStoreConformanceCase => ({
  name,
  run: build({
    ensure: (condition, message) =>
      condition
        ? Effect.void
        : Effect.fail(ConversationStoreConformanceViolation.make({ caseName: name, message })),
    expectFailure: (description, effect) =>
      Effect.flip(effect).pipe(
        Effect.mapError(() =>
          ConversationStoreConformanceViolation.make({
            caseName: name,
            message: `Expected failure but the operation succeeded: ${description}`,
          }),
        ),
      ),
  }).pipe(Effect.withSpan(`ConversationStoreConformance.${name}`)),
});

const atomicBatchVisibility = conformanceCase(
  "commits batches atomically with no partial reader visibility",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("conformance-atomic");
      const store = yield* ConversationStore;
      yield* materialize(conversationId, EPOCH_ONE);

      const committed = yield* append(
        conversationId,
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

      const read = yield* readAll(conversationId);
      yield* ensure(read.length === 3, "Readers must observe the full committed batch");
      yield* ensure(
        read.every((envelope, index) => envelope.sequence === index + 1),
        "Committed records must be gap-free in declaration order",
      );

      const observed = yield* store
        .observe(ConversationObservation.make({ conversationId }))
        .pipe(Stream.take(3), Stream.runCollect);
      yield* ensure(
        observed.map((envelope) => envelope.record.recordId).join(",") ===
          read.map((envelope) => envelope.record.recordId).join(","),
        "Observers must see exactly the committed batch records in order",
      );

      const duplicate = record("atomic-record-4", "duplicate");
      const intraBatch = yield* expectFailure(
        "a batch containing one record ID twice",
        append(conversationId, batch("atomic-batch-2", [duplicate, duplicate]), committed),
      );
      yield* ensure(
        intraBatch instanceof AppendConflict && intraBatch.reason === "record-identity",
        "A duplicated record ID inside one batch must conflict with reason record-identity",
      );

      const crossBatch = yield* expectFailure(
        "a batch reusing an already-committed record ID",
        append(
          conversationId,
          batch("atomic-batch-3", [record("atomic-record-1", "reused")]),
          committed,
        ),
      );
      yield* ensure(
        crossBatch instanceof AppendConflict && crossBatch.reason === "record-identity",
        "A record ID reused across batches must conflict with reason record-identity",
      );

      const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
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
      const conversationId = decodeConversationId("conformance-replay");
      yield* materialize(conversationId, EPOCH_ONE);

      const original = batch("replay-batch-1", [
        record("replay-record-1", "Lisbon"),
        record("replay-record-2", "Porto"),
      ]);
      const first = yield* append(conversationId, original);
      yield* ensure(!first.replayed, "The first append of a batch must not report replayed");

      const replayed = yield* append(conversationId, original);
      yield* ensure(
        replayed.replayed &&
          replayed.firstSequence === first.firstSequence &&
          replayed.lastSequence === first.lastSequence &&
          replayed.tailDigest === first.tailDigest,
        "Replaying an identical batch must return the original result with replayed set",
      );

      const altered = yield* expectFailure(
        "replaying a batch ID with different canonical content",
        append(conversationId, batch("replay-batch-1", [record("replay-record-3", "Faro")])),
      );
      yield* ensure(
        altered instanceof AppendConflict && altered.reason === "batch-digest",
        "A batch ID replayed with different content must conflict with reason batch-digest",
      );

      const read = yield* readAll(conversationId);
      yield* ensure(read.length === 2, "Replay and conflict must not duplicate committed records");
    }),
);

const tailConflict = conformanceCase(
  "rejects stale expected tails and reports the actual tail",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("conformance-tail");
      yield* materialize(conversationId, EPOCH_ONE);
      const first = yield* append(
        conversationId,
        batch("tail-batch-1", [record("tail-record-1", "first")]),
      );

      const staleSequence = yield* expectFailure(
        "an append declaring the pre-append tail sequence",
        append(conversationId, batch("tail-batch-2", [record("tail-record-2", "second")])),
      );
      yield* ensure(
        staleSequence instanceof AppendConflict && staleSequence.reason === "tail",
        "A stale expected tail sequence must conflict with reason tail",
      );
      yield* ensure(
        staleSequence instanceof AppendConflict &&
          staleSequence.actualTailSequence === first.lastSequence &&
          staleSequence.actualTailDigest === first.tailDigest,
        "A tail conflict must carry the actual committed tail as a resume hint",
      );

      const staleDigest = yield* expectFailure(
        "an append declaring the right sequence with the wrong digest",
        append(conversationId, batch("tail-batch-2", [record("tail-record-2", "second")]), {
          lastSequence: first.lastSequence,
          tailDigest: EMPTY_TAIL_DIGEST,
        }),
      );
      yield* ensure(
        staleDigest instanceof AppendConflict && staleDigest.reason === "tail",
        "A stale expected tail digest must conflict with reason tail",
      );

      const recovered = yield* append(
        conversationId,
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
      const conversationId = decodeConversationId("conformance-fencing");
      yield* materialize(conversationId, EPOCH_ONE);
      const first = yield* append(
        conversationId,
        batch("fence-batch-1", [record("fence-record-1", "first")]),
      );

      yield* materialize(conversationId, EPOCH_TWO);

      const staleAppend = yield* expectFailure(
        "an append using the pre-takeover producer epoch",
        append(
          conversationId,
          batch("fence-batch-2", [record("fence-record-2", "second")]),
          first,
          EPOCH_ONE,
        ),
      );
      yield* ensure(
        staleAppend instanceof FenceRejected &&
          staleAppend.actualEpoch === EPOCH_TWO &&
          staleAppend.attemptedEpoch === EPOCH_ONE,
        "A stale producer epoch must be fenced with both epochs reported",
      );

      const staleMaterialize = yield* expectFailure(
        "re-materializing with the pre-takeover producer epoch",
        materialize(conversationId, EPOCH_ONE),
      );
      yield* ensure(
        staleMaterialize instanceof FenceRejected,
        "A stale producer epoch must not re-register through materialization",
      );

      const takeover = yield* append(
        conversationId,
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
      const conversationId = decodeConversationId("conformance-offsets");
      const store = yield* ConversationStore;
      yield* materialize(conversationId, EPOCH_ONE);
      const first = yield* append(
        conversationId,
        batch("offset-batch-1", [
          record("offset-record-1", "first"),
          record("offset-record-2", "second"),
        ]),
      );
      const second = yield* append(
        conversationId,
        batch("offset-batch-2", [record("offset-record-3", "third")]),
        first,
      );
      yield* append(
        conversationId,
        batch("offset-batch-3", [record("offset-record-4", "fourth")]),
        second,
      );

      const committed = yield* readAll(conversationId);
      yield* ensure(committed.length === 4, "All committed records must be readable");
      const savedOffset = committed[1]?.offset;
      yield* ensure(savedOffset !== undefined, "Committed records must carry observation offsets");
      if (savedOffset === undefined) return;

      const resumed = yield* store
        .observe(ConversationObservation.make({ conversationId, afterOffset: savedOffset }))
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
        .observe(ConversationObservation.make({ conversationId }))
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
      const conversationId = decodeConversationId("conformance-checkpoints");
      const store = yield* ConversationStore;
      yield* materialize(conversationId, EPOCH_ONE);
      const first = yield* append(
        conversationId,
        batch("checkpoint-batch-1", [record("checkpoint-record-1", "Kyoto")]),
      );

      const aheadOfTail = yield* expectFailure(
        "a checkpoint claiming a sequence beyond the committed tail",
        store.saveCheckpoint(
          SaveCheckpointRequest.make({
            checkpoint: checkpointAt(
              conversationId,
              decodeSequence(first.lastSequence + 1),
              first.tailDigest,
            ),
          }),
        ),
      );
      yield* ensure(
        aheadOfTail instanceof CheckpointRejected && aheadOfTail.reason === "ahead-of-tail",
        "A checkpoint ahead of the tail must be rejected with reason ahead-of-tail",
      );

      const digestMismatch = yield* expectFailure(
        "a checkpoint whose digest does not match the canonical chain",
        store.saveCheckpoint(
          SaveCheckpointRequest.make({
            checkpoint: checkpointAt(conversationId, first.lastSequence, EMPTY_TAIL_DIGEST),
          }),
        ),
      );
      yield* ensure(
        digestMismatch instanceof CheckpointRejected && digestMismatch.reason === "digest-mismatch",
        "A checkpoint with a mismatched digest must be rejected with reason digest-mismatch",
      );
      yield* ensure(
        Option.isNone(yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId }))),
        "Rejected checkpoints must not become loadable",
      );

      const valid = checkpointAt(conversationId, first.lastSequence, first.tailDigest);
      yield* store.saveCheckpoint(SaveCheckpointRequest.make({ checkpoint: valid }));
      yield* store.saveCheckpoint(SaveCheckpointRequest.make({ checkpoint: valid }));

      const loaded = yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId }));
      yield* ensure(
        Option.isSome(loaded) &&
          loaded.value.throughSequence === first.lastSequence &&
          loaded.value.tailDigest === first.tailDigest,
        "A valid checkpoint must load bound to its canonical sequence and digest",
      );

      const beforeCheckpoint = yield* store.loadCheckpoint(
        LoadCheckpointRequest.make({ conversationId, atOrBeforeSequence: ZERO_SEQUENCE }),
      );
      yield* ensure(
        Option.isNone(beforeCheckpoint),
        "Checkpoint lookup must respect the atOrBeforeSequence bound",
      );

      yield* append(
        conversationId,
        batch("checkpoint-batch-2", [record("checkpoint-record-2", "Nara")]),
        first,
      );
      const afterAppend = yield* store.loadCheckpoint(
        LoadCheckpointRequest.make({ conversationId }),
      );
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
      const conversationId = decodeConversationId("conformance-tail-inspection");
      const store = yield* ConversationStore;

      const missing = yield* expectFailure(
        "inspecting the tail of an unmaterialized Conversation",
        store.inspectTail(ConversationTailRequest.make({ conversationId })),
      );
      yield* ensure(
        missing instanceof ConversationNotMaterialized && missing.conversationId === conversationId,
        "Tail inspection of an unmaterialized Conversation must fail as not materialized",
      );

      yield* materialize(conversationId, EPOCH_ONE);
      const empty = yield* store.inspectTail(ConversationTailRequest.make({ conversationId }));
      yield* ensure(
        empty.tailSequence === ZERO_SEQUENCE &&
          empty.tailDigest === EMPTY_TAIL_DIGEST &&
          empty.producerEpoch === EPOCH_ONE,
        "An empty Conversation must report the zero tail and its registered epoch",
      );

      const first = yield* append(
        conversationId,
        batch("inspect-batch-1", [record("inspect-record-1", "first")]),
      );
      const afterAppend = yield* store.inspectTail(
        ConversationTailRequest.make({ conversationId }),
      );
      yield* ensure(
        afterAppend.tailSequence === first.lastSequence &&
          afterAppend.tailDigest === first.tailDigest,
        "Tail inspection must match the latest AppendResult",
      );

      const resumed = yield* append(
        conversationId,
        batch("inspect-batch-2", [record("inspect-record-2", "second")]),
        { lastSequence: afterAppend.tailSequence, tailDigest: afterAppend.tailDigest },
        afterAppend.producerEpoch,
      );
      yield* ensure(
        resumed.firstSequence === afterAppend.tailSequence + 1,
        "An append composed from the inspected tail must commit without exporting the log",
      );

      yield* materialize(conversationId, EPOCH_TWO);
      const afterTakeover = yield* store.inspectTail(
        ConversationTailRequest.make({ conversationId }),
      );
      yield* ensure(
        afterTakeover.producerEpoch === EPOCH_TWO &&
          afterTakeover.tailSequence === resumed.lastSequence,
        "Tail inspection must reflect epoch takeover without disturbing the tail",
      );
    }),
);

const notMaterializedOperations = conformanceCase(
  "fails every read and write against an unmaterialized Conversation",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("conformance-not-materialized");
      const store = yield* ConversationStore;

      const failures = [
        yield* expectFailure(
          "appending to an unmaterialized Conversation",
          append(conversationId, batch("missing-batch-1", [record("missing-record-1", "first")])),
        ),
        yield* expectFailure("reading an unmaterialized Conversation", readAll(conversationId)),
        yield* expectFailure(
          "observing an unmaterialized Conversation",
          store
            .observe(ConversationObservation.make({ conversationId }))
            .pipe(Stream.take(1), Stream.runCollect),
        ),
        yield* expectFailure(
          "exporting an unmaterialized Conversation",
          store.export(ConversationExportRequest.make({ conversationId })),
        ),
        yield* expectFailure(
          "inspecting the tail of an unmaterialized Conversation",
          store.inspectTail(ConversationTailRequest.make({ conversationId })),
        ),
        yield* expectFailure(
          "saving a checkpoint for an unmaterialized Conversation",
          store.saveCheckpoint(
            SaveCheckpointRequest.make({
              checkpoint: checkpointAt(conversationId, ZERO_SEQUENCE, EMPTY_TAIL_DIGEST),
            }),
          ),
        ),
        yield* expectFailure(
          "loading a checkpoint for an unmaterialized Conversation",
          store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId })),
        ),
      ];
      yield* Effect.forEach(
        failures,
        (failure) =>
          ensure(
            failure instanceof ConversationNotMaterialized &&
              failure.conversationId === conversationId,
            "Operations against an unmaterialized Conversation must fail as not materialized",
          ),
        { discard: true },
      );
    }),
);

/**
 * The shared, adapter-parameterized ConversationStore contract suite (STORE-010). Every
 * durable adapter test suite must execute each case against its own store provisioning.
 */
export const conversationStoreConformanceCases: ReadonlyArray<ConversationStoreConformanceCase> = [
  atomicBatchVisibility,
  idempotentReplay,
  tailConflict,
  producerFencing,
  offsetResumability,
  checkpointBoundaries,
  tailInspection,
  notMaterializedOperations,
];
