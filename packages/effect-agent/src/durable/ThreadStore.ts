import type { Option, Stream } from "effect";
import { Context, Effect, Schema } from "effect";

import { RunId, SubmissionId, ThreadId } from "../core/Identifiers.ts";
import {
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ObservationOffset,
  PersistedJson,
  ProducerEpoch,
  RecordId,
  ToolCallPrepared,
  WorkerInputRequested,
} from "./Records.ts";
import { runIdForSubmission } from "./RunJournal.ts";
import { SubmissionLedger, SubmissionLookupById } from "./SubmissionLedger.ts";

export const MAX_THREAD_EXPORT_RECORDS = 131_072;

/** Exact canonical identity; absence is not proof that an admission was never accepted. */
export const ThreadRecordRequest = Schema.Struct({ threadId: ThreadId, recordId: RecordId });
export type ThreadRecordRequest = typeof ThreadRecordRequest.Type;

/** The original user input for a Run, excluding later joined inputs. */
export const ThreadRunInputRequest = Schema.Struct({ threadId: ThreadId, runId: RunId });
export type ThreadRunInputRequest = typeof ThreadRunInputRequest.Type;

export const ThreadOutstandingRequest = Schema.Struct({
  threadId: ThreadId,
  /** Exceeding this bound fails; a truncated inventory never grants authority. */
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(4_096)),
});

export type ThreadOutstandingRequest = typeof ThreadOutstandingRequest.Type;

export const ThreadOutstanding = Schema.Struct({
  threadId: ThreadId,
  throughSequence: CanonicalSequence,
  /** False only for legacy worker acknowledgements awaiting native repair. */
  complete: Schema.Boolean,
  operations: Schema.Array(
    Schema.Struct({
      submissionId: SubmissionId,
      prepared: ToolCallPrepared,
      /** Prepared is not an unknown outcome or a grant to execute. */
      state: Schema.Literals(["prepared", "unknown"]),
    }),
  ),
  /** Source reservations without an acknowledgement proving external effects are resolved. */
  workerInputs: Schema.Array(WorkerInputRequested),
});

export type ThreadOutstanding = typeof ThreadOutstanding.Type;

/** Runtime maintenance cursor; never use a partial page to authorize an action. */
export const ThreadWorkerInputsPageRequest = Schema.Struct({
  threadId: ThreadId,
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
});

export type ThreadWorkerInputsPageRequest = typeof ThreadWorkerInputsPageRequest.Type;

export const ThreadWorkerInputsPage = Schema.Struct({
  inputs: Schema.Array(WorkerInputRequested),
  next: Schema.NullOr(CanonicalSequence),
});

export type ThreadWorkerInputsPage = typeof ThreadWorkerInputsPage.Type;

/** Native worker lifetime reservations and selected Run accounting, excluding conversation records. */
export const ThreadWorkerStateRequest = Schema.Struct({
  threadId: ThreadId,
  sourceSubmissionId: Schema.optionalKey(SubmissionId),
  limit: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(MAX_THREAD_EXPORT_RECORDS),
  ),
});

export type ThreadWorkerStateRequest = typeof ThreadWorkerStateRequest.Type;

export const ThreadWorkerState = Schema.Struct({
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  records: Schema.Array(CanonicalRecordEnvelope),
});

export type ThreadWorkerState = typeof ThreadWorkerState.Type;

/** Saturating lifetime peer-message count: the result is at most the requested cap. */
export const ThreadPeerCountRequest = Schema.Struct({
  threadId: ThreadId,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1000)),
});

export type ThreadPeerCountRequest = typeof ThreadPeerCountRequest.Type;

/** Native indexed reads. Canonical append maintains these atomically; no history fallback. */
export interface ThreadNativeReads {
  readonly countPeerMessages: (
    request: ThreadPeerCountRequest,
  ) => Effect.Effect<number, ThreadStoreError | ThreadNotMaterialized>;
  readonly readWorkerState: (
    request: ThreadWorkerStateRequest,
  ) => Effect.Effect<ThreadWorkerState, ThreadStoreError | ThreadNotMaterialized>;
  readonly readWorkerInputsPage: (
    request: ThreadWorkerInputsPageRequest,
  ) => Effect.Effect<ThreadWorkerInputsPage, ThreadStoreError | ThreadNotMaterialized>;
  readonly getRecord: (
    request: ThreadRecordRequest,
  ) => Effect.Effect<
    Option.Option<CanonicalRecordEnvelope>,
    ThreadStoreError | ThreadNotMaterialized
  >;
  readonly getRunInput: (
    request: ThreadRunInputRequest,
  ) => Effect.Effect<
    Option.Option<CanonicalRecordEnvelope>,
    ThreadStoreError | ThreadNotMaterialized
  >;
  readonly readOutstanding: (
    request: ThreadOutstandingRequest,
  ) => Effect.Effect<ThreadOutstanding, ThreadStoreError | ThreadNotMaterialized>;
}

const nativeReads = Effect.gen(function* () {
  const store = yield* ThreadStore;

  if (store.nativeReads === undefined)
    return yield* ThreadStoreError.make({
      operation: "native read",
      message: "This adapter does not support native indexed reads",
    });

  return store.nativeReads;
});

/** Host-owned read: authenticate the Thread and exact locator before calling. */
export const getRecord = Effect.fn("ThreadStore.getRecord")(function* (
  request: ThreadRecordRequest,
) {
  return yield* (yield* nativeReads).getRecord(request);
});

export const getRunInput = Effect.fn("ThreadStore.getRunInput")(function* (
  request: ThreadRunInputRequest,
) {
  return yield* (yield* nativeReads).getRunInput(request);
});

/**
 * Work is proportional to outstanding records, independent of completed history. Unknown
 * effects survive abort and resolution intent; only canonical closure removes them. A
 * preparation from another Run still requires native recovery before it can be treated as
 * safe. This snapshot grants no execution authority and does not freeze other owners.
 */
export const readOutstanding = Effect.fn("ThreadStore.readOutstanding")(function* (
  request: ThreadOutstandingRequest,
) {
  const state = yield* (yield* nativeReads).readOutstanding(request);

  if (!state.complete)
    return yield* ThreadStoreError.make({
      operation: "readOutstanding",
      message: "Legacy worker acknowledgements require native repair",
    });
  const ledger = yield* SubmissionLedger;

  for (const operation of state.operations) {
    const submission = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: operation.submissionId }),
    );

    if (
      submission._tag === "None" ||
      submission.value.threadId !== request.threadId ||
      runIdForSubmission(operation.submissionId) !== operation.prepared.runId
    )
      return yield* ThreadStoreError.make({
        operation: "readOutstanding",
        message: "Canonical operation has no matching native admission",
      });
  }

  return state;
});

export class ThreadMaterialization extends Schema.Class<ThreadMaterialization>(
  "@effect-agent/thread/ThreadMaterialization",
)({
  threadId: ThreadId,
  producerEpoch: ProducerEpoch,
}) {}

export class FencedAppendRequest extends Schema.Class<FencedAppendRequest>(
  "@effect-agent/thread/FencedAppendRequest",
)({
  threadId: ThreadId,
  batch: CanonicalBatch,
  expectedTailSequence: CanonicalSequence,
  expectedTailDigest: Digest,
  producerEpoch: ProducerEpoch,
}) {}

export class AppendResult extends Schema.Class<AppendResult>("@effect-agent/thread/AppendResult")({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  tailDigest: Digest,
  replayed: Schema.Boolean,
}) {}

export class ThreadRead extends Schema.Class<ThreadRead>("@effect-agent/thread/ThreadRead")({
  threadId: ThreadId,
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.Natural.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class ThreadObservation extends Schema.Class<ThreadObservation>(
  "@effect-agent/thread/ThreadObservation",
)({
  threadId: ThreadId,
  afterOffset: Schema.optionalKey(ObservationOffset),
}) {}

export class ThreadExportRequest extends Schema.Class<ThreadExportRequest>(
  "@effect-agent/thread/ThreadExportRequest",
)({
  threadId: ThreadId,
}) {}

export class ThreadTailRequest extends Schema.Class<ThreadTailRequest>(
  "@effect-agent/thread/ThreadTailRequest",
)({
  threadId: ThreadId,
}) {}

/**
 * The committed tail of one Thread Log. A resuming producer composes its next
 * FencedAppendRequest from this value instead of exporting the whole log.
 */
export class ThreadTail extends Schema.Class<ThreadTail>("@effect-agent/thread/ThreadTail")({
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  producerEpoch: ProducerEpoch,
}) {}

/** Maximum canonical records represented by one Thread export. */

export class ThreadExport extends Schema.Class<ThreadExport>("@effect-agent/thread/ThreadExport")({
  format: Schema.Literal("effect-agent/thread@1"),
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  records: Schema.Array(CanonicalRecordEnvelope).check(
    Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS),
  ),
}) {}

/**
 * A disposable projection snapshot. Adapters bind its sequence and digest to the canonical
 * log; consumers decode `state` and decide projection compatibility before suffix replay.
 * Legacy metadata is optional for application projections and preserved when supplied.
 * Runtime-owned recovery checkpoints populate and compare it before using cached state.
 */
export class ThreadCheckpoint extends Schema.Class<ThreadCheckpoint>(
  "@effect-agent/thread/ThreadCheckpoint",
)({
  schemaVersion: Schema.Literal(1),
  threadId: ThreadId,
  throughSequence: CanonicalSequence,
  tailDigest: Digest,
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  engineVersion: Schema.optionalKey(Schema.NonEmptyString),
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  agentDefinitionDigest: Schema.optionalKey(Digest),
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  modelDigest: Schema.optionalKey(Digest),
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  toolDigest: Schema.optionalKey(Digest),
  state: PersistedJson,
  createdAt: Schema.DateTimeUtcFromString,
}) {}

export class SaveCheckpointRequest extends Schema.Class<SaveCheckpointRequest>(
  "@effect-agent/thread/SaveCheckpointRequest",
)({
  checkpoint: ThreadCheckpoint,
}) {}

/** Replace the disposable recovery view only under the current canonical producer fence. */
export class SaveRecoveryCheckpointRequest extends Schema.Class<SaveRecoveryCheckpointRequest>(
  "@effect-agent/thread/SaveRecoveryCheckpointRequest",
)({
  checkpoint: ThreadCheckpoint,
  producerEpoch: ProducerEpoch,
}) {}

export class LoadCheckpointRequest extends Schema.Class<LoadCheckpointRequest>(
  "@effect-agent/thread/LoadCheckpointRequest",
)({
  threadId: ThreadId,
  atOrBeforeSequence: Schema.optionalKey(CanonicalSequence),
}) {}

export class ThreadStoreError extends Schema.TaggedError<ThreadStoreError>()("ThreadStoreError", {
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export class ThreadNotMaterialized extends Schema.TaggedError<ThreadNotMaterialized>()(
  "ThreadNotMaterialized",
  { threadId: ThreadId },
) {}

/**
 * A canonical append that cannot commit: `batch-digest` replays a batch ID with different
 * content, `record-identity` reuses a canonical record ID, and `tail` declares a stale expected
 * tail. Tail conflicts carry the actual committed tail as a diagnostic resume hint.
 */
export class AppendConflict extends Schema.TaggedError<AppendConflict>()("AppendConflict", {
  threadId: ThreadId,
  batchId: BatchId,
  reason: Schema.Literals(["batch-digest", "record-identity", "tail"]),
  actualTailSequence: Schema.optionalKey(CanonicalSequence),
  actualTailDigest: Schema.optionalKey(Digest),
}) {}

export class FenceRejected extends Schema.TaggedError<FenceRejected>()("FenceRejected", {
  threadId: ThreadId,
  actualEpoch: ProducerEpoch,
  attemptedEpoch: ProducerEpoch,
}) {}

export class CheckpointRejected extends Schema.TaggedError<CheckpointRejected>()(
  "CheckpointRejected",
  {
    threadId: ThreadId,
    reason: Schema.Literals(["ahead-of-tail", "digest-mismatch", "unsupported-version", "corrupt"]),
  },
) {}

export type ThreadStoreFailure =
  | ThreadStoreError
  | ThreadNotMaterialized
  | AppendConflict
  | FenceRejected;

/**
 * Optional, disposable projection storage. Neither history execution nor durable recovery
 * requires it. Adapters that offer it must bind every checkpoint to a canonical batch tail.
 * Application consumers own projection compatibility; this port does not interpret metadata.
 */
export interface ThreadCheckpoints {
  readonly save: (
    request: SaveCheckpointRequest,
  ) => Effect.Effect<void, ThreadStoreError | ThreadNotMaterialized | CheckpointRejected>;
  readonly load: (
    request: LoadCheckpointRequest,
  ) => Effect.Effect<
    Option.Option<ThreadCheckpoint>,
    ThreadStoreError | ThreadNotMaterialized | CheckpointRejected
  >;
}

/**
 * Optional latest-only recovery cache, independent of application projection checkpoints.
 * Saves atomically validate the producer epoch and canonical batch tail. An older snapshot
 * cannot replace a newer one; equal-tail replacement repairs disposable state. Invalid cached
 * data fails with CheckpointRejected, while infrastructure failures remain ThreadStoreError.
 * Loading at an earlier tail or outside the adapter's cache locality may return none so callers
 * can replay canonical records. Canonical records and the ledger remain authority.
 */
export interface ThreadRecoveryCheckpoints {
  readonly save: (
    request: SaveRecoveryCheckpointRequest,
  ) => Effect.Effect<
    void,
    ThreadStoreError | ThreadNotMaterialized | CheckpointRejected | FenceRejected
  >;
  readonly load: ThreadCheckpoints["load"];
}

export class ThreadStore extends Context.Service<
  ThreadStore,
  {
    readonly materialize: (
      request: ThreadMaterialization,
    ) => Effect.Effect<void, ThreadStoreError | FenceRejected>;
    readonly append: (
      request: FencedAppendRequest,
    ) => Effect.Effect<
      AppendResult,
      ThreadStoreError | ThreadNotMaterialized | AppendConflict | FenceRejected
    >;
    readonly read: (
      request: ThreadRead,
    ) => Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized>;
    readonly observe: (
      request: ThreadObservation,
    ) => Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized>;
    readonly export: (
      request: ThreadExportRequest,
    ) => Effect.Effect<ThreadExport, ThreadStoreError | ThreadNotMaterialized>;
    readonly inspectTail: (
      request: ThreadTailRequest,
    ) => Effect.Effect<ThreadTail, ThreadStoreError | ThreadNotMaterialized>;
    /** Absent when this adapter does not support disposable checkpoints. */
    readonly checkpoints?: ThreadCheckpoints | undefined;
    readonly recoveryCheckpoints?: ThreadRecoveryCheckpoints | undefined;
    readonly nativeReads?: ThreadNativeReads | undefined;
  }
>()("@effect-agent/thread/ThreadStore") {}
