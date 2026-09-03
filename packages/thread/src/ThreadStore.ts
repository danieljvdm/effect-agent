import { ThreadId } from "@effect-agent/core/Identifiers";
import type { Effect, Option, Stream } from "effect";
import { Context, Schema } from "effect";

import {
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ObservationOffset,
  PersistedJson,
  ProducerEpoch,
} from "./Records.ts";

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
export const MAX_THREAD_EXPORT_RECORDS = 65_536;

export class ThreadExport extends Schema.Class<ThreadExport>("@effect-agent/thread/ThreadExport")({
  format: Schema.Literal("effect-agent/thread@1"),
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  records: Schema.Array(CanonicalRecordEnvelope).check(
    Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS),
  ),
}) {}

export class ThreadCheckpoint extends Schema.Class<ThreadCheckpoint>(
  "@effect-agent/thread/ThreadCheckpoint",
)({
  schemaVersion: Schema.Literal(1),
  threadId: ThreadId,
  throughSequence: CanonicalSequence,
  tailDigest: Digest,
  engineVersion: Schema.NonEmptyString,
  agentDefinitionDigest: Digest,
  modelDigest: Digest,
  toolDigest: Digest,
  state: PersistedJson,
  createdAt: Schema.DateTimeUtcFromString,
}) {}

export class SaveCheckpointRequest extends Schema.Class<SaveCheckpointRequest>(
  "@effect-agent/thread/SaveCheckpointRequest",
)({
  checkpoint: ThreadCheckpoint,
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
    reason: Schema.Literals(["ahead-of-tail", "digest-mismatch", "unsupported-version"]),
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
  }
>()("@effect-agent/thread/ThreadStore") {}
