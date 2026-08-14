import { ConversationId } from "@effect-agent/core";
import { Context, Effect, Option, Schema, Stream } from "effect";

import {
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ObservationOffset,
  PersistedJson,
  ProducerEpoch,
} from "./records.ts";

export class ConversationMaterialization extends Schema.Class<ConversationMaterialization>(
  "@effect-agent/session/ConversationMaterialization",
)({
  conversationId: ConversationId,
  producerEpoch: ProducerEpoch,
}) {}

export class FencedAppendRequest extends Schema.Class<FencedAppendRequest>(
  "@effect-agent/session/FencedAppendRequest",
)({
  conversationId: ConversationId,
  batch: CanonicalBatch,
  expectedTailSequence: CanonicalSequence,
  expectedTailDigest: Digest,
  producerEpoch: ProducerEpoch,
}) {}

export class AppendResult extends Schema.Class<AppendResult>("@effect-agent/session/AppendResult")({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  tailDigest: Digest,
  replayed: Schema.Boolean,
}) {}

export class ConversationRead extends Schema.Class<ConversationRead>(
  "@effect-agent/session/ConversationRead",
)({
  conversationId: ConversationId,
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.Natural.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class ConversationObservation extends Schema.Class<ConversationObservation>(
  "@effect-agent/session/ConversationObservation",
)({
  conversationId: ConversationId,
  afterOffset: Schema.optionalKey(ObservationOffset),
}) {}

export class ConversationExportRequest extends Schema.Class<ConversationExportRequest>(
  "@effect-agent/session/ConversationExportRequest",
)({
  conversationId: ConversationId,
}) {}

export class ConversationTailRequest extends Schema.Class<ConversationTailRequest>(
  "@effect-agent/session/ConversationTailRequest",
)({
  conversationId: ConversationId,
}) {}

/**
 * The committed tail of one Conversation Log. A resuming producer composes its next
 * FencedAppendRequest from this value instead of exporting the whole log.
 */
export class ConversationTail extends Schema.Class<ConversationTail>(
  "@effect-agent/session/ConversationTail",
)({
  conversationId: ConversationId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  producerEpoch: ProducerEpoch,
}) {}

export class ConversationExport extends Schema.Class<ConversationExport>(
  "@effect-agent/session/ConversationExport",
)({
  format: Schema.Literal("effect-agent/conversation@1"),
  conversationId: ConversationId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  records: Schema.Array(CanonicalRecordEnvelope).check(Schema.isMaxLength(65_536)),
}) {}

export class ConversationCheckpoint extends Schema.Class<ConversationCheckpoint>(
  "@effect-agent/session/ConversationCheckpoint",
)({
  schemaVersion: Schema.Literal(1),
  conversationId: ConversationId,
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
  "@effect-agent/session/SaveCheckpointRequest",
)({
  checkpoint: ConversationCheckpoint,
}) {}

export class LoadCheckpointRequest extends Schema.Class<LoadCheckpointRequest>(
  "@effect-agent/session/LoadCheckpointRequest",
)({
  conversationId: ConversationId,
  atOrBeforeSequence: Schema.optionalKey(CanonicalSequence),
}) {}

export class ConversationStoreError extends Schema.TaggedError<ConversationStoreError>()(
  "ConversationStoreError",
  {
    operation: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

export class ConversationNotMaterialized extends Schema.TaggedError<ConversationNotMaterialized>()(
  "ConversationNotMaterialized",
  { conversationId: ConversationId },
) {}

/**
 * A canonical append that cannot commit: `batch-digest` replays a batch ID with different
 * content, `record-identity` reuses a canonical record ID, and `tail` declares a stale expected
 * tail. Tail conflicts carry the actual committed tail as a diagnostic resume hint.
 */
export class AppendConflict extends Schema.TaggedError<AppendConflict>()("AppendConflict", {
  conversationId: ConversationId,
  batchId: BatchId,
  reason: Schema.Literals(["batch-digest", "record-identity", "tail"]),
  actualTailSequence: Schema.optionalKey(CanonicalSequence),
  actualTailDigest: Schema.optionalKey(Digest),
}) {}

export class FenceRejected extends Schema.TaggedError<FenceRejected>()("FenceRejected", {
  conversationId: ConversationId,
  actualEpoch: ProducerEpoch,
  attemptedEpoch: ProducerEpoch,
}) {}

export class CheckpointRejected extends Schema.TaggedError<CheckpointRejected>()(
  "CheckpointRejected",
  {
    conversationId: ConversationId,
    reason: Schema.Literals(["ahead-of-tail", "digest-mismatch", "unsupported-version"]),
  },
) {}

export type ConversationStoreFailure =
  | ConversationStoreError
  | ConversationNotMaterialized
  | AppendConflict
  | FenceRejected
  | CheckpointRejected;

export class ConversationStore extends Context.Service<
  ConversationStore,
  {
    readonly materialize: (
      request: ConversationMaterialization,
    ) => Effect.Effect<void, ConversationStoreError | FenceRejected>;
    readonly append: (
      request: FencedAppendRequest,
    ) => Effect.Effect<
      AppendResult,
      ConversationStoreError | ConversationNotMaterialized | AppendConflict | FenceRejected
    >;
    readonly read: (
      request: ConversationRead,
    ) => Stream.Stream<
      CanonicalRecordEnvelope,
      ConversationStoreError | ConversationNotMaterialized
    >;
    readonly observe: (
      request: ConversationObservation,
    ) => Stream.Stream<
      CanonicalRecordEnvelope,
      ConversationStoreError | ConversationNotMaterialized
    >;
    readonly export: (
      request: ConversationExportRequest,
    ) => Effect.Effect<ConversationExport, ConversationStoreError | ConversationNotMaterialized>;
    readonly inspectTail: (
      request: ConversationTailRequest,
    ) => Effect.Effect<ConversationTail, ConversationStoreError | ConversationNotMaterialized>;
    readonly saveCheckpoint: (
      request: SaveCheckpointRequest,
    ) => Effect.Effect<
      void,
      ConversationStoreError | ConversationNotMaterialized | CheckpointRejected
    >;
    readonly loadCheckpoint: (
      request: LoadCheckpointRequest,
    ) => Effect.Effect<
      Option.Option<ConversationCheckpoint>,
      ConversationStoreError | ConversationNotMaterialized | CheckpointRejected
    >;
  }
>()("@effect-agent/session/ConversationStore") {}
