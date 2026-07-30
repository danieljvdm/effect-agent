import { AgentId, ConversationId, RunId, SubmissionId, ToolCallId } from "@effect-agent/core";
import { Schema } from "effect";

const identifier = <const Name extends string>(name: Name) =>
  Schema.NonEmptyString.pipe(Schema.brand(`@effect-agent/session/${name}`));

/** Stable identity of one canonical record. */
export const RecordId = identifier("RecordId");
export type RecordId = typeof RecordId.Type;

/** Stable idempotency identity of one atomic append. */
export const BatchId = identifier("BatchId");
export type BatchId = typeof BatchId.Type;

/** Identity of the deployment that produced a record. */
export const DeploymentId = identifier("DeploymentId");
export type DeploymentId = typeof DeploymentId.Type;

/** Identity of a fenced canonical-log producer. */
export const ProducerId = identifier("ProducerId");
export type ProducerId = typeof ProducerId.Type;

/** SHA-256 digest encoded as lowercase hexadecimal text. */
export const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)).pipe(
  Schema.brand("@effect-agent/session/Digest"),
);
export type Digest = typeof Digest.Type;

/** Adapter-owned resume cursor. Callers must not parse or synthesize it. */
export const ObservationOffset = identifier("ObservationOffset");
export type ObservationOffset = typeof ObservationOffset.Type;

/** Gap-free position in one Conversation's canonical sequence. */
export const CanonicalSequence = Schema.Natural;
export type CanonicalSequence = typeof CanonicalSequence.Type;

/** Monotonic fencing epoch for a Conversation producer. */
export const ProducerEpoch = Schema.Natural;
export type ProducerEpoch = typeof ProducerEpoch.Type;

const BoundedText = Schema.String.check(Schema.isMaxLength(64 * 1024));
const BoundedName = Schema.NonEmptyString.check(Schema.isMaxLength(256));

/** Digests make a replay-visible definition/configuration change explicit. */
export class DefinitionDigests extends Schema.Class<DefinitionDigests>(
  "@effect-agent/session/DefinitionDigests",
)({
  agent: Digest,
  model: Digest,
  tools: Digest,
}) {}

export class ConversationCreated extends Schema.TaggedClass<ConversationCreated>(
  "@effect-agent/session/ConversationCreated",
)("ConversationCreated", {
  agentId: AgentId,
  definitions: DefinitionDigests,
}) {}

/**
 * One accepted conversational input. `kind` distinguishes the normal, steering, and follow-up
 * delivery seams without creating parallel wire families for the same logical value.
 */
export class UserInputRecorded extends Schema.TaggedClass<UserInputRecorded>(
  "@effect-agent/session/UserInputRecorded",
)("UserInputRecorded", {
  submissionId: SubmissionId,
  kind: Schema.Literals(["user", "steering", "follow-up"]),
  runId: Schema.optionalKey(RunId),
  input: Schema.Json,
}) {}

export class ModelCompleted extends Schema.TaggedClass<ModelCompleted>(
  "@effect-agent/session/ModelCompleted",
)("ModelCompleted", {
  runId: RunId,
  output: Schema.Json,
}) {}

export class ToolCallSettled extends Schema.TaggedClass<ToolCallSettled>(
  "@effect-agent/session/ToolCallSettled",
)("ToolCallSettled", {
  runId: RunId,
  toolCallId: ToolCallId,
  toolName: BoundedName,
  result: Schema.Json,
  isFailure: Schema.Boolean,
}) {}

export class CompactionCreated extends Schema.TaggedClass<CompactionCreated>(
  "@effect-agent/session/CompactionCreated",
)("CompactionCreated", {
  runId: RunId,
  sourceDigest: Digest,
  summary: BoundedText,
}) {}

export class RunFailed extends Schema.TaggedClass<RunFailed>("@effect-agent/session/RunFailed")(
  "RunFailed",
  {
    runId: RunId,
    failure: Schema.Json,
  },
) {}

export class RunCompleted extends Schema.TaggedClass<RunCompleted>(
  "@effect-agent/session/RunCompleted",
)("RunCompleted", {
  runId: RunId,
  output: Schema.Json,
}) {}

export class RepairAnnotated extends Schema.TaggedClass<RepairAnnotated>(
  "@effect-agent/session/RepairAnnotated",
)("RepairAnnotated", {
  reason: BoundedText,
  details: Schema.Json,
}) {}

/** Current private-development canonical payload family. */
export const CanonicalRecordPayload = Schema.Union([
  ConversationCreated,
  UserInputRecorded,
  ModelCompleted,
  ToolCallSettled,
  CompactionCreated,
  RunFailed,
  RunCompleted,
  RepairAnnotated,
]);
export type CanonicalRecordPayload = typeof CanonicalRecordPayload.Type;

/**
 * Versioned record envelope stored in an atomic batch. Conversation ordering is assigned only
 * when the batch is committed.
 */
export class RecordEnvelope extends Schema.Class<RecordEnvelope>(
  "@effect-agent/session/RecordEnvelope",
)({
  recordId: RecordId,
  family: Schema.Literal("conversation"),
  schemaVersion: Schema.Literal(1),
  createdAt: Schema.DateTimeUtcFromString,
  deploymentId: DeploymentId,
  payload: CanonicalRecordPayload,
}) {}

/** Backward-compatible domain name for a canonical record. */
export const CanonicalRecord = RecordEnvelope;
export type CanonicalRecord = RecordEnvelope;

/** One non-empty, bounded, idempotent atomic append unit. */
export class CanonicalBatch extends Schema.Class<CanonicalBatch>(
  "@effect-agent/session/CanonicalBatch",
)({
  batchId: BatchId,
  producerId: ProducerId,
  records: Schema.NonEmptyArray(RecordEnvelope).check(Schema.isMaxLength(256)),
}) {}

/**
 * A committed record with its Conversation ordering and opaque resumable observation cursor.
 */
export class CanonicalRecordEnvelope extends Schema.Class<CanonicalRecordEnvelope>(
  "@effect-agent/session/CanonicalRecordEnvelope",
)({
  conversationId: ConversationId,
  batchId: BatchId,
  sequence: CanonicalSequence,
  offset: ObservationOffset,
  record: RecordEnvelope,
}) {}

export const CURRENT_CANONICAL_SCHEMA_VERSION = 1 as const;
