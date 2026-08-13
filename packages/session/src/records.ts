import {
  AgentId,
  ConversationId,
  ReceiptId,
  RunId,
  SettlementId,
  SubmissionId,
  ToolCallId,
  TurnId,
} from "@effect-agent/core";
import { Encoding, Schema } from "effect";

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
export const CanonicalSequence = Schema.Natural.pipe(
  Schema.brand("@effect-agent/session/CanonicalSequence"),
);
export type CanonicalSequence = typeof CanonicalSequence.Type;

/** Monotonic fencing epoch for a Conversation producer. */
export const ProducerEpoch = Schema.Natural.pipe(
  Schema.brand("@effect-agent/session/ProducerEpoch"),
);
export type ProducerEpoch = typeof ProducerEpoch.Type;

const BoundedText = Schema.String.check(Schema.isMaxLength(64 * 1024));
const BoundedName = Schema.NonEmptyString.check(Schema.isMaxLength(256));

export const MAX_PERSISTED_JSON_DEPTH = 64;
export const MAX_PERSISTED_JSON_COLLECTION_LENGTH = 4_096;
export const MAX_PERSISTED_JSON_NODES = 65_536;
export const MAX_PERSISTED_JSON_BYTES = 1024 * 1024;

/**
 * Iteratively preflights an unknown value before Schema's recursive JSON validation. This is the
 * one narrow `Schema.declare` exception in the persistence model: Effect v4's `Unknown.decodeTo`
 * preserves `unknown` as the encoded type, which would leak through every nested record codec.
 * Schema.Json still owns the accepted value shape after this resource preflight succeeds.
 */
const isJson = Schema.is(Schema.Json);

const isPersistedJson = (input: unknown): input is Schema.Json => {
  const pending: Array<{ readonly value: unknown; readonly depth: number }> = [
    { value: input, depth: 0 },
  ];
  const visited = new WeakSet<object>();
  let nodes = 0;
  let textUnits = 0;

  try {
    while (pending.length > 0) {
      const current = pending.pop();
      if (current === undefined) return false;
      if (current.depth > MAX_PERSISTED_JSON_DEPTH || ++nodes > MAX_PERSISTED_JSON_NODES) {
        return false;
      }

      const value = current.value;
      if (value === null || typeof value === "boolean") continue;
      if (typeof value === "number") {
        if (!Number.isFinite(value)) return false;
        continue;
      }
      if (typeof value === "string") {
        textUnits += value.length;
        if (textUnits > MAX_PERSISTED_JSON_BYTES) return false;
        continue;
      }
      if (typeof value !== "object" || visited.has(value)) return false;
      visited.add(value);

      const entries = Array.isArray(value)
        ? value.map((entry, index) => [index, entry] as const)
        : Object.entries(value);
      if (entries.length > MAX_PERSISTED_JSON_COLLECTION_LENGTH) return false;
      for (const [key, entry] of entries) {
        textUnits += typeof key === "string" ? key.length : 0;
        if (textUnits > MAX_PERSISTED_JSON_BYTES) return false;
        pending.push({ value: entry, depth: current.depth + 1 });
      }
    }

    if (!isJson(input)) return false;
    const encoded = JSON.stringify(input);
    return (
      encoded !== undefined && Encoding.encodeHex(encoded).length / 2 <= MAX_PERSISTED_JSON_BYTES
    );
  } catch {
    return false;
  }
};

/** Canonical JSON admitted to persisted records and checkpoints under explicit resource limits. */
export const PersistedJson = Schema.declare(isPersistedJson, {
  identifier: "@effect-agent/session/PersistedJson",
  description: "JSON bounded by canonical persistence depth, collection, node, and byte limits",
});
export type PersistedJson = typeof PersistedJson.Type;

/** Schema-owned replay inputs whose individual definitions are incorporated into digests. */
export class DefinitionDigestInput extends Schema.Class<DefinitionDigestInput>(
  "@effect-agent/session/DefinitionDigestInput",
)({
  agent: PersistedJson,
  model: PersistedJson,
  tools: PersistedJson,
}) {}

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
  input: PersistedJson,
}) {}

export class ModelCompleted extends Schema.TaggedClass<ModelCompleted>(
  "@effect-agent/session/ModelCompleted",
)("ModelCompleted", {
  runId: RunId,
  output: PersistedJson,
}) {}

export class ToolCallSettled extends Schema.TaggedClass<ToolCallSettled>(
  "@effect-agent/session/ToolCallSettled",
)("ToolCallSettled", {
  runId: RunId,
  toolCallId: ToolCallId,
  toolName: BoundedName,
  result: PersistedJson,
  isFailure: Schema.Boolean,
}) {}

/**
 * One committed model Turn. `messages` carries the Schema-encoded Effect AI Prompt messages this
 * Turn appended (assistant response plus any tool-call declarations), committed atomically at the
 * Turn boundary so a recovering Attempt can rebuild the next Prompt from canonical records alone.
 * `messagesDigest` pins the exact encoded content.
 */
export class ModelResponseRecorded extends Schema.TaggedClass<ModelResponseRecorded>(
  "@effect-agent/session/ModelResponseRecorded",
)("ModelResponseRecorded", {
  runId: RunId,
  turnId: TurnId,
  turn: Schema.Int.check(Schema.isGreaterThan(0)),
  messages: PersistedJson,
  messagesDigest: Digest,
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
    failure: PersistedJson,
  },
) {}

export class RunCompleted extends Schema.TaggedClass<RunCompleted>(
  "@effect-agent/session/RunCompleted",
)("RunCompleted", {
  runId: RunId,
  output: PersistedJson,
}) {}

export class RepairAnnotated extends Schema.TaggedClass<RepairAnnotated>(
  "@effect-agent/session/RepairAnnotated",
)("RepairAnnotated", {
  reason: BoundedText,
  details: PersistedJson,
}) {}

/** Terminal outcome family for one accepted Submission (DUR-002). */
export const SettlementOutcome = Schema.Literals(["completed", "failed", "aborted"]);
export type SettlementOutcome = typeof SettlementOutcome.Type;

/**
 * A durable abort command made canonical before the active worker is interrupted (DUR-012).
 * Repeating the same abort command is idempotent; abort never rewrites a prior terminal outcome.
 */
export class AbortRequested extends Schema.TaggedClass<AbortRequested>(
  "@effect-agent/session/AbortRequested",
)("AbortRequested", {
  submissionId: SubmissionId,
  author: BoundedName,
  reason: BoundedText,
}) {}

/**
 * The single canonical settlement record owed to one accepted Submission (DUR-002, DUR-011).
 * Canonical history is the outcome authority: the ledger row is finalized from this record and
 * never the other way around (DUR-015).
 */
export class SubmissionSettled extends Schema.TaggedClass<SubmissionSettled>(
  "@effect-agent/session/SubmissionSettled",
)("SubmissionSettled", {
  submissionId: SubmissionId,
  settlementId: SettlementId,
  receiptId: ReceiptId,
  outcome: SettlementOutcome,
  runId: Schema.optionalKey(RunId),
  result: Schema.optionalKey(PersistedJson),
}) {}

/** Current private-development canonical payload family. */
export const CanonicalRecordPayload = Schema.Union([
  ConversationCreated,
  UserInputRecorded,
  ModelCompleted,
  ModelResponseRecorded,
  ToolCallSettled,
  CompactionCreated,
  RunFailed,
  RunCompleted,
  AbortRequested,
  SubmissionSettled,
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
