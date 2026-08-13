import {
  AgentId,
  AttemptId,
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

/** Positive canonical (Run-relative, Attempt-independent) Turn number. */
const TurnNumber = Schema.Int.check(Schema.isGreaterThan(0));

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
  turn: TurnNumber,
  messages: PersistedJson,
  messagesDigest: Digest,
}) {}

/**
 * One approved uncertain/idempotent ordinary Tool Call made durable BEFORE any handler starts
 * (durability §10). `parameters` is the Schema-encoded wire form of the declared call and
 * `parametersDigest` pins it; recovery that sees this record without a matching `ToolCallSettled`
 * (or `ToolCallUnknown` → `ToolCallResolved`) must reconcile or mark unknown, never silently
 * replay (DUR-009). `readonly`-class Tools never produce this record (P4 parity).
 */
export class ToolCallPrepared extends Schema.TaggedClass<ToolCallPrepared>(
  "@effect-agent/session/ToolCallPrepared",
)("ToolCallPrepared", {
  runId: RunId,
  turnId: TurnId,
  turn: TurnNumber,
  toolCallId: ToolCallId,
  toolName: BoundedName,
  parameters: PersistedJson,
  parametersDigest: Digest,
}) {}

/**
 * A durable Unknown Outcome: the external effect of one prepared ordinary Tool Call may have
 * happened but was not confirmed canonically (DUR-009/DUR-017). It is neither success nor
 * ordinary failure; automatic continuation stops until an authorized resolution arrives.
 */
export class ToolCallUnknown extends Schema.TaggedClass<ToolCallUnknown>(
  "@effect-agent/session/ToolCallUnknown",
)("ToolCallUnknown", {
  runId: RunId,
  turn: TurnNumber,
  toolCallId: ToolCallId,
  toolName: BoundedName,
  reason: BoundedText,
}) {}

/** How one open/unknown Tool Call was authoritatively closed (DUR-017 resolution audit). */
export const ToolCallResolution = Schema.Literals([
  "completed-with-result",
  "failed-with-error",
  "never-started",
  "safe-retry",
]);
export type ToolCallResolution = typeof ToolCallResolution.Type;

/**
 * The canonical audit record closing one open or unknown Tool Call: reconciler-recovered supplier
 * truth or an authorized `resolveUnknown` command. `author`/`reason` make every resolution an
 * attributable decision (DUR-017); a `completed-with-result` resolution is accompanied by the
 * per-call `ToolCallSettled` record carrying the recovered result.
 */
export class ToolCallResolved extends Schema.TaggedClass<ToolCallResolved>(
  "@effect-agent/session/ToolCallResolved",
)("ToolCallResolved", {
  runId: RunId,
  toolCallId: ToolCallId,
  resolution: ToolCallResolution,
  author: BoundedName,
  reason: BoundedText,
}) {}

/**
 * One accepted Durable Step result (durability §11): exactly-once-recorded while the Step's
 * external side effect stays honestly at-least-once-executed. Only success is recorded — a failing
 * Step body fails into the handler's error channel and re-executes on re-entry. `output` is the
 * Schema-encoded Step output; `outputDigest` pins it for replay-divergence detection.
 */
export class ToolStepSettled extends Schema.TaggedClass<ToolStepSettled>(
  "@effect-agent/session/ToolStepSettled",
)("ToolStepSettled", {
  runId: RunId,
  toolCallId: ToolCallId,
  stepName: BoundedName,
  output: PersistedJson,
  outputDigest: Digest,
}) {}

/**
 * A canonical approval request for one declared Tool Call (CAP-006, durability §8): with this
 * record durable, "waiting for explicit approval" is a safe suspension boundary — the resumed
 * Attempt replays the declared batch instead of re-invoking the model.
 */
export class ToolApprovalRequested extends Schema.TaggedClass<ToolApprovalRequested>(
  "@effect-agent/session/ToolApprovalRequested",
)("ToolApprovalRequested", {
  runId: RunId,
  turnId: TurnId,
  turn: TurnNumber,
  toolCallId: ToolCallId,
  toolName: BoundedName,
  parametersDigest: Digest,
}) {}

/** The two-valued approval decision family shared by canonical records and ledger intents. */
export const ApprovalDecision = Schema.Literals(["approved", "denied"]);
export type ApprovalDecision = typeof ApprovalDecision.Type;

/**
 * The canonical decision for one requested approval. Appended by the deciding Attempt (policy
 * auto-decisions) or by the resuming Attempt after a durable `resolveApproval` intent; it is the
 * deterministic decision authority for every later Attempt of the same Run.
 */
export class ToolApprovalDecided extends Schema.TaggedClass<ToolApprovalDecided>(
  "@effect-agent/session/ToolApprovalDecided",
)("ToolApprovalDecided", {
  runId: RunId,
  turn: TurnNumber,
  toolCallId: ToolCallId,
  decision: ApprovalDecision,
  resolver: BoundedName,
  reason: BoundedText,
}) {}

/**
 * First-class interruption audit (durability §9): appended by a superseding Attempt before it
 * re-invokes the model for a Turn whose prior owner died without a complete canonical response.
 * Duplicate provider cost is thereby possible AND observable in canonical history.
 */
export class ModelResponseInterrupted extends Schema.TaggedClass<ModelResponseInterrupted>(
  "@effect-agent/session/ModelResponseInterrupted",
)("ModelResponseInterrupted", {
  runId: RunId,
  supersededEpoch: ProducerEpoch,
  attemptId: AttemptId,
  reason: BoundedText,
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

/**
 * Current private-development canonical payload family. Phase 5 adds the seven durable-Tool tags
 * (prepared/unknown/resolved/step/approval-request/approval-decision/interrupted) additively, so
 * the envelope keeps `schemaVersion: 1` (P4 precedent for additive payload tags).
 */
export const CanonicalRecordPayload = Schema.Union([
  ConversationCreated,
  UserInputRecorded,
  ModelCompleted,
  ModelResponseRecorded,
  ToolCallPrepared,
  ToolCallSettled,
  ToolCallUnknown,
  ToolCallResolved,
  ToolStepSettled,
  ToolApprovalRequested,
  ToolApprovalDecided,
  ModelResponseInterrupted,
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
