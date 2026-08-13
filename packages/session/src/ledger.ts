import {
  AgentId,
  AttemptId,
  ConversationId,
  ReceiptId,
  SettlementId,
  SubmissionId,
} from "@effect-agent/core";
import { Context, Duration, Effect, Option, Schema, Stream } from "effect";

import {
  AbortRequested,
  BatchId,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  SettlementOutcome,
} from "./records.ts";

const identifier = <const Name extends string>(name: Name) =>
  Schema.NonEmptyString.check(Schema.isMaxLength(256)).pipe(
    Schema.brand(`@effect-agent/session/${name}`),
  );

/** Configured client identity that scopes admission idempotency keys (durability §2). */
export const Principal = identifier("Principal");
export type Principal = typeof Principal.Type;

/** Client-supplied idempotency key, scoped to one (conversation, principal) pair. */
export const IdempotencyKey = identifier("IdempotencyKey");
export type IdempotencyKey = typeof IdempotencyKey.Type;

/**
 * Opaque proof that one Attempt currently owns a Submission's lane. It authorizes ledger
 * mutations only; canonical-log fencing authority remains the producer epoch (DUR-006).
 */
export const OwnershipToken = identifier("OwnershipToken");
export type OwnershipToken = typeof OwnershipToken.Type;

/** Conversation-local FIFO position allocated once at admission (DUR-004). */
export const QueueSequence = Schema.Natural.pipe(
  Schema.brand("@effect-agent/session/QueueSequence"),
);
export type QueueSequence = typeof QueueSequence.Type;

/**
 * Ledger lifecycle states for one Submission. `settled` is the only terminal state; every other
 * state carries an outstanding accepted-work obligation (Accepted-work Contract, DUR-002).
 */
export const SubmissionState = Schema.Literals([
  "admitted",
  "ready",
  "running",
  "input-applied",
  "terminalizing",
  "settled",
]);
export type SubmissionState = typeof SubmissionState.Type;

/**
 * Default ownership lease duration (D5). The lease is a liveness hint that makes an abandoned
 * claim reclaimable; correctness never depends on it because every canonical append is fenced by
 * producer epoch. Adapters expose this as configuration and use this value when unconfigured.
 */
export const DEFAULT_OWNERSHIP_LEASE_DURATION: Duration.Duration = Duration.seconds(30);

/**
 * What a SubmissionLedger adapter claims about itself. `durable-node` asserts single-node
 * process-crash durability (fsync-backed storage); `non-durable` marks reference adapters whose
 * state does not survive the process (in-memory conformance/test adapters).
 */
export class LedgerCapabilities extends Schema.Class<LedgerCapabilities>(
  "@effect-agent/session/LedgerCapabilities",
)({
  durability: Schema.Literals(["durable-node", "non-durable"]),
}) {}

/**
 * Durable admission input. `inputDigest` must be the digest of the canonical JSON encoding of
 * `inputPayload` (see `digestJson`); admission idempotency compares digests, never re-encodes.
 * `agentId`/`agentDigests`/`deploymentId` are persisted so recovery can complete Conversation
 * materialization without the submitting client.
 */
export class AdmissionRequest extends Schema.Class<AdmissionRequest>(
  "@effect-agent/session/AdmissionRequest",
)({
  conversationId: ConversationId,
  principal: Principal,
  idempotencyKey: IdempotencyKey,
  agentId: AgentId,
  agentDigests: DefinitionDigests,
  deploymentId: DeploymentId,
  inputPayload: PersistedJson,
  inputDigest: Digest,
}) {}

/**
 * The committed admission row. `replayed` is true when an identical (conversation, principal,
 * idempotencyKey, inputDigest) admission already existed; the original identities are returned
 * unchanged so a client retry resumes rather than duplicates (DUR-001).
 */
export class AdmissionResult extends Schema.Class<AdmissionResult>(
  "@effect-agent/session/AdmissionResult",
)({
  submissionId: SubmissionId,
  receiptId: ReceiptId,
  queueSequence: QueueSequence,
  state: SubmissionState,
  replayed: Schema.Boolean,
}) {}

export class MarkReadyRequest extends Schema.Class<MarkReadyRequest>(
  "@effect-agent/session/MarkReadyRequest",
)({
  submissionId: SubmissionId,
}) {}

export class SubmissionLookupById extends Schema.TaggedClass<SubmissionLookupById>(
  "@effect-agent/session/SubmissionLookupById",
)("SubmissionLookupById", {
  submissionId: SubmissionId,
}) {}

export class SubmissionLookupByKey extends Schema.TaggedClass<SubmissionLookupByKey>(
  "@effect-agent/session/SubmissionLookupByKey",
)("SubmissionLookupByKey", {
  conversationId: ConversationId,
  principal: Principal,
  idempotencyKey: IdempotencyKey,
}) {}

/** Lookup by Submission identity or by the scoped client idempotency key. */
export const SubmissionLookup = Schema.Union([SubmissionLookupById, SubmissionLookupByKey]);
export type SubmissionLookup = typeof SubmissionLookup.Type;

/** The full durable ledger view of one Submission. */
export class SubmissionSnapshot extends Schema.Class<SubmissionSnapshot>(
  "@effect-agent/session/SubmissionSnapshot",
)({
  submissionId: SubmissionId,
  conversationId: ConversationId,
  queueSequence: QueueSequence,
  principal: Principal,
  idempotencyKey: IdempotencyKey,
  agentId: AgentId,
  agentDigests: DefinitionDigests,
  deploymentId: DeploymentId,
  inputPayload: PersistedJson,
  inputDigest: Digest,
  receiptId: ReceiptId,
  state: SubmissionState,
  settledOutcome: Schema.optionalKey(SettlementOutcome),
  createdAt: Schema.DateTimeUtcFromString,
  readyAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
}) {}

export class ClaimRequest extends Schema.Class<ClaimRequest>("@effect-agent/session/ClaimRequest")({
  conversationId: ConversationId,
  producerId: ProducerId,
}) {}

/**
 * One granted ownership period. `producerEpoch` is the fencing token for every canonical append
 * this Attempt performs; `leaseExpiresAt` is when the claim becomes reclaimable unless renewed.
 */
export class Claim extends Schema.Class<Claim>("@effect-agent/session/Claim")({
  submissionId: SubmissionId,
  attemptId: AttemptId,
  ownershipToken: OwnershipToken,
  producerEpoch: ProducerEpoch,
  leaseExpiresAt: Schema.DateTimeUtcFromString,
  inputPayload: PersistedJson,
}) {}

export class RenewOwnershipRequest extends Schema.Class<RenewOwnershipRequest>(
  "@effect-agent/session/RenewOwnershipRequest",
)({
  submissionId: SubmissionId,
  ownershipToken: OwnershipToken,
}) {}

/**
 * A renewed lease. Adapters MAY rotate the token; callers must use the returned token for every
 * later ledger mutation.
 */
export class OwnershipRenewal extends Schema.Class<OwnershipRenewal>(
  "@effect-agent/session/OwnershipRenewal",
)({
  ownershipToken: OwnershipToken,
  leaseExpiresAt: Schema.DateTimeUtcFromString,
}) {}

export class ReleaseOwnershipRequest extends Schema.Class<ReleaseOwnershipRequest>(
  "@effect-agent/session/ReleaseOwnershipRequest",
)({
  submissionId: SubmissionId,
  ownershipToken: OwnershipToken,
}) {}

/**
 * The canonical position of the applied `UserInputRecorded` record for one Submission.
 */
export class MarkInputAppliedRequest extends Schema.Class<MarkInputAppliedRequest>(
  "@effect-agent/session/MarkInputAppliedRequest",
)({
  submissionId: SubmissionId,
  ownershipToken: OwnershipToken,
  recordId: RecordId,
  sequence: CanonicalSequence,
}) {}

export class InputAppliedMarker extends Schema.Class<InputAppliedMarker>(
  "@effect-agent/session/InputAppliedMarker",
)({
  recordId: RecordId,
  sequence: CanonicalSequence,
}) {}

/**
 * Reservation of the single exact settlement record for one Submission (DUR-011). `record` is
 * the complete canonical envelope that will be appended — including `createdAt` — so a recovering
 * Attempt can re-append byte-identical content without a batch-digest conflict. `recordDigest`
 * must be the digest of the canonical JSON encoding of the Schema-encoded envelope.
 */
export class SettlementReservation extends Schema.Class<SettlementReservation>(
  "@effect-agent/session/SettlementReservation",
)({
  submissionId: SubmissionId,
  ownershipToken: OwnershipToken,
  settlementId: SettlementId,
  outcome: SettlementOutcome,
  record: RecordEnvelope,
  recordDigest: Digest,
}) {}

/**
 * The committed reservation. `replayed` is true when an identical reservation already existed;
 * the stored exact record is returned so recovery appends precisely what was reserved.
 */
export class ReservedSettlement extends Schema.Class<ReservedSettlement>(
  "@effect-agent/session/ReservedSettlement",
)({
  submissionId: SubmissionId,
  settlementId: SettlementId,
  outcome: SettlementOutcome,
  record: RecordEnvelope,
  recordDigest: Digest,
  replayed: Schema.Boolean,
}) {}

export class SettlementFinalization extends Schema.Class<SettlementFinalization>(
  "@effect-agent/session/SettlementFinalization",
)({
  submissionId: SubmissionId,
  settlementId: SettlementId,
}) {}

/** The single durable terminal outcome recorded for one accepted Submission (DUR-002). */
export class Settlement extends Schema.Class<Settlement>("@effect-agent/session/Settlement")({
  submissionId: SubmissionId,
  settlementId: SettlementId,
  receiptId: ReceiptId,
  outcome: SettlementOutcome,
  settledAt: Schema.DateTimeUtcFromString,
}) {}

/**
 * A durable abort command (durability §13). Field bounds are exactly those of the canonical
 * `AbortRequested` payload so the intent can become canonical without re-validation.
 */
export class AbortCommand extends Schema.Class<AbortCommand>("@effect-agent/session/AbortCommand")({
  submissionId: SubmissionId,
  author: AbortRequested.fields.author,
  reason: AbortRequested.fields.reason,
}) {}

/**
 * The recorded abort intent. `canonicalRecordId` is present once the corresponding canonical
 * `AbortRequested` record has been appended (its identity is deterministic; see
 * `submissionAbortRecordId`).
 */
export class AbortIntent extends Schema.Class<AbortIntent>("@effect-agent/session/AbortIntent")({
  submissionId: SubmissionId,
  author: AbortRequested.fields.author,
  reason: AbortRequested.fields.reason,
  requestedAt: Schema.DateTimeUtcFromString,
  canonicalRecordId: Schema.optionalKey(RecordId),
}) {}

/** Current ownership as recovery sees it. The live owner's token is never exposed here. */
export class OwnershipSnapshot extends Schema.Class<OwnershipSnapshot>(
  "@effect-agent/session/OwnershipSnapshot",
)({
  attemptId: AttemptId,
  ownerProducerId: ProducerId,
  producerEpoch: ProducerEpoch,
  leaseExpiresAt: Schema.DateTimeUtcFromString,
}) {}

/** A settlement reservation as recovery sees it. */
export class SettlementReservationSnapshot extends Schema.Class<SettlementReservationSnapshot>(
  "@effect-agent/session/SettlementReservationSnapshot",
)({
  settlementId: SettlementId,
  outcome: SettlementOutcome,
  record: RecordEnvelope,
  recordDigest: Digest,
  finalized: Schema.Boolean,
}) {}

export class RecoverySnapshotRequest extends Schema.Class<RecoverySnapshotRequest>(
  "@effect-agent/session/RecoverySnapshotRequest",
)({
  submissionId: SubmissionId,
}) {}

/**
 * Everything the pure recovery classifier needs about one Submission, read strongly consistently
 * (STORE-003). Canonical history still wins over every marker in this snapshot (DUR-015).
 */
export class RecoverySnapshot extends Schema.Class<RecoverySnapshot>(
  "@effect-agent/session/RecoverySnapshot",
)({
  submission: SubmissionSnapshot,
  ownership: Schema.optionalKey(OwnershipSnapshot),
  inputApplied: Schema.optionalKey(InputAppliedMarker),
  reservation: Schema.optionalKey(SettlementReservationSnapshot),
  abortIntent: Schema.optionalKey(AbortIntent),
}) {}

/** The same idempotency key was admitted with different canonical input content. */
export class AdmissionConflict extends Schema.TaggedErrorClass<AdmissionConflict>()(
  "AdmissionConflict",
  {
    conversationId: ConversationId,
    principal: Principal,
    idempotencyKey: IdempotencyKey,
    existingInputDigest: Digest,
    attemptedInputDigest: Digest,
  },
) {}

/**
 * The presented ownership token no longer owns the Submission's lane. `actualEpoch` is the
 * conversation's current producer epoch so a stale Attempt can prove to itself it was superseded.
 */
export class OwnershipLost extends Schema.TaggedErrorClass<OwnershipLost>()("OwnershipLost", {
  submissionId: SubmissionId,
  actualEpoch: ProducerEpoch,
}) {}

/** A conflicting terminal outcome was already reserved or finalized (DUR-002, DUR-012). */
export class SettlementConflict extends Schema.TaggedErrorClass<SettlementConflict>()(
  "SettlementConflict",
  {
    submissionId: SubmissionId,
    existingOutcome: SettlementOutcome,
  },
) {}

/** Adapter-level ledger failure (storage unavailable, unknown submission, corrupt row, ...). */
export class LedgerError extends Schema.TaggedErrorClass<LedgerError>()("LedgerError", {
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export type SubmissionLedgerFailure =
  | AdmissionConflict
  | OwnershipLost
  | SettlementConflict
  | LedgerError;

/**
 * Operational durable state for accepted work: admission, FIFO readiness, ownership, leases,
 * abort intent, and settlement obligations (STORE-001). The Conversation Log remains the
 * authority for applied input and terminal outcomes; every marker in this ledger can be rebuilt
 * from canonical history (DUR-015).
 *
 * Operation contracts:
 *
 * - `admit` — atomic and idempotent by (conversationId, principal, idempotencyKey). A replay
 *   with the same `inputDigest` returns the original `AdmissionResult` with `replayed` set; a
 *   replay with a different digest fails with `AdmissionConflict`. Admission allocates the
 *   Submission's `queueSequence` (FIFO position, DUR-004) and mints `submissionId`/`receiptId`.
 * - `markReady` — idempotent transition admitted → ready after Conversation materialization.
 *   Marking an already-ready (or later-state) Submission is a no-op.
 * - `lookup` — read one Submission by identity or scoped idempotency key; strongly consistent
 *   with prior ledger writes.
 * - `claim` — FIFO-head claim rule: claims ONLY the lowest unsettled `queueSequence` of the
 *   requested Conversation lane (DUR-004/DUR-005), regardless of that head's nonterminal state,
 *   and returns `Option.none` when the lane has no unsettled work or the head's lease is still
 *   live under another owner. A successful claim atomically bumps the Conversation's producer
 *   epoch — fencing every stale Attempt out of canonical appends (DUR-006) — mints a fresh
 *   `attemptId` and `ownershipToken`, and starts the ownership lease (D5: adapters default to
 *   `DEFAULT_OWNERSHIP_LEASE_DURATION`, configurable). An expired lease makes the head
 *   claimable; expiry alone never revokes correctness, only permission to assume liveness.
 * - `renewOwnership` — extends the lease while the token still owns the lane; fails with
 *   `OwnershipLost` once superseded. Renewal after lease expiry succeeds if no other claim has
 *   taken the lane in between.
 * - `releaseOwnership` — graceful drain: ends the ownership period without settling, returning
 *   a nonterminal head to claimable state. Fails with `OwnershipLost` once superseded.
 * - `markInputApplied` — idempotent marker that the deterministic `UserInputRecorded` record is
 *   canonical (state → input-applied). Recovery repairs this marker from history when the append
 *   committed but the marker write was lost.
 * - `reserveSettlement` — reserves the Submission's single exact settlement record
 *   (state → terminalizing). Idempotent: an identical reservation replays with `replayed` set; a
 *   different outcome or content fails with `SettlementConflict{existingOutcome}` (DUR-011).
 * - `finalizeSettlement` — idempotent terminal transition (state → settled) after the reserved
 *   record is canonical; releases the lane so the next `queueSequence` becomes claimable. It
 *   requires no ownership token: canonical history authorizes finalization (DUR-015). A
 *   finalization that disagrees with the recorded outcome fails with `SettlementConflict`.
 * - `requestAbort` — durable, idempotent by `submissionId`: repeating returns the recorded
 *   intent unchanged (DUR-012). Fails with `SettlementConflict` once the Submission is settled;
 *   abort never rewrites a terminal outcome.
 * - `scanNonterminal` — streams every Submission whose state is not `settled`, ordered by
 *   (conversationId, queueSequence); recovery's admission-independent worklist (DUR-014).
 * - `loadRecoverySnapshot` — strongly consistent full snapshot for the pure recovery classifier.
 *
 * No operation claims exactly-once external side effects; the ledger records decisions
 * exactly once (DUR-003).
 */
export class SubmissionLedger extends Context.Service<
  SubmissionLedger,
  {
    readonly capabilities: Effect.Effect<LedgerCapabilities>;
    readonly admit: (
      request: AdmissionRequest,
    ) => Effect.Effect<AdmissionResult, AdmissionConflict | LedgerError>;
    readonly markReady: (request: MarkReadyRequest) => Effect.Effect<void, LedgerError>;
    readonly lookup: (
      request: SubmissionLookup,
    ) => Effect.Effect<Option.Option<SubmissionSnapshot>, LedgerError>;
    readonly claim: (request: ClaimRequest) => Effect.Effect<Option.Option<Claim>, LedgerError>;
    readonly renewOwnership: (
      request: RenewOwnershipRequest,
    ) => Effect.Effect<OwnershipRenewal, OwnershipLost | LedgerError>;
    readonly releaseOwnership: (
      request: ReleaseOwnershipRequest,
    ) => Effect.Effect<void, OwnershipLost | LedgerError>;
    readonly markInputApplied: (
      request: MarkInputAppliedRequest,
    ) => Effect.Effect<void, OwnershipLost | LedgerError>;
    readonly reserveSettlement: (
      request: SettlementReservation,
    ) => Effect.Effect<ReservedSettlement, SettlementConflict | OwnershipLost | LedgerError>;
    readonly finalizeSettlement: (
      request: SettlementFinalization,
    ) => Effect.Effect<Settlement, SettlementConflict | LedgerError>;
    readonly requestAbort: (
      request: AbortCommand,
    ) => Effect.Effect<AbortIntent, SettlementConflict | LedgerError>;
    readonly scanNonterminal: Stream.Stream<SubmissionSnapshot, LedgerError>;
    readonly loadRecoverySnapshot: (
      request: RecoverySnapshotRequest,
    ) => Effect.Effect<RecoverySnapshot, LedgerError>;
  }
>()("@effect-agent/session/SubmissionLedger") {}

const decodeBatchId = Schema.decodeSync(BatchId);
const decodeRecordId = Schema.decodeSync(RecordId);
const decodeSettlementId = Schema.decodeSync(SettlementId);

/**
 * Deterministic identity rules shared by every adapter and the durable coordinator. The batch
 * carrying one Submission's canonical `UserInputRecorded` record is `submission-input:{sid}` and
 * the record is `input:{sid}`, so batch idempotency (DUR-007) makes the input append
 * exactly-once-canonical across Attempts.
 */
export const submissionInputBatchId = (submissionId: SubmissionId): BatchId =>
  decodeBatchId(`submission-input:${submissionId}`);

/** Deterministic canonical record identity of one Submission's `UserInputRecorded` record. */
export const submissionInputRecordId = (submissionId: SubmissionId): RecordId =>
  decodeRecordId(`input:${submissionId}`);

/** Deterministic `SettlementId` for one Submission: `settlement:{sid}`. */
export const submissionSettlementId = (submissionId: SubmissionId): SettlementId =>
  decodeSettlementId(`settlement:${submissionId}`);

/** Deterministic batch identity of one Submission's canonical `SubmissionSettled` append. */
export const submissionSettlementBatchId = (submissionId: SubmissionId): BatchId =>
  decodeBatchId(`submission-settlement:${submissionId}`);

/** Deterministic canonical record identity of one Submission's `SubmissionSettled` record. */
export const submissionSettlementRecordId = (submissionId: SubmissionId): RecordId =>
  decodeRecordId(`settlement:${submissionId}`);

/** Deterministic batch identity of one Submission's canonical `AbortRequested` append. */
export const submissionAbortBatchId = (submissionId: SubmissionId): BatchId =>
  decodeBatchId(`submission-abort:${submissionId}`);

/** Deterministic canonical record identity of one Submission's `AbortRequested` record. */
export const submissionAbortRecordId = (submissionId: SubmissionId): RecordId =>
  decodeRecordId(`abort:${submissionId}`);
