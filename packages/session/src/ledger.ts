import {
  AgentId,
  AttemptId,
  ConversationId,
  ReceiptId,
  SettlementId,
  SubmissionId,
  ToolCallId,
} from "@effect-agent/core";
import { Context, Duration, Effect, Option, Schema, Stream } from "effect";

import {
  AbortRequested,
  ApprovalDecision,
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
  ToolApprovalDecided,
  ToolCallResolved,
  ToolCallUnknown,
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
 *
 * Phase 5 states:
 *
 * - `joining` — a queued Submission claimed by an active host Run, before its canonical
 *   `UserInputRecorded` append; recovery reverts it to `ready` (DUR-016).
 * - `joined` — the input is canonical and linked to its host; the Submission settles WITH the
 *   host Run's outcome.
 * - `suspended` — durably waiting for explicit approval decisions; the ownership period has
 *   ended and the lane consumes no worker permit while the obligation stays owed.
 * - `unknown` — at least one ordinary Tool Call has a durable Unknown Outcome; the lane is
 *   blocked until an authorized resolution covers every open call (DUR-017).
 *
 * `claim` never grants a `joining`, `joined`, `suspended`, or `unknown` head: the lane is
 * host-owned or blocked, never worker-claimable.
 */
export const SubmissionState = Schema.Literals([
  "admitted",
  "ready",
  "joining",
  "joined",
  "running",
  "input-applied",
  "suspended",
  "unknown",
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

/**
 * Atomic claim of the contiguous ready prefix of strictly-later queued Submissions for joining
 * into the active host Run (plan §2.5). Fenced by the host Attempt's ownership token — no epoch
 * bump, the host already owns the lane. An `admitted`-but-not-`ready` row breaks the prefix.
 */
export class ClaimJoiningRequest extends Schema.Class<ClaimJoiningRequest>(
  "@effect-agent/session/ClaimJoiningRequest",
)({
  conversationId: ConversationId,
  hostSubmissionId: SubmissionId,
  ownershipToken: OwnershipToken,
  maxCount: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

/** One queued Submission transitioned to `joining` under the host's ownership. */
export class JoiningClaim extends Schema.Class<JoiningClaim>("@effect-agent/session/JoiningClaim")({
  submissionId: SubmissionId,
  queueSequence: QueueSequence,
  inputPayload: PersistedJson,
}) {}

/**
 * The canonical position of one joined Submission's `UserInputRecorded` record (`input:{sid}`).
 * Marking transitions `joining → joined` and records the host linkage established at claim time.
 */
export class MarkJoinedRequest extends Schema.Class<MarkJoinedRequest>(
  "@effect-agent/session/MarkJoinedRequest",
)({
  submissionId: SubmissionId,
  ownershipToken: OwnershipToken,
  recordId: RecordId,
  sequence: CanonicalSequence,
}) {}

/**
 * Recovery-only revert of a `joining` Submission whose canonical input append never committed
 * (DUR-016): `joining → ready`, idempotent, a no-op when the Submission already joined.
 */
export class RevertJoiningRequest extends Schema.Class<RevertJoiningRequest>(
  "@effect-agent/session/RevertJoiningRequest",
)({
  submissionId: SubmissionId,
}) {}

/**
 * Why one Submission is durably suspended. Approval is the only Phase 5 reason; the union keeps
 * later suspension families (e.g. operator quarantine) additive.
 */
export class ApprovalPendingSuspension extends Schema.TaggedClass<ApprovalPendingSuspension>(
  "@effect-agent/session/ApprovalPendingSuspension",
)("ApprovalPending", {
  toolCallIds: Schema.NonEmptyArray(ToolCallId),
}) {}

export const SuspensionReason = Schema.Union([ApprovalPendingSuspension]);
export type SuspensionReason = typeof SuspensionReason.Type;

/**
 * Durable suspension of owned work (plan §2.6): `input-applied`/`running` → `suspended`, the
 * ownership period ends, NO settlement occurs — the accepted-work obligation stays owed while
 * the lane consumes no worker permit.
 */
export class SuspendRequest extends Schema.Class<SuspendRequest>(
  "@effect-agent/session/SuspendRequest",
)({
  submissionId: SubmissionId,
  ownershipToken: OwnershipToken,
  reason: SuspensionReason,
}) {}

/**
 * `suspended` when the lane is now durably waiting; `resume-immediately` when recorded decision
 * intents already cover every pending call of the suspension reason (a decision raced ahead of
 * the suspend transaction), so the caller resumes without releasing the lane.
 */
export const SuspensionOutcome = Schema.Literals(["suspended", "resume-immediately"]);
export type SuspensionOutcome = typeof SuspensionOutcome.Type;

/**
 * A durable approval decision command (plan §2.6, `abort`-shaped). Field bounds are exactly those
 * of the canonical `ToolApprovalDecided` payload so the intent can become canonical without
 * re-validation.
 */
export class ApprovalDecisionCommand extends Schema.Class<ApprovalDecisionCommand>(
  "@effect-agent/session/ApprovalDecisionCommand",
)({
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
  decision: ApprovalDecision,
  resolver: ToolApprovalDecided.fields.resolver,
  reason: ToolApprovalDecided.fields.reason,
}) {}

/**
 * The recorded approval-decision intent, idempotent per `(submissionId, toolCallId)`.
 * `canonicalRecordId` is present once the corresponding canonical `ToolApprovalDecided` record
 * has been appended (see `toolApprovalDecisionRecordId`).
 */
export class ApprovalDecisionIntent extends Schema.Class<ApprovalDecisionIntent>(
  "@effect-agent/session/ApprovalDecisionIntent",
)({
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
  decision: ApprovalDecision,
  resolver: ToolApprovalDecided.fields.resolver,
  reason: ToolApprovalDecided.fields.reason,
  decidedAt: Schema.DateTimeUtcFromString,
  canonicalRecordId: Schema.optionalKey(RecordId),
}) {}

/**
 * Durable Unknown marking for one Submission's open ordinary Tool Calls (DUR-009): state →
 * `unknown`, the lane blocks, ownership-free (recovery may mark without a claim). Idempotent.
 */
export class MarkUnknownRequest extends Schema.Class<MarkUnknownRequest>(
  "@effect-agent/session/MarkUnknownRequest",
)({
  submissionId: SubmissionId,
  toolCallIds: Schema.NonEmptyArray(ToolCallId),
  reason: ToolCallUnknown.fields.reason,
}) {}

/** The recovered supplier truth: the external effect completed with this exact result. */
export class ResolutionCompletedWithResult extends Schema.TaggedClass<ResolutionCompletedWithResult>(
  "@effect-agent/session/ResolutionCompletedWithResult",
)("CompletedWithResult", {
  result: PersistedJson,
  isFailure: Schema.Boolean,
}) {}

/** The external effect provably never happened; the call may re-execute on resume. */
export class ResolutionNeverHappened extends Schema.TaggedClass<ResolutionNeverHappened>(
  "@effect-agent/session/ResolutionNeverHappened",
)("NeverHappened", {}) {}

/** The call is safe to repeat under the external system's idempotency contract. */
export class ResolutionSafeToRetry extends Schema.TaggedClass<ResolutionSafeToRetry>(
  "@effect-agent/session/ResolutionSafeToRetry",
)("SafeToRetry", {}) {}

/** Give up on the Submission: route into the abort path (unknown calls stay recorded; abort
 * never claims external rollback, durability §13). */
export class ResolutionAbortSubmission extends Schema.TaggedClass<ResolutionAbortSubmission>(
  "@effect-agent/session/ResolutionAbortSubmission",
)("AbortSubmission", {}) {}

/** How an authorized resolver closed one Unknown Outcome (DUR-017). */
export const UnknownResolution = Schema.Union([
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  ResolutionSafeToRetry,
  ResolutionAbortSubmission,
]);
export type UnknownResolution = typeof UnknownResolution.Type;

/**
 * A durable Unknown-Outcome resolution command (DUR-017, `abort`-shaped). Possession of the
 * runtime service plus the mandatory `author`/`reason` audit fields is the Phase 5 authorization
 * boundary — identical to `abort`; the authenticated operator surface is a P7 deliverable.
 * Field bounds are exactly those of the canonical `ToolCallResolved` payload.
 */
export class UnknownResolutionCommand extends Schema.Class<UnknownResolutionCommand>(
  "@effect-agent/session/UnknownResolutionCommand",
)({
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
  author: ToolCallResolved.fields.author,
  reason: ToolCallResolved.fields.reason,
  resolution: UnknownResolution,
}) {}

/**
 * The recorded resolution intent, idempotent per `(submissionId, toolCallId)`. The canonical
 * `ToolCallResolved` (+ `ToolCallSettled` for `CompletedWithResult`) is appended by the recovery
 * pass or the next owning Attempt; `canonicalRecordId` is present once it is.
 */
export class UnknownResolutionIntent extends Schema.Class<UnknownResolutionIntent>(
  "@effect-agent/session/UnknownResolutionIntent",
)({
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
  author: ToolCallResolved.fields.author,
  reason: ToolCallResolved.fields.reason,
  resolution: UnknownResolution,
  resolvedAt: Schema.DateTimeUtcFromString,
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

/** One joined/joining Submission of a host Run, as the host's recovery sees it. */
export class JoinSnapshot extends Schema.Class<JoinSnapshot>("@effect-agent/session/JoinSnapshot")({
  submissionId: SubmissionId,
  state: SubmissionState,
  hostSubmissionId: SubmissionId,
}) {}

/** A durable suspension as recovery sees it. */
export class SuspensionSnapshot extends Schema.Class<SuspensionSnapshot>(
  "@effect-agent/session/SuspensionSnapshot",
)({
  reason: SuspensionReason,
  suspendedAt: Schema.DateTimeUtcFromString,
}) {}

/**
 * Everything the pure recovery classifier needs about one Submission, read strongly consistently
 * (STORE-003). Canonical history still wins over every marker in this snapshot (DUR-015).
 *
 * Phase 5 fields: `joins` is the host-side view (every Submission `joining`/`joined` to this
 * one); `hostSubmissionId` is the joined-side view; `suspension` mirrors the `suspended` state;
 * `approvalDecisions` and `unknownResolutions` are the durable intents recorded against this
 * Submission (empty when none exist).
 */
export class RecoverySnapshot extends Schema.Class<RecoverySnapshot>(
  "@effect-agent/session/RecoverySnapshot",
)({
  submission: SubmissionSnapshot,
  ownership: Schema.optionalKey(OwnershipSnapshot),
  inputApplied: Schema.optionalKey(InputAppliedMarker),
  reservation: Schema.optionalKey(SettlementReservationSnapshot),
  abortIntent: Schema.optionalKey(AbortIntent),
  joins: Schema.Array(JoinSnapshot),
  hostSubmissionId: Schema.optionalKey(SubmissionId),
  suspension: Schema.optionalKey(SuspensionSnapshot),
  approvalDecisions: Schema.Array(ApprovalDecisionIntent),
  unknownResolutions: Schema.Array(UnknownResolutionIntent),
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

/**
 * A divergent approval re-decision for an already-decided `(submissionId, toolCallId)` pair.
 * Repeating the SAME decision replays the recorded intent instead (idempotency).
 */
export class ApprovalConflict extends Schema.TaggedErrorClass<ApprovalConflict>()(
  "ApprovalConflict",
  {
    submissionId: SubmissionId,
    toolCallId: ToolCallId,
    existingDecision: ApprovalDecision,
  },
) {}

/**
 * A divergent unknown-outcome re-resolution for an already-resolved `(submissionId, toolCallId)`
 * pair (DUR-017). Repeating the SAME resolution replays the recorded intent instead.
 */
export class UnknownResolutionConflict extends Schema.TaggedErrorClass<UnknownResolutionConflict>()(
  "UnknownResolutionConflict",
  {
    submissionId: SubmissionId,
    toolCallId: ToolCallId,
  },
) {}

/**
 * The target Submission is `joined` to a host Run: it settles with the host, so the abort target
 * is the host Submission carried here (plan §2.5).
 */
export class JoinedToHost extends Schema.TaggedErrorClass<JoinedToHost>()("JoinedToHost", {
  submissionId: SubmissionId,
  hostSubmissionId: SubmissionId,
}) {}

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
  | ApprovalConflict
  | UnknownResolutionConflict
  | JoinedToHost
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
 *   For a `joined` Submission the reservation is authorized by its recorded host linkage — a
 *   joined lane is never worker-claimable, so no ownership token can exist for it and the
 *   presented token is not consulted (plan §2.5: joined Submissions settle with the host).
 * - `finalizeSettlement` — idempotent terminal transition (state → settled) after the reserved
 *   record is canonical; releases the lane so the next `queueSequence` becomes claimable. It
 *   requires no ownership token: canonical history authorizes finalization (DUR-015). A
 *   finalization that disagrees with the recorded outcome fails with `SettlementConflict`.
 * - `requestAbort` — durable, idempotent by `submissionId`: repeating returns the recorded
 *   intent unchanged (DUR-012). Fails with `SettlementConflict` once the Submission is settled;
 *   abort never rewrites a terminal outcome. Fails with `JoinedToHost` for a `joined` Submission
 *   — it settles with its host, so the abort target is the host (plan §2.5). Abort of a
 *   `joining` Submission records the intent; it is honored only if the host has not consumed the
 *   input (revert-then-abort).
 * - `claimJoining` — atomic: transitions the contiguous `ready` prefix of strictly-later
 *   `queueSequence`s (up to `maxCount`) to `joining` under the host's ownership token, recording
 *   the host linkage; no epoch bump (the host Attempt already owns the lane). An
 *   `admitted`-not-`ready` row breaks the prefix. Fails with `OwnershipLost` once superseded.
 * - `markJoined` — idempotent `joining → joined` with the canonical input record position;
 *   repairable from history when the append committed but the marker write was lost (DUR-016).
 * - `revertJoining` — recovery-only, idempotent `joining → ready`; a no-op when already joined.
 * - `suspend` — `input-applied`/`running` → `suspended`; ends the ownership period WITHOUT
 *   settling (the obligation stays owed, the lane consumes no worker permit). Returns
 *   `resume-immediately` when recorded decisions already cover the reason's pending calls.
 *   Fails with `SettlementConflict` once settled.
 * - `recordApprovalDecision` — durable, idempotent per `(submissionId, toolCallId)`: repeating
 *   the same decision replays the intent; a divergent re-decision fails with `ApprovalConflict`.
 *   Transitions `suspended → input-applied` once every pending call of the suspension reason is
 *   decided, waking the lane. Fails with `SettlementConflict` once settled.
 * - `markUnknown` — idempotent, ownership-free (canonical evidence authorizes it): state →
 *   `unknown`, the lane blocks and stops consuming worker permits while the accepted settlement
 *   obligation stays visible (DUR-009/DUR-017). Fails with `SettlementConflict` once settled.
 * - `recordUnknownResolution` — durable, idempotent per `(submissionId, toolCallId)`; a
 *   divergent re-resolution fails with `UnknownResolutionConflict`. Transitions
 *   `unknown → input-applied` once no open call remains, waking the lane. Fails with
 *   `SettlementConflict` once settled.
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
    ) => Effect.Effect<AbortIntent, SettlementConflict | JoinedToHost | LedgerError>;
    readonly claimJoining: (
      request: ClaimJoiningRequest,
    ) => Effect.Effect<ReadonlyArray<JoiningClaim>, OwnershipLost | LedgerError>;
    readonly markJoined: (
      request: MarkJoinedRequest,
    ) => Effect.Effect<void, OwnershipLost | LedgerError>;
    readonly revertJoining: (request: RevertJoiningRequest) => Effect.Effect<void, LedgerError>;
    readonly suspend: (
      request: SuspendRequest,
    ) => Effect.Effect<SuspensionOutcome, OwnershipLost | SettlementConflict | LedgerError>;
    readonly recordApprovalDecision: (
      command: ApprovalDecisionCommand,
    ) => Effect.Effect<ApprovalDecisionIntent, ApprovalConflict | SettlementConflict | LedgerError>;
    readonly markUnknown: (
      request: MarkUnknownRequest,
    ) => Effect.Effect<void, SettlementConflict | LedgerError>;
    readonly recordUnknownResolution: (
      command: UnknownResolutionCommand,
    ) => Effect.Effect<
      UnknownResolutionIntent,
      UnknownResolutionConflict | SettlementConflict | LedgerError
    >;
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
