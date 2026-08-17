import {
  AgentId,
  AttemptId,
  ConversationId,
  ReceiptId,
  SettlementId,
  SubmissionId,
  ToolCallId,
} from "@effect-agent/core";
import { Context, Crypto, Duration, Effect, Option, Schema, Stream } from "effect";

import { digestJson } from "./digest.ts";
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
 * process-crash durability (fsync-backed storage); `durable-cloudflare` asserts the same
 * single-serialized-owner crash durability through Durable Object storage output gates;
 * `non-durable` marks reference adapters whose state does not survive the process (in-memory
 * conformance/test adapters). The label is an honest self-description for reports and
 * evidence — no runtime behavior branches on it.
 */
export class LedgerCapabilities extends Schema.Class<LedgerCapabilities>(
  "@effect-agent/session/LedgerCapabilities",
)({
  durability: Schema.Literals(["durable-node", "durable-cloudflare", "non-durable"]),
}) {}

/**
 * Immutable lineage from a child Submission to the exact parent Tool Call that established it
 * (spec §12 step 5, SUB-004). Recorded once at admission and never rewritten; it is an
 * identifier, not an authorization capability (D10) — the join path verifies the canonical
 * Parent Link records, never this marker alone.
 */
export class ParentLinkage extends Schema.Class<ParentLinkage>(
  "@effect-agent/session/ParentLinkage",
)({
  parentSubmissionId: SubmissionId,
  parentToolCallId: ToolCallId,
}) {}

/**
 * Durable admission input. `inputDigest` must be the digest of the canonical JSON encoding of
 * `inputPayload` (see `digestJson`); admission idempotency compares digests, never re-encodes.
 * `agentId`/`agentDigests`/`deploymentId` are persisted so recovery can complete Conversation
 * materialization without the submitting client. `parentLinkage` is present exactly when the
 * Submission is a durable attached child (spec §12); an admission replay must repeat the same
 * linkage (or its absence) or fail with `AdmissionConflict`.
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
  parentLinkage: Schema.optionalKey(ParentLinkage),
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
  parentLinkage: Schema.optionalKey(ParentLinkage),
}) {}

/** The authoritative store proves the scoped idempotency key was never admitted (SUB-031). */
export class AdmissionNotAdmitted extends Schema.TaggedClass<AdmissionNotAdmitted>(
  "@effect-agent/session/AdmissionNotAdmitted",
)("NotAdmitted", {}) {}

/** The scoped idempotency key was admitted; the full Submission snapshot is authoritative. */
export class AdmissionAdmitted extends Schema.TaggedClass<AdmissionAdmitted>(
  "@effect-agent/session/AdmissionAdmitted",
)("Admitted", {
  submission: SubmissionSnapshot,
}) {}

/**
 * The adapter cannot currently prove admission or its absence (e.g. the authoritative child
 * owner is unreachable on a partitioned platform). Callers wait and retry; an indeterminate
 * answer NEVER permits a second admission attempt (SUB-031).
 */
export class AdmissionIndeterminate extends Schema.TaggedClass<AdmissionIndeterminate>(
  "@effect-agent/session/AdmissionIndeterminate",
)("Indeterminate", {
  reason: Schema.String.check(Schema.isMaxLength(4096)),
}) {}

/**
 * The tri-state answer of `resolveAdmission` (spec §12: "absence from an eventually consistent
 * projection is never proof"). Single strongly consistent stores derive `NotAdmitted`/`Admitted`
 * from a direct lookup; only adapters that can genuinely fail to reach the authoritative owner
 * answer `Indeterminate`.
 */
export const AdmissionResolution = Schema.Union([
  AdmissionNotAdmitted,
  AdmissionAdmitted,
  AdmissionIndeterminate,
]);
export type AdmissionResolution = typeof AdmissionResolution.Type;

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

/**
 * Exact canonical evidence used to reconstruct or overwrite the Submission Ledger's operational
 * settlement state (DUR-015). The Conversation Log record is the authority; `recordDigest`
 * protects the exact envelope, including its timestamp and optional outcome metadata.
 */
export class CanonicalSettlementRepair extends Schema.Class<CanonicalSettlementRepair>(
  "@effect-agent/session/CanonicalSettlementRepair",
)({
  submissionId: SubmissionId,
  record: RecordEnvelope,
  recordDigest: Digest,
}) {}

/** Canonical settlement fields extracted only after the shared exact-record validation passes. */
export class ValidatedCanonicalSettlement extends Schema.Class<ValidatedCanonicalSettlement>(
  "@effect-agent/session/ValidatedCanonicalSettlement",
)({
  submissionId: SubmissionId,
  settlementId: SettlementId,
  receiptId: ReceiptId,
  outcome: SettlementOutcome,
  record: RecordEnvelope,
  recordDigest: Digest,
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
 * bump, the host already owns the lane. An `admitted`-but-not-`ready` row breaks the prefix;
 * an aborted-settled row is a closed obligation and is skipped as a non-gap (P7 §7(c)).
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
 * Why one Submission is durably suspended. Approval was the only Phase 5 reason; S2 adds
 * `WaitingForChild` (spec §12 step 10) and the union keeps later suspension families (e.g.
 * operator quarantine) additive.
 */
export class ApprovalPendingSuspension extends Schema.TaggedClass<ApprovalPendingSuspension>(
  "@effect-agent/session/ApprovalPendingSuspension",
)("ApprovalPending", {
  toolCallIds: Schema.NonEmptyArray(ToolCallId),
}) {}

/** One attached child a `waitingForChild` parent is durably waiting on (spec §12 step 10). */
export class WaitingChild extends Schema.Class<WaitingChild>("@effect-agent/session/WaitingChild")({
  toolCallId: ToolCallId,
  childSubmissionId: SubmissionId,
}) {}

/**
 * The parent Attempt ended without settling because attached durable children must settle and
 * join first (spec §12 step 10, SUB-030). The suspended lane holds no worker permit; each listed
 * child settles on its own Conversation lane and records operational coverage; only the
 * coordinator's canonical-evidence-authorized `resumeSuspension` may wake the parent.
 */
export class WaitingForChildSuspension extends Schema.TaggedClass<WaitingForChildSuspension>(
  "@effect-agent/session/WaitingForChildSuspension",
)("WaitingForChild", {
  children: Schema.NonEmptyArray(WaitingChild),
}) {}

export const SuspensionReason = Schema.Union([
  ApprovalPendingSuspension,
  WaitingForChildSuspension,
]);
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
 * `suspended` when the lane is now durably waiting; `resume-immediately` when the suspension
 * reason is already fully covered (a covering decision or child settlement raced ahead of the
 * suspend transaction), so the caller resumes without releasing the lane.
 */
export const SuspensionOutcome = Schema.Literals(["suspended", "resume-immediately"]);
export type SuspensionOutcome = typeof SuspensionOutcome.Type;

/**
 * Coordinator-authorized wake of one exact suspension. The coordinator may construct this only
 * after matching `expectedReason` against canonical Conversation evidence; the adapter atomically
 * rechecks the stored reason and operational coverage before making the lane runnable.
 */
export class ResumeSuspensionRequest extends Schema.Class<ResumeSuspensionRequest>(
  "@effect-agent/session/ResumeSuspensionRequest",
)({
  submissionId: SubmissionId,
  expectedReason: SuspensionReason,
}) {}

/** Result of an exact, canonical-evidence-authorized suspension wake attempt. */
export const SuspensionResumeOutcome = Schema.Literals(["resumed", "not-suspended", "not-covered"]);
export type SuspensionResumeOutcome = typeof SuspensionResumeOutcome.Type;

/**
 * A durable child-settlement notification for one waitingForChild parent (spec §12 step 10).
 * The child's canonical Settlement is the authority; this command only drives the parent lane's
 * wake transition.
 */
export class ChildSettledNotification extends Schema.Class<ChildSettledNotification>(
  "@effect-agent/session/ChildSettledNotification",
)({
  parentSubmissionId: SubmissionId,
  childSubmissionId: SubmissionId,
}) {}

/**
 * This notification records operational child-settlement coverage but never makes a lane
 * runnable by itself. `still-waiting` means the parent currently has a matching suspension;
 * `not-waiting` means it does not. `woken` remains decode-compatible with stored/protocol results
 * from the earlier combined operation but conforming adapters no longer produce it; only
 * `resumeSuspension` may clear a suspension.
 */
export const ChildSettledOutcome = Schema.Literals(["woken", "still-waiting", "not-waiting"]);
export type ChildSettledOutcome = typeof ChildSettledOutcome.Type;

/** Parent-owned child budget reservation identity, deterministically derived by the
 * coordinator from the parent Run and Tool Call identity (spec §12 step 2). */
export const ChildReservationId = identifier("ChildReservationId");
export type ChildReservationId = typeof ChildReservationId.Type;

/**
 * The reservation state machine (spec §12 steps 2 and 6): `reserved` while the child may still
 * consume the allocation, `releasePending` once the final accounting decision is frozen, and
 * `released` after the unused allocation returned exactly once.
 */
export const ChildReservationStatus = Schema.Literals(["reserved", "releasePending", "released"]);
export type ChildReservationStatus = typeof ChildReservationStatus.Type;

/**
 * Creation input for one parent-owned child budget reservation. The allocation is an opaque
 * Schema-encoded budget document (D8: adapters implement transitions only and never interpret
 * budget dimensions); `allocationDigest` must be the digest of its canonical JSON encoding.
 */
export class ChildBudgetReservationRequest extends Schema.Class<ChildBudgetReservationRequest>(
  "@effect-agent/session/ChildBudgetReservationRequest",
)({
  reservationId: ChildReservationId,
  parentSubmissionId: SubmissionId,
  parentToolCallId: ToolCallId,
  ownershipToken: OwnershipToken,
  allocation: PersistedJson,
  allocationDigest: Digest,
}) {}

/** One child budget reservation row as the port exposes it. */
export class ChildBudgetReservationSnapshot extends Schema.Class<ChildBudgetReservationSnapshot>(
  "@effect-agent/session/ChildBudgetReservationSnapshot",
)({
  reservationId: ChildReservationId,
  parentSubmissionId: SubmissionId,
  parentToolCallId: ToolCallId,
  childSubmissionId: Schema.optionalKey(SubmissionId),
  status: ChildReservationStatus,
  allocation: PersistedJson,
  allocationDigest: Digest,
  accounting: Schema.optionalKey(PersistedJson),
  reservedAt: Schema.DateTimeUtcFromString,
  releaseBeganAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  releasedAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
}) {}

/**
 * The committed reservation. `replayed` is true when an identical reservation already existed;
 * the stored row is returned unchanged so a recovering caller resumes rather than duplicates
 * (SUB-016).
 */
export class ReservedChildBudget extends Schema.Class<ReservedChildBudget>(
  "@effect-agent/session/ReservedChildBudget",
)({
  reservation: ChildBudgetReservationSnapshot,
  replayed: Schema.Boolean,
}) {}

/** Records the admitted child on its reservation row (repairable from `SubagentStarted`). */
export class AttachChildToReservationRequest extends Schema.Class<AttachChildToReservationRequest>(
  "@effect-agent/session/AttachChildToReservationRequest",
)({
  reservationId: ChildReservationId,
  ownershipToken: OwnershipToken,
  childSubmissionId: SubmissionId,
}) {}

/**
 * Freezes the final consumed/released accounting decision (spec §12 join step 6). The
 * accounting is an opaque Schema-encoded document replayed from the canonical `SubagentJoined`
 * record (or the deterministic zero-consumed decision for an orphaned reservation), so the
 * decision itself authorizes the transition — no ownership token is consulted (DUR-015).
 */
export class BeginChildBudgetReleaseRequest extends Schema.Class<BeginChildBudgetReleaseRequest>(
  "@effect-agent/session/BeginChildBudgetReleaseRequest",
)({
  reservationId: ChildReservationId,
  accounting: PersistedJson,
}) {}

/** Applies the frozen accounting decision: `releasePending → released`, exactly once. */
export class ReleaseChildBudgetRequest extends Schema.Class<ReleaseChildBudgetRequest>(
  "@effect-agent/session/ReleaseChildBudgetRequest",
)({
  reservationId: ChildReservationId,
}) {}

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
 * One attached child of this parent as its recovery sees it: the reservation's recorded child
 * plus that child's current lane state. A disposable derived view — the canonical
 * `SubagentRequested`/`SubagentStarted` records and the child's own canonical Settlement remain
 * the recovery truth (DUR-015).
 */
export class ChildAttachmentSnapshot extends Schema.Class<ChildAttachmentSnapshot>(
  "@effect-agent/session/ChildAttachmentSnapshot",
)({
  toolCallId: ToolCallId,
  childSubmissionId: SubmissionId,
  childState: SubmissionState,
  childOutcome: Schema.optionalKey(SettlementOutcome),
}) {}

/**
 * Everything the pure recovery classifier needs about one Submission, read strongly consistently
 * (STORE-003). Canonical history still wins over every marker in this snapshot (DUR-015).
 *
 * Phase 5 fields: `joins` is the host-side view (every Submission `joining`/`joined` to this
 * one); `hostSubmissionId` is the joined-side view; `suspension` mirrors the `suspended` state;
 * `approvalDecisions` and `unknownResolutions` are the durable intents recorded against this
 * Submission (empty when none exist).
 *
 * S2 fields: `childReservations`/`childAttachments` are the parent-side subagent view (ordered
 * by parent Tool Call id) and `parentLinkage` is the child-side view. They default to
 * empty/absent at construction so pre-S2 call sites stay valid; adapters always populate them.
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
  childReservations: Schema.Array(ChildBudgetReservationSnapshot).pipe(
    Schema.withConstructorDefault(Effect.succeed([])),
  ),
  childAttachments: Schema.Array(ChildAttachmentSnapshot).pipe(
    Schema.withConstructorDefault(Effect.succeed([])),
  ),
  parentLinkage: Schema.optionalKey(ParentLinkage),
}) {}

/** The same idempotency key was admitted with different canonical input content. */
export class AdmissionConflict extends Schema.TaggedError<AdmissionConflict>()(
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
export class OwnershipLost extends Schema.TaggedError<OwnershipLost>()("OwnershipLost", {
  submissionId: SubmissionId,
  actualEpoch: ProducerEpoch,
}) {}

/** A conflicting terminal outcome was already reserved or finalized (DUR-002, DUR-012). */
export class SettlementConflict extends Schema.TaggedError<SettlementConflict>()(
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
export class ApprovalConflict extends Schema.TaggedError<ApprovalConflict>()("ApprovalConflict", {
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
  existingDecision: ApprovalDecision,
}) {}

/**
 * A divergent unknown-outcome re-resolution for an already-resolved `(submissionId, toolCallId)`
 * pair (DUR-017). Repeating the SAME resolution replays the recorded intent instead.
 */
export class UnknownResolutionConflict extends Schema.TaggedError<UnknownResolutionConflict>()(
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
export class JoinedToHost extends Schema.TaggedError<JoinedToHost>()("JoinedToHost", {
  submissionId: SubmissionId,
  hostSubmissionId: SubmissionId,
}) {}

/**
 * A child budget reservation request contradicts recorded reservation state: a divergent
 * allocation for the same reservation id, a second reservation id for the same parent Tool
 * Call, a divergent child attachment, a divergent accounting freeze, or an out-of-order state
 * transition. `status` reports the existing row's status. Identical replays never conflict.
 */
export class ChildReservationConflict extends Schema.TaggedError<ChildReservationConflict>()(
  "ChildReservationConflict",
  {
    reservationId: ChildReservationId,
    status: ChildReservationStatus,
    message: Schema.String,
  },
) {}

/** Adapter-level ledger failure (storage unavailable, unknown submission, corrupt row, ...). */
export class LedgerError extends Schema.TaggedError<LedgerError>()("LedgerError", {
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
  | ChildReservationConflict
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
 *   An optional `parentLinkage` is recorded immutably at first admission (SUB-016); a replay
 *   whose linkage diverges (present vs. absent, or different identities) fails with
 *   `AdmissionConflict` even when the input digest matches.
 * - `resolveAdmission` — authoritative tri-state admission lookup by scoped idempotency key
 *   (SUB-031): `NotAdmitted` proves absence, `Admitted` carries the full snapshot, and
 *   `Indeterminate` states that the adapter cannot currently prove either. Only `NotAdmitted`
 *   permits an admission attempt. Single strongly consistent stores derive the answer from a
 *   direct lookup and never answer `Indeterminate`.
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
 *   Likewise, an ABORTED reservation for a `ready`/`terminalizing` Submission that carries a
 *   durable abort intent and holds no live ownership is authorized by that intent (P7 §7(c)):
 *   recovery settles aborted never-claimed queued work immediately, without waiting for it to
 *   head the lane. Both exceptions are outcome- and state-narrow; everything else stays fenced.
 * - `finalizeSettlement` — idempotent terminal transition (state → settled) after the reserved
 *   record is canonical; releases the lane so the next `queueSequence` becomes claimable. It
 *   requires no ownership token: canonical history authorizes finalization (DUR-015). A
 *   finalization that disagrees with the recorded outcome fails with `SettlementConflict`.
 * - `repairSettlementFromCanonical` — atomic recovery authority after the exact canonical
 *   `SubmissionSettled` record is present. The adapter validates the deterministic record and
 *   digest through `validateCanonicalSettlementRepair`, then reconstructs or overwrites every
 *   missing/divergent operational reservation and settlement column from that record, marks the
 *   row settled, clears any stale nonterminal suspension state, and releases the lane. Divergent
 *   cached ledger state is repaired rather than treated as a competing outcome: canonical history
 *   wins (DUR-015).
 * - `requestAbort` — durable, idempotent by `submissionId`: repeating returns the recorded
 *   intent unchanged (DUR-012). Fails with `SettlementConflict` once the Submission is settled;
 *   abort never rewrites a terminal outcome. Fails with `JoinedToHost` for a `joined` Submission
 *   — it settles with its host, so the abort target is the host (plan §2.5). Abort of a
 *   `joining` Submission records the intent; it is honored only if the host has not consumed the
 *   input (revert-then-abort).
 * - `claimJoining` — atomic: transitions the contiguous `ready` prefix of strictly-later
 *   `queueSequence`s (up to `maxCount`) to `joining` under the host's ownership token, recording
 *   the host linkage; no epoch bump (the host Attempt already owns the lane). An
 *   `admitted`-not-`ready` row breaks the prefix; an aborted-settled row is a closed obligation
 *   and is skipped as a non-gap (P7 §7(c)). Fails with `OwnershipLost` once superseded.
 * - `markJoined` — idempotent `joining → joined` with the canonical input record position;
 *   repairable from history when the append committed but the marker write was lost (DUR-016).
 * - `revertJoining` — recovery-only, idempotent `joining → ready`; a no-op when already joined.
 * - `suspend` — `input-applied`/`running` → `suspended`; ends the ownership period WITHOUT
 *   settling (the obligation stays owed, the lane consumes no worker permit). Returns
 *   `resume-immediately` when the reason is already fully covered: for `ApprovalPending`,
 *   recorded decisions cover every pending call; for `WaitingForChild`, every listed child is
 *   already provably settled. Adapters must guarantee that a child settlement reported before
 *   the suspend transaction commits is observed by that suspend (single-store adapters derive
 *   this from the child's own row; cross-store adapters record a durable notification marker).
 *   Fails with `SettlementConflict` once settled.
 * - `resumeSuspension` — coordinator-owned wake transition after canonical evidence validation.
 *   Atomically requires the stored suspension reason to equal `expectedReason` byte-for-field and
 *   all corresponding operational decisions/child-settlement notifications to cover it, then
 *   transitions `suspended → input-applied`. A divergent reason fails as `LedgerError`; incomplete
 *   coverage returns `not-covered`; a replay after wake returns `not-suspended`.
 * - `recordApprovalDecision` — durable, idempotent per `(submissionId, toolCallId)`: repeating
 *   the same decision replays the intent; a divergent re-decision fails with `ApprovalConflict`.
 *   It records operational coverage only and never clears suspension. Fails with
 *   `SettlementConflict` once settled.
 * - `recordChildSettled` — idempotently records cross-lane operational coverage. The child's
 *   canonical Settlement is the authority — a notification for an unsettled child is an
 *   adapter-checked caller error (`LedgerError` on single-store adapters). It never settles or
 *   wakes the parent and requires no ownership token.
 * - `reserveChildBudget` — idempotent get-or-create of the parent-owned child budget
 *   reservation (spec §12 step 2), fenced by the parent lane's live `OwnershipToken`. An
 *   identical replay returns the stored row with `replayed` set (unfenced, mirroring
 *   `reserveSettlement`); a divergent allocation for the same id, or a second id for the same
 *   `(parentSubmissionId, parentToolCallId)`, fails with `ChildReservationConflict`.
 * - `attachChildToReservation` — idempotent: records the admitted child on the reservation row
 *   (repairable from the canonical `SubagentStarted` record). A replay with the recorded child
 *   is a no-op; the first attach is fenced by the parent token and requires status `reserved`;
 *   a divergent child fails with `ChildReservationConflict`.
 * - `beginChildBudgetRelease` — `reserved → releasePending`: freezes the final accounting
 *   decision exactly once. Replaying the identical accounting (at `releasePending` or
 *   `released`) is a no-op; a divergent accounting fails with `ChildReservationConflict`.
 *   Requires no ownership token: the accounting is replayed from the canonical `SubagentJoined`
 *   record (or is the deterministic zero-consumed decision for an orphaned reservation), so
 *   canonical history authorizes the transition (DUR-015) and the freeze comparison — not a
 *   fence — guarantees one decision.
 * - `releaseChildBudget` — `releasePending → released`, applied exactly once; replaying returns
 *   the released row unchanged. Releasing a still-`reserved` row (no frozen accounting) fails
 *   with `ChildReservationConflict`. "A crash before release leaves budget unavailable until
 *   repair, never available twice" (spec §12).
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
    readonly resolveAdmission: (
      request: SubmissionLookupByKey,
    ) => Effect.Effect<AdmissionResolution, LedgerError>;
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
    readonly repairSettlementFromCanonical: (
      request: CanonicalSettlementRepair,
    ) => Effect.Effect<Settlement, LedgerError>;
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
    readonly resumeSuspension: (
      request: ResumeSuspensionRequest,
    ) => Effect.Effect<SuspensionResumeOutcome, LedgerError>;
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
    readonly recordChildSettled: (
      request: ChildSettledNotification,
    ) => Effect.Effect<ChildSettledOutcome, LedgerError>;
    readonly reserveChildBudget: (
      request: ChildBudgetReservationRequest,
    ) => Effect.Effect<ReservedChildBudget, ChildReservationConflict | OwnershipLost | LedgerError>;
    readonly attachChildToReservation: (
      request: AttachChildToReservationRequest,
    ) => Effect.Effect<
      ChildBudgetReservationSnapshot,
      ChildReservationConflict | OwnershipLost | LedgerError
    >;
    readonly beginChildBudgetRelease: (
      request: BeginChildBudgetReleaseRequest,
    ) => Effect.Effect<ChildBudgetReservationSnapshot, ChildReservationConflict | LedgerError>;
    readonly releaseChildBudget: (
      request: ReleaseChildBudgetRequest,
    ) => Effect.Effect<ChildBudgetReservationSnapshot, ChildReservationConflict | LedgerError>;
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

/**
 * Shared fail-closed validator used by every SubmissionLedger adapter before canonical settlement
 * repair. It validates the exact deterministic identity, record family, payload identity and
 * outcome, row receipt identity, and SHA-256 digest. Adapters must call it before mutating any
 * operational settlement columns and then use only the returned fields as repair authority.
 */
export const validateCanonicalSettlementRepair = Effect.fn(
  "SubmissionLedger.validateCanonicalSettlementRepair",
)(function* (
  unvalidated: unknown,
  expectedReceiptId: ReceiptId,
): Effect.fn.Return<ValidatedCanonicalSettlement, LedgerError, Crypto.Crypto> {
  const invalid = (message: string) =>
    LedgerError.make({ operation: "repairSettlementFromCanonical", message });
  const familyProjection = yield* Schema.decodeUnknownEffect(
    Schema.Struct({ record: Schema.Struct({ family: Schema.String }) }),
  )(unvalidated).pipe(
    Effect.mapError((cause) =>
      invalid(`Canonical settlement repair is malformed: ${cause.message}`),
    ),
  );
  if (familyProjection.record.family !== "conversation") {
    return yield* invalid("Canonical settlement repair requires a conversation-family record");
  }
  const request = yield* Schema.decodeUnknownEffect(Schema.toType(CanonicalSettlementRepair))(
    unvalidated,
  ).pipe(
    Effect.mapError((cause) =>
      invalid(`Canonical settlement repair is malformed: ${cause.message}`),
    ),
  );
  const payload = request.record.payload;
  if (request.record.recordId !== submissionSettlementRecordId(request.submissionId)) {
    return yield* invalid(
      `Canonical settlement record id does not match Submission ${request.submissionId}`,
    );
  }
  if (payload._tag !== "SubmissionSettled") {
    return yield* invalid("Canonical settlement repair requires a SubmissionSettled record");
  }
  if (payload.submissionId !== request.submissionId) {
    return yield* invalid("Canonical settlement payload carries a different Submission identity");
  }
  if (payload.settlementId !== submissionSettlementId(request.submissionId)) {
    return yield* invalid("Canonical settlement payload carries a non-deterministic SettlementId");
  }
  if (payload.receiptId !== expectedReceiptId) {
    return yield* invalid("Canonical settlement payload carries a different Receipt identity");
  }
  const encoded = yield* Schema.encodeEffect(RecordEnvelope)(request.record).pipe(
    Effect.mapError((cause) =>
      invalid(`Canonical settlement record failed Schema encoding: ${cause.message}`),
    ),
  );
  const actualDigest = yield* digestJson(encoded).pipe(
    Effect.mapError((cause) => invalid(`Canonical settlement digest failed: ${cause.message}`)),
  );
  if (actualDigest !== request.recordDigest) {
    return yield* invalid("Canonical settlement record digest does not match its exact envelope");
  }
  return ValidatedCanonicalSettlement.make({
    submissionId: request.submissionId,
    settlementId: payload.settlementId,
    receiptId: payload.receiptId,
    outcome: payload.outcome,
    record: request.record,
    recordDigest: request.recordDigest,
  });
});

/** Deterministic batch identity of one Submission's canonical `AbortRequested` append. */
export const submissionAbortBatchId = (submissionId: SubmissionId): BatchId =>
  decodeBatchId(`submission-abort:${submissionId}`);

/** Deterministic canonical record identity of one Submission's `AbortRequested` record. */
export const submissionAbortRecordId = (submissionId: SubmissionId): RecordId =>
  decodeRecordId(`abort:${submissionId}`);
