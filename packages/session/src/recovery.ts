import { ConversationId, SettlementId, SubmissionId, ToolCallId } from "@effect-agent/core";
import { Effect, Schema } from "effect";

import {
  ChildReservationId,
  submissionSettlementId,
  WaitingChild,
  type ChildAttachmentSnapshot,
  type ChildBudgetReservationSnapshot,
  type RecoverySnapshot,
} from "./ledger.ts";
import { SettlementOutcome, ToolCallPrepared } from "./records.ts";

/** One canonical `ToolCallPrepared` without a settling or resolving canonical record (DUR-009). */
export class OpenToolCallEvidence extends Schema.Class<OpenToolCallEvidence>(
  "@effect-agent/session/OpenToolCallEvidence",
)({
  toolCallId: ToolCallId,
  toolName: ToolCallPrepared.fields.toolName,
  turn: ToolCallPrepared.fields.turn,
}) {}

/**
 * A committed tool-declaring response with ZERO prepared and ZERO settled records for its Turn:
 * the provably-safe durability §15 window — no prepared records means no handler ran, so the
 * declared batch resumes without model re-invocation and without Unknown.
 */
export class DeclaredPendingBatchEvidence extends Schema.Class<DeclaredPendingBatchEvidence>(
  "@effect-agent/session/DeclaredPendingBatchEvidence",
)({
  turn: ToolCallPrepared.fields.turn,
  callCount: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

/** One canonical `ToolApprovalRequested` without a canonical `ToolApprovalDecided`. */
export class PendingApprovalEvidence extends Schema.Class<PendingApprovalEvidence>(
  "@effect-agent/session/PendingApprovalEvidence",
)({
  toolCallId: ToolCallId,
  turn: ToolCallPrepared.fields.turn,
}) {}

/**
 * The authoritative tri-state admission answer recorded as pure classifier evidence (SUB-031):
 * the recovery pass queries `resolveAdmission` with the deterministic child idempotency key for
 * a requested-but-unstarted delegation call and records the result here. Absence of this field
 * means the lookup was not performed — the classifier treats that exactly like `indeterminate`
 * (fail-closed: only a proven `not-admitted` ever permits an admission attempt).
 */
export const DelegationAdmissionEvidence = Schema.Literals([
  "not-admitted",
  "admitted",
  "indeterminate",
]);
export type DelegationAdmissionEvidence = typeof DelegationAdmissionEvidence.Type;

/**
 * One prepared-without-outcome parent Tool Call that IS a delegation (plan §4.1), separated from
 * `openToolCalls` because its establishment protocol is idempotent by construction and must
 * NEVER be marked Unknown (spec §13 vs. DUR-009). Delegation detection is durable and
 * fail-closed: a matching `ChildBudgetReservation` row or `SubagentRequested` record, plus — for
 * the pre-reservation window — the core-owned `delegate_` naming rule. The
 * `requested`/`started`/`joined` flags come from the parent-log canonical records; the child
 * identity fields are present exactly when those records (or the reservation attachment) carry
 * them.
 */
export class OpenDelegationCallEvidence extends Schema.Class<OpenDelegationCallEvidence>(
  "@effect-agent/session/OpenDelegationCallEvidence",
)({
  toolCallId: ToolCallId,
  toolName: ToolCallPrepared.fields.toolName,
  turn: ToolCallPrepared.fields.turn,
  /** The canonical `SubagentRequested` record exists for this call. */
  requested: Schema.Boolean,
  /** The canonical `SubagentStarted` record exists for this call. */
  started: Schema.Boolean,
  /** The canonical `SubagentJoined` record exists for this call. */
  joined: Schema.Boolean,
  childConversationId: Schema.optionalKey(ConversationId),
  childSubmissionId: Schema.optionalKey(SubmissionId),
  admission: Schema.optionalKey(DelegationAdmissionEvidence),
}) {}

/**
 * Canonical-log facts about one Submission, read strongly consistently alongside the ledger
 * `RecoverySnapshot` (STORE-003). Canonical history wins over every ledger marker (DUR-015), so
 * the classifier consults these facts before trusting ledger state — with one deliberate Phase 5
 * inversion: a RECORDED terminal outcome now beats open tool calls, because a canonical
 * settlement is never revisited (DUR-002/DUR-015); the P4 rule that unknown-beats-settlement is
 * gone.
 *
 * `openToolCalls` is the DUR-009 seam: every `ToolCallPrepared` without a matching
 * `ToolCallSettled` or `ToolCallResolved`. S2 separates `openDelegationCalls` from it (plan
 * §4.1): a delegation's prepared-without-outcome state is provably replay-safe, so those calls
 * route through the Subagent recovery rows and never through `MarkUnknown`. Names and
 * operational reservation rows cannot upgrade ordinary classification. `joinedInputCovered`
 * implements the plan §2.5 prompt-coverage rule for the joined side: a joined input is covered
 * iff a later `ModelResponseRecorded` of the host Run exists after the `input:{sid}` record.
 * `hostSettlementOutcome` is the joined-side view of the host's canonical `SubmissionSettled`
 * record, present exactly when that record exists.
 */
export class RecoveryEvidence extends Schema.Class<RecoveryEvidence>(
  "@effect-agent/session/RecoveryEvidence",
)({
  /** The Conversation exists in the ConversationStore. */
  conversationMaterialized: Schema.Boolean,
  /** The deterministic canonical `UserInputRecorded` record (`input:{sid}`) is committed. */
  inputRecorded: Schema.Boolean,
  /**
   * The committed canonical `SubmissionSettled` outcome (`settlement:{sid}`). Present exactly
   * when the canonical settlement record exists, so the flag and the outcome cannot diverge.
   */
  recordedSettlementOutcome: Schema.optionalKey(SettlementOutcome),
  /** The deterministic canonical `AbortRequested` record (`abort:{sid}`) is committed. */
  abortRecorded: Schema.Boolean,
  /** Prepared ordinary Tool Calls without a canonical settled/resolved outcome (DUR-009). */
  openToolCalls: Schema.Array(OpenToolCallEvidence),
  /**
   * Delegation Tool Calls with an open parent-side obligation (plan §4.1), removed from
   * `openToolCalls`. Wire-required; construction alone defaults it so pre-S2 evidence
   * assemblers stay valid while the classifier's fail-closed detection still protects them.
   */
  openDelegationCalls: Schema.Array(OpenDelegationCallEvidence).pipe(
    Schema.withConstructorDefault(Effect.succeed([])),
  ),
  /** A declared-but-unprepared tool batch: response canonical, zero prepared, zero settled. */
  declaredPendingBatch: Schema.optionalKey(DeclaredPendingBatchEvidence),
  /** Canonically requested approvals without a canonical decision. */
  approvalsPending: Schema.Array(PendingApprovalEvidence),
  /** Joined-side prompt coverage (plan §2.5); `true` whenever the Submission is not joined. */
  joinedInputCovered: Schema.Boolean,
  /** Joined-side view of the host's canonical settlement record, when it exists. */
  hostSettlementOutcome: Schema.optionalKey(SettlementOutcome),
  /**
   * The Conversation carries its canonical `SubagentLineageRecorded` record (P7 §7(a)). Only
   * consulted for a parent-linked Submission: a child whose lineage is not yet canonical defers
   * its own materialization/readiness repair to the parent's idempotent establishment
   * (`AwaitParentEstablishment`). Defaults to `false` so an assembler that does not compute it
   * stays fail-closed — deferring is safe (the parent's establishment completes the child),
   * running a Turn before lineage is the model-checked race (`formal/SubagentEstablishmentRace.cfg`).
   */
  subagentLineageRecorded: Schema.Boolean.pipe(
    Schema.withConstructorDefault(Effect.succeed(false)),
  ),
}) {}

/** Ledger admission committed but the Conversation is not durable: finish materialization,
 * append `ConversationCreated`, and mark readiness (crash between admit and materialize). */
export class CompleteMaterialization extends Schema.TaggedClass<CompleteMaterialization>(
  "@effect-agent/session/CompleteMaterialization",
)("CompleteMaterialization", { submissionId: SubmissionId }) {}

/** Conversation durable but the readiness marker is missing: idempotent `markReady`. */
export class RepairReadiness extends Schema.TaggedClass<RepairReadiness>(
  "@effect-agent/session/RepairReadiness",
)("RepairReadiness", { submissionId: SubmissionId }) {}

/** No canonical input yet: append the deterministic `UserInputRecorded` record and mark it. */
export class ApplyInput extends Schema.TaggedClass<ApplyInput>("@effect-agent/session/ApplyInput")(
  "ApplyInput",
  { submissionId: SubmissionId },
) {}

/** Canonical input exists but the ledger marker was lost: repair the marker only (DUR-015). */
export class RepairInputMarker extends Schema.TaggedClass<RepairInputMarker>(
  "@effect-agent/session/RepairInputMarker",
)("RepairInputMarker", { submissionId: SubmissionId }) {}

/** Input applied and no terminal work reserved: a worker resumes the Run from the last committed
 * Turn boundary (D6; the model may be re-invoked, duplicate cost is observable — the resuming
 * worker appends `ModelResponseInterrupted` when superseding a prior owner, durability §9). */
export class ResumeFromTurnBoundary extends Schema.TaggedClass<ResumeFromTurnBoundary>(
  "@effect-agent/session/ResumeFromTurnBoundary",
)("ResumeFromTurnBoundary", { submissionId: SubmissionId }) {}

/** A settlement is reserved but not canonical: append the EXACT reserved record, then finalize. */
export class AppendReservedSettlement extends Schema.TaggedClass<AppendReservedSettlement>(
  "@effect-agent/session/AppendReservedSettlement",
)("AppendReservedSettlement", {
  submissionId: SubmissionId,
  settlementId: SettlementId,
  outcome: SettlementOutcome,
}) {}

/** The canonical settlement record exists: rebuild/finalize the ledger from history and never
 * rewrite history from the cached ledger status (DUR-011, DUR-015). */
export class FinalizeLedgerFromHistory extends Schema.TaggedClass<FinalizeLedgerFromHistory>(
  "@effect-agent/session/FinalizeLedgerFromHistory",
)("FinalizeLedgerFromHistory", {
  submissionId: SubmissionId,
  settlementId: SettlementId,
  outcome: SettlementOutcome,
}) {}

/** A durable abort intent exists, no terminal outcome is reserved, and no attached-child
 * obligation remains open: settle aborted (DUR-012). The executor first appends
 * `ToolCallUnknown` audit records for any open ordinary calls — abort settles the obligation but
 * never asserts external rollback (durability §13). */
export class SettleAborted extends Schema.TaggedClass<SettleAborted>(
  "@effect-agent/session/SettleAborted",
)("SettleAborted", { submissionId: SubmissionId }) {}

/** Prepared ordinary Tool Calls have no canonical outcome: the executor reconciles each open
 * call (recovered result / never-started / safe-retry) and marks the remainder Unknown — never
 * an automatic replay (DUR-009/DUR-017). Delegation calls are excluded: their establishment is
 * idempotent by construction and routes through the Subagent rows instead (plan §4.3). */
export class MarkUnknown extends Schema.TaggedClass<MarkUnknown>(
  "@effect-agent/session/MarkUnknown",
)("MarkUnknown", {
  submissionId: SubmissionId,
  reason: Schema.String,
  openToolCallIds: Schema.Array(ToolCallId),
}) {}

/** A committed tool-declaring response has zero prepared and zero settled records: a worker
 * resumes the declared batch — no model re-invocation, no Unknown (durability §15). S2 also
 * routes an open delegation call WITHOUT establishment evidence here (spec §13 row 1): the
 * declared batch re-executes and the idempotent establishment converges on one child. */
export class ResumePendingToolBatch extends Schema.TaggedClass<ResumePendingToolBatch>(
  "@effect-agent/session/ResumePendingToolBatch",
)("ResumePendingToolBatch", {
  submissionId: SubmissionId,
  turn: DeclaredPendingBatchEvidence.fields.turn,
}) {}

/** Canonically requested approvals lack decisions: the lane waits durably (the executor repairs
 * the ledger suspension from history when the suspend transition itself was lost). */
export class AwaitApprovalDecision extends Schema.TaggedClass<AwaitApprovalDecision>(
  "@effect-agent/session/AwaitApprovalDecision",
)("AwaitApprovalDecision", { submissionId: SubmissionId }) {}

/** Every pending approval has a durable decision: a worker resumes the declared batch via the
 * batch-resume seam, appending the canonical `ToolApprovalDecided` records first. */
export class ResumeSuspended extends Schema.TaggedClass<ResumeSuspended>(
  "@effect-agent/session/ResumeSuspended",
)("ResumeSuspended", { submissionId: SubmissionId }) {}

/** Unknown Outcomes lack covering resolutions: the lane stays blocked awaiting the authorized
 * DUR-017 resolution path; the obligation stays visible, nothing replays. */
export class AwaitUnknownResolution extends Schema.TaggedClass<AwaitUnknownResolution>(
  "@effect-agent/session/AwaitUnknownResolution",
)("AwaitUnknownResolution", { submissionId: SubmissionId }) {}

/** Durable resolution intents cover every open call: apply them canonically
 * (`ToolCallResolved` + `ToolCallSettled` for recovered results) and reopen the lane. */
export class ApplyUnknownResolutions extends Schema.TaggedClass<ApplyUnknownResolutions>(
  "@effect-agent/session/ApplyUnknownResolutions",
)("ApplyUnknownResolutions", { submissionId: SubmissionId }) {}

/** `joining` without a canonical `input:{sid}` record: revert to `ready` — the input was never
 * consumed and will be delivered exactly once later (DUR-016). */
export class RevertJoining extends Schema.TaggedClass<RevertJoining>(
  "@effect-agent/session/RevertJoining",
)("RevertJoining", { submissionId: SubmissionId }) {}

/** The canonical input exists but the `joined` marker was lost: repair the marker only, exactly
 * like `RepairInputMarker` (DUR-015/DUR-016); the input reattaches, never duplicates. */
export class RepairJoinMarker extends Schema.TaggedClass<RepairJoinMarker>(
  "@effect-agent/session/RepairJoinMarker",
)("RepairJoinMarker", { submissionId: SubmissionId }) {}

/** The host Run settled canonically: settle this joined Submission with the host outcome
 * (each joined Submission is owed its own settlement, DUR-002). */
export class SettleJoinedWithHost extends Schema.TaggedClass<SettleJoinedWithHost>(
  "@effect-agent/session/SettleJoinedWithHost",
)("SettleJoinedWithHost", {
  submissionId: SubmissionId,
  outcome: SettlementOutcome,
}) {}

/** The Submission is `joined` and its host Run is still live: nothing to repair — the input
 * reattaches through the host's resume and the Submission settles with the host. */
export class AwaitHostSettlement extends Schema.TaggedClass<AwaitHostSettlement>(
  "@effect-agent/session/AwaitHostSettlement",
)("AwaitHostSettlement", {
  submissionId: SubmissionId,
  hostSubmissionId: Schema.optionalKey(SubmissionId),
}) {}

/** A canonical `SubagentRequested` exists and the authoritative admission lookup proved
 * `notAdmitted`: idempotently admit the intended child from the canonical request payload — it
 * carries the encoded child input (D3), the intended child identity, and every digest — then
 * materialize the child Conversation, append its immutable lineage, and mark it ready. No live
 * delegation handler is required (spec §13, SUB-016/SUB-031). */
export class CompleteChildAdmission extends Schema.TaggedClass<CompleteChildAdmission>(
  "@effect-agent/session/CompleteChildAdmission",
)("CompleteChildAdmission", {
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
}) {}

/** A canonical `SubagentRequested` exists but the authoritative admission lookup answered
 * `indeterminate` — or was not (yet) performed: wait and retry the authoritative owner directly.
 * Absence from a projection is never proof of absence and a second admission NEVER occurs
 * (spec §12, SUB-031). */
export class AwaitChildAdmissionResolution extends Schema.TaggedClass<AwaitChildAdmissionResolution>(
  "@effect-agent/session/AwaitChildAdmissionResolution",
)("AwaitChildAdmissionResolution", {
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
}) {}

/** The child Submission is admitted but the parent's `SubagentStarted` link is missing (or its
 * reservation attachment was lost): resolve the SAME child by its deterministic idempotency key
 * and append the exact deterministic start record, then attach the child to its reservation —
 * the same Receipt on every replay (spec §13, SUB-016/SUB-017). */
export class RepairSubagentStartLink extends Schema.TaggedClass<RepairSubagentStartLink>(
  "@effect-agent/session/RepairSubagentStartLink",
)("RepairSubagentStartLink", {
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
}) {}

/** Children are established and at least one is nonterminal while the parent lane is NOT
 * durably suspended: restore the `waitingForChild` suspension so the lane holds no worker
 * permit while each child runs on its own lane. Recovery reattaches the one existing child and
 * NEVER spawns a replacement (spec §13, SUB-018/SUB-030). */
export class EnsureWaitingForChild extends Schema.TaggedClass<EnsureWaitingForChild>(
  "@effect-agent/session/EnsureWaitingForChild",
)("EnsureWaitingForChild", {
  submissionId: SubmissionId,
  children: Schema.NonEmptyArray(WaitingChild),
}) {}

/** The parent is durably suspended `WaitingForChild` and at least one listed child is not yet
 * provably settled: the lane stays dormant and consumes no permit; the child's Settlement wakes
 * it durably (spec §12 step 10). An unresolved ordinary Tool inside a child keeps the parent
 * here honestly — the obligation stays visible and aged, nothing is fabricated (SUB-021). */
export class AwaitChildSettlement extends Schema.TaggedClass<AwaitChildSettlement>(
  "@effect-agent/session/AwaitChildSettlement",
)("AwaitChildSettlement", { submissionId: SubmissionId }) {}

/** Every relevant child is provably settled but the parent has not joined (legacy data, or a
 * canonical settlement whose idempotent wake still needs replay): record/replay the durable wake
 * so a claiming worker resumes the declared batch and joins each child's canonical Settlement —
 * the join itself needs the parent Binding and is deferred to that worker (spec §13). */
export class ResumeWaitingParent extends Schema.TaggedClass<ResumeWaitingParent>(
  "@effect-agent/session/ResumeWaitingParent",
)("ResumeWaitingParent", {
  submissionId: SubmissionId,
  children: Schema.NonEmptyArray(WaitingChild),
}) {}

/** The canonical `SubagentJoined` record exists but the reservation release is incomplete:
 * replay `beginChildBudgetRelease` with the accounting decision FROM the canonical record, then
 * apply `releaseChildBudget` — budget stays unavailable until repair, never available twice
 * (spec §12 join step 6, DUR-015). */
export class ApplyJoinAccounting extends Schema.TaggedClass<ApplyJoinAccounting>(
  "@effect-agent/session/ApplyJoinAccounting",
)("ApplyJoinAccounting", {
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
  reservationId: ChildReservationId,
}) {}

/** Parent abort is canonical while attached children are nonterminal: idempotently issue each
 * child's durable abort command and keep the parent waiting for the joins —
 * request-abort-and-join is the ONLY close behavior; the replayed command returns the recorded
 * child intent unchanged, so the intent row IS the propagation marker (spec §13.1, SUB-022,
 * DUR-012). */
export class PropagateChildAbort extends Schema.TaggedClass<PropagateChildAbort>(
  "@effect-agent/session/PropagateChildAbort",
)("PropagateChildAbort", {
  submissionId: SubmissionId,
  children: Schema.NonEmptyArray(WaitingChild),
}) {}

/** A child budget reservation can no longer bind a child — its request is absent with no
 * resumable batch, or the parent is aborting and the child provably never admitted: freeze the
 * deterministic zero-consumed accounting decision and release the unused allocation exactly
 * once (spec §13/§14: "releases the reservation exactly once"). */
export class ReleaseOrphanChildReservation extends Schema.TaggedClass<ReleaseOrphanChildReservation>(
  "@effect-agent/session/ReleaseOrphanChildReservation",
)("ReleaseOrphanChildReservation", {
  submissionId: SubmissionId,
  reservationIds: Schema.NonEmptyArray(ChildReservationId),
}) {}

/**
 * A parent-linked `admitted` Submission whose Conversation lacks the canonical
 * `SubagentLineageRecorded` record: the child lane DEFERS its own materialization/readiness
 * repair — the parent's idempotent establishment (admit → materialize → lineage → ready)
 * completes it, so a child never runs a Turn before its lineage record is canonical (P7 §7(a)).
 * Model-checked before implementation: under the pre-P7 discipline TLC reaches the
 * lineage-less child Turn (`formal/SubagentEstablishmentRace.cfg`); under this decision the
 * race is eliminated and liveness is preserved (`formal/SubagentEstablishmentFix.cfg`,
 * `AwaitParentEstablishment = TRUE`). Ordering/liveness hygiene, not a safety repair — the
 * join already fails closed without lineage (SUB-019).
 */
export class AwaitParentEstablishment extends Schema.TaggedClass<AwaitParentEstablishment>(
  "@effect-agent/session/AwaitParentEstablishment",
)("AwaitParentEstablishment", {
  submissionId: SubmissionId,
  parentSubmissionId: SubmissionId,
  parentToolCallId: ToolCallId,
}) {}

/** The Submission is settled; nothing is owed. */
export class NoAction extends Schema.TaggedClass<NoAction>("@effect-agent/session/NoAction")(
  "NoAction",
  { submissionId: SubmissionId },
) {}

/** The executable recovery decision table's output alphabet (plan §Recovery classifier). */
export const RecoveryDecision = Schema.Union([
  CompleteMaterialization,
  RepairReadiness,
  ApplyInput,
  RepairInputMarker,
  ResumeFromTurnBoundary,
  AppendReservedSettlement,
  FinalizeLedgerFromHistory,
  SettleAborted,
  MarkUnknown,
  ResumePendingToolBatch,
  AwaitApprovalDecision,
  ResumeSuspended,
  AwaitUnknownResolution,
  ApplyUnknownResolutions,
  RevertJoining,
  RepairJoinMarker,
  SettleJoinedWithHost,
  AwaitHostSettlement,
  CompleteChildAdmission,
  AwaitChildAdmissionResolution,
  RepairSubagentStartLink,
  EnsureWaitingForChild,
  AwaitChildSettlement,
  ResumeWaitingParent,
  ApplyJoinAccounting,
  PropagateChildAbort,
  ReleaseOrphanChildReservation,
  AwaitParentEstablishment,
  NoAction,
]);
export type RecoveryDecision = typeof RecoveryDecision.Type;

/**
 * The classifier's merged per-call view of one delegation Tool Call: canonical parent-log flags
 * from the evidence, the reservation row, and the child attachment from the ledger snapshot.
 */
interface DelegationCallView {
  readonly toolCallId: ToolCallId;
  turn: number | undefined;
  /** A prepared-without-outcome record exists, so the declared batch is worker-resumable. */
  open: boolean;
  requested: boolean;
  started: boolean;
  joined: boolean;
  admission: DelegationAdmissionEvidence | undefined;
  childSubmissionId: SubmissionId | undefined;
  reservation: ChildBudgetReservationSnapshot | undefined;
  attachment: ChildAttachmentSnapshot | undefined;
}

/** The attachment proves the child's canonical Settlement exists. */
const isTerminalAttachment = (attachment: ChildAttachmentSnapshot): boolean =>
  attachment.childState === "settled" || attachment.childOutcome !== undefined;

/**
 * Merge every durable delegation-detection source into per-call views (plan §4.1): the
 * explicitly classified `openDelegationCalls`, reservation rows and child attachments.
 * Reservations may need cleanup, but never upgrade an ordinary call into a replayable delegation.
 */
const delegationViewsOf = (
  snapshot: RecoverySnapshot,
  evidence: RecoveryEvidence,
): Array<DelegationCallView> => {
  const views = new Map<ToolCallId, DelegationCallView>();
  const ensure = (toolCallId: ToolCallId): DelegationCallView => {
    const existing = views.get(toolCallId);
    if (existing !== undefined) return existing;
    const created: DelegationCallView = {
      toolCallId,
      turn: undefined,
      open: false,
      requested: false,
      started: false,
      joined: false,
      admission: undefined,
      childSubmissionId: undefined,
      reservation: undefined,
      attachment: undefined,
    };
    views.set(toolCallId, created);
    return created;
  };

  for (const call of evidence.openDelegationCalls) {
    const view = ensure(call.toolCallId);
    view.turn = call.turn;
    view.open = true;
    view.requested = call.requested;
    view.started = call.started;
    view.joined = call.joined;
    view.admission = call.admission;
    view.childSubmissionId = call.childSubmissionId;
  }
  for (const reservation of snapshot.childReservations) {
    let view = views.get(reservation.parentToolCallId);
    if (view === undefined) {
      view = ensure(reservation.parentToolCallId);
    }
    view.reservation = reservation;
    if (view.childSubmissionId === undefined && reservation.childSubmissionId !== undefined) {
      view.childSubmissionId = reservation.childSubmissionId;
    }
  }
  for (const attachment of snapshot.childAttachments) {
    const view = ensure(attachment.toolCallId);
    view.attachment = attachment;
    if (view.childSubmissionId === undefined) {
      view.childSubmissionId = attachment.childSubmissionId;
    }
  }
  return [...views.values()];
};

const waitingChildOf = (view: DelegationCallView, childSubmissionId: SubmissionId): WaitingChild =>
  WaitingChild.make({ toolCallId: view.toolCallId, childSubmissionId });

const nonEmpty = <A>(values: ReadonlyArray<A>): readonly [A, ...Array<A>] | undefined => {
  const [first, ...rest] = values;
  return first === undefined ? undefined : [first, ...rest];
};

/**
 * The reservation's frozen accounting decision has not been applied: whether frozen by a
 * canonical join or by an orphan zero-consumed decision, `releasePending` always finishes
 * through the idempotent `releaseChildBudget` replay — never through a second freeze, whose
 * divergent accounting would conflict (`ChildReservationConflict`).
 */
const releasePendingReservation = (
  view: DelegationCallView,
): ChildBudgetReservationSnapshot | undefined =>
  view.reservation !== undefined && view.reservation.status === "releasePending"
    ? view.reservation
    : undefined;

/** The reservation still holds unfrozen allocation (`reserved`). */
const reservedReservation = (
  view: DelegationCallView,
): ChildBudgetReservationSnapshot | undefined =>
  view.reservation !== undefined && view.reservation.status === "reserved"
    ? view.reservation
    : undefined;

/**
 * The spec §13 Subagent rows for a live (non-suspended) parent WITHOUT a durable abort intent,
 * in most-repairing-first order. Returns `undefined` when no delegation obligation is open so
 * the ordinary rows apply.
 */
const classifyDelegationRepairs = (
  submissionId: SubmissionId,
  views: ReadonlyArray<DelegationCallView>,
): RecoveryDecision | undefined => {
  const applyJoins: Array<DelegationCallView> = [];
  const completeAdmissions: Array<DelegationCallView> = [];
  const repairStartLinks: Array<DelegationCallView> = [];
  const awaitAdmissions: Array<DelegationCallView> = [];
  const resumeBatches: Array<DelegationCallView> = [];
  const waiting: Array<WaitingChild> = [];
  const terminalUnjoined: Array<WaitingChild> = [];
  const orphanReservations: Array<ChildBudgetReservationSnapshot> = [];

  for (const view of views) {
    if (view.reservation !== undefined && view.reservation.status === "released") {
      // The reservation lifecycle is complete: release happens only after the canonical join
      // (or a provably-childless orphan decision), so no parent-side budget or waiting
      // obligation remains on this call.
      continue;
    }
    if (releasePendingReservation(view) !== undefined) {
      // Row: budget release incomplete with the accounting decision already frozen → finish
      // the idempotent release; a second freeze would conflict.
      applyJoins.push(view);
      continue;
    }
    if (view.joined) {
      // Row: parent join canonical, budget release incomplete → apply the canonical accounting.
      if (reservedReservation(view) !== undefined) applyJoins.push(view);
      continue;
    }
    if (view.started && view.childSubmissionId !== undefined) {
      // Rows: started + child nonterminal → waiting; child terminal + join missing → wake.
      const child = waitingChildOf(view, view.childSubmissionId);
      if (view.attachment !== undefined && isTerminalAttachment(view.attachment)) {
        terminalUnjoined.push(child);
      } else {
        waiting.push(child);
      }
      continue;
    }
    if (view.started || view.childSubmissionId !== undefined || view.admission === "admitted") {
      // Row: child admitted, parent start record missing (or the start/attachment link is
      // incomplete) → resolve the same child by key and append the exact link.
      repairStartLinks.push(view);
      continue;
    }
    if (view.requested) {
      // Rows: requested + notAdmitted → admit exactly one child; indeterminate (or unqueried,
      // fail-closed) → wait/retry the authoritative owner, never a second admission.
      if (view.admission === "not-admitted") completeAdmissions.push(view);
      else awaitAdmissions.push(view);
      continue;
    }
    if (view.open) {
      // Row: no reservation and no request (or reservation without a request on a live parent)
      // → the declared batch re-executes; establishment is idempotent by construction.
      resumeBatches.push(view);
      continue;
    }
    const reservation = reservedReservation(view);
    if (reservation !== undefined) {
      // Row: reservation exists, request absent, no resumable batch → release exactly once.
      orphanReservations.push(reservation);
    }
  }

  const applyJoin = applyJoins[0];
  if (applyJoin?.reservation !== undefined) {
    return ApplyJoinAccounting.make({
      submissionId,
      toolCallId: applyJoin.toolCallId,
      reservationId: applyJoin.reservation.reservationId,
    });
  }
  const completeAdmission = completeAdmissions[0];
  if (completeAdmission !== undefined) {
    return CompleteChildAdmission.make({ submissionId, toolCallId: completeAdmission.toolCallId });
  }
  const repairStartLink = repairStartLinks[0];
  if (repairStartLink !== undefined) {
    return RepairSubagentStartLink.make({ submissionId, toolCallId: repairStartLink.toolCallId });
  }
  const awaitAdmission = awaitAdmissions[0];
  if (awaitAdmission !== undefined) {
    return AwaitChildAdmissionResolution.make({
      submissionId,
      toolCallId: awaitAdmission.toolCallId,
    });
  }
  const resumeBatch = resumeBatches[0];
  if (resumeBatch?.turn !== undefined) {
    return ResumePendingToolBatch.make({ submissionId, turn: resumeBatch.turn });
  }
  const waitingChildren = nonEmpty([...waiting, ...terminalUnjoined]);
  if (waiting.length > 0 && waitingChildren !== undefined) {
    return EnsureWaitingForChild.make({ submissionId, children: waitingChildren });
  }
  const settledChildren = nonEmpty(terminalUnjoined);
  if (settledChildren !== undefined) {
    return ResumeWaitingParent.make({ submissionId, children: settledChildren });
  }
  const orphans = nonEmpty(orphanReservations.map((reservation) => reservation.reservationId));
  if (orphans !== undefined) {
    return ReleaseOrphanChildReservation.make({ submissionId, reservationIds: orphans });
  }
  return undefined;
};

/**
 * The spec §13/§13.1 Subagent rows under a canonical parent abort intent: request-abort-and-join
 * is the only close behavior, so a nonterminal attached child converts `SettleAborted` into
 * `PropagateChildAbort`, terminal-but-unjoined children are joined first, incomplete releases
 * are applied, provably childless reservations release exactly once, and only a parent with no
 * open child obligation settles aborted. Returns `undefined` for exactly that last case.
 */
const classifyDelegationAbort = (
  submissionId: SubmissionId,
  views: ReadonlyArray<DelegationCallView>,
): RecoveryDecision | undefined => {
  const awaitAdmissions: Array<DelegationCallView> = [];
  const repairStartLinks: Array<DelegationCallView> = [];
  const nonterminal: Array<WaitingChild> = [];
  const terminalUnjoined: Array<WaitingChild> = [];
  const applyJoins: Array<DelegationCallView> = [];
  const orphanReservations: Array<ChildBudgetReservationSnapshot> = [];

  for (const view of views) {
    if (view.reservation !== undefined && view.reservation.status === "released") {
      // Fully closed (release only follows the canonical join or a provably-childless orphan
      // decision): nothing left to abort, join, or release for this call.
      continue;
    }
    if (releasePendingReservation(view) !== undefined) {
      // The accounting decision is already frozen: finish the idempotent release; a second
      // (zero-consumed) freeze would conflict with the frozen decision.
      applyJoins.push(view);
      continue;
    }
    if (view.joined) {
      if (reservedReservation(view) !== undefined) applyJoins.push(view);
      continue;
    }
    if (view.childSubmissionId !== undefined) {
      if (!view.started) {
        // The child exists but the canonical start link is missing: repair it first so the
        // abort command targets a canonically linked child.
        repairStartLinks.push(view);
        continue;
      }
      const child = waitingChildOf(view, view.childSubmissionId);
      if (view.attachment !== undefined && isTerminalAttachment(view.attachment)) {
        terminalUnjoined.push(child);
      } else {
        nonterminal.push(child);
      }
      continue;
    }
    if (view.started || view.admission === "admitted") {
      repairStartLinks.push(view);
      continue;
    }
    if (view.requested) {
      if (view.admission === "not-admitted") {
        // Provably childless request under abort: never admit a child merely to abort it;
        // release the unused reservation exactly once.
        const reservation = reservedReservation(view);
        if (reservation !== undefined) orphanReservations.push(reservation);
      } else {
        // Indeterminate or unqueried: a child may exist — never release, settle, or re-admit
        // until the authoritative owner answers (SUB-031).
        awaitAdmissions.push(view);
      }
      continue;
    }
    const reservation = reservedReservation(view);
    if (reservation !== undefined) orphanReservations.push(reservation);
    // An open delegation call with no reservation carries no child obligation: establishment
    // never started, so the ordinary aborted settlement (with its unknown-audit for open
    // calls) is honest.
  }

  const awaitAdmission = awaitAdmissions[0];
  if (awaitAdmission !== undefined) {
    return AwaitChildAdmissionResolution.make({
      submissionId,
      toolCallId: awaitAdmission.toolCallId,
    });
  }
  const repairStartLink = repairStartLinks[0];
  if (repairStartLink !== undefined) {
    return RepairSubagentStartLink.make({ submissionId, toolCallId: repairStartLink.toolCallId });
  }
  const abortChildren = nonEmpty(nonterminal);
  if (abortChildren !== undefined) {
    return PropagateChildAbort.make({ submissionId, children: abortChildren });
  }
  const settledChildren = nonEmpty(terminalUnjoined);
  if (settledChildren !== undefined) {
    return ResumeWaitingParent.make({ submissionId, children: settledChildren });
  }
  const applyJoin = applyJoins[0];
  if (applyJoin?.reservation !== undefined) {
    return ApplyJoinAccounting.make({
      submissionId,
      toolCallId: applyJoin.toolCallId,
      reservationId: applyJoin.reservation.reservationId,
    });
  }
  const orphans = nonEmpty(orphanReservations.map((reservation) => reservation.reservationId));
  if (orphans !== undefined) {
    return ReleaseOrphanChildReservation.make({ submissionId, reservationIds: orphans });
  }
  return undefined;
};

/**
 * Pure recovery classifier (durability §14, DUR-013): a finite persisted snapshot plus canonical
 * evidence deterministically selects exactly one decision. Precedence, most-settled first
 * (plan §4.2/§4.3 — note the deliberate P5 reorder: canonical settlement and reservation beat
 * open tool calls, because a recorded terminal outcome is never revisited, DUR-002/DUR-015; and
 * the three S2 edits: the abort row propagates to nonterminal attached children, delegation
 * calls never mark Unknown, and the suspended row branches on the suspension reason):
 *
 * 1. `settled` → NoAction — terminal outcomes are never revisited (DUR-002).
 * 2. canonical settlement record → FinalizeLedgerFromHistory — history beats every ledger
 *    marker (DUR-015), including a missing or divergent reservation AND open tool calls.
 * 3. reservation → FinalizeLedgerFromHistory when finalized, else AppendReservedSettlement —
 *    the reserved record is the single exact outcome owed (DUR-011); it also beats a pending
 *    abort intent (DUR-012).
 * 4. joined-side states (DUR-016): `joining` without canonical input → RevertJoining; `joining`
 *    with canonical input → RepairJoinMarker (marker lost); `joined` + host settled →
 *    SettleJoinedWithHost; `joined` + host live → AwaitHostSettlement (deferred).
 * 5. abort intent → the S2 abort rows (spec §13.1 request-abort-and-join): an unproven child
 *    admission waits, an admitted-but-unlinked child repairs its start link, a nonterminal
 *    attached child gets PropagateChildAbort — the parent does NOT settle — terminal children
 *    are joined, incomplete releases applied, provably childless reservations released; ONLY a
 *    parent with no open child obligation reaches SettleAborted, where open ordinary calls
 *    become `ToolCallUnknown` audit records, never a rollback claim (§13). SettleAborted is
 *    position-blind by design (P7 §7(c)): an aborted, never-claimed `ready` Submission earns it
 *    ANYWHERE in the queue — the executor settles it immediately without waiting for it to head
 *    the lane, because settlement order of never-run work is not execution order (DUR-004
 *    bounds execution order; DUR-012 permits settling inactive accepted work without an
 *    Attempt).
 * 6. ordinary open tool calls (state not yet `unknown`) → MarkUnknown — reconcile-then-mark,
 *    never an automatic replay (DUR-009). Delegation calls are EXCLUDED (plan §4.3's most
 *    important edit): their prepared-without-outcome state is provably replay-safe and routes
 *    through rows 5/8/9 instead. A lane already `unknown` is in the DUR-017 resolution regime
 *    (row 7) and is not re-marked.
 * 7. state `unknown` → ApplyUnknownResolutions when durable intents cover every open ordinary
 *    call, else AwaitUnknownResolution.
 * 8. state `suspended` branches on the stored reason: `WaitingForChild` → ResumeWaitingParent
 *    when every listed child is provably settled (replay the idempotent wake), else
 *    AwaitChildSettlement (SUB-030); `ApprovalPending` keeps the P5 behavior — undecided
 *    canonical approval requests → AwaitApprovalDecision (repairing a lost suspend transition
 *    from history), every request decided → ResumeSuspended.
 * 9. the S2 establishment/join rows for a live parent (spec §13, most-repairing first):
 *    ApplyJoinAccounting → CompleteChildAdmission → RepairSubagentStartLink →
 *    AwaitChildAdmissionResolution → ResumePendingToolBatch (idempotent handler re-entry) →
 *    EnsureWaitingForChild → ResumeWaitingParent → ReleaseOrphanChildReservation.
 * 10. declared-but-unprepared tool batch → ResumePendingToolBatch — no handler ran (no prepared
 *     records), so the batch resumes with no model re-invocation and no Unknown (§15).
 * 11. `admitted` → a parent-linked Submission whose Conversation lacks the canonical lineage
 *     record defers (AwaitParentEstablishment, P7 §7(a) — the parent's idempotent establishment
 *     completes it; model-checked in `formal/SubagentEstablishmentFix.cfg`); otherwise
 *     CompleteMaterialization / RepairReadiness by Conversation durability.
 * 12. otherwise → ApplyInput / RepairInputMarker / ResumeFromTurnBoundary by canonical input
 *     evidence, with the ledger marker repaired from history, never the reverse.
 *
 * A CHILD Submission (one whose snapshot carries `parentLinkage`) classifies through exactly
 * the same rows as any Submission — child recovery proceeds under its own ownership token and
 * epoch (spec §13, SUB-020); nothing here consults the parent's lane.
 *
 * Ownership leases are deliberately ignored here: the executor's fenced claim is the only
 * authority over liveness, and a stale Attempt is fenced by producer epoch regardless (DUR-006).
 */
export const classifyRecovery = (
  snapshot: RecoverySnapshot,
  evidence: RecoveryEvidence,
): RecoveryDecision => {
  const submissionId = snapshot.submission.submissionId;
  const state = snapshot.submission.state;
  if (state === "settled") {
    return NoAction.make({ submissionId });
  }
  if (evidence.recordedSettlementOutcome !== undefined) {
    return FinalizeLedgerFromHistory.make({
      submissionId,
      settlementId: snapshot.reservation?.settlementId ?? submissionSettlementId(submissionId),
      outcome: evidence.recordedSettlementOutcome,
    });
  }
  if (snapshot.reservation !== undefined) {
    if (snapshot.reservation.finalized) {
      return FinalizeLedgerFromHistory.make({
        submissionId,
        settlementId: snapshot.reservation.settlementId,
        outcome: snapshot.reservation.outcome,
      });
    }
    return AppendReservedSettlement.make({
      submissionId,
      settlementId: snapshot.reservation.settlementId,
      outcome: snapshot.reservation.outcome,
    });
  }
  if (state === "joining") {
    return evidence.inputRecorded
      ? RepairJoinMarker.make({ submissionId })
      : RevertJoining.make({ submissionId });
  }
  if (state === "joined") {
    if (evidence.hostSettlementOutcome !== undefined) {
      return SettleJoinedWithHost.make({
        submissionId,
        outcome: evidence.hostSettlementOutcome,
      });
    }
    return AwaitHostSettlement.make({
      submissionId,
      ...(snapshot.hostSubmissionId === undefined
        ? {}
        : { hostSubmissionId: snapshot.hostSubmissionId }),
    });
  }
  const delegationViews = delegationViewsOf(snapshot, evidence);
  if (snapshot.abortIntent !== undefined) {
    return (
      classifyDelegationAbort(submissionId, delegationViews) ?? SettleAborted.make({ submissionId })
    );
  }
  const delegationCallIds = new Set<string>(
    evidence.openDelegationCalls.map((call) => call.toolCallId),
  );
  const ordinaryOpenCalls = evidence.openToolCalls.filter(
    (call) => !delegationCallIds.has(call.toolCallId),
  );
  if (state !== "unknown" && ordinaryOpenCalls.length > 0) {
    return MarkUnknown.make({
      submissionId,
      reason: "An ordinary Tool call may have executed without a canonical outcome",
      openToolCallIds: ordinaryOpenCalls.map((call) => call.toolCallId),
    });
  }
  if (state === "unknown") {
    const resolved = new Set(
      snapshot.unknownResolutions.map((resolution) => resolution.toolCallId),
    );
    const covered = ordinaryOpenCalls.every((call) => resolved.has(call.toolCallId));
    return covered
      ? ApplyUnknownResolutions.make({ submissionId })
      : AwaitUnknownResolution.make({ submissionId });
  }
  const decided = new Set(snapshot.approvalDecisions.map((decision) => decision.toolCallId));
  const undecidedApprovals = evidence.approvalsPending.filter(
    (pending) => !decided.has(pending.toolCallId),
  );
  if (state === "suspended") {
    const reason = snapshot.suspension?.reason;
    if (reason !== undefined && reason._tag === "WaitingForChild") {
      const terminalChildIds = new Set(
        snapshot.childAttachments
          .filter(isTerminalAttachment)
          .map((attachment) => attachment.childSubmissionId),
      );
      return reason.children.every((child) => terminalChildIds.has(child.childSubmissionId))
        ? ResumeWaitingParent.make({ submissionId, children: reason.children })
        : AwaitChildSettlement.make({ submissionId });
    }
    return undecidedApprovals.length === 0
      ? ResumeSuspended.make({ submissionId })
      : AwaitApprovalDecision.make({ submissionId });
  }
  if (undecidedApprovals.length > 0) {
    return AwaitApprovalDecision.make({ submissionId });
  }
  const delegationDecision = classifyDelegationRepairs(submissionId, delegationViews);
  if (delegationDecision !== undefined) {
    return delegationDecision;
  }
  if (evidence.declaredPendingBatch !== undefined) {
    return ResumePendingToolBatch.make({
      submissionId,
      turn: evidence.declaredPendingBatch.turn,
    });
  }
  if (state === "admitted") {
    // P7 §7(a): a parent-linked child never self-repairs readiness before its immutable
    // lineage record is canonical — the parent's establishment appends lineage BEFORE
    // markReady, so deferring here is the exact fix discipline TLC verified
    // (`formal/SubagentEstablishmentFix.cfg`); the parent completes the child idempotently.
    const linkage = snapshot.submission.parentLinkage ?? snapshot.parentLinkage;
    if (linkage !== undefined && !evidence.subagentLineageRecorded) {
      return AwaitParentEstablishment.make({
        submissionId,
        parentSubmissionId: linkage.parentSubmissionId,
        parentToolCallId: linkage.parentToolCallId,
      });
    }
    return evidence.conversationMaterialized
      ? RepairReadiness.make({ submissionId })
      : CompleteMaterialization.make({ submissionId });
  }
  if (!evidence.inputRecorded) {
    return ApplyInput.make({ submissionId });
  }
  if (snapshot.inputApplied === undefined) {
    return RepairInputMarker.make({ submissionId });
  }
  return ResumeFromTurnBoundary.make({ submissionId });
};
