import { SettlementId, SubmissionId, ToolCallId } from "@effect-agent/core";
import { Schema } from "effect";

import { submissionSettlementId, type RecoverySnapshot } from "./ledger.ts";
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
 * Canonical-log facts about one Submission, read strongly consistently alongside the ledger
 * `RecoverySnapshot` (STORE-003). Canonical history wins over every ledger marker (DUR-015), so
 * the classifier consults these facts before trusting ledger state — with one deliberate Phase 5
 * inversion: a RECORDED terminal outcome now beats open tool calls, because a canonical
 * settlement is never revisited (DUR-002/DUR-015); the P4 rule that unknown-beats-settlement is
 * gone.
 *
 * `openToolCalls` is the DUR-009 seam: every `ToolCallPrepared` without a matching
 * `ToolCallSettled` or `ToolCallResolved`. `joinedInputCovered` implements the plan §2.5
 * prompt-coverage rule for the joined side: a joined input is covered iff a later
 * `ModelResponseRecorded` of the host Run exists after the `input:{sid}` record.
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
  /** A declared-but-unprepared tool batch: response canonical, zero prepared, zero settled. */
  declaredPendingBatch: Schema.optionalKey(DeclaredPendingBatchEvidence),
  /** Canonically requested approvals without a canonical decision. */
  approvalsPending: Schema.Array(PendingApprovalEvidence),
  /** Joined-side prompt coverage (plan §2.5); `true` whenever the Submission is not joined. */
  joinedInputCovered: Schema.Boolean,
  /** Joined-side view of the host's canonical settlement record, when it exists. */
  hostSettlementOutcome: Schema.optionalKey(SettlementOutcome),
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

/** A durable abort intent exists and no terminal outcome is reserved: settle aborted (DUR-012).
 * The executor first appends `ToolCallUnknown` audit records for any open calls — abort settles
 * the obligation but never asserts external rollback (durability §13). */
export class SettleAborted extends Schema.TaggedClass<SettleAborted>(
  "@effect-agent/session/SettleAborted",
)("SettleAborted", { submissionId: SubmissionId }) {}

/** Prepared ordinary Tool Calls have no canonical outcome: the executor reconciles each open
 * call (recovered result / never-started / safe-retry) and marks the remainder Unknown — never
 * an automatic replay (DUR-009/DUR-017). */
export class MarkUnknown extends Schema.TaggedClass<MarkUnknown>(
  "@effect-agent/session/MarkUnknown",
)("MarkUnknown", {
  submissionId: SubmissionId,
  reason: Schema.String,
  openToolCallIds: Schema.Array(ToolCallId),
}) {}

/** A committed tool-declaring response has zero prepared and zero settled records: a worker
 * resumes the declared batch — no model re-invocation, no Unknown (durability §15). */
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
  NoAction,
]);
export type RecoveryDecision = typeof RecoveryDecision.Type;

/**
 * Pure recovery classifier (durability §14, DUR-013): a finite persisted snapshot plus canonical
 * evidence deterministically selects exactly one decision. Precedence, most-settled first
 * (plan §4.2 — note the deliberate P5 reorder: canonical settlement and reservation now beat
 * open tool calls, because a recorded terminal outcome is never revisited, DUR-002/DUR-015):
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
 * 5. abort intent → SettleAborted — inactive accepted work settles aborted without an Attempt;
 *    open calls become `ToolCallUnknown` audit records, never a rollback claim (§13).
 * 6. open tool calls (state not yet `unknown`) → MarkUnknown — reconcile-then-mark, never an
 *    automatic replay (DUR-009). A lane already `unknown` is in the DUR-017 resolution regime
 *    (row 7) and is not re-marked.
 * 7. state `unknown` → ApplyUnknownResolutions when durable intents cover every open call,
 *    else AwaitUnknownResolution.
 * 8. undecided canonical approval requests → AwaitApprovalDecision (repairing a lost suspend
 *    transition from history); state `suspended` with every request decided → ResumeSuspended.
 * 9. declared-but-unprepared tool batch → ResumePendingToolBatch — no handler ran (no prepared
 *    records), so the batch resumes with no model re-invocation and no Unknown (§15).
 * 10. `admitted` → CompleteMaterialization / RepairReadiness by Conversation durability.
 * 11. otherwise → ApplyInput / RepairInputMarker / ResumeFromTurnBoundary by canonical input
 *     evidence, with the ledger marker repaired from history, never the reverse.
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
  if (snapshot.abortIntent !== undefined) {
    return SettleAborted.make({ submissionId });
  }
  if (state !== "unknown" && evidence.openToolCalls.length > 0) {
    return MarkUnknown.make({
      submissionId,
      reason: "An ordinary Tool call may have executed without a canonical outcome",
      openToolCallIds: evidence.openToolCalls.map((call) => call.toolCallId),
    });
  }
  if (state === "unknown") {
    const resolved = new Set(
      snapshot.unknownResolutions.map((resolution) => resolution.toolCallId),
    );
    const covered = evidence.openToolCalls.every((call) => resolved.has(call.toolCallId));
    return covered
      ? ApplyUnknownResolutions.make({ submissionId })
      : AwaitUnknownResolution.make({ submissionId });
  }
  const decided = new Set(snapshot.approvalDecisions.map((decision) => decision.toolCallId));
  const undecidedApprovals = evidence.approvalsPending.filter(
    (pending) => !decided.has(pending.toolCallId),
  );
  if (state === "suspended") {
    return undecidedApprovals.length === 0
      ? ResumeSuspended.make({ submissionId })
      : AwaitApprovalDecision.make({ submissionId });
  }
  if (undecidedApprovals.length > 0) {
    return AwaitApprovalDecision.make({ submissionId });
  }
  if (evidence.declaredPendingBatch !== undefined) {
    return ResumePendingToolBatch.make({
      submissionId,
      turn: evidence.declaredPendingBatch.turn,
    });
  }
  if (state === "admitted") {
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
