import { SettlementId, SubmissionId } from "@effect-agent/core";
import { Schema } from "effect";

import { submissionSettlementId, type RecoverySnapshot } from "./ledger.ts";
import { SettlementOutcome } from "./records.ts";

/**
 * Canonical-log facts about one Submission, read strongly consistently alongside the ledger
 * `RecoverySnapshot` (STORE-003). Canonical history wins over every ledger marker (DUR-015), so
 * the classifier consults these flags before trusting ledger state.
 *
 * `unresolvedToolCall` is the DUR-009 seam: a `ToolCallPrepared` without `ToolCallSettled`.
 * Phase 4 commits ordinary Tool results atomically inside the Turn batch, so no P4 flow can set
 * it (D6: the MarkUnknown branch exists but stays untriggered until durable Tools land in P5).
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
  /** An ordinary Tool call may have started without a canonical outcome (DUR-009; P5 seam). */
  unresolvedToolCall: Schema.Boolean,
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
 * Turn boundary (D6; the model may be re-invoked, duplicate cost is observable). */
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

/** A durable abort intent exists and no terminal outcome is reserved: settle aborted (DUR-012). */
export class SettleAborted extends Schema.TaggedClass<SettleAborted>(
  "@effect-agent/session/SettleAborted",
)("SettleAborted", { submissionId: SubmissionId }) {}

/** An external effect may have happened without canonical confirmation: automatic continuation
 * stops and the accepted-work obligation stays visible (DUR-009/DUR-017; untriggered in P4). */
export class MarkUnknown extends Schema.TaggedClass<MarkUnknown>(
  "@effect-agent/session/MarkUnknown",
)("MarkUnknown", { submissionId: SubmissionId, reason: Schema.String }) {}

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
  NoAction,
]);
export type RecoveryDecision = typeof RecoveryDecision.Type;

/**
 * Pure recovery classifier (durability §14, DUR-013): a finite persisted snapshot plus canonical
 * evidence deterministically selects exactly one decision. Precedence, most-settled first:
 *
 * 1. `settled` → NoAction — terminal outcomes are never revisited (DUR-002).
 * 2. unresolved ordinary Tool → MarkUnknown — never auto-replayed (DUR-009; untriggered in P4).
 * 3. canonical settlement record → FinalizeLedgerFromHistory — history beats every ledger marker
 *    (DUR-015), including a missing or divergent reservation.
 * 4. unfinalized reservation → AppendReservedSettlement — the reserved record is the single exact
 *    outcome owed (DUR-011); it also beats a pending abort intent (DUR-012).
 * 5. abort intent → SettleAborted — inactive accepted work settles aborted without an Attempt.
 * 6. `admitted` → CompleteMaterialization / RepairReadiness by Conversation durability.
 * 7. otherwise → ApplyInput / RepairInputMarker / ResumeFromTurnBoundary by canonical input
 *    evidence, with the ledger marker repaired from history, never the reverse.
 *
 * Ownership leases are deliberately ignored here: the executor's fenced claim is the only
 * authority over liveness, and a stale Attempt is fenced by producer epoch regardless (DUR-006).
 */
export const classifyRecovery = (
  snapshot: RecoverySnapshot,
  evidence: RecoveryEvidence,
): RecoveryDecision => {
  const submissionId = snapshot.submission.submissionId;
  if (snapshot.submission.state === "settled") {
    return NoAction.make({ submissionId });
  }
  if (evidence.unresolvedToolCall) {
    return MarkUnknown.make({
      submissionId,
      reason: "An ordinary Tool call may have executed without a canonical outcome",
    });
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
  if (snapshot.abortIntent !== undefined) {
    return SettleAborted.make({ submissionId });
  }
  if (snapshot.submission.state === "admitted") {
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
