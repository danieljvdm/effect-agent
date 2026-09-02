import { ThreadId, SubmissionId, ToolCallId } from "@effect-agent/core";
import { DateTime, Schema } from "effect";

import {
  AbortIntent,
  AbortCommand,
  ApprovalDecisionIntent,
  ChildAttachmentSnapshot,
  JoinSnapshot,
  ParentLinkage,
  QueueSequence,
  SubmissionState,
  SuspensionSnapshot,
  UnknownResolutionIntent,
  type RecoverySnapshot,
} from "./ledger.ts";
import { CanonicalSequence, SettlementOutcome, ToolCallUnknown } from "./records.ts";
import {
  OpenDelegationCallEvidence,
  OpenToolCallEvidence,
  PendingApprovalEvidence,
  RecoveryDecision,
} from "./recovery.ts";

/**
 * Pure report logic for the P7 administrative operations (plan §3): the Schema classes carried
 * by `DurableAgentRuntime.explain`/`verify`/`scanObligations`, the operator meaning table for
 * every recovery decision tag, and the text renderer. Everything here is pure — no port access,
 * no clocks, no writes — so hosts, CLIs, and wire envelopes share one report vocabulary.
 */

const BoundedDetail = Schema.String.check(Schema.isMaxLength(4_096));

/** Non-negative whole seconds derived from the Effect Clock. */
const AgeSeconds = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

// ---------------------------------------------------------------------------
// explain — RecoveryExplanation
// ---------------------------------------------------------------------------

/**
 * The disposition family of `RecoveryReport`, reused verbatim so an explanation predicts exactly
 * what `runRecovery`/`retry` would report for the same decision.
 */
export const RecoveryDisposition = Schema.Literals(["repaired", "deferred", "none", "unknown"]);
export type RecoveryDisposition = typeof RecoveryDisposition.Type;

/** Submission identity, lane position, and Clock-derived ages (DUR-017 obligation visibility). */
export class ExplainedSubmission extends Schema.Class<ExplainedSubmission>(
  "@effect-agent/thread/ExplainedSubmission",
)({
  submissionId: SubmissionId,
  threadId: ThreadId,
  state: SubmissionState,
  queueSequence: QueueSequence,
  createdAt: Schema.DateTimeUtcFromString,
  readyAt: Schema.optionalKey(Schema.DateTimeUtcFromString),
  /** Whole seconds since admission (`createdAt`). */
  ageSeconds: AgeSeconds,
  /** Whole seconds since readiness, present exactly when `readyAt` is recorded. */
  readyAgeSeconds: Schema.optionalKey(AgeSeconds),
  parentLinkage: Schema.optionalKey(ParentLinkage),
}) {}

/** One durable Unknown Outcome and whether a covering resolution intent exists (DUR-017). */
export class ExplainedUnknownCall extends Schema.Class<ExplainedUnknownCall>(
  "@effect-agent/thread/ExplainedUnknownCall",
)({
  toolCallId: ToolCallId,
  toolName: ToolCallUnknown.fields.toolName,
  reason: ToolCallUnknown.fields.reason,
  recordedAt: Schema.DateTimeUtcFromString,
  /** A durable `resolveUnknown` intent covers this call. */
  resolved: Schema.Boolean,
}) {}

/**
 * The evidence summary of one explanation: the classifier's canonical-history inputs plus the
 * durable intents and subagent attachments the ledger snapshot carries. Everything is read-only
 * observation — assembling this value performs zero writes.
 */
export class ExplainedEvidence extends Schema.Class<ExplainedEvidence>(
  "@effect-agent/thread/ExplainedEvidence",
)({
  threadMaterialized: Schema.Boolean,
  inputRecorded: Schema.Boolean,
  abortRecorded: Schema.Boolean,
  recordedSettlementOutcome: Schema.optionalKey(SettlementOutcome),
  /** Prepared ordinary Tool Calls without a canonical outcome (DUR-009). */
  openToolCalls: Schema.Array(OpenToolCallEvidence),
  /** Delegation Tool Calls with an open parent-side obligation. */
  openDelegationCalls: Schema.Array(OpenDelegationCallEvidence),
  /** Canonically requested approvals without a canonical decision. */
  approvalsPending: Schema.Array(PendingApprovalEvidence),
  /** Durable Unknown Outcomes with their resolution coverage (DUR-017). */
  unknownCalls: Schema.Array(ExplainedUnknownCall),
  /** Durable approval decisions recorded against this Submission. */
  approvalDecisions: Schema.Array(ApprovalDecisionIntent),
  /** Durable unknown-outcome resolutions recorded against this Submission. */
  unknownResolutions: Schema.Array(UnknownResolutionIntent),
  /** Parent-side child attachments. */
  childAttachments: Schema.Array(ChildAttachmentSnapshot),
  /** Host-side joined/joining members of this Submission's Run (DUR-016). */
  joins: Schema.Array(JoinSnapshot),
  /** Joined-side host identity, when this Submission is joined to a host Run. */
  hostSubmissionId: Schema.optionalKey(SubmissionId),
  suspension: Schema.optionalKey(SuspensionSnapshot),
  abortIntent: Schema.optionalKey(AbortIntent),
}) {}

/**
 * The read-only recovery explanation of one Submission (P7 exit gate "operators can explain
 * recovery state without editing storage"): the submission summary, the evidence the pure
 * classifier consumed, the exact `RecoveryDecision` it selected, that decision's operator
 * meaning, and the disposition the recovery pass would report for it.
 */
export class RecoveryExplanation extends Schema.Class<RecoveryExplanation>(
  "@effect-agent/thread/RecoveryExplanation",
)({
  submission: ExplainedSubmission,
  evidence: ExplainedEvidence,
  decision: RecoveryDecision,
  /** Static operator meaning of `decision._tag` (see `RECOVERY_DECISION_MEANINGS`). */
  decisionMeaning: BoundedDetail,
  /**
   * The disposition `runRecovery` would report when this decision executes without claim
   * contention (see `predictRecoveryDisposition`): `repaired` = the recovery pass itself fixes
   * it; `deferred` = a claiming worker must finish it; `unknown` = durably blocked on the
   * authorized DUR-017 resolution path; `none` = settled, nothing owed.
   */
  disposition: RecoveryDisposition,
  explainedAt: Schema.DateTimeUtcFromString,
}) {}

/**
 * Operator meaning of every recovery decision tag, matching the classifier docstrings in
 * `recovery.ts` (the classifier remains the semantic authority; this table is its operator
 * rendering). Exhaustive over `RecoveryDecision` — adding a decision without a meaning is a
 * compile error here.
 */
export const RECOVERY_DECISION_MEANINGS: Readonly<Record<RecoveryDecision["_tag"], string>> = {
  CompleteMaterialization:
    "Ledger admission committed but the Thread is not durable: recovery finishes materialization, appends ThreadCreated, and marks readiness.",
  RepairReadiness:
    "The Thread is durable but the readiness marker is missing: recovery replays the idempotent markReady.",
  ApplyInput:
    "No canonical input record exists yet: recovery appends the deterministic UserInputRecorded record and marks it applied.",
  RepairInputMarker:
    "The canonical input record exists but the ledger marker was lost: recovery repairs the marker from history, never the reverse (DUR-015).",
  ResumeFromTurnBoundary:
    "Input is applied and no terminal work is reserved: a claiming worker resumes the Run from the last committed Turn boundary; the model may be re-invoked and any duplicate cost is observable (durability §9).",
  AppendReservedSettlement:
    "A settlement is reserved but not canonical: recovery appends the EXACT reserved record, then finalizes (DUR-011).",
  FinalizeLedgerFromHistory:
    "The canonical settlement record exists: recovery rebuilds/finalizes the ledger from history and never rewrites history from the cached ledger status (DUR-011, DUR-015).",
  SettleAborted:
    "A durable abort intent exists with no reserved outcome and no open attached-child obligation: recovery settles aborted, first recording ToolCallUnknown audits for open ordinary calls — abort never asserts external rollback (durability §13).",
  MarkUnknown:
    "Prepared ordinary Tool Calls have no canonical outcome: recovery reconciles each open call and marks the remainder Unknown — never an automatic replay (DUR-009/DUR-017).",
  ResumePendingToolBatch:
    "A committed tool-declaring response has zero prepared and zero settled records: a claiming worker resumes the declared batch without model re-invocation and without Unknown (durability §15).",
  AwaitApprovalDecision:
    "Canonically requested approvals lack decisions: the lane waits durably for the authorized resolveApproval path (recovery repairs a lost suspend transition from history).",
  ResumeSuspended:
    "Every pending approval has a durable decision: a claiming worker resumes the declared batch, appending the canonical ToolApprovalDecided records first.",
  AwaitUnknownResolution:
    "Unknown Outcomes lack covering resolutions: the lane stays blocked awaiting the authorized DUR-017 resolveUnknown path; the obligation stays visible, nothing replays.",
  ApplyUnknownResolutions:
    "Durable resolution intents cover every open call: recovery applies them canonically and reopens the lane.",
  RevertJoining:
    "The Submission is joining without a canonical input record: recovery reverts it to ready — the input was never consumed and will be delivered exactly once later (DUR-016).",
  RepairJoinMarker:
    "The canonical joined input exists but the joined marker was lost: recovery repairs the marker only; the input reattaches, never duplicates (DUR-015/DUR-016).",
  SettleJoinedWithHost:
    "The host Run settled canonically: recovery settles this joined Submission with the host outcome — each joined Submission is owed its own settlement (DUR-002).",
  AwaitHostSettlement:
    "The Submission is joined and its host Run is still live: nothing to repair — the input reattaches through the host's resume and the Submission settles with the host.",
  CompleteChildAdmission:
    "A canonical SubagentRequested exists and the authoritative lookup proved not-admitted: recovery idempotently admits the intended child from the canonical request payload alone.",
  AwaitChildAdmissionResolution:
    "The authoritative child-admission lookup answered indeterminate (or was not performed): recovery waits and retries the authoritative owner; a second admission never occurs (SUB-031).",
  RepairSubagentStartLink:
    "The child is admitted but the parent's SubagentStarted link (or reservation attachment) is missing: recovery resolves the SAME child by its deterministic key and appends the exact link (SUB-016/SUB-017).",
  EnsureWaitingForChild:
    "Children are established and at least one is nonterminal while the parent lane is not durably suspended: recovery restores the waitingForChild suspension; it reattaches the one existing child and never spawns a replacement (SUB-018/SUB-030).",
  AwaitChildSettlement:
    "The parent is durably suspended waitingForChild and a listed child is not yet provably settled: the lane stays dormant, consumes no worker permit, and the child's Settlement wakes it durably (SUB-021/SUB-030).",
  ResumeWaitingParent:
    "Every relevant child is provably settled but the parent has not joined: recovery replays the idempotent wake so a claiming worker resumes the declared batch and joins each child's canonical Settlement.",
  ApplyJoinAccounting:
    "The canonical SubagentJoined record exists but the reservation release is incomplete: recovery replays the accounting decision FROM the canonical record — budget stays unavailable until repair, never available twice (DUR-015).",
  PropagateChildAbort:
    "Parent abort is canonical while attached children are nonterminal: recovery idempotently issues each child's durable abort command and keeps the parent waiting for the joins — request-abort-and-join is the only close behavior.",
  ReleaseOrphanChildReservation:
    "A child budget reservation can no longer bind a child: recovery freezes the deterministic zero-consumed accounting decision and releases the unused allocation exactly once.",
  AwaitParentEstablishment:
    "The Submission is a parent-linked child whose Thread lacks the canonical lineage record: the child lane defers its own readiness repair — the parent's idempotent establishment completes it, so a child never runs a Turn before its lineage is canonical (P7 §7(a), SUB-016).",
  NoAction: "The Submission is settled; nothing is owed.",
};

/** The operator meaning of one recovery decision tag. */
export const recoveryDecisionMeaning = (tag: RecoveryDecision["_tag"]): string =>
  RECOVERY_DECISION_MEANINGS[tag];

/**
 * The disposition the recovery executor reports for a decision when its repair executes without
 * claim contention (pure prediction; `executeRecoveryDecision` in the coordinator remains the
 * execution authority — a lane concurrently claimed by a worker can always defer instead).
 * `AwaitApprovalDecision` depends on the snapshot: an already-suspended lane waits (`deferred`),
 * while a claimable lane with a lost suspend transition is repaired.
 */
export const predictRecoveryDisposition = (
  decision: RecoveryDecision,
  snapshot: RecoverySnapshot,
): RecoveryDisposition => {
  switch (decision._tag) {
    case "NoAction":
      return "none";
    case "AwaitUnknownResolution":
    case "MarkUnknown":
      return "unknown";
    case "ResumeFromTurnBoundary":
    case "ResumePendingToolBatch":
    case "ResumeSuspended":
    case "AwaitHostSettlement":
    case "AwaitChildAdmissionResolution":
    case "AwaitChildSettlement":
    case "AwaitParentEstablishment":
      return "deferred";
    case "AwaitApprovalDecision":
      return snapshot.submission.state === "suspended" ? "deferred" : "repaired";
    default:
      return "repaired";
  }
};

const formatUtc = (value: DateTime.Utc): string => DateTime.formatIso(value);

/** Pure operator text for one explanation (the CLI's `explain` output body). */
export const renderRecoveryExplanation = (explanation: RecoveryExplanation): string => {
  const submission = explanation.submission;
  const evidence = explanation.evidence;

  const lines: Array<string> = [
    `Submission ${submission.submissionId} (thread ${submission.threadId})`,
    `  state: ${submission.state}  queue: ${submission.queueSequence}  age: ${submission.ageSeconds}s`,
    `  admitted: ${formatUtc(submission.createdAt)}${
      submission.readyAt === undefined ? "" : `  ready: ${formatUtc(submission.readyAt)}`
    }`,
  ];

  if (submission.parentLinkage !== undefined) {
    lines.push(
      `  attached child of ${submission.parentLinkage.parentSubmissionId} (tool call ${submission.parentLinkage.parentToolCallId})`,
    );
  }
  lines.push(
    `  evidence: input=${evidence.inputRecorded ? "recorded" : "missing"}` +
      ` settlement=${evidence.recordedSettlementOutcome ?? "none"}` +
      ` abort=${evidence.abortRecorded ? "recorded" : "none"}` +
      ` materialized=${evidence.threadMaterialized ? "yes" : "no"}`,
  );
  if (evidence.openToolCalls.length > 0) {
    lines.push(
      `  open ordinary tool calls: ${evidence.openToolCalls
        .map((call) => `${call.toolName}#${call.toolCallId}`)
        .join(", ")}`,
    );
  }
  if (evidence.openDelegationCalls.length > 0) {
    lines.push(
      `  open delegation calls: ${evidence.openDelegationCalls
        .map(
          (call) =>
            `${call.toolName}#${call.toolCallId}(requested=${call.requested} started=${call.started} joined=${call.joined})`,
        )
        .join(", ")}`,
    );
  }
  if (evidence.approvalsPending.length > 0) {
    lines.push(
      `  approvals pending: ${evidence.approvalsPending
        .map((pending) => pending.toolCallId)
        .join(", ")}`,
    );
  }
  for (const unknown of evidence.unknownCalls) {
    lines.push(
      `  unknown outcome: ${unknown.toolName}#${unknown.toolCallId} recorded ${formatUtc(unknown.recordedAt)} ${
        unknown.resolved ? "(resolution intent recorded)" : "(awaiting resolveUnknown)"
      }`,
    );
  }
  if (evidence.childAttachments.length > 0) {
    lines.push(
      `  attached children: ${evidence.childAttachments
        .map((child) => `${child.childSubmissionId}(${child.childState})`)
        .join(", ")}`,
    );
  }
  if (evidence.joins.length > 0) {
    lines.push(
      `  joined members: ${evidence.joins
        .map((join) => `${join.submissionId}(${join.state})`)
        .join(", ")}`,
    );
  }
  if (evidence.hostSubmissionId !== undefined) {
    lines.push(`  joined to host: ${evidence.hostSubmissionId}`);
  }
  if (evidence.suspension !== undefined) {
    lines.push(
      `  suspended (${evidence.suspension.reason._tag}) since ${formatUtc(evidence.suspension.suspendedAt)}`,
    );
  }
  if (evidence.abortIntent !== undefined) {
    lines.push(
      `  abort intent by ${evidence.abortIntent.author} at ${formatUtc(evidence.abortIntent.requestedAt)}`,
    );
  }
  lines.push(
    `  decision: ${explanation.decision._tag} → disposition ${explanation.disposition}`,
    `  meaning: ${explanation.decisionMeaning}`,
  );

  return lines.join("\n");
};

// ---------------------------------------------------------------------------
// verify — IntegrityReport
// ---------------------------------------------------------------------------

/** The named integrity checks `verifyThreadInvariants` performs. */
export const IntegrityCheckName = Schema.Literals([
  /** Every exported envelope round-trips its canonical Schema. */
  "schema-round-trip",
  /** No canonical record identity appears twice (DUR-007). */
  "record-identity",
  /** Canonical sequences are gap-free from 1 through the exported tail. */
  "sequence-contiguity",
  /** The tail digest chain recomputes from EMPTY_TAIL_DIGEST over every batch (DUR-006/DUR-007). */
  "digest-chain",
  /** Canonical `input:{sid}` records follow the admitted FIFO order (DUR-004). */
  "fifo-input-order",
  /** Canonical settlement records follow the admitted FIFO order (DUR-004); aborted
   * settlements of never-run work are exempt by design (P7 §7(c)). */
  "fifo-settlement-order",
  /** At most one canonical terminal record per Submission; exactly one per settled row (DUR-002). */
  "terminal-uniqueness",
  /** Ledger terminal state agrees with the canonical settlement record (DUR-015). */
  "ledger-canonical-agreement",
  /** The stored checkpoint binds to the recomputed digest chain at its sequence. */
  "checkpoint-binding",
  /** Convergence mode only: every known Submission is settled. */
  "all-settled",
]);

export type IntegrityCheckName = typeof IntegrityCheckName.Type;

export const IntegrityCheckStatus = Schema.Literals(["passed", "failed", "skipped"]);
export type IntegrityCheckStatus = typeof IntegrityCheckStatus.Type;

/** One typed check result; `detail` explains a failure or the honest reason for a skip. */
export class IntegrityCheck extends Schema.Class<IntegrityCheck>(
  "@effect-agent/thread/IntegrityCheck",
)({
  name: IntegrityCheckName,
  status: IntegrityCheckStatus,
  detail: Schema.optionalKey(BoundedDetail),
}) {}

/**
 * The read-only integrity verdict for one Thread (plan §3 `verify`): typed per-check
 * results, never a repair. `ok` is true exactly when no check failed — skipped checks are
 * honest scope statements, not silent passes.
 */
export class IntegrityReport extends Schema.Class<IntegrityReport>(
  "@effect-agent/thread/IntegrityReport",
)({
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  recordCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  submissionCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  checks: Schema.Array(IntegrityCheck),
  ok: Schema.Boolean,
}) {}

// ---------------------------------------------------------------------------
// scanObligations — ObligationReport
// ---------------------------------------------------------------------------

/**
 * What one nonterminal Submission is visibly blocked on (DUR-017 operator surface; OPS-001).
 * `ready-aged`/`running-aged` are not blocks — they are accepted work whose settlement age is
 * measurable and alertable.
 */
export const ObligationBlockedOn = Schema.Literals([
  "unknown",
  "approval",
  "waitingForChild",
  "ready-aged",
  "running-aged",
]);

export type ObligationBlockedOn = typeof ObligationBlockedOn.Type;

export const ObligationSeverity = Schema.Literals(["ok", "aging", "overdue"]);
export type ObligationSeverity = typeof ObligationSeverity.Type;

/** Host-supplied age thresholds; the framework deliberately does not own the alert loop. */
export class ObligationThresholds extends Schema.Class<ObligationThresholds>(
  "@effect-agent/thread/ObligationThresholds",
)({
  /** Age (whole seconds) at which an obligation is reported `aging`. */
  agingSeconds: AgeSeconds,
  /** Age (whole seconds) at which an obligation is reported `overdue`. */
  overdueSeconds: AgeSeconds,
}) {}

/** One nonterminal accepted-work obligation with its age and severity. */
export class ObligationEntry extends Schema.Class<ObligationEntry>(
  "@effect-agent/thread/ObligationEntry",
)({
  submissionId: SubmissionId,
  threadId: ThreadId,
  state: SubmissionState,
  blockedOn: ObligationBlockedOn,
  ageSeconds: AgeSeconds,
  severity: ObligationSeverity,
}) {}

/**
 * The scan-based DUR-017/OPS-001 operator surface (plan §3): a fold of the ledger's nonterminal
 * scan into aged, severity-classified rows. Scan-based, never a daemon — hosts run it
 * periodically and export logs/metrics; ages need no storage-version bump because every source
 * timestamp already exists (`createdAt`/`readyAt`, `suspendedAt`, the canonical
 * `ToolCallUnknown` record's `createdAt`).
 */
export class ObligationReport extends Schema.Class<ObligationReport>(
  "@effect-agent/thread/ObligationReport",
)({
  thresholds: ObligationThresholds,
  entries: Schema.Array(ObligationEntry),
  generatedAt: Schema.DateTimeUtcFromString,
}) {}

/** Severity of one obligation age under the supplied thresholds. */
export const obligationSeverityOf = (
  ageSeconds: number,
  thresholds: ObligationThresholds,
): ObligationSeverity =>
  ageSeconds >= thresholds.overdueSeconds
    ? "overdue"
    : ageSeconds >= thresholds.agingSeconds
      ? "aging"
      : "ok";

// ---------------------------------------------------------------------------
// retry — typed command and refusals
// ---------------------------------------------------------------------------

/**
 * The mandatory-audit retry command (SEC-011): `author`/`reason` carry the same bounds as the
 * canonical abort command so every mutating administrative operation stays attributable. The
 * executed repair itself appends the deterministic `RepairAnnotated` audit record (DUR-013) —
 * retry adds no new canonical record type.
 */
export class RetryCommand extends Schema.Class<RetryCommand>("@effect-agent/thread/RetryCommand")({
  submissionId: SubmissionId,
  author: AbortCommand.fields.author,
  reason: AbortCommand.fields.reason,
}) {}

/** Why a retry was refused without executing anything. */
export const RetryRefusalReason = Schema.Literals([
  /** The Submission is settled; terminal outcomes are never revisited (DUR-002). */
  "settled",
  /** The lane is durably blocked on Unknown Outcomes; use `resolveUnknown` (DUR-017). */
  "await-unknown-resolution",
  /** The lane is durably waiting for approval decisions; use `resolveApproval`. */
  "await-approval-decision",
]);

export type RetryRefusalReason = typeof RetryRefusalReason.Type;

/**
 * Typed refusal of `retry` (plan §3): lanes blocked on `AwaitUnknownResolution` or
 * `AwaitApprovalDecision` have their own authorized operations, and settled work is never
 * re-driven. The refusal names the classifier decision so the operator sees WHY.
 */
export class RetryRefused extends Schema.TaggedError<RetryRefused>()("RetryRefused", {
  submissionId: SubmissionId,
  refusal: RetryRefusalReason,
  decisionTag: Schema.String.check(Schema.isMaxLength(256)),
  message: Schema.String.check(Schema.isMaxLength(4_096)),
}) {}
