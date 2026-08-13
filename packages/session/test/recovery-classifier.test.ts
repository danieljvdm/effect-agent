import { describe, expect, it } from "@effect/vitest";
import { DateTime, Schema } from "effect";
import {
  AgentId,
  AttemptId,
  ConversationId,
  ReceiptId,
  SubmissionId,
  ToolCallId,
} from "@effect-agent/core";

import {
  AbortIntent,
  ApprovalDecisionIntent,
  CanonicalSequence,
  classifyRecovery,
  DeclaredPendingBatchEvidence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  IdempotencyKey,
  InputAppliedMarker,
  OpenToolCallEvidence,
  OwnershipSnapshot,
  PendingApprovalEvidence,
  Principal,
  ProducerEpoch,
  ProducerId,
  QueueSequence,
  RecordEnvelope,
  RecoveryEvidence,
  RecoverySnapshot,
  ResolutionNeverHappened,
  ResolutionSafeToRetry,
  SettlementReservationSnapshot,
  SubmissionSnapshot,
  submissionInputRecordId,
  submissionSettlementId,
  submissionSettlementRecordId,
  SuspensionSnapshot,
  ApprovalPendingSuspension,
  UnknownResolutionIntent,
  type SettlementOutcome,
  type SubmissionState,
} from "../src/index.ts";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const SHA_B = Schema.decodeSync(Digest)("b".repeat(64));

const SUBMISSION_ID = Schema.decodeSync(SubmissionId)("submission-classifier-1");
const HOST_SUBMISSION_ID = Schema.decodeSync(SubmissionId)("submission-classifier-host");
const CONVERSATION_ID = Schema.decodeSync(ConversationId)("conversation-classifier");
const CREATED_AT = DateTime.toUtc(DateTime.makeUnsafe(1_000));

const CALL_ONE = Schema.decodeSync(ToolCallId)("call-1");
const CALL_TWO = Schema.decodeSync(ToolCallId)("call-2");

const digests = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });

const submission = (state: SubmissionState): SubmissionSnapshot =>
  SubmissionSnapshot.make({
    submissionId: SUBMISSION_ID,
    conversationId: CONVERSATION_ID,
    queueSequence: Schema.decodeSync(QueueSequence)(1),
    principal: Schema.decodeSync(Principal)("principal-classifier"),
    idempotencyKey: Schema.decodeSync(IdempotencyKey)("key-classifier"),
    agentId: Schema.decodeSync(AgentId)("agent-classifier"),
    agentDigests: digests,
    deploymentId: Schema.decodeSync(DeploymentId)("deployment-classifier"),
    inputPayload: { question: "resume?" },
    inputDigest: SHA_B,
    receiptId: Schema.decodeSync(ReceiptId)("receipt-classifier-1"),
    state,
    createdAt: CREATED_AT,
    ...(state === "admitted" ? {} : { readyAt: CREATED_AT }),
  });

const ownership = OwnershipSnapshot.make({
  attemptId: Schema.decodeSync(AttemptId)("attempt-classifier-1"),
  ownerProducerId: Schema.decodeSync(ProducerId)("producer-classifier"),
  producerEpoch: Schema.decodeSync(ProducerEpoch)(1),
  leaseExpiresAt: CREATED_AT,
});

const inputMarker = InputAppliedMarker.make({
  recordId: submissionInputRecordId(SUBMISSION_ID),
  sequence: Schema.decodeSync(CanonicalSequence)(2),
});

const settlementRecord = (outcome: SettlementOutcome) =>
  Schema.decodeSync(RecordEnvelope)({
    recordId: submissionSettlementRecordId(SUBMISSION_ID),
    family: "conversation",
    schemaVersion: 1,
    createdAt: "2026-08-12T12:00:00.000Z",
    deploymentId: "deployment-classifier",
    payload: {
      _tag: "SubmissionSettled",
      submissionId: SUBMISSION_ID,
      settlementId: submissionSettlementId(SUBMISSION_ID),
      receiptId: "receipt-classifier-1",
      outcome,
    },
  });

const reservation = (outcome: SettlementOutcome, finalized: boolean) =>
  SettlementReservationSnapshot.make({
    settlementId: submissionSettlementId(SUBMISSION_ID),
    outcome,
    record: settlementRecord(outcome),
    recordDigest: SHA_A,
    finalized,
  });

const abortIntent = AbortIntent.make({
  submissionId: SUBMISSION_ID,
  author: "operator",
  reason: "user cancelled",
  requestedAt: CREATED_AT,
});

const approvalDecision = (toolCallId: ToolCallId) =>
  ApprovalDecisionIntent.make({
    submissionId: SUBMISSION_ID,
    toolCallId,
    decision: "approved",
    resolver: "policy-operator",
    reason: "reviewed",
    decidedAt: CREATED_AT,
  });

const unknownResolution = (
  toolCallId: ToolCallId,
  resolution: UnknownResolutionIntent["resolution"] = ResolutionNeverHappened.make(),
) =>
  UnknownResolutionIntent.make({
    submissionId: SUBMISSION_ID,
    toolCallId,
    author: "operator",
    reason: "supplier store checked",
    resolution,
    resolvedAt: CREATED_AT,
  });

const suspension = SuspensionSnapshot.make({
  reason: ApprovalPendingSuspension.make({ toolCallIds: [CALL_ONE] }),
  suspendedAt: CREATED_AT,
});

interface SnapshotOverrides {
  readonly ownership?: OwnershipSnapshot;
  readonly inputApplied?: InputAppliedMarker;
  readonly reservation?: SettlementReservationSnapshot;
  readonly abortIntent?: AbortIntent;
  readonly hostSubmissionId?: SubmissionId;
  readonly suspension?: SuspensionSnapshot;
  readonly approvalDecisions?: ReadonlyArray<ApprovalDecisionIntent>;
  readonly unknownResolutions?: ReadonlyArray<UnknownResolutionIntent>;
}

const snapshot = (state: SubmissionState, overrides: SnapshotOverrides = {}): RecoverySnapshot =>
  RecoverySnapshot.make({
    submission: submission(state),
    joins: [],
    approvalDecisions: overrides.approvalDecisions ?? [],
    unknownResolutions: overrides.unknownResolutions ?? [],
    ...(overrides.ownership === undefined ? {} : { ownership: overrides.ownership }),
    ...(overrides.inputApplied === undefined ? {} : { inputApplied: overrides.inputApplied }),
    ...(overrides.reservation === undefined ? {} : { reservation: overrides.reservation }),
    ...(overrides.abortIntent === undefined ? {} : { abortIntent: overrides.abortIntent }),
    ...(overrides.hostSubmissionId === undefined
      ? {}
      : { hostSubmissionId: overrides.hostSubmissionId }),
    ...(overrides.suspension === undefined ? {} : { suspension: overrides.suspension }),
  });

const openCall = (toolCallId: ToolCallId, turn = 1) =>
  OpenToolCallEvidence.make({ toolCallId, toolName: "book_flight", turn });

const pendingApproval = (toolCallId: ToolCallId, turn = 1) =>
  PendingApprovalEvidence.make({ toolCallId, turn });

interface EvidenceOverrides {
  readonly conversationMaterialized?: boolean;
  readonly inputRecorded?: boolean;
  readonly recordedSettlementOutcome?: SettlementOutcome;
  readonly abortRecorded?: boolean;
  readonly openToolCalls?: ReadonlyArray<OpenToolCallEvidence>;
  readonly declaredPendingBatch?: DeclaredPendingBatchEvidence;
  readonly approvalsPending?: ReadonlyArray<PendingApprovalEvidence>;
  readonly joinedInputCovered?: boolean;
  readonly hostSettlementOutcome?: SettlementOutcome;
}

const evidence = (overrides: EvidenceOverrides = {}): RecoveryEvidence =>
  RecoveryEvidence.make({
    conversationMaterialized: overrides.conversationMaterialized ?? true,
    inputRecorded: overrides.inputRecorded ?? false,
    abortRecorded: overrides.abortRecorded ?? false,
    openToolCalls: overrides.openToolCalls ?? [],
    approvalsPending: overrides.approvalsPending ?? [],
    joinedInputCovered: overrides.joinedInputCovered ?? true,
    ...(overrides.recordedSettlementOutcome === undefined
      ? {}
      : { recordedSettlementOutcome: overrides.recordedSettlementOutcome }),
    ...(overrides.declaredPendingBatch === undefined
      ? {}
      : { declaredPendingBatch: overrides.declaredPendingBatch }),
    ...(overrides.hostSettlementOutcome === undefined
      ? {}
      : { hostSettlementOutcome: overrides.hostSettlementOutcome }),
  });

/**
 * The executable crash-matrix decision table (plan §4.3, durability §15): one deterministic
 * classification per persisted crash shape.
 */
describe("recovery classifier crash matrix", () => {
  it("kill submit:after-admit — admitted without a Conversation completes materialization", () => {
    const decision = classifyRecovery(
      snapshot("admitted"),
      evidence({ conversationMaterialized: false }),
    );
    expect(decision._tag).toBe("CompleteMaterialization");
    expect(decision.submissionId).toBe(SUBMISSION_ID);
  });

  it("kill submit:after-materialize — admitted with a Conversation repairs readiness", () => {
    const decision = classifyRecovery(snapshot("admitted"), evidence());
    expect(decision._tag).toBe("RepairReadiness");
  });

  it("kill ledger:mark-ready:after — ready and unclaimed applies input", () => {
    const decision = classifyRecovery(snapshot("ready"), evidence());
    expect(decision._tag).toBe("ApplyInput");
  });

  it("kill claim:after-claim — running without canonical input re-applies input", () => {
    const decision = classifyRecovery(snapshot("running", { ownership }), evidence());
    expect(decision._tag).toBe("ApplyInput");
  });

  it("kill input:after-canonical-append — canonical input wins and only the marker is repaired", () => {
    const decision = classifyRecovery(
      snapshot("running", { ownership }),
      evidence({ inputRecorded: true }),
    );
    expect(decision._tag).toBe("RepairInputMarker");
  });

  it("kill mid model turn — input applied resumes from the last committed Turn boundary", () => {
    const decision = classifyRecovery(
      snapshot("input-applied", { ownership, inputApplied: inputMarker }),
      evidence({ inputRecorded: true }),
    );
    expect(decision._tag).toBe("ResumeFromTurnBoundary");
  });

  it("kill terminalize:before-reserve — no reservation recomputes from the canonical boundary", () => {
    const decision = classifyRecovery(
      snapshot("input-applied", { inputApplied: inputMarker }),
      evidence({ inputRecorded: true }),
    );
    expect(decision._tag).toBe("ResumeFromTurnBoundary");
  });

  it("kill terminalize:after-reserve — the exact reserved record is appended", () => {
    const decision = classifyRecovery(
      snapshot("terminalizing", {
        inputApplied: inputMarker,
        reservation: reservation("completed", false),
      }),
      evidence({ inputRecorded: true }),
    );
    expect(decision._tag).toBe("AppendReservedSettlement");
    if (decision._tag === "AppendReservedSettlement") {
      expect(decision.settlementId).toBe(submissionSettlementId(SUBMISSION_ID));
      expect(decision.outcome).toBe("completed");
    }
  });

  it("kill terminalize:after-canonical-append — the ledger is finalized from history", () => {
    const decision = classifyRecovery(
      snapshot("terminalizing", {
        inputApplied: inputMarker,
        reservation: reservation("completed", false),
      }),
      evidence({ inputRecorded: true, recordedSettlementOutcome: "completed" }),
    );
    expect(decision._tag).toBe("FinalizeLedgerFromHistory");
    if (decision._tag === "FinalizeLedgerFromHistory") {
      expect(decision.outcome).toBe("completed");
    }
  });

  it("canonical settlement without any reservation still finalizes from history (DUR-015)", () => {
    const decision = classifyRecovery(
      snapshot("running", { ownership }),
      evidence({ inputRecorded: true, recordedSettlementOutcome: "failed" }),
    );
    expect(decision._tag).toBe("FinalizeLedgerFromHistory");
    if (decision._tag === "FinalizeLedgerFromHistory") {
      expect(decision.settlementId).toBe(submissionSettlementId(SUBMISSION_ID));
      expect(decision.outcome).toBe("failed");
    }
  });

  it("finalized reservation on a nonterminal row finalizes from history", () => {
    const decision = classifyRecovery(
      snapshot("terminalizing", { reservation: reservation("aborted", true) }),
      evidence({ inputRecorded: true }),
    );
    expect(decision._tag).toBe("FinalizeLedgerFromHistory");
  });

  it("abort of ready, inactive work settles aborted without an Attempt", () => {
    const decision = classifyRecovery(snapshot("ready", { abortIntent }), evidence());
    expect(decision._tag).toBe("SettleAborted");
  });

  it("abort intent after input applied still settles aborted", () => {
    const decision = classifyRecovery(
      snapshot("input-applied", { inputApplied: inputMarker, abortIntent }),
      evidence({ inputRecorded: true, abortRecorded: true }),
    );
    expect(decision._tag).toBe("SettleAborted");
  });

  it("a reserved terminal outcome beats a pending abort intent (DUR-012)", () => {
    const decision = classifyRecovery(
      snapshot("terminalizing", { reservation: reservation("completed", false), abortIntent }),
      evidence({ inputRecorded: true }),
    );
    expect(decision._tag).toBe("AppendReservedSettlement");
  });

  it("settled work needs no action, even with a stale abort intent", () => {
    const decision = classifyRecovery(
      snapshot("settled", { abortIntent }),
      evidence({ inputRecorded: true }),
    );
    expect(decision._tag).toBe("NoAction");
  });
});

describe("recovery classifier P5 durable-tool rows (plan §4.3)", () => {
  it("kill turn:after-response-append — a declared, unprepared batch resumes without model re-invocation", () => {
    const decision = classifyRecovery(
      snapshot("input-applied", { ownership, inputApplied: inputMarker }),
      evidence({
        inputRecorded: true,
        declaredPendingBatch: DeclaredPendingBatchEvidence.make({ turn: 2, callCount: 2 }),
      }),
    );
    expect(decision._tag).toBe("ResumePendingToolBatch");
    if (decision._tag === "ResumePendingToolBatch") {
      expect(decision.turn).toBe(2);
    }
  });

  it("kill tools:after-prepared-append — a prepared call without an outcome marks unknown (DUR-009)", () => {
    const decision = classifyRecovery(
      snapshot("input-applied", { inputApplied: inputMarker }),
      evidence({ inputRecorded: true, openToolCalls: [openCall(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("MarkUnknown");
    if (decision._tag === "MarkUnknown") {
      expect(decision.openToolCallIds).toEqual([CALL_ONE]);
    }
  });

  it("kill mid-handler / after handler return before results append — same reconcile-or-unknown row", () => {
    const decision = classifyRecovery(
      snapshot("running", { ownership, inputApplied: inputMarker }),
      evidence({
        inputRecorded: true,
        openToolCalls: [openCall(CALL_ONE), openCall(CALL_TWO)],
      }),
    );
    expect(decision._tag).toBe("MarkUnknown");
    if (decision._tag === "MarkUnknown") {
      expect(decision.openToolCallIds).toEqual([CALL_ONE, CALL_TWO]);
    }
  });

  it("kill between step commits — the durable Tool call is still one open call, never auto-replayed", () => {
    // Step results are exactly-once-recorded, but the owning Tool Call keeps DUR-009 semantics:
    // the classifier sees one open prepared call; the executor's reconciler decides re-entry.
    const decision = classifyRecovery(
      snapshot("running", { ownership, inputApplied: inputMarker }),
      evidence({ inputRecorded: true, openToolCalls: [openCall(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("MarkUnknown");
  });

  it("a canonical settlement beats open tool calls (P5 precedence inversion, DUR-002/DUR-015)", () => {
    const decision = classifyRecovery(
      snapshot("running", { ownership }),
      evidence({
        inputRecorded: true,
        recordedSettlementOutcome: "completed",
        openToolCalls: [openCall(CALL_ONE)],
      }),
    );
    expect(decision._tag).toBe("FinalizeLedgerFromHistory");
  });

  it("an unfinalized reservation beats open tool calls (P5 precedence inversion)", () => {
    const decision = classifyRecovery(
      snapshot("terminalizing", { reservation: reservation("failed", false) }),
      evidence({ inputRecorded: true, openToolCalls: [openCall(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("AppendReservedSettlement");
  });

  it("settled state beats open tool calls", () => {
    const decision = classifyRecovery(
      snapshot("settled"),
      evidence({ inputRecorded: true, openToolCalls: [openCall(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("NoAction");
  });

  it("an abort intent beats open tool calls — abort settles the obligation, never claims rollback", () => {
    const decision = classifyRecovery(
      snapshot("running", { ownership, abortIntent }),
      evidence({ inputRecorded: true, openToolCalls: [openCall(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("SettleAborted");
  });

  it("state unknown without covering resolutions awaits the authorized DUR-017 path", () => {
    const decision = classifyRecovery(
      snapshot("unknown", { inputApplied: inputMarker }),
      evidence({ inputRecorded: true, openToolCalls: [openCall(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("AwaitUnknownResolution");
  });

  it("state unknown is never re-marked — the resolution regime owns the lane", () => {
    const decision = classifyRecovery(
      snapshot("unknown", {
        inputApplied: inputMarker,
        unknownResolutions: [unknownResolution(CALL_ONE)],
      }),
      evidence({
        inputRecorded: true,
        openToolCalls: [openCall(CALL_ONE), openCall(CALL_TWO)],
      }),
    );
    expect(decision._tag).not.toBe("MarkUnknown");
    expect(decision._tag).toBe("AwaitUnknownResolution");
  });

  it("resolutions covering every open call apply canonically and reopen the lane", () => {
    const decision = classifyRecovery(
      snapshot("unknown", {
        inputApplied: inputMarker,
        unknownResolutions: [
          unknownResolution(CALL_ONE),
          unknownResolution(CALL_TWO, ResolutionSafeToRetry.make()),
        ],
      }),
      evidence({
        inputRecorded: true,
        openToolCalls: [openCall(CALL_ONE), openCall(CALL_TWO)],
      }),
    );
    expect(decision._tag).toBe("ApplyUnknownResolutions");
  });

  it("state unknown whose calls were all resolved canonically still applies (ledger repair from history)", () => {
    const decision = classifyRecovery(
      snapshot("unknown", { inputApplied: inputMarker }),
      evidence({ inputRecorded: true, openToolCalls: [] }),
    );
    expect(decision._tag).toBe("ApplyUnknownResolutions");
  });
});

describe("recovery classifier P5 approval rows (plan §4.3)", () => {
  it("kill approval:after-request-append — request canonical, ledger not suspended, awaits the decision", () => {
    const decision = classifyRecovery(
      snapshot("running", { ownership, inputApplied: inputMarker }),
      evidence({ inputRecorded: true, approvalsPending: [pendingApproval(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("AwaitApprovalDecision");
  });

  it("kill approval:after-suspend — suspended without decisions awaits durably", () => {
    const decision = classifyRecovery(
      snapshot("suspended", { inputApplied: inputMarker, suspension }),
      evidence({ inputRecorded: true, approvalsPending: [pendingApproval(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("AwaitApprovalDecision");
  });

  it("resolveApproval from a second process — suspended with covering decisions resumes", () => {
    const decision = classifyRecovery(
      snapshot("suspended", {
        inputApplied: inputMarker,
        suspension,
        approvalDecisions: [approvalDecision(CALL_ONE)],
      }),
      evidence({ inputRecorded: true, approvalsPending: [pendingApproval(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("ResumeSuspended");
  });

  it("a decision recorded before suspension resumes the declared batch immediately", () => {
    const decision = classifyRecovery(
      snapshot("input-applied", {
        inputApplied: inputMarker,
        approvalDecisions: [approvalDecision(CALL_ONE)],
      }),
      evidence({
        inputRecorded: true,
        approvalsPending: [pendingApproval(CALL_ONE)],
        declaredPendingBatch: DeclaredPendingBatchEvidence.make({ turn: 1, callCount: 1 }),
      }),
    );
    expect(decision._tag).toBe("ResumePendingToolBatch");
  });

  it("undecided approvals beat a declared pending batch — nothing prepares before the decision", () => {
    const decision = classifyRecovery(
      snapshot("input-applied", { inputApplied: inputMarker }),
      evidence({
        inputRecorded: true,
        approvalsPending: [pendingApproval(CALL_ONE)],
        declaredPendingBatch: DeclaredPendingBatchEvidence.make({ turn: 1, callCount: 1 }),
      }),
    );
    expect(decision._tag).toBe("AwaitApprovalDecision");
  });

  it("an abort intent beats a pending approval — suspension never outlives the abort obligation", () => {
    const decision = classifyRecovery(
      snapshot("suspended", { inputApplied: inputMarker, suspension, abortIntent }),
      evidence({ inputRecorded: true, approvalsPending: [pendingApproval(CALL_ONE)] }),
    );
    expect(decision._tag).toBe("SettleAborted");
  });
});

describe("recovery classifier P5 joined-input rows (plan §4.3, DUR-016)", () => {
  it("kill join:after-claim — joining without canonical input reverts to ready", () => {
    const decision = classifyRecovery(
      snapshot("joining", { hostSubmissionId: HOST_SUBMISSION_ID }),
      evidence({ inputRecorded: false }),
    );
    expect(decision._tag).toBe("RevertJoining");
  });

  it("kill join:after-canonical-append — canonical input wins and only the join marker is repaired", () => {
    const decision = classifyRecovery(
      snapshot("joining", { hostSubmissionId: HOST_SUBMISSION_ID }),
      evidence({ inputRecorded: true }),
    );
    expect(decision._tag).toBe("RepairJoinMarker");
  });

  it("kill of host after join, before host settlement — joined with a live host defers", () => {
    const decision = classifyRecovery(
      snapshot("joined", { hostSubmissionId: HOST_SUBMISSION_ID }),
      evidence({ inputRecorded: true, joinedInputCovered: false }),
    );
    expect(decision._tag).toBe("AwaitHostSettlement");
    if (decision._tag === "AwaitHostSettlement") {
      expect(decision.hostSubmissionId).toBe(HOST_SUBMISSION_ID);
    }
  });

  it("kill between host finalization and joined settlement — joined settles with the host outcome", () => {
    const decision = classifyRecovery(
      snapshot("joined", { hostSubmissionId: HOST_SUBMISSION_ID }),
      evidence({ inputRecorded: true, hostSettlementOutcome: "completed" }),
    );
    expect(decision._tag).toBe("SettleJoinedWithHost");
    if (decision._tag === "SettleJoinedWithHost") {
      expect(decision.outcome).toBe("completed");
    }
  });

  it("a joined Submission's own canonical settlement beats the join rows", () => {
    const decision = classifyRecovery(
      snapshot("joined", { hostSubmissionId: HOST_SUBMISSION_ID }),
      evidence({ inputRecorded: true, recordedSettlementOutcome: "completed" }),
    );
    expect(decision._tag).toBe("FinalizeLedgerFromHistory");
  });
});
