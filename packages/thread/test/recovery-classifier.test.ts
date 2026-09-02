import {
  AgentId,
  AttemptId,
  ThreadId,
  ReceiptId,
  SubmissionId,
  ToolCallId,
} from "@effect-agent/core";
import { describe, expect, it } from "@effect/vitest";
import { DateTime, Schema } from "effect";

import {
  AbortIntent,
  ApprovalDecisionIntent,
  CanonicalSequence,
  ChildAttachmentSnapshot,
  ChildBudgetReservationSnapshot,
  ChildReservationId,
  classifyRecovery,
  DeclaredPendingBatchEvidence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  IdempotencyKey,
  InputAppliedMarker,
  OpenDelegationCallEvidence,
  OpenToolCallEvidence,
  OwnershipSnapshot,
  ParentLinkage,
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
  WaitingChild,
  WaitingForChildSuspension,
  type ChildReservationStatus,
  type DelegationAdmissionEvidence,
  type SettlementOutcome,
  type SubmissionState,
} from "../src/index.ts";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const SHA_B = Schema.decodeSync(Digest)("b".repeat(64));

const SUBMISSION_ID = Schema.decodeSync(SubmissionId)("submission-classifier-1");
const HOST_SUBMISSION_ID = Schema.decodeSync(SubmissionId)("submission-classifier-host");
const THREAD_ID = Schema.decodeSync(ThreadId)("thread-classifier");
const CREATED_AT = DateTime.toUtc(DateTime.makeUnsafe(1_000));

const CALL_ONE = Schema.decodeSync(ToolCallId)("call-1");
const CALL_TWO = Schema.decodeSync(ToolCallId)("call-2");

const digests = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });

const submission = (state: SubmissionState): SubmissionSnapshot =>
  SubmissionSnapshot.make({
    submissionId: SUBMISSION_ID,
    threadId: THREAD_ID,
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
    family: "thread",
    schemaVersion: 1,
    createdAt: "2026-08-12T12:00:00.000Z",
    deploymentId: "deployment-classifier",
    payload: {
      _tag: "SubmissionSettled",
      submissionId: SUBMISSION_ID,
      settlementId: submissionSettlementId(SUBMISSION_ID),
      receiptId: "receipt-classifier-1",
      outcome,
      ...(outcome === "failed"
        ? { result: { errorTag: "ClassifierFailure", message: "The classified Run failed" } }
        : {}),
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

const CALL_DELEGATE = Schema.decodeSync(ToolCallId)("call-delegate-1");
const CALL_DELEGATE_TWO = Schema.decodeSync(ToolCallId)("call-delegate-2");
const CHILD_ONE = Schema.decodeSync(SubmissionId)("submission-child-1");
const CHILD_TWO = Schema.decodeSync(SubmissionId)("submission-child-2");
const RESERVATION_ONE = Schema.decodeSync(ChildReservationId)("child-reservation-1");

const waitingChild = (toolCallId: ToolCallId, childSubmissionId: SubmissionId) =>
  WaitingChild.make({ toolCallId, childSubmissionId });

const waitingSuspension = (children: readonly [WaitingChild, ...Array<WaitingChild>]) =>
  SuspensionSnapshot.make({
    reason: WaitingForChildSuspension.make({ children }),
    suspendedAt: CREATED_AT,
  });

interface ChildReservationOverrides {
  readonly toolCallId?: ToolCallId;
  readonly reservationId?: ChildReservationId;
  readonly status?: ChildReservationStatus;
  readonly childSubmissionId?: SubmissionId;
}

const childReservation = (overrides: ChildReservationOverrides = {}) =>
  ChildBudgetReservationSnapshot.make({
    reservationId: overrides.reservationId ?? RESERVATION_ONE,
    parentSubmissionId: SUBMISSION_ID,
    parentToolCallId: overrides.toolCallId ?? CALL_DELEGATE,
    status: overrides.status ?? "reserved",
    allocation: { turns: 4 },
    allocationDigest: SHA_A,
    reservedAt: CREATED_AT,
    ...(overrides.childSubmissionId === undefined
      ? {}
      : { childSubmissionId: overrides.childSubmissionId }),
  });

const childAttachment = (
  toolCallId: ToolCallId,
  childSubmissionId: SubmissionId,
  childState: SubmissionState,
  childOutcome?: SettlementOutcome,
) =>
  ChildAttachmentSnapshot.make({
    toolCallId,
    childSubmissionId,
    childState,
    ...(childOutcome === undefined ? {} : { childOutcome }),
  });

interface DelegationCallOverrides {
  readonly turn?: number;
  readonly requested?: boolean;
  readonly started?: boolean;
  readonly joined?: boolean;
  readonly childSubmissionId?: SubmissionId;
  readonly admission?: DelegationAdmissionEvidence;
}

const delegationCall = (toolCallId: ToolCallId, overrides: DelegationCallOverrides = {}) =>
  OpenDelegationCallEvidence.make({
    toolCallId,
    toolName: "delegate_destination_research",
    turn: overrides.turn ?? 1,
    requested: overrides.requested ?? false,
    started: overrides.started ?? false,
    joined: overrides.joined ?? false,
    ...(overrides.childSubmissionId === undefined
      ? {}
      : { childSubmissionId: overrides.childSubmissionId }),
    ...(overrides.admission === undefined ? {} : { admission: overrides.admission }),
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
  readonly childReservations?: ReadonlyArray<ChildBudgetReservationSnapshot>;
  readonly childAttachments?: ReadonlyArray<ChildAttachmentSnapshot>;
  readonly parentLinkage?: ParentLinkage;
}

const snapshot = (state: SubmissionState, overrides: SnapshotOverrides = {}): RecoverySnapshot =>
  RecoverySnapshot.make({
    submission: submission(state),
    joins: [],
    approvalDecisions: overrides.approvalDecisions ?? [],
    unknownResolutions: overrides.unknownResolutions ?? [],
    childReservations: overrides.childReservations ?? [],
    childAttachments: overrides.childAttachments ?? [],
    ...(overrides.ownership === undefined ? {} : { ownership: overrides.ownership }),
    ...(overrides.inputApplied === undefined ? {} : { inputApplied: overrides.inputApplied }),
    ...(overrides.reservation === undefined ? {} : { reservation: overrides.reservation }),
    ...(overrides.abortIntent === undefined ? {} : { abortIntent: overrides.abortIntent }),
    ...(overrides.hostSubmissionId === undefined
      ? {}
      : { hostSubmissionId: overrides.hostSubmissionId }),
    ...(overrides.suspension === undefined ? {} : { suspension: overrides.suspension }),
    ...(overrides.parentLinkage === undefined ? {} : { parentLinkage: overrides.parentLinkage }),
  });

const openCall = (toolCallId: ToolCallId, turn = 1, toolName = "book_flight") =>
  OpenToolCallEvidence.make({ toolCallId, toolName, turn });

const pendingApproval = (toolCallId: ToolCallId, turn = 1) =>
  PendingApprovalEvidence.make({ toolCallId, turn });

interface EvidenceOverrides {
  readonly threadMaterialized?: boolean;
  readonly inputRecorded?: boolean;
  readonly recordedSettlementOutcome?: SettlementOutcome;
  readonly abortRecorded?: boolean;
  readonly openToolCalls?: ReadonlyArray<OpenToolCallEvidence>;
  readonly openDelegationCalls?: ReadonlyArray<OpenDelegationCallEvidence>;
  readonly declaredPendingBatch?: DeclaredPendingBatchEvidence;
  readonly approvalsPending?: ReadonlyArray<PendingApprovalEvidence>;
  readonly joinedInputCovered?: boolean;
  readonly hostSettlementOutcome?: SettlementOutcome;
  readonly subagentLineageRecorded?: boolean;
}

const evidence = (overrides: EvidenceOverrides = {}): RecoveryEvidence =>
  RecoveryEvidence.make({
    threadMaterialized: overrides.threadMaterialized ?? true,
    inputRecorded: overrides.inputRecorded ?? false,
    abortRecorded: overrides.abortRecorded ?? false,
    openToolCalls: overrides.openToolCalls ?? [],
    openDelegationCalls: overrides.openDelegationCalls ?? [],
    approvalsPending: overrides.approvalsPending ?? [],
    joinedInputCovered: overrides.joinedInputCovered ?? true,
    subagentLineageRecorded: overrides.subagentLineageRecorded ?? false,
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
  it("kill submit:after-admit — admitted without a Thread completes materialization", () => {
    const decision = classifyRecovery(
      snapshot("admitted"),
      evidence({ threadMaterialized: false }),
    );

    expect(decision._tag).toBe("CompleteMaterialization");
    expect(decision.submissionId).toBe(SUBMISSION_ID);
  });

  it("kill submit:after-materialize — admitted with a Thread repairs readiness", () => {
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

/**
 * Durable child recovery: one deterministic classification per persisted boundary
 * (plan §4.2), against the establishment failpoints of plan §4.4.
 */
describe("recovery classifier S2 subagent establishment rows (spec §13)", () => {
  const running = (overrides: SnapshotOverrides = {}) =>
    snapshot("running", { ownership, inputApplied: inputMarker, ...overrides });

  it("kill before subagent:after-reserve — no reservation, no request: the declared batch resumes and establishment re-executes idempotently (row 1)", () => {
    const decision = classifyRecovery(
      running(),
      evidence({ inputRecorded: true, openDelegationCalls: [delegationCall(CALL_DELEGATE)] }),
    );

    expect(decision._tag).toBe("ResumePendingToolBatch");
    if (decision._tag === "ResumePendingToolBatch") {
      expect(decision.turn).toBe(1);
    }
  });

  it("kill at subagent:after-reserve — reservation without a request on a live parent resumes the batch so the handler appends the fixed request (row 2)", () => {
    const decision = classifyRecovery(
      running({ childReservations: [childReservation()] }),
      evidence({ inputRecorded: true, openDelegationCalls: [delegationCall(CALL_DELEGATE)] }),
    );

    expect(decision._tag).toBe("ResumePendingToolBatch");
  });

  it("a reservation without a request and no resumable batch releases the orphan exactly once (row 2)", () => {
    const decision = classifyRecovery(
      running({ childReservations: [childReservation()] }),
      evidence({ inputRecorded: true }),
    );

    expect(decision._tag).toBe("ReleaseOrphanChildReservation");
    if (decision._tag === "ReleaseOrphanChildReservation") {
      expect(decision.reservationIds).toEqual([RESERVATION_ONE]);
    }
  });

  it("kill at subagent:after-request-append — requested with authoritative notAdmitted admits exactly one child (row 3)", () => {
    const decision = classifyRecovery(
      running({ childReservations: [childReservation()] }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, { requested: true, admission: "not-admitted" }),
        ],
      }),
    );

    expect(decision._tag).toBe("CompleteChildAdmission");
    if (decision._tag === "CompleteChildAdmission") {
      expect(decision.toolCallId).toBe(CALL_DELEGATE);
    }
  });

  it("requested with an indeterminate admission waits and never admits a second child (row 4, SUB-031)", () => {
    const decision = classifyRecovery(
      running({ childReservations: [childReservation()] }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, { requested: true, admission: "indeterminate" }),
        ],
      }),
    );

    expect(decision._tag).toBe("AwaitChildAdmissionResolution");
  });

  it("requested without an admission answer stays fail-closed at the authoritative wait (SUB-031)", () => {
    const decision = classifyRecovery(
      running(),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [delegationCall(CALL_DELEGATE, { requested: true })],
      }),
    );

    expect(decision._tag).toBe("AwaitChildAdmissionResolution");
  });

  it("kill at subagent:after-child-ready — admitted child without the start link repairs the exact SubagentStarted (row 5)", () => {
    const decision = classifyRecovery(
      running({ childReservations: [childReservation()] }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, { requested: true, admission: "admitted" }),
        ],
      }),
    );

    expect(decision._tag).toBe("RepairSubagentStartLink");
    if (decision._tag === "RepairSubagentStartLink") {
      expect(decision.toolCallId).toBe(CALL_DELEGATE);
    }
  });

  it("kill at subagent:after-start-append — started child nonterminal on an unsuspended parent restores waitingForChild, never a replacement (row 6, SUB-018)", () => {
    const decision = classifyRecovery(
      running({
        childReservations: [childReservation({ childSubmissionId: CHILD_ONE })],
        childAttachments: [childAttachment(CALL_DELEGATE, CHILD_ONE, "running")],
      }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            childSubmissionId: CHILD_ONE,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("EnsureWaitingForChild");
    if (decision._tag === "EnsureWaitingForChild") {
      expect(decision.children).toEqual([waitingChild(CALL_DELEGATE, CHILD_ONE)]);
    }
  });

  it("a mixed batch with one running and one settled child waits on both (row 6)", () => {
    const decision = classifyRecovery(
      running({
        childAttachments: [
          childAttachment(CALL_DELEGATE, CHILD_ONE, "running"),
          childAttachment(CALL_DELEGATE_TWO, CHILD_TWO, "settled", "completed"),
        ],
      }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            childSubmissionId: CHILD_ONE,
          }),
          delegationCall(CALL_DELEGATE_TWO, {
            requested: true,
            started: true,
            childSubmissionId: CHILD_TWO,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("EnsureWaitingForChild");
    if (decision._tag === "EnsureWaitingForChild") {
      expect(decision.children).toEqual([
        waitingChild(CALL_DELEGATE, CHILD_ONE),
        waitingChild(CALL_DELEGATE_TWO, CHILD_TWO),
      ]);
    }
  });

  it("kill at subagent:after-suspend replayed — suspended WaitingForChild with a nonterminal child awaits its settlement (row 6)", () => {
    const decision = classifyRecovery(
      snapshot("suspended", {
        inputApplied: inputMarker,
        suspension: waitingSuspension([waitingChild(CALL_DELEGATE, CHILD_ONE)]),
        childAttachments: [childAttachment(CALL_DELEGATE, CHILD_ONE, "running")],
      }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            childSubmissionId: CHILD_ONE,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("AwaitChildSettlement");
  });

  it("a child blocked at an unknown ordinary outcome keeps the parent waiting with a visible obligation (row 8, SUB-021)", () => {
    const decision = classifyRecovery(
      snapshot("suspended", {
        inputApplied: inputMarker,
        suspension: waitingSuspension([waitingChild(CALL_DELEGATE, CHILD_ONE)]),
        childAttachments: [childAttachment(CALL_DELEGATE, CHILD_ONE, "unknown")],
      }),
      evidence({ inputRecorded: true }),
    );

    expect(decision._tag).toBe("AwaitChildSettlement");
  });

  it("legacy settled child without a parent marker replays the idempotent wake (row 9)", () => {
    const decision = classifyRecovery(
      running({
        childAttachments: [childAttachment(CALL_DELEGATE, CHILD_ONE, "settled", "completed")],
      }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            childSubmissionId: CHILD_ONE,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("ResumeWaitingParent");
    if (decision._tag === "ResumeWaitingParent") {
      expect(decision.children).toEqual([waitingChild(CALL_DELEGATE, CHILD_ONE)]);
    }
  });

  it("kill at subagent:after-join-append — joined with a reserved reservation applies the canonical accounting (row 10)", () => {
    const decision = classifyRecovery(
      running({
        childReservations: [childReservation({ childSubmissionId: CHILD_ONE })],
      }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            joined: true,
            childSubmissionId: CHILD_ONE,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("ApplyJoinAccounting");
    if (decision._tag === "ApplyJoinAccounting") {
      expect(decision.toolCallId).toBe(CALL_DELEGATE);
      expect(decision.reservationId).toBe(RESERVATION_ONE);
    }
  });

  it("kill at subagent:after-release-pending — a frozen accounting decision finishes through release, never a second freeze", () => {
    const decision = classifyRecovery(
      running({
        childReservations: [
          childReservation({ status: "releasePending", childSubmissionId: CHILD_ONE }),
        ],
      }),
      evidence({ inputRecorded: true }),
    );

    expect(decision._tag).toBe("ApplyJoinAccounting");
    if (decision._tag === "ApplyJoinAccounting") {
      expect(decision.reservationId).toBe(RESERVATION_ONE);
    }
  });

  it("kill at subagent:after-release — a released reservation with the call settled owes nothing more", () => {
    const decision = classifyRecovery(
      running({
        childReservations: [childReservation({ status: "released", childSubmissionId: CHILD_ONE })],
      }),
      evidence({ inputRecorded: true }),
    );

    expect(decision._tag).toBe("ResumeFromTurnBoundary");
  });

  it("a child Submission with parentLinkage classifies through the ordinary rows under its own fence (SUB-020)", () => {
    const decision = classifyRecovery(
      snapshot("input-applied", {
        ownership,
        inputApplied: inputMarker,
        parentLinkage: ParentLinkage.make({
          parentSubmissionId: HOST_SUBMISSION_ID,
          parentToolCallId: CALL_ONE,
        }),
      }),
      evidence({ inputRecorded: true }),
    );

    expect(decision._tag).toBe("ResumeFromTurnBoundary");
  });

  it("undecided approvals still precede delegation repairs — nothing prepares before the decision", () => {
    const decision = classifyRecovery(
      running(),
      evidence({
        inputRecorded: true,
        approvalsPending: [pendingApproval(CALL_ONE)],
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, { requested: true, admission: "not-admitted" }),
        ],
      }),
    );

    expect(decision._tag).toBe("AwaitApprovalDecision");
  });

  it("a canonical settlement beats every subagent row (DUR-002/DUR-015)", () => {
    const decision = classifyRecovery(
      running({
        childAttachments: [childAttachment(CALL_DELEGATE, CHILD_ONE, "running")],
      }),
      evidence({
        inputRecorded: true,
        recordedSettlementOutcome: "completed",
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            childSubmissionId: CHILD_ONE,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("FinalizeLedgerFromHistory");
  });
});

/** The three S2 precedence changes over the P4/P5 classifier, pinned (plan §4.3/§5 WP2). */
describe("recovery classifier S2 changed-precedence pins (plan §4.3)", () => {
  it("abort with a nonterminal attached child propagates instead of settling (SUB-022, §13.1)", () => {
    const decision = classifyRecovery(
      snapshot("suspended", {
        inputApplied: inputMarker,
        abortIntent,
        suspension: waitingSuspension([waitingChild(CALL_DELEGATE, CHILD_ONE)]),
        childAttachments: [childAttachment(CALL_DELEGATE, CHILD_ONE, "running")],
      }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            childSubmissionId: CHILD_ONE,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("PropagateChildAbort");
    if (decision._tag === "PropagateChildAbort") {
      expect(decision.children).toEqual([waitingChild(CALL_DELEGATE, CHILD_ONE)]);
    }
  });

  it("the replayed propagation classifies identically once the child abort is canonical — the intent row IS the marker (DUR-012)", () => {
    // The classifier cannot (and must not) read the child lane: while the attached child is
    // nonterminal under a parent abort, every pass replays the same idempotent command, whose
    // ledger replay returns the recorded child intent unchanged.
    const decision = classifyRecovery(
      snapshot("suspended", {
        inputApplied: inputMarker,
        abortIntent,
        suspension: waitingSuspension([waitingChild(CALL_DELEGATE, CHILD_ONE)]),
        childAttachments: [childAttachment(CALL_DELEGATE, CHILD_ONE, "running")],
      }),
      evidence({
        inputRecorded: true,
        abortRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            childSubmissionId: CHILD_ONE,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("PropagateChildAbort");
  });

  it("an open delegation call never marks Unknown while an ordinary sibling still does", () => {
    const decision = classifyRecovery(
      snapshot("running", { ownership, inputApplied: inputMarker }),
      evidence({
        inputRecorded: true,
        openToolCalls: [openCall(CALL_ONE)],
        openDelegationCalls: [delegationCall(CALL_DELEGATE)],
      }),
    );

    expect(decision._tag).toBe("MarkUnknown");
    if (decision._tag === "MarkUnknown") {
      expect(decision.openToolCallIds).toEqual([CALL_ONE]);
    }
  });

  it("an ordinary delegate-prefixed call remains Unknown without explicit delegation evidence", () => {
    const decision = classifyRecovery(
      snapshot("running", { ownership, inputApplied: inputMarker }),
      evidence({
        inputRecorded: true,
        openToolCalls: [openCall(CALL_DELEGATE, 1, "delegate_destination_research")],
      }),
    );

    expect(decision._tag).toBe("MarkUnknown");
  });

  it("a reservation cannot authorize replay of an ordinary Tool", () => {
    const decision = classifyRecovery(
      snapshot("running", {
        ownership,
        inputApplied: inputMarker,
        childReservations: [childReservation({ toolCallId: CALL_ONE })],
      }),
      evidence({ inputRecorded: true, openToolCalls: [openCall(CALL_ONE)] }),
    );

    expect(decision._tag).toBe("MarkUnknown");
  });

  it("suspended WaitingForChild with all children settled resumes the waiting parent", () => {
    const decision = classifyRecovery(
      snapshot("suspended", {
        inputApplied: inputMarker,
        suspension: waitingSuspension([
          waitingChild(CALL_DELEGATE, CHILD_ONE),
          waitingChild(CALL_DELEGATE_TWO, CHILD_TWO),
        ]),
        childAttachments: [
          childAttachment(CALL_DELEGATE, CHILD_ONE, "settled", "completed"),
          childAttachment(CALL_DELEGATE_TWO, CHILD_TWO, "settled", "failed"),
        ],
      }),
      evidence({ inputRecorded: true }),
    );

    expect(decision._tag).toBe("ResumeWaitingParent");
    if (decision._tag === "ResumeWaitingParent") {
      expect(decision.children).toEqual([
        waitingChild(CALL_DELEGATE, CHILD_ONE),
        waitingChild(CALL_DELEGATE_TWO, CHILD_TWO),
      ]);
    }
  });

  it("suspended WaitingForChild with an unproven child settlement stays awaiting (fail-closed)", () => {
    const decision = classifyRecovery(
      snapshot("suspended", {
        inputApplied: inputMarker,
        suspension: waitingSuspension([waitingChild(CALL_DELEGATE, CHILD_ONE)]),
      }),
      evidence({ inputRecorded: true }),
    );

    expect(decision._tag).toBe("AwaitChildSettlement");
  });

  it("suspended ApprovalPending keeps the P5 behavior byte-identical", () => {
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
});

/** The S2 abort rows: request-abort-and-join is the only parent close behavior (spec §13.1). */
describe("recovery classifier S2 abort rows (spec §13.1)", () => {
  const aborting = (overrides: SnapshotOverrides = {}) =>
    snapshot("running", { ownership, inputApplied: inputMarker, abortIntent, ...overrides });

  it("abort with an unproven child admission waits — never settle while a child may exist (SUB-031)", () => {
    const decision = classifyRecovery(
      aborting({ childReservations: [childReservation()] }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, { requested: true, admission: "indeterminate" }),
        ],
      }),
    );

    expect(decision._tag).toBe("AwaitChildAdmissionResolution");
  });

  it("abort with a provably unadmitted request releases the reservation exactly once, never admits", () => {
    const decision = classifyRecovery(
      aborting({ childReservations: [childReservation()] }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, { requested: true, admission: "not-admitted" }),
        ],
      }),
    );

    expect(decision._tag).toBe("ReleaseOrphanChildReservation");
    if (decision._tag === "ReleaseOrphanChildReservation") {
      expect(decision.reservationIds).toEqual([RESERVATION_ONE]);
    }
  });

  it("abort with an admitted-but-unlinked child repairs the start link so the abort targets a linked child", () => {
    const decision = classifyRecovery(
      aborting(),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, { requested: true, admission: "admitted" }),
        ],
      }),
    );

    expect(decision._tag).toBe("RepairSubagentStartLink");
  });

  it("abort with every child terminal but unjoined wakes the parent to join first (§13.1: joins precede settlement)", () => {
    const decision = classifyRecovery(
      aborting({
        childAttachments: [childAttachment(CALL_DELEGATE, CHILD_ONE, "settled", "aborted")],
      }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            childSubmissionId: CHILD_ONE,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("ResumeWaitingParent");
  });

  it("abort after the join with an incomplete release applies the accounting before settling", () => {
    const decision = classifyRecovery(
      aborting({
        childReservations: [childReservation({ childSubmissionId: CHILD_ONE })],
      }),
      evidence({
        inputRecorded: true,
        openDelegationCalls: [
          delegationCall(CALL_DELEGATE, {
            requested: true,
            started: true,
            joined: true,
            childSubmissionId: CHILD_ONE,
          }),
        ],
      }),
    );

    expect(decision._tag).toBe("ApplyJoinAccounting");
  });

  it("abort with every child joined and released settles aborted — no open child obligation remains", () => {
    const decision = classifyRecovery(
      aborting({
        childReservations: [childReservation({ status: "released", childSubmissionId: CHILD_ONE })],
      }),
      evidence({ inputRecorded: true }),
    );

    expect(decision._tag).toBe("SettleAborted");
  });

  it("abort of an open delegation call that never reserved keeps the P5 aborted settlement (no child obligation)", () => {
    const decision = classifyRecovery(
      aborting(),
      evidence({ inputRecorded: true, openDelegationCalls: [delegationCall(CALL_DELEGATE)] }),
    );

    expect(decision._tag).toBe("SettleAborted");
  });
});

/**
 * Classifier rows for the AwaitParentEstablishment establishment-race fix and the
 * position-blind SettleAborted precedence pin for queued-abort settlement.
 */
describe("recovery classifier rows (SUB-016 establishment race, DUR-004/DUR-012 queued abort)", () => {
  const parentLinkage = ParentLinkage.make({
    parentSubmissionId: HOST_SUBMISSION_ID,
    parentToolCallId: CALL_ONE,
  });

  it("a parent-linked admitted Submission without canonical lineage defers to AwaitParentEstablishment", () => {
    const decision = classifyRecovery(
      snapshot("admitted", { parentLinkage }),
      evidence({ threadMaterialized: false }),
    );

    expect(decision._tag).toBe("AwaitParentEstablishment");
    if (decision._tag === "AwaitParentEstablishment") {
      expect(decision.parentSubmissionId).toBe(HOST_SUBMISSION_ID);
      expect(decision.parentToolCallId).toBe(CALL_ONE);
    }
  });

  it("a parent-linked admitted Submission defers even when its Thread is already materialized", () => {
    // The race window includes a crash between the parent's materialize and its lineage
    // append: readiness self-repair stays deferred until the lineage record is canonical.
    const decision = classifyRecovery(snapshot("admitted", { parentLinkage }), evidence());

    expect(decision._tag).toBe("AwaitParentEstablishment");
  });

  it("a parent-linked admitted Submission WITH canonical lineage repairs readiness normally", () => {
    const decision = classifyRecovery(
      snapshot("admitted", { parentLinkage }),
      evidence({ subagentLineageRecorded: true }),
    );

    expect(decision._tag).toBe("RepairReadiness");
  });

  it("a parent-linked admitted Submission WITH lineage but no Thread completes materialization", () => {
    // Unreachable under this coordinator's establishment order (materialize precedes the
    // lineage append) but the row stays total: lineage present means the parent finished the
    // dangerous half, so the ordinary admitted repairs apply.
    const decision = classifyRecovery(
      snapshot("admitted", { parentLinkage }),
      evidence({ threadMaterialized: false, subagentLineageRecorded: true }),
    );

    expect(decision._tag).toBe("CompleteMaterialization");
  });

  it("a ROOT admitted Submission never consults lineage evidence", () => {
    const decision = classifyRecovery(snapshot("admitted"), evidence());

    expect(decision._tag).toBe("RepairReadiness");
  });

  it("an aborted never-claimed ready Submission classifies SettleAborted regardless of queue position", () => {
    // The classifier is deliberately position-blind: SettleAborted names the one
    // decision and the EXECUTOR settles a non-head ready row immediately at the current tail
    // (settlement order of never-run work is not execution order; DUR-004 bounds execution).
    const decision = classifyRecovery(snapshot("ready", { abortIntent }), evidence());

    expect(decision._tag).toBe("SettleAborted");
  });

  it("a crashed queued-abort settlement (reservation committed) classifies AppendReservedSettlement", () => {
    const decision = classifyRecovery(
      snapshot("terminalizing", { reservation: reservation("aborted", false), abortIntent }),
      evidence(),
    );

    expect(decision._tag).toBe("AppendReservedSettlement");
  });
});
