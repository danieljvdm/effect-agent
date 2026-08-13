import { describe, expect, it } from "@effect/vitest";
import { DateTime, Schema } from "effect";
import { AgentId, AttemptId, ConversationId, ReceiptId, SubmissionId } from "@effect-agent/core";

import {
  AbortIntent,
  CanonicalSequence,
  classifyRecovery,
  DefinitionDigests,
  DeploymentId,
  Digest,
  IdempotencyKey,
  InputAppliedMarker,
  OwnershipSnapshot,
  Principal,
  ProducerEpoch,
  ProducerId,
  QueueSequence,
  RecordEnvelope,
  RecoveryEvidence,
  RecoverySnapshot,
  SettlementReservationSnapshot,
  SubmissionSnapshot,
  submissionInputRecordId,
  submissionSettlementId,
  submissionSettlementRecordId,
  type SettlementOutcome,
  type SubmissionState,
} from "../src/index.ts";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const SHA_B = Schema.decodeSync(Digest)("b".repeat(64));

const SUBMISSION_ID = Schema.decodeSync(SubmissionId)("submission-classifier-1");
const CONVERSATION_ID = Schema.decodeSync(ConversationId)("conversation-classifier");
const CREATED_AT = DateTime.toUtc(DateTime.makeUnsafe(1_000));

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

interface SnapshotOverrides {
  readonly ownership?: OwnershipSnapshot;
  readonly inputApplied?: InputAppliedMarker;
  readonly reservation?: SettlementReservationSnapshot;
  readonly abortIntent?: AbortIntent;
}

const snapshot = (state: SubmissionState, overrides: SnapshotOverrides = {}): RecoverySnapshot =>
  RecoverySnapshot.make({
    submission: submission(state),
    ...(overrides.ownership === undefined ? {} : { ownership: overrides.ownership }),
    ...(overrides.inputApplied === undefined ? {} : { inputApplied: overrides.inputApplied }),
    ...(overrides.reservation === undefined ? {} : { reservation: overrides.reservation }),
    ...(overrides.abortIntent === undefined ? {} : { abortIntent: overrides.abortIntent }),
  });

interface EvidenceOverrides {
  readonly conversationMaterialized?: boolean;
  readonly inputRecorded?: boolean;
  readonly recordedSettlementOutcome?: SettlementOutcome;
  readonly abortRecorded?: boolean;
  readonly unresolvedToolCall?: boolean;
}

const evidence = (overrides: EvidenceOverrides = {}): RecoveryEvidence =>
  RecoveryEvidence.make({
    conversationMaterialized: overrides.conversationMaterialized ?? true,
    inputRecorded: overrides.inputRecorded ?? false,
    abortRecorded: overrides.abortRecorded ?? false,
    unresolvedToolCall: overrides.unresolvedToolCall ?? false,
    ...(overrides.recordedSettlementOutcome === undefined
      ? {}
      : { recordedSettlementOutcome: overrides.recordedSettlementOutcome }),
  });

/**
 * The executable crash-matrix decision table (plan §Crash matrix, durability §15): one
 * deterministic classification per persisted crash shape.
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

  it("an unresolved ordinary Tool call marks unknown and is never auto-replayed (DUR-009)", () => {
    const decision = classifyRecovery(
      snapshot("input-applied", { inputApplied: inputMarker }),
      evidence({ inputRecorded: true, unresolvedToolCall: true }),
    );
    expect(decision._tag).toBe("MarkUnknown");
  });
});
