import { AgentId, ConversationId, SubmissionId, ToolCallId } from "@effect-agent/core";
import {
  AdmissionRequest,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  CanonicalSequence,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimJoiningRequest,
  ClaimRequest,
  DefinitionDigests,
  DeploymentId,
  Digest,
  IdempotencyKey,
  LedgerError,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  OwnershipToken,
  Principal,
  ProducerId,
  RecoverySnapshotRequest,
  ReleaseChildBudgetRequest,
  RenewOwnershipRequest,
  ResolutionNeverHappened,
  RevertJoiningRequest,
  SettlementFinalization,
  SubmissionLedger,
  SubmissionLookupByKey,
  SuspendRequest,
  UnknownResolutionCommand,
  WaitingChild,
  WaitingForChildSuspension,
  submissionInputRecordId,
  submissionLedgerConformanceCases,
  submissionSettlementId,
} from "@effect-agent/session";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Ref, Schema, Stream } from "effect";

import { MemorySubmissionLedgerLive, memorySubmissionLedgerLayer } from "../src/index.ts";

const testLayer = MemorySubmissionLedgerLive.pipe(Layer.provideMerge(NodeCrypto.layer));

const conversationId = Schema.decodeSync(ConversationId)("conversation-memory-ledger-1");
const principal = Schema.decodeSync(Principal)("principal-memory-ledger");
const agentId = Schema.decodeSync(AgentId)("agent-memory-ledger");
const deploymentId = Schema.decodeSync(DeploymentId)("deployment-memory-ledger");
const producerA = Schema.decodeSync(ProducerId)("producer-memory-ledger-a");
const definitionDigest = Schema.decodeSync(Digest)("e".repeat(64));
const unknownSubmission = Schema.decodeSync(SubmissionId)("submission-memory-unknown");
const unknownToken = Schema.decodeSync(OwnershipToken)("ownership-memory-unknown");
const unknownCall = Schema.decodeSync(ToolCallId)("call-memory-unknown");
const unknownReservation = Schema.decodeSync(ChildReservationId)("child-reservation-unknown");
const sequenceOne = Schema.decodeSync(CanonicalSequence)(1);
const waitingParentLane = Schema.decodeSync(ConversationId)("conversation-memory-waiting");
const waitingChildLane = Schema.decodeSync(ConversationId)("conversation-memory-waiting-child");
const indeterminateKey = Schema.decodeSync(IdempotencyKey)("indeterminate-key");
const waitingParentKey = Schema.decodeSync(IdempotencyKey)("waiting-parent");
const waitingChildKey = Schema.decodeSync(IdempotencyKey)("waiting-child");
const waitingParentDigest = Schema.decodeSync(Digest)("d1".padEnd(64, "0"));
const waitingChildDigest = Schema.decodeSync(Digest)("d2".padEnd(64, "0"));
const agentDigests = DefinitionDigests.make({
  agent: definitionDigest,
  model: definitionDigest,
  tools: definitionDigest,
});

const admissionRequest = (idempotencyKey: string, digestSeed: string): AdmissionRequest =>
  AdmissionRequest.make({
    conversationId,
    principal,
    idempotencyKey: Schema.decodeSync(IdempotencyKey)(idempotencyKey),
    agentId,
    agentDigests,
    deploymentId,
    inputPayload: { work: idempotencyKey },
    inputDigest: Schema.decodeSync(Digest)(digestSeed.padEnd(64, "0")),
  });

describe("MemorySubmissionLedger", () => {
  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it.effect(conformanceCase.name, () => conformanceCase.run.pipe(Effect.provide(testLayer)));
    }
  });

  it.layer(testLayer)((it) => {
    it.effect("claims non-durable capabilities", () =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const capabilities = yield* ledger.capabilities;
        expect(capabilities.durability).toBe("non-durable");
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("reports the current state when replaying an admission", () =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const request = admissionRequest("replay-state-key", "ab");
        const first = yield* ledger.admit(request);
        expect(first.replayed).toBe(false);
        expect(first.state).toBe("admitted");

        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: first.submissionId }));
        const replayed = yield* ledger.admit(request);
        expect(replayed.replayed).toBe(true);
        expect(replayed.submissionId).toBe(first.submissionId);
        expect(replayed.receiptId).toBe(first.receiptId);
        expect(replayed.state).toBe("ready");
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("lets the same producer reclaim its own live lease and fences the old token", () =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const admitted = yield* ledger.admit(admissionRequest("same-producer-key", "ac"));
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));

        const first = yield* ledger.claim(
          ClaimRequest.make({ conversationId, producerId: producerA }),
        );
        expect(Option.isSome(first)).toBe(true);
        if (Option.isNone(first)) return;

        const reclaimed = yield* ledger.claim(
          ClaimRequest.make({ conversationId, producerId: producerA }),
        );
        expect(Option.isSome(reclaimed)).toBe(true);
        if (Option.isNone(reclaimed)) return;
        expect(reclaimed.value.submissionId).toBe(admitted.submissionId);
        expect(reclaimed.value.attemptId).not.toBe(first.value.attemptId);
        expect(reclaimed.value.producerEpoch).toBeGreaterThan(first.value.producerEpoch);

        const staleRenew = yield* ledger
          .renewOwnership(
            RenewOwnershipRequest.make({
              submissionId: admitted.submissionId,
              ownershipToken: first.value.ownershipToken,
            }),
          )
          .pipe(Effect.flip);
        expect(staleRenew).toBeInstanceOf(OwnershipLost);
        expect(staleRenew).toMatchObject({
          _tag: "OwnershipLost",
          submissionId: admitted.submissionId,
          actualEpoch: reclaimed.value.producerEpoch,
        });

        const liveRenew = yield* ledger.renewOwnership(
          RenewOwnershipRequest.make({
            submissionId: admitted.submissionId,
            ownershipToken: reclaimed.value.ownershipToken,
          }),
        );
        expect(liveRenew.ownershipToken).toBe(reclaimed.value.ownershipToken);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("rejects malformed admissions at the schema boundary without mutating state", () =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const invalid = {
          conversationId: "conversation-memory-ledger-invalid",
          principal: "principal-memory-ledger",
          idempotencyKey: "invalid-key",
          agentId: "agent-memory-ledger",
          agentDigests: {
            agent: "not-a-digest",
            model: "not-a-digest",
            tools: "not-a-digest",
          },
          deploymentId: "deployment-memory-ledger",
          inputPayload: { work: "invalid" },
          inputDigest: "not-a-digest",
        };
        const admitBoundary: unknown = ledger.admit;
        if (typeof admitBoundary !== "function") {
          return yield* Effect.die(new Error("Expected an admit function"));
        }
        const unvalidatedResult: unknown = admitBoundary(invalid);
        if (!Effect.isEffect(unvalidatedResult)) {
          return yield* Effect.die(new Error("Expected admit to return an Effect"));
        }
        const failure = yield* unvalidatedResult.pipe(Effect.flip);
        if (!(failure instanceof LedgerError)) {
          return yield* Effect.die(new Error("Expected a LedgerError"));
        }
        expect(failure).toMatchObject({ _tag: "LedgerError", operation: "admit" });
        expect(failure.cause).toBeDefined();

        const scanned = yield* ledger.scanNonterminal.pipe(Stream.runCollect);
        expect(scanned).toEqual([]);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("fails with LedgerError for operations on unknown submissions", () =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        const readyFailure = yield* ledger
          .markReady(MarkReadyRequest.make({ submissionId: unknownSubmission }))
          .pipe(Effect.flip);
        expect(readyFailure).toMatchObject({ _tag: "LedgerError", operation: "markReady" });

        const renewFailure = yield* ledger
          .renewOwnership(
            RenewOwnershipRequest.make({
              submissionId: unknownSubmission,
              ownershipToken: unknownToken,
            }),
          )
          .pipe(Effect.flip);
        expect(renewFailure).toMatchObject({ _tag: "LedgerError", operation: "renewOwnership" });

        const finalizeFailure = yield* ledger
          .finalizeSettlement(
            SettlementFinalization.make({
              submissionId: unknownSubmission,
              settlementId: submissionSettlementId(unknownSubmission),
            }),
          )
          .pipe(Effect.flip);
        expect(finalizeFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "finalizeSettlement",
        });

        const snapshotFailure = yield* ledger
          .loadRecoverySnapshot(RecoverySnapshotRequest.make({ submissionId: unknownSubmission }))
          .pipe(Effect.flip);
        expect(snapshotFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "loadRecoverySnapshot",
        });

        const claimJoiningFailure = yield* ledger
          .claimJoining(
            ClaimJoiningRequest.make({
              conversationId,
              hostSubmissionId: unknownSubmission,
              ownershipToken: unknownToken,
              maxCount: 1,
            }),
          )
          .pipe(Effect.flip);
        expect(claimJoiningFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "claimJoining",
        });

        const markJoinedFailure = yield* ledger
          .markJoined(
            MarkJoinedRequest.make({
              submissionId: unknownSubmission,
              ownershipToken: unknownToken,
              recordId: submissionInputRecordId(unknownSubmission),
              sequence: sequenceOne,
            }),
          )
          .pipe(Effect.flip);
        expect(markJoinedFailure).toMatchObject({ _tag: "LedgerError", operation: "markJoined" });

        const revertFailure = yield* ledger
          .revertJoining(RevertJoiningRequest.make({ submissionId: unknownSubmission }))
          .pipe(Effect.flip);
        expect(revertFailure).toMatchObject({ _tag: "LedgerError", operation: "revertJoining" });

        const suspendFailure = yield* ledger
          .suspend(
            SuspendRequest.make({
              submissionId: unknownSubmission,
              ownershipToken: unknownToken,
              reason: ApprovalPendingSuspension.make({ toolCallIds: [unknownCall] }),
            }),
          )
          .pipe(Effect.flip);
        expect(suspendFailure).toMatchObject({ _tag: "LedgerError", operation: "suspend" });

        const decisionFailure = yield* ledger
          .recordApprovalDecision(
            ApprovalDecisionCommand.make({
              submissionId: unknownSubmission,
              toolCallId: unknownCall,
              decision: "approved",
              resolver: "memory-ledger-test",
              reason: "unknown submission",
            }),
          )
          .pipe(Effect.flip);
        expect(decisionFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "recordApprovalDecision",
        });

        const markUnknownFailure = yield* ledger
          .markUnknown(
            MarkUnknownRequest.make({
              submissionId: unknownSubmission,
              toolCallIds: [unknownCall],
              reason: "unknown submission",
            }),
          )
          .pipe(Effect.flip);
        expect(markUnknownFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "markUnknown",
        });

        const resolutionFailure = yield* ledger
          .recordUnknownResolution(
            UnknownResolutionCommand.make({
              submissionId: unknownSubmission,
              toolCallId: unknownCall,
              author: "memory-ledger-test",
              reason: "unknown submission",
              resolution: ResolutionNeverHappened.make(),
            }),
          )
          .pipe(Effect.flip);
        expect(resolutionFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "recordUnknownResolution",
        });

        const reserveFailure = yield* ledger
          .reserveChildBudget(
            ChildBudgetReservationRequest.make({
              reservationId: unknownReservation,
              parentSubmissionId: unknownSubmission,
              parentToolCallId: unknownCall,
              ownershipToken: unknownToken,
              allocation: { turns: 1 },
              allocationDigest: definitionDigest,
            }),
          )
          .pipe(Effect.flip);
        expect(reserveFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "reserveChildBudget",
        });

        const attachFailure = yield* ledger
          .attachChildToReservation(
            AttachChildToReservationRequest.make({
              reservationId: unknownReservation,
              ownershipToken: unknownToken,
              childSubmissionId: unknownSubmission,
            }),
          )
          .pipe(Effect.flip);
        expect(attachFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "attachChildToReservation",
        });

        const beginFailure = yield* ledger
          .beginChildBudgetRelease(
            BeginChildBudgetReleaseRequest.make({
              reservationId: unknownReservation,
              accounting: { consumed: {} },
            }),
          )
          .pipe(Effect.flip);
        expect(beginFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "beginChildBudgetRelease",
        });

        const releaseFailure = yield* ledger
          .releaseChildBudget(ReleaseChildBudgetRequest.make({ reservationId: unknownReservation }))
          .pipe(Effect.flip);
        expect(releaseFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "releaseChildBudget",
        });

        const settledFailure = yield* ledger
          .recordChildSettled(
            ChildSettledNotification.make({
              parentSubmissionId: unknownSubmission,
              childSubmissionId: unknownSubmission,
            }),
          )
          .pipe(Effect.flip);
        expect(settledFailure).toMatchObject({
          _tag: "LedgerError",
          operation: "recordChildSettled",
        });
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("rejects a premature child settlement notification fail-closed", () =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const parent = yield* ledger.admit(admissionRequest("premature-parent-key", "b1"));
        const child = yield* ledger.admit(admissionRequest("premature-child-key", "b2"));

        // The child is admitted but NOT settled: the single-store adapter verifies the
        // canonical settlement authority instead of trusting the caller.
        const premature = yield* ledger
          .recordChildSettled(
            ChildSettledNotification.make({
              parentSubmissionId: parent.submissionId,
              childSubmissionId: child.submissionId,
            }),
          )
          .pipe(Effect.flip);
        expect(premature).toMatchObject({
          _tag: "LedgerError",
          operation: "recordChildSettled",
        });
      }),
    );
  });

  it.effect("answers Indeterminate from the fault seam and never treats it as absence", () =>
    Effect.gen(function* () {
      const fault = yield* Ref.make<Option.Option<string>>(Option.none());
      const faultLayer = memorySubmissionLedgerLayer({
        resolveAdmissionFault: Ref.get(fault),
      }).pipe(Layer.provideMerge(NodeCrypto.layer));
      yield* Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const key = SubmissionLookupByKey.make({
          conversationId,
          principal,
          idempotencyKey: indeterminateKey,
        });

        const beforeAdmission = yield* ledger.resolveAdmission(key);
        expect(beforeAdmission._tag).toBe("NotAdmitted");

        const admitted = yield* ledger.admit(admissionRequest("indeterminate-key", "c1"));

        // The authoritative owner becomes unreachable: the adapter answers Indeterminate —
        // a typed "cannot prove either", never NotAdmitted (SUB-031).
        yield* Ref.set(fault, Option.some("the authoritative child owner is unreachable"));
        const during = yield* ledger.resolveAdmission(key);
        expect(during._tag).toBe("Indeterminate");
        if (during._tag === "Indeterminate") {
          expect(during.reason).toBe("the authoritative child owner is unreachable");
        }

        // Once reachable again, the SAME admission resolves: indeterminate was never absence,
        // so no second child was ever permitted.
        yield* Ref.set(fault, Option.none());
        const after = yield* ledger.resolveAdmission(key);
        expect(after._tag).toBe("Admitted");
        if (after._tag === "Admitted") {
          expect(after.submission.submissionId).toBe(admitted.submissionId);
          expect(after.submission.receiptId).toBe(admitted.receiptId);
        }
      }).pipe(Effect.provide(faultLayer));
    }),
  );

  it.layer(testLayer)((it) => {
    it.effect("keeps a waitingForChild parent dormant while children run in their own lanes", () =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        const parent = yield* ledger.admit(
          AdmissionRequest.make({
            conversationId: waitingParentLane,
            principal,
            idempotencyKey: waitingParentKey,
            agentId,
            agentDigests,
            deploymentId,
            inputPayload: { work: "parent" },
            inputDigest: waitingParentDigest,
          }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: parent.submissionId }));
        const child = yield* ledger.admit(
          AdmissionRequest.make({
            conversationId: waitingChildLane,
            principal,
            idempotencyKey: waitingChildKey,
            agentId,
            agentDigests,
            deploymentId,
            inputPayload: { work: "child" },
            inputDigest: waitingChildDigest,
            parentLinkage: {
              parentSubmissionId: parent.submissionId,
              parentToolCallId: unknownCall,
            },
          }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: child.submissionId }));

        const parentClaim = yield* ledger.claim(
          ClaimRequest.make({ conversationId: waitingParentLane, producerId: producerA }),
        );
        expect(Option.isSome(parentClaim)).toBe(true);
        if (Option.isNone(parentClaim)) return;
        const suspended = yield* ledger.suspend(
          SuspendRequest.make({
            submissionId: parent.submissionId,
            ownershipToken: parentClaim.value.ownershipToken,
            reason: WaitingForChildSuspension.make({
              children: [
                WaitingChild.make({
                  toolCallId: unknownCall,
                  childSubmissionId: child.submissionId,
                }),
              ],
            }),
          }),
        );
        expect(suspended).toBe("suspended");

        // Independent fencing (SUB-020): the child lane claims under its OWN epoch while the
        // suspended parent lane produces no claim at all.
        const childClaim = yield* ledger.claim(
          ClaimRequest.make({ conversationId: waitingChildLane, producerId: producerA }),
        );
        expect(Option.isSome(childClaim)).toBe(true);
        const blockedParent = yield* ledger.claim(
          ClaimRequest.make({ conversationId: waitingParentLane, producerId: producerA }),
        );
        expect(Option.isNone(blockedParent)).toBe(true);
      }),
    );
  });
});
