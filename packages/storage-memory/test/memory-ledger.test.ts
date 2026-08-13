import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Schema, Stream } from "effect";

import { AgentId, ConversationId, SubmissionId } from "@effect-agent/core";
import {
  AdmissionRequest,
  ClaimRequest,
  DefinitionDigests,
  DeploymentId,
  Digest,
  IdempotencyKey,
  LedgerError,
  MarkReadyRequest,
  OwnershipLost,
  OwnershipToken,
  Principal,
  ProducerId,
  RecoverySnapshotRequest,
  RenewOwnershipRequest,
  SettlementFinalization,
  SubmissionLedger,
  submissionLedgerConformanceCases,
  submissionSettlementId,
} from "@effect-agent/session";

import { MemorySubmissionLedgerLive } from "../src/index.ts";

const testLayer = Layer.mergeAll(MemorySubmissionLedgerLive, NodeCrypto.layer);

const conversationId = Schema.decodeSync(ConversationId)("conversation-memory-ledger-1");
const principal = Schema.decodeSync(Principal)("principal-memory-ledger");
const agentId = Schema.decodeSync(AgentId)("agent-memory-ledger");
const deploymentId = Schema.decodeSync(DeploymentId)("deployment-memory-ledger");
const producerA = Schema.decodeSync(ProducerId)("producer-memory-ledger-a");
const definitionDigest = Schema.decodeSync(Digest)("e".repeat(64));
const unknownSubmission = Schema.decodeSync(SubmissionId)("submission-memory-unknown");
const unknownToken = Schema.decodeSync(OwnershipToken)("ownership-memory-unknown");
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
      }),
    );
  });
});
