import {
  AdmissionRequest,
  ApprovalDecisionCommand,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  digestJson,
  IdempotencyKey,
  Principal,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  SettlementReservation,
  SubmissionSettled,
  submissionSettlementId,
  submissionSettlementRecordId,
  type AdmissionResult,
  type OwnershipToken,
  type ParentLinkage,
  type PersistedJson,
  type SettlementOutcome,
} from "@effect-agent/session";
import { env, runInDurableObject } from "cloudflare:test";
import { DateTime, Effect, Schema } from "effect";
import { TestClock } from "effect/testing";

import type { ConversationStorageObject } from "./worker.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      CONVERSATIONS: DurableObjectNamespace<ConversationStorageObject>;
    }
  }
}

export const conversationStub = (name: string) =>
  env.CONVERSATIONS.get(env.CONVERSATIONS.idFromName(name));

/**
 * Run one Effect program against a named Conversation Durable Object's real SQLite storage
 * inside workerd. The 0.21.x pool shares Durable Object storage across tests within a run,
 * so callers mint a UNIQUE object name per test/case (mirroring how the SQLite suites mint a
 * temporary database file per case). A manual `TestClock.layer()` is provided at the root —
 * the WP0-proven standalone path — so the shared conformance cases can drive lease expiry
 * through virtual time exactly as they do under `@effect/vitest`'s `it.effect` on Node.
 */
export const withConversationStorage = <A, E>(
  name: string,
  build: (storage: DurableObjectStorage, state: DurableObjectState) => Effect.Effect<A, E>,
): Promise<A> =>
  runInDurableObject(conversationStub(name), (_instance, state) =>
    Effect.runPromise(build(state.storage, state).pipe(Effect.provide(TestClock.layer()))),
  );

export const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

export const conversation = (value: string) => id(AdmissionRequest.fields.conversationId, value);
export const epoch = (value: number) => Schema.decodeSync(ProducerEpoch)(value);
export const sequence = (value: number) => Schema.decodeSync(CanonicalSequence)(value);
export const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));
export const toolCall = (value: string) => id(ApprovalDecisionCommand.fields.toolCallId, value);

export const TEST_PRINCIPAL = id(Principal, "principal-do-ledger");
export const TEST_PRODUCER = id(ProducerId, "producer-do-ledger");
export const OTHER_PRODUCER = id(ProducerId, "producer-do-ledger-other");
export const TEST_AGENT = id(AdmissionRequest.fields.agentId, "agent-do-ledger");
export const TEST_DEPLOYMENT = id(DeploymentId, "deployment-do-ledger");
export const TEST_DEFINITION_DIGEST = Schema.decodeSync(Digest)("a".repeat(64));
export const TEST_DIGESTS = DefinitionDigests.make({
  agent: TEST_DEFINITION_DIGEST,
  model: TEST_DEFINITION_DIGEST,
  tools: TEST_DEFINITION_DIGEST,
});

export const admission = Effect.fn("DoLedgerTest.admission")(function* (
  conversationId: string,
  idempotencyKey: string,
  input: PersistedJson,
  parentLinkage?: ParentLinkage,
) {
  const inputDigest = yield* digestJson(input);
  return AdmissionRequest.make({
    conversationId: conversation(conversationId),
    principal: TEST_PRINCIPAL,
    idempotencyKey: id(IdempotencyKey, idempotencyKey),
    agentId: TEST_AGENT,
    agentDigests: TEST_DIGESTS,
    deploymentId: TEST_DEPLOYMENT,
    inputPayload: input,
    inputDigest,
    ...(parentLinkage === undefined ? {} : { parentLinkage }),
  });
});

export const settlementReservation = Effect.fn("DoLedgerTest.settlementReservation")(function* (
  admitted: AdmissionResult,
  ownershipToken: OwnershipToken,
  outcome: SettlementOutcome,
) {
  const settlementId = submissionSettlementId(admitted.submissionId);
  const record = RecordEnvelope.make({
    recordId: submissionSettlementRecordId(admitted.submissionId),
    family: "conversation",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload: SubmissionSettled.make({
      submissionId: admitted.submissionId,
      settlementId,
      receiptId: admitted.receiptId,
      outcome,
    }),
  });
  const encoded = yield* Schema.encodeEffect(RecordEnvelope)(record).pipe(Effect.orDie);
  const recordDigest = yield* digestJson(encoded);
  return SettlementReservation.make({
    submissionId: admitted.submissionId,
    ownershipToken,
    settlementId,
    outcome,
    record,
    recordDigest,
  });
});
