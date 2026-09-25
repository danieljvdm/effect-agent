import {
  PostgresStorageFailpointError,
  type PostgresStorageFailpointLocation,
  PostgresWriteContention,
} from "@effect-agent/storage-postgres/postgres-storage-error";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import type { Crypto } from "effect";
import { Cause, DateTime, Effect, Exit, Layer, Option, Ref, Schema } from "effect";
import { digestJson } from "effect-agent/digest";
import {
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  SubmissionSettled,
  SubmissionSettledRecord,
  type PersistedJson,
  type SettlementOutcome,
} from "effect-agent/records";
import {
  type ParentLinkage,
  AbortCommand,
  AdmissionRequest,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimJoiningRequest,
  ClaimRequest,
  IdempotencyKey,
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  Principal,
  ReleaseChildBudgetRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  RevertJoiningRequest,
  SettlementFinalization,
  SettlementReservation,
  SubmissionLedger,
  SuspendRequest,
  UnknownResolutionCommand,
  WaitingChild,
  WaitingForChildSuspension,
  submissionInputRecordId,
  submissionSettlementId,
  submissionSettlementRecordId,
  type AdmissionResult,
  type OwnershipToken,
} from "effect-agent/submission-ledger";
import { ThreadMaterialization } from "effect-agent/thread-store";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import {
  clientLayer,
  storage as makeStorage,
  singleConnectionStorage,
  whileHoldingWriterLock,
  withTemporaryDatabase,
} from "./harness.ts";

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const thread = (value: string) => id(ThreadMaterialization.fields.threadId, value);
const sequence = (value: number) => Schema.decodeSync(CanonicalSequence)(value);
const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const toolCall = (value: string) => id(ApprovalDecisionCommand.fields.toolCallId, value);
const isLedgerError = Schema.is(LedgerError);
const isPostgresStorageFailpointError = Schema.is(PostgresStorageFailpointError);

const TEST_PRINCIPAL = id(Principal, "principal-postgres-ledger");
const TEST_PRODUCER = id(ProducerId, "producer-postgres-ledger");
const OTHER_PRODUCER = id(ProducerId, "producer-postgres-ledger-other");
const TEST_AGENT = id(AdmissionRequest.fields.agentId, "agent-postgres-ledger");
const TEST_DEPLOYMENT = id(DeploymentId, "deployment-postgres-ledger");
const TEST_DEFINITION_DIGEST = Schema.decodeSync(Digest)("a".repeat(64));

const S2_FAILPOINT_RESERVATION = Schema.decodeSync(ChildReservationId)(
  "child-reservation:run-s2fp:call-1",
);

const TEST_DIGESTS = DefinitionDigests.make({
  agent: TEST_DEFINITION_DIGEST,
  model: TEST_DEFINITION_DIGEST,
  tools: TEST_DEFINITION_DIGEST,
});

const admission = Effect.fn("PostgresLedgerTest.admission")(function* (
  threadId: string,
  idempotencyKey: string,
  input: PersistedJson,
  parentLinkage?: ParentLinkage,
) {
  const inputDigest = yield* digestJson(input);

  return AdmissionRequest.make({
    threadId: thread(threadId),
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

const settlementReservation = Effect.fn("PostgresLedgerTest.settlementReservation")(function* (
  admitted: AdmissionResult,
  ownershipToken: OwnershipToken,
  outcome: SettlementOutcome,
) {
  const settlementId = submissionSettlementId(admitted.submissionId);

  const payload = yield* Schema.decodeEffect(SubmissionSettledRecord)(
    SubmissionSettled.make({
      submissionId: admitted.submissionId,
      settlementId,
      receiptId: admitted.receiptId,
      outcome,
      ...(outcome === "failed"
        ? {
            result: {
              errorTag: "PostgresLedgerTestFailure",
              message: "The Postgres ledger test Submission failed",
            },
          }
        : {}),
    }),
  ).pipe(Effect.orDie);

  const record = RecordEnvelope.make({
    recordId: submissionSettlementRecordId(admitted.submissionId),
    family: "thread",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload,
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

const withLedger = <A, E>(
  url: string,
  effect: Effect.Effect<A, E, SubmissionLedger | Crypto.Crypto>,
) => Effect.provide(effect, [makeStorage(url).submissionLedger, NodeCrypto.layer]);

const withSql = <A, E>(url: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, clientLayer(url));

const singleConnectionLedger = (url: string, lockTimeout: number) => {
  const storage = singleConnectionStorage(url, lockTimeout);

  return Layer.mergeAll(storage.submissionLedger, storage.clientLayer, NodeCrypto.layer);
};

const expectInjectedFailure = <A>(
  exit: Exit.Exit<A, unknown>,
  location: PostgresStorageFailpointLocation,
) => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isFailure(exit)) {
    const error = Cause.squash(exit.cause);

    expect(error).toBeInstanceOf(LedgerError);
    if (isLedgerError(error)) {
      expect(error.cause).toBeInstanceOf(PostgresStorageFailpointError);
      if (isPostgresStorageFailpointError(error.cause)) {
        expect(error.cause.location).toBe(location);
      }
    }
  }
};

const makeFailpointHarness = (url: string) =>
  Effect.gen(function* () {
    const active = yield* Ref.make<PostgresStorageFailpointLocation | undefined>(undefined);

    const select = (location: PostgresStorageFailpointLocation | undefined) =>
      Ref.set(active, location);

    const failingLedger = <A, E>(effect: Effect.Effect<A, E, SubmissionLedger | Crypto.Crypto>) =>
      Effect.provide(effect, [
        makeStorage(url, {
          failpoint: (location) =>
            Ref.get(active).pipe(
              Effect.flatMap((selected) =>
                selected === location
                  ? Effect.fail(PostgresStorageFailpointError.make({ location }))
                  : Effect.void,
              ),
            ),
        }).submissionLedger,
        NodeCrypto.layer,
      ]);

    return { select, failingLedger } as const;
  });

describe("PostgresSubmissionLedger faults", () => {
  it.effect("classifies cross-connection write contention as retryable typed contention", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* withLedger(
          url,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.admit(yield* admission("thread-busy", "busy-key-1", { step: 1 }));
          }),
        );

        // Opening a ledger takes the writer lock itself, so the competing producer must arrive
        // after the ledger is open. `lock_timeout = 0` disables the bound in Postgres, so the
        // shortest bounded wait is used instead.
        const contended = yield* Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const request = yield* admission("thread-busy", "busy-key-2", { step: 2 });

          return yield* whileHoldingWriterLock(url, ledger.admit(request).pipe(Effect.exit));
        }).pipe(Effect.provide(singleConnectionLedger(url, 50)));

        expect(Exit.isFailure(contended)).toBe(true);
        if (Exit.isFailure(contended)) {
          const error = Cause.squash(contended.cause);

          expect(error).toBeInstanceOf(LedgerError);
          if (isLedgerError(error)) {
            expect(error.cause).toBeInstanceOf(PostgresWriteContention);
          }
        }

        // Once the competing writer releases the lock, the identical admission commits.
        const recovered = yield* withLedger(
          url,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.admit(yield* admission("thread-busy", "busy-key-2", { step: 2 }));
          }),
        );

        expect(recovered.replayed).toBe(false);
      }),
    ),
  );

  it.effect("recovers ledger identity and ownership after lost acknowledgements", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const { select, failingLedger } = yield* makeFailpointHarness(url);

        const submissionStates = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, state, receipt_id, input_applied_record_id
              FROM effect_agent_submissions
              ORDER BY thread_id, queue_sequence
            `;
          }),
        );

        const ownershipRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, attempt_id, ownership_token, producer_epoch, lease_expires_at
              FROM effect_agent_submission_ownership
            `;
          }),
        );

        const attemptRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT attempt_id, submission_id, producer_epoch
              FROM effect_agent_attempts
              ORDER BY producer_epoch
            `;
          }),
        );

        const reservationRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, settlement_id, outcome, finalized_at
              FROM effect_agent_settlement_reservations
            `;
          }),
        );

        const abortRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, reason, requested_at
              FROM effect_agent_abort_intents
            `;
          }),
        );

        const threadRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT thread_id, producer_epoch
              FROM effect_agent_threads
            `;
          }),
        );

        const lane = "thread-failpoints";

        const admitOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.admit(yield* admission(lane, "failpoint-key", { work: "fail" }));
          }),
        );

        // Admission commits before the lost acknowledgement; retry preserves its identity.
        yield* select("ledger:admit:after");
        expectInjectedFailure(yield* admitOnce.pipe(Effect.exit), "ledger:admit:after");
        const admittedRows = yield* submissionStates;

        expect(admittedRows).toHaveLength(1);
        expect(admittedRows[0]?.state).toBe("admitted");
        yield* select(undefined);
        const admitted = yield* admitOnce;

        expect(admitted.replayed).toBe(true);
        expect(admitted.submissionId).toBe(admittedRows[0]?.submission_id);
        expect(admitted.receiptId).toBe(admittedRows[0]?.receipt_id);

        // Retrying the committed ready transition is a no-op.
        const markReadyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          }),
        );

        yield* select("ledger:mark-ready:after");
        expectInjectedFailure(yield* markReadyOnce.pipe(Effect.exit), "ledger:mark-ready:after");
        expect((yield* submissionStates)[0]?.state).toBe("ready");
        yield* select(undefined);
        yield* markReadyOnce;

        // The lost claim acknowledgement leaves ownership, audit and epoch changes durable.
        const claimOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.claim(
              ClaimRequest.make({ threadId: thread(lane), producerId: TEST_PRODUCER }),
            );
          }),
        );

        yield* select("ledger:claim:after");
        expectInjectedFailure(yield* claimOnce.pipe(Effect.exit), "ledger:claim:after");
        const orphanedOwnership = yield* ownershipRows;

        expect(orphanedOwnership).toHaveLength(1);
        expect(orphanedOwnership[0]?.producer_epoch).toBe(1n);
        expect(yield* threadRows).toEqual([{ thread_id: lane, producer_epoch: 1n }]);
        expect((yield* submissionStates)[0]?.state).toBe("running");
        expect(yield* attemptRows).toHaveLength(1);
        // The orphaned lease blocks until expiry; a later Attempt reclaims at a higher epoch.
        yield* select(undefined);
        expect(Option.isNone(yield* claimOnce)).toBe(true);
        yield* TestClock.adjust(30_001);
        const claim = yield* claimOnce;

        expect(Option.isSome(claim)).toBe(true);
        if (Option.isNone(claim)) return;
        expect(claim.value.producerEpoch).toBe(2);
        expect(yield* attemptRows).toHaveLength(2);

        // The committed input marker survives reopen; retry is a no-op.
        const markInputOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.markInputApplied(
              MarkInputAppliedRequest.make({
                submissionId: admitted.submissionId,
                ownershipToken: claim.value.ownershipToken,
                recordId: submissionInputRecordId(admitted.submissionId),
                sequence: sequence(1),
              }),
            );
          }),
        );

        yield* select("ledger:mark-input-applied:after");
        expectInjectedFailure(
          yield* markInputOnce.pipe(Effect.exit),
          "ledger:mark-input-applied:after",
        );
        expect((yield* submissionStates)[0]?.input_applied_record_id).toBe(
          submissionInputRecordId(admitted.submissionId),
        );
        expect((yield* submissionStates)[0]?.state).toBe("input-applied");
        yield* select(undefined);
        yield* markInputOnce;

        // The lease extension survives the lost acknowledgement.
        const renewOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.renewOwnership(
              RenewOwnershipRequest.make({
                submissionId: admitted.submissionId,
                ownershipToken: claim.value.ownershipToken,
              }),
            );
          }),
        );

        const leaseBeforeRenew = (yield* ownershipRows)[0]?.lease_expires_at;

        yield* TestClock.adjust(1_000);
        yield* select("ledger:renew:after");
        expectInjectedFailure(yield* renewOnce.pipe(Effect.exit), "ledger:renew:after");
        const leaseAfterRenew = (yield* ownershipRows)[0]?.lease_expires_at;

        expect(leaseAfterRenew).not.toBe(leaseBeforeRenew);
        yield* select(undefined);
        yield* renewOnce;

        // The durable reservation leaves the submission terminalizing for recovery.
        const reservation = yield* settlementReservation(
          admitted,
          claim.value.ownershipToken,
          "completed",
        ).pipe(Effect.provide(NodeCrypto.layer));

        const reserveOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.reserveSettlement(reservation);
          }),
        );

        yield* select("ledger:reserve-settlement:after");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:reserve-settlement:after",
        );
        const reservedRows = yield* reservationRows;

        expect(reservedRows).toHaveLength(1);
        expect(reservedRows[0]?.finalized_at).toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("terminalizing");
        yield* select(undefined);
        const replayedReservation = yield* reserveOnce;

        expect(replayedReservation.replayed).toBe(true);

        // Retrying finalization replays the recorded settlement.
        const finalizeOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.finalizeSettlement(
              SettlementFinalization.make({
                submissionId: admitted.submissionId,
                settlementId: reservation.settlementId,
              }),
            );
          }),
        );

        yield* select("ledger:finalize-settlement:after");
        expectInjectedFailure(
          yield* finalizeOnce.pipe(Effect.exit),
          "ledger:finalize-settlement:after",
        );
        expect((yield* reservationRows)[0]?.finalized_at).not.toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("settled");
        expect(yield* ownershipRows).toEqual([]);
        yield* select(undefined);
        const settlement = yield* finalizeOnce;

        expect(settlement.outcome).toBe("completed");

        // Retrying the committed abort intent returns it unchanged.
        const abortLane = "thread-failpoints-abort";

        yield* select(undefined);

        const abortAdmitted = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const result = yield* ledger.admit(
              yield* admission(abortLane, "failpoint-abort-key", { work: "abort" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: result.submissionId }));

            return result;
          }),
        );

        const abortOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.requestAbort(
              AbortCommand.make({
                submissionId: abortAdmitted.submissionId,
                author: "failpoint-operator",
                reason: "failpoint abort",
              }),
            );
          }),
        );

        yield* select("ledger:request-abort:after");
        expectInjectedFailure(yield* abortOnce.pipe(Effect.exit), "ledger:request-abort:after");
        const abortIntents = yield* abortRows;

        expect(abortIntents).toHaveLength(1);
        yield* select(undefined);
        const intent = yield* abortOnce;

        expect(intent.reason).toBe("failpoint abort");

        // The committed release makes a retry observe OwnershipLost.
        const abortClaim = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.claim(
              ClaimRequest.make({
                threadId: thread(abortLane),
                producerId: OTHER_PRODUCER,
              }),
            );
          }),
        );

        expect(Option.isSome(abortClaim)).toBe(true);
        if (Option.isNone(abortClaim)) return;

        const releaseOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.releaseOwnership(
              ReleaseOwnershipRequest.make({
                submissionId: abortAdmitted.submissionId,
                ownershipToken: abortClaim.value.ownershipToken,
              }),
            );
          }),
        );

        yield* select("ledger:release:after");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:release:after");
        expect(yield* ownershipRows).toEqual([]);
        expect(
          (yield* submissionStates).find((row) => row.submission_id === abortAdmitted.submissionId)
            ?.state,
        ).toBe("ready");
        yield* select(undefined);
        const retriedRelease = yield* releaseOnce.pipe(Effect.exit);

        expect(Exit.isFailure(retriedRelease)).toBe(true);
        if (Exit.isFailure(retriedRelease)) {
          expect(Cause.squash(retriedRelease.cause)).toBeInstanceOf(OwnershipLost);
        }
      }),
    ),
  );

  it.effect("recovers join, approval and unknown-operation evidence", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const { select, failingLedger } = yield* makeFailpointHarness(url);

        const submissionMarkers = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT
                submission_id,
                state,
                joined_host_submission_id,
                input_applied_record_id,
                suspended_reason_json,
                unknown_reason,
                unknown_tool_call_ids_json
              FROM effect_agent_submissions
              ORDER BY thread_id, queue_sequence
            `;
          }),
        );

        const ownershipRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, ownership_token
              FROM effect_agent_submission_ownership
            `;
          }),
        );

        const approvalRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, tool_call_id, decision, decided_at
              FROM effect_agent_approval_decisions
              ORDER BY tool_call_id
            `;
          }),
        );

        const resolutionRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, tool_call_id, resolution_json
              FROM effect_agent_unknown_resolutions
              ORDER BY tool_call_id
            `;
          }),
        );

        const markerFor = (rows: ReadonlyArray<Record<string, unknown>>, submissionId: string) =>
          rows.find((row) => row.submission_id === submissionId);

        const lane = "thread-p5-failpoints";

        const { host, hostClaim, queued, queuedSecond } = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const host = yield* ledger.admit(
              yield* admission(lane, "p5-host-key", { work: "host" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: host.submissionId }));

            const queued = yield* ledger.admit(
              yield* admission(lane, "p5-queued-key", { queued: 2 }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: queued.submissionId }));

            const queuedSecond = yield* ledger.admit(
              yield* admission(lane, "p5-queued-second-key", { queued: 3 }),
            );

            yield* ledger.markReady(
              MarkReadyRequest.make({ submissionId: queuedSecond.submissionId }),
            );

            const claim = yield* ledger.claim(
              ClaimRequest.make({ threadId: thread(lane), producerId: TEST_PRODUCER }),
            );

            if (Option.isNone(claim)) return yield* Effect.die("missing host claim");

            return { host, hostClaim: claim.value, queued, queuedSecond };
          }),
        );

        // Recovery can identify the durable joining state without canonical input.
        const claimJoiningOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.claimJoining(
              ClaimJoiningRequest.make({
                threadId: thread(lane),
                hostSubmissionId: host.submissionId,
                ownershipToken: hostClaim.ownershipToken,
                maxCount: 1,
              }),
            );
          }),
        );

        yield* select("ledger:claim-joining:after");
        expectInjectedFailure(
          yield* claimJoiningOnce.pipe(Effect.exit),
          "ledger:claim-joining:after",
        );
        const joiningMarker = markerFor(yield* submissionMarkers, queued.submissionId);

        expect(joiningMarker?.state).toBe("joining");
        expect(joiningMarker?.joined_host_submission_id).toBe(host.submissionId);
        expect(joiningMarker?.input_applied_record_id).toBeNull();
        yield* select(undefined);
        const secondClaims = yield* claimJoiningOnce;

        expect(secondClaims.map((claim) => claim.submissionId)).toEqual([
          queuedSecond.submissionId,
        ]);

        // The committed join marker makes retry an idempotent no-op.
        const markJoinedOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.markJoined(
              MarkJoinedRequest.make({
                submissionId: queued.submissionId,
                ownershipToken: hostClaim.ownershipToken,
                recordId: submissionInputRecordId(queued.submissionId),
                sequence: sequence(2),
              }),
            );
          }),
        );

        yield* select("ledger:mark-joined:after");
        expectInjectedFailure(yield* markJoinedOnce.pipe(Effect.exit), "ledger:mark-joined:after");
        const joinedMarker = markerFor(yield* submissionMarkers, queued.submissionId);

        expect(joinedMarker?.state).toBe("joined");
        expect(joinedMarker?.input_applied_record_id).toBe(
          submissionInputRecordId(queued.submissionId),
        );
        yield* select(undefined);
        yield* markJoinedOnce;

        // The committed revert clears the linkage; retry is a no-op.
        const revertOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.revertJoining(
              RevertJoiningRequest.make({ submissionId: queuedSecond.submissionId }),
            );
          }),
        );

        yield* select("ledger:revert-joining:after");
        expectInjectedFailure(yield* revertOnce.pipe(Effect.exit), "ledger:revert-joining:after");
        const revertedMarker = markerFor(yield* submissionMarkers, queuedSecond.submissionId);

        expect(revertedMarker?.state).toBe("ready");
        expect(revertedMarker?.joined_host_submission_id).toBeNull();
        yield* select(undefined);
        yield* revertOnce;

        // Retrying the committed approval decision preserves the recorded intent.
        const decideOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: host.submissionId,
                toolCallId: toolCall("call-fp-a"),
                decision: "approved",
                resolver: "failpoint-approver",
                reason: "failpoint decision",
              }),
            );
          }),
        );

        yield* select("ledger:approval-decision:after");
        expectInjectedFailure(
          yield* decideOnce.pipe(Effect.exit),
          "ledger:approval-decision:after",
        );
        const decidedRows = yield* approvalRows;

        expect(decidedRows).toHaveLength(1);
        yield* select(undefined);
        const replayedIntent = yield* decideOnce;

        expect(replayedIntent.decision).toBe("approved");
        expect(yield* approvalRows).toHaveLength(1);

        // The durable suspension ends ownership; retry observes OwnershipLost.
        const suspendOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.suspend(
              SuspendRequest.make({
                submissionId: host.submissionId,
                ownershipToken: hostClaim.ownershipToken,
                reason: ApprovalPendingSuspension.make({ toolCallIds: [toolCall("call-fp-b")] }),
              }),
            );
          }),
        );

        yield* select("ledger:suspend:after");
        expectInjectedFailure(yield* suspendOnce.pipe(Effect.exit), "ledger:suspend:after");
        const suspendedMarker = markerFor(yield* submissionMarkers, host.submissionId);

        expect(suspendedMarker?.state).toBe("suspended");
        expect(suspendedMarker?.suspended_reason_json).not.toBeNull();
        expect(yield* ownershipRows).toEqual([]);
        yield* select(undefined);
        const retriedSuspend = yield* suspendOnce.pipe(Effect.exit);

        expect(Exit.isFailure(retriedSuspend)).toBe(true);
        if (Exit.isFailure(retriedSuspend)) {
          expect(Cause.squash(retriedSuspend.cause)).toBeInstanceOf(OwnershipLost);
        }

        // Wake the lane and reclaim it for the unknown-outcome failpoints.
        yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: host.submissionId,
                toolCallId: toolCall("call-fp-b"),
                decision: "approved",
                resolver: "failpoint-approver",
                reason: "wake the suspended lane",
              }),
            );
          }),
        );
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe("input-applied");

        // The unknown marker survives reopen; retry is a no-op.
        const markUnknownOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.markUnknown(
              MarkUnknownRequest.make({
                submissionId: host.submissionId,
                toolCallIds: [toolCall("call-fp-c"), toolCall("call-fp-d")],
                reason: "failpoint uncertainty",
              }),
            );
          }),
        );

        yield* select("ledger:mark-unknown:after");
        expectInjectedFailure(
          yield* markUnknownOnce.pipe(Effect.exit),
          "ledger:mark-unknown:after",
        );
        const unknownMarker = markerFor(yield* submissionMarkers, host.submissionId);

        expect(unknownMarker?.state).toBe("unknown");
        expect(unknownMarker?.unknown_reason).toBe("failpoint uncertainty");
        expect(unknownMarker?.unknown_tool_call_ids_json).not.toBeNull();
        yield* select(undefined);
        yield* markUnknownOnce;

        // Resolution intent and its wake transition must commit atomically.
        const resolveOnce = (call: string, resolution: "never" | "completed") =>
          failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              return yield* ledger.recordUnknownResolution(
                UnknownResolutionCommand.make({
                  submissionId: host.submissionId,
                  toolCallId: toolCall(call),
                  author: "failpoint-operator",
                  reason: "failpoint resolution",
                  resolution:
                    resolution === "never"
                      ? ResolutionNeverHappened.make()
                      : ResolutionCompletedWithResult.make({
                          result: { bookingRef: "booking-fp-1" },
                          isFailure: false,
                        }),
                }),
              );
            }),
          );

        yield* select("ledger:unknown-resolution:after");
        expectInjectedFailure(
          yield* resolveOnce("call-fp-c", "never").pipe(Effect.exit),
          "ledger:unknown-resolution:after",
        );
        expect(yield* resolutionRows).toHaveLength(1);
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe("unknown");
        yield* select(undefined);
        yield* resolveOnce("call-fp-c", "never");

        yield* select("ledger:unknown-resolution:after");
        expectInjectedFailure(
          yield* resolveOnce("call-fp-d", "completed").pipe(Effect.exit),
          "ledger:unknown-resolution:after",
        );
        // The covering resolution and its wake transition are one atomic durable step.
        expect(yield* resolutionRows).toHaveLength(2);
        const wokenMarker = markerFor(yield* submissionMarkers, host.submissionId);

        expect(wokenMarker?.state).toBe("input-applied");
        expect(wokenMarker?.unknown_reason).toBeNull();
        expect(wokenMarker?.unknown_tool_call_ids_json).toBeNull();
        yield* select(undefined);
        const replayedResolution = yield* resolveOnce("call-fp-d", "completed");

        expect(replayedResolution.resolution._tag).toBe("CompletedWithResult");
        expect(yield* resolutionRows).toHaveLength(2);
      }),
    ),
  );

  it.effect("preserves child accounting and wake identity after lost acknowledgements", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const { select, failingLedger } = yield* makeFailpointHarness(url);

        const reservationRows = withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT
                reservation_id,
                status,
                child_submission_id,
                accounting_json,
                release_began_at,
                released_at
              FROM effect_agent_child_reservations
            `;
          }),
        );

        const parentMarkers = (submissionId: string) =>
          withSql(
            url,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              return yield* sql<Record<string, unknown>>`
                SELECT state, suspended_reason_json
                FROM effect_agent_submissions
                WHERE submission_id = ${submissionId}
              `;
            }),
          );

        const parentLane = "thread-s2-failpoints";
        const childLane = "thread-s2-failpoints-child";
        const reservationId = S2_FAILPOINT_RESERVATION;
        const delegationCall = toolCall("call-s2-fp");

        const { child, parent, parentClaim } = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const parent = yield* ledger.admit(
              yield* admission(parentLane, "s2-fp-parent-key", { work: "parent" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: parent.submissionId }));

            const child = yield* ledger.admit(
              yield* admission(childLane, "s2-fp-child-key", { task: "child" }),
            );

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: child.submissionId }));

            const claim = yield* ledger.claim(
              ClaimRequest.make({
                threadId: thread(parentLane),
                producerId: TEST_PRODUCER,
              }),
            );

            if (Option.isNone(claim)) return yield* Effect.die("missing parent claim");

            return { parent, child, parentClaim: claim.value };
          }),
        );

        // Retrying the committed child reservation preserves its identity.
        const allocation = { turns: 2 };

        const allocationDigest = yield* digestJson(allocation).pipe(
          Effect.provide(NodeCrypto.layer),
        );

        const reserveOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.reserveChildBudget(
              ChildBudgetReservationRequest.make({
                reservationId,
                parentSubmissionId: parent.submissionId,
                parentToolCallId: delegationCall,
                ownershipToken: parentClaim.ownershipToken,
                allocation,
                allocationDigest,
              }),
            );
          }),
        );

        yield* select("ledger:child-reservation:after");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:child-reservation:after",
        );
        const reservedRows = yield* reservationRows;

        expect(reservedRows).toHaveLength(1);
        expect(reservedRows[0]?.status).toBe("reserved");
        expect(reservedRows[0]?.child_submission_id).toBeNull();
        yield* select(undefined);
        const replayedReserve = yield* reserveOnce;

        expect(replayedReserve.replayed).toBe(true);

        // The child attachment survives reopen; retry is a no-op.
        const attachOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.attachChildToReservation(
              AttachChildToReservationRequest.make({
                reservationId,
                ownershipToken: parentClaim.ownershipToken,
                childSubmissionId: child.submissionId,
              }),
            );
          }),
        );

        yield* select("ledger:child-attach:after");
        expectInjectedFailure(yield* attachOnce.pipe(Effect.exit), "ledger:child-attach:after");
        expect((yield* reservationRows)[0]?.child_submission_id).toBe(child.submissionId);
        yield* select(undefined);
        const replayedAttach = yield* attachOnce;

        expect(replayedAttach.childSubmissionId).toBe(child.submissionId);

        // The pending release preserves its frozen accounting across retry.
        const accounting = { consumed: { turns: 1 }, released: { turns: 1 } };

        const beginOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.beginChildBudgetRelease(
              BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
            );
          }),
        );

        yield* select("ledger:child-release-pending:after");
        expectInjectedFailure(
          yield* beginOnce.pipe(Effect.exit),
          "ledger:child-release-pending:after",
        );
        const frozenRows = yield* reservationRows;

        expect(frozenRows[0]?.status).toBe("releasePending");
        expect(frozenRows[0]?.accounting_json).not.toBeNull();
        yield* select(undefined);
        const replayedBegin = yield* beginOnce;

        expect(replayedBegin.status).toBe("releasePending");

        // Retry replays the released row; the unused allocation never returns twice.
        const releaseOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.releaseChildBudget(
              ReleaseChildBudgetRequest.make({ reservationId }),
            );
          }),
        );

        yield* select("ledger:child-release:after");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:child-release:after");
        expect((yield* reservationRows)[0]?.status).toBe("released");
        expect((yield* reservationRows)[0]?.released_at).not.toBeNull();
        yield* select(undefined);
        const replayedRelease = yield* releaseOnce;

        expect(replayedRelease.status).toBe("released");

        // The committed wake transition makes retry answer not-waiting.
        yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const suspended = yield* ledger.suspend(
              SuspendRequest.make({
                submissionId: parent.submissionId,
                ownershipToken: parentClaim.ownershipToken,
                reason: WaitingForChildSuspension.make({
                  children: [
                    WaitingChild.make({
                      toolCallId: delegationCall,
                      childSubmissionId: child.submissionId,
                    }),
                  ],
                }),
              }),
            );

            expect(suspended).toBe("suspended");

            const childClaim = yield* ledger.claim(
              ClaimRequest.make({
                threadId: thread(childLane),
                producerId: TEST_PRODUCER,
              }),
            );

            if (Option.isNone(childClaim)) return yield* Effect.die("missing child claim");

            const reservation = yield* settlementReservation(
              child,
              childClaim.value.ownershipToken,
              "completed",
            );

            yield* ledger.reserveSettlement(reservation);
            yield* ledger.finalizeSettlement(
              SettlementFinalization.make({
                submissionId: child.submissionId,
                settlementId: reservation.settlementId,
              }),
            );
          }),
        );

        const notifyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.recordChildSettled(
              ChildSettledNotification.make({
                parentSubmissionId: parent.submissionId,
                childSubmissionId: child.submissionId,
              }),
            );
          }),
        );

        yield* select("ledger:child-settled:after");
        expectInjectedFailure(yield* notifyOnce.pipe(Effect.exit), "ledger:child-settled:after");
        const wokenMarkers = yield* parentMarkers(parent.submissionId);

        expect(wokenMarkers[0]?.state).toBe("input-applied");
        expect(wokenMarkers[0]?.suspended_reason_json).toBeNull();
        yield* select(undefined);
        const replayedNotification = yield* notifyOnce;

        expect(replayedNotification).toBe("not-waiting");
      }),
    ),
  );
});
