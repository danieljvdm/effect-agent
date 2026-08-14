import {
  AbortCommand,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimJoiningRequest,
  ClaimRequest,
  digestJson,
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  ReleaseChildBudgetRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  RevertJoiningRequest,
  SettlementFinalization,
  SubmissionLedger,
  SuspendRequest,
  UnknownResolutionCommand,
  WaitingChild,
  WaitingForChildSuspension,
  submissionInputRecordId,
} from "@effect-agent/session";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Cause, Crypto, Effect, Exit, Option, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
  DoStorageFailpointError,
  ledgerLayer,
  type DoStorageFailpointLocation,
} from "../src/index.ts";
import {
  admission,
  conversation,
  OTHER_PRODUCER,
  sequence,
  settlementReservation,
  TEST_PRODUCER,
  toolCall,
  withConversationStorage,
} from "./harness.ts";

/**
 * The DC crash matrix at the storage level: every ledger failpoint location keeps the SAME
 * name as the Node/SQLite adapter's (`SqliteStorageFailpointLocation` copied verbatim), and
 * every row asserts the same durable-state/recovery pair — before → nothing durable to
 * repair; after → the mutation is durable even though the caller never observed it, and the
 * retry converges idempotently. These are the exact rows WP3's eviction harness re-runs with
 * `ctx.abort()` instead of a typed failure.
 */

const S2_FAILPOINT_RESERVATION = Schema.decodeSync(ChildReservationId)(
  "child-reservation:run-s2fp:call-1",
);

const expectInjectedFailure = <A>(
  exit: Exit.Exit<A, unknown>,
  location: DoStorageFailpointLocation,
) => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isFailure(exit)) {
    const error = Cause.squash(exit.cause);
    expect(error).toBeInstanceOf(LedgerError);
    if (error instanceof LedgerError) {
      expect(error.cause).toBeInstanceOf(DoStorageFailpointError);
      if (error.cause instanceof DoStorageFailpointError) {
        expect(error.cause.location).toBe(location);
      }
    }
  }
};

const makeFailpointHarness = (storage: DurableObjectStorage) =>
  Effect.gen(function* () {
    const active = yield* Ref.make<DoStorageFailpointLocation | undefined>(undefined);
    const select = (location: DoStorageFailpointLocation | undefined) => Ref.set(active, location);
    const failingLedger = <A, E>(effect: Effect.Effect<A, E, SubmissionLedger | Crypto.Crypto>) =>
      Effect.provide(effect, [
        ledgerLayer({
          storage,
          failpoint: (location) =>
            Ref.get(active).pipe(
              Effect.flatMap((selected) =>
                selected === location
                  ? Effect.fail(DoStorageFailpointError.make({ location }))
                  : Effect.void,
              ),
            ),
        }),
        BrowserCrypto.layer,
      ]);
    const withSql = <A, E>(effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
      Effect.provide(effect, SqliteClient.layer({ storage }));
    return { select, failingLedger, withSql } as const;
  });

describe("DoSubmissionLedger failpoints", () => {
  it("leaves a recovery-classifiable state at every ledger failpoint", () =>
    withConversationStorage("wp1-failpoints-base", (storage) =>
      Effect.gen(function* () {
        const { select, failingLedger, withSql } = yield* makeFailpointHarness(storage);
        const submissionStates = withSql(
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, state, receipt_id, input_applied_record_id
              FROM effect_agent_submissions
              ORDER BY conversation_id, queue_sequence
            `;
          }),
        );
        const ownershipRows = withSql(
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, attempt_id, ownership_token, producer_epoch, lease_expires_at
              FROM effect_agent_submission_ownership
            `;
          }),
        );
        const attemptRows = withSql(
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
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, settlement_id, outcome, finalized_at
              FROM effect_agent_settlement_reservations
            `;
          }),
        );
        const abortRows = withSql(
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, reason, requested_at
              FROM effect_agent_abort_intents
            `;
          }),
        );
        const conversationRows = withSql(
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT conversation_id, producer_epoch
              FROM effect_agent_conversations
            `;
          }),
        );

        const lane = "conversation-failpoints";
        const admitOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.admit(yield* admission(lane, "failpoint-key", { work: "fail" }));
          }),
        );

        // admit: before → nothing durable; after → row durable, retry replays the same identity.
        yield* select("ledger:admit:before");
        expectInjectedFailure(yield* admitOnce.pipe(Effect.exit), "ledger:admit:before");
        expect(yield* submissionStates).toEqual([]);
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

        // markReady: before → still admitted; after → ready durable, retry is a no-op.
        const markReadyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          }),
        );
        yield* select("ledger:mark-ready:before");
        expectInjectedFailure(yield* markReadyOnce.pipe(Effect.exit), "ledger:mark-ready:before");
        expect((yield* submissionStates)[0]?.state).toBe("admitted");
        yield* select("ledger:mark-ready:after");
        expectInjectedFailure(yield* markReadyOnce.pipe(Effect.exit), "ledger:mark-ready:after");
        expect((yield* submissionStates)[0]?.state).toBe("ready");
        yield* select(undefined);
        yield* markReadyOnce;

        // claim: before → no ownership, no conversation row, no epoch consumed; after → the
        // claim is durable (ownership + audit + epoch bump) even though the caller never saw it.
        const claimOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.claim(
              ClaimRequest.make({ conversationId: conversation(lane), producerId: TEST_PRODUCER }),
            );
          }),
        );
        yield* select("ledger:claim:before");
        expectInjectedFailure(yield* claimOnce.pipe(Effect.exit), "ledger:claim:before");
        expect(yield* ownershipRows).toEqual([]);
        expect(yield* conversationRows).toEqual([]);
        yield* select("ledger:claim:after");
        expectInjectedFailure(yield* claimOnce.pipe(Effect.exit), "ledger:claim:after");
        const orphanedOwnership = yield* ownershipRows;
        expect(orphanedOwnership).toHaveLength(1);
        expect(orphanedOwnership[0]?.producer_epoch).toBe(1);
        expect(yield* conversationRows).toEqual([{ conversation_id: lane, producer_epoch: 1 }]);
        expect((yield* submissionStates)[0]?.state).toBe("running");
        expect(yield* attemptRows).toHaveLength(1);
        // The orphaned lease blocks until expiry; a later Attempt (in DC: the next
        // incarnation's alarm pass) reclaims at a higher epoch.
        yield* select(undefined);
        expect(Option.isNone(yield* claimOnce)).toBe(true);
        yield* TestClock.adjust(30_001);
        const claim = yield* claimOnce;
        expect(Option.isSome(claim)).toBe(true);
        if (Option.isNone(claim)) return;
        expect(claim.value.producerEpoch).toBe(2);
        expect(yield* attemptRows).toHaveLength(2);

        // markInputApplied: before → no marker; after → marker durable, retry is a no-op.
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
        yield* select("ledger:mark-input-applied:before");
        expectInjectedFailure(
          yield* markInputOnce.pipe(Effect.exit),
          "ledger:mark-input-applied:before",
        );
        expect((yield* submissionStates)[0]?.input_applied_record_id).toBeNull();
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

        // renew: before → lease unchanged; after → extension durable.
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
        yield* select("ledger:renew:before");
        expectInjectedFailure(yield* renewOnce.pipe(Effect.exit), "ledger:renew:before");
        expect((yield* ownershipRows)[0]?.lease_expires_at).toBe(leaseBeforeRenew);
        yield* select("ledger:renew:after");
        expectInjectedFailure(yield* renewOnce.pipe(Effect.exit), "ledger:renew:after");
        const leaseAfterRenew = (yield* ownershipRows)[0]?.lease_expires_at;
        expect(leaseAfterRenew).not.toBe(leaseBeforeRenew);
        yield* select(undefined);
        yield* renewOnce;

        // reserveSettlement: before → no reservation, submission nonterminal; after → the
        // reservation row is durable and the submission is terminalizing but NOT settled,
        // exactly the state a recovery pass classifies as append-then-finalize.
        const reservation = yield* settlementReservation(
          admitted,
          claim.value.ownershipToken,
          "completed",
        ).pipe(Effect.provide(BrowserCrypto.layer));
        const reserveOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.reserveSettlement(reservation);
          }),
        );
        yield* select("ledger:reserve-settlement:before");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:reserve-settlement:before",
        );
        expect(yield* reservationRows).toEqual([]);
        expect((yield* submissionStates)[0]?.state).toBe("input-applied");
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

        // finalizeSettlement: before → reservation unfinalized, still terminalizing; after →
        // settled durably; the retry replays the recorded Settlement.
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
        yield* select("ledger:finalize-settlement:before");
        expectInjectedFailure(
          yield* finalizeOnce.pipe(Effect.exit),
          "ledger:finalize-settlement:before",
        );
        expect((yield* reservationRows)[0]?.finalized_at).toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("terminalizing");
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

        // requestAbort: before → no intent; after → intent durable, retry returns it unchanged.
        const abortLane = "conversation-failpoints-abort";
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
        yield* select("ledger:request-abort:before");
        expectInjectedFailure(yield* abortOnce.pipe(Effect.exit), "ledger:request-abort:before");
        expect(yield* abortRows).toEqual([]);
        yield* select("ledger:request-abort:after");
        expectInjectedFailure(yield* abortOnce.pipe(Effect.exit), "ledger:request-abort:after");
        const abortIntents = yield* abortRows;
        expect(abortIntents).toHaveLength(1);
        yield* select(undefined);
        const intent = yield* abortOnce;
        expect(intent.reason).toBe("failpoint abort");

        // release: before → ownership retained; after → ownership released durably, the retry
        // observes OwnershipLost exactly as a recovering caller would.
        const abortClaim = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation(abortLane),
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
        yield* select("ledger:release:before");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:release:before");
        expect(yield* ownershipRows).toHaveLength(1);
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
    ));

  it("leaves a recovery-classifiable state at every Phase 5 ledger failpoint", () =>
    withConversationStorage("wp1-failpoints-p5", (storage) =>
      Effect.gen(function* () {
        const { select, failingLedger, withSql } = yield* makeFailpointHarness(storage);
        const submissionMarkers = withSql(
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
              ORDER BY conversation_id, queue_sequence
            `;
          }),
        );
        const ownershipRows = withSql(
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, ownership_token
              FROM effect_agent_submission_ownership
            `;
          }),
        );
        const approvalRows = withSql(
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

        const lane = "conversation-p5-failpoints";
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
              ClaimRequest.make({ conversationId: conversation(lane), producerId: TEST_PRODUCER }),
            );
            if (Option.isNone(claim)) return yield* Effect.die("missing host claim");
            return { host, hostClaim: claim.value, queued, queuedSecond };
          }),
        );

        // claimJoining: before → nothing claimed; after → the joining transition and host
        // linkage are durable even though the caller never saw the claims (recovery sees a
        // joining Submission without canonical input → RevertJoining).
        const claimJoiningOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.claimJoining(
              ClaimJoiningRequest.make({
                conversationId: conversation(lane),
                hostSubmissionId: host.submissionId,
                ownershipToken: hostClaim.ownershipToken,
                maxCount: 1,
              }),
            );
          }),
        );
        yield* select("ledger:claim-joining:before");
        expectInjectedFailure(
          yield* claimJoiningOnce.pipe(Effect.exit),
          "ledger:claim-joining:before",
        );
        expect(markerFor(yield* submissionMarkers, queued.submissionId)?.state).toBe("ready");
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

        // markJoined: before → still joining without a marker; after → joined durably;
        // the retry is an idempotent no-op.
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
        yield* select("ledger:mark-joined:before");
        expectInjectedFailure(yield* markJoinedOnce.pipe(Effect.exit), "ledger:mark-joined:before");
        expect(
          markerFor(yield* submissionMarkers, queued.submissionId)?.input_applied_record_id,
        ).toBeNull();
        yield* select("ledger:mark-joined:after");
        expectInjectedFailure(yield* markJoinedOnce.pipe(Effect.exit), "ledger:mark-joined:after");
        const joinedMarker = markerFor(yield* submissionMarkers, queued.submissionId);
        expect(joinedMarker?.state).toBe("joined");
        expect(joinedMarker?.input_applied_record_id).toBe(
          submissionInputRecordId(queued.submissionId),
        );
        yield* select(undefined);
        yield* markJoinedOnce;

        // revertJoining: before → still joining; after → ready with the linkage cleared;
        // the retry is an idempotent no-op.
        const revertOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.revertJoining(
              RevertJoiningRequest.make({ submissionId: queuedSecond.submissionId }),
            );
          }),
        );
        yield* select("ledger:revert-joining:before");
        expectInjectedFailure(yield* revertOnce.pipe(Effect.exit), "ledger:revert-joining:before");
        expect(markerFor(yield* submissionMarkers, queuedSecond.submissionId)?.state).toBe(
          "joining",
        );
        yield* select("ledger:revert-joining:after");
        expectInjectedFailure(yield* revertOnce.pipe(Effect.exit), "ledger:revert-joining:after");
        const revertedMarker = markerFor(yield* submissionMarkers, queuedSecond.submissionId);
        expect(revertedMarker?.state).toBe("ready");
        expect(revertedMarker?.joined_host_submission_id).toBeNull();
        yield* select(undefined);
        yield* revertOnce;

        // recordApprovalDecision: before → no intent; after → intent durable; the retry
        // replays the recorded intent unchanged.
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
        yield* select("ledger:approval-decision:before");
        expectInjectedFailure(
          yield* decideOnce.pipe(Effect.exit),
          "ledger:approval-decision:before",
        );
        expect(yield* approvalRows).toEqual([]);
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

        // suspend: before → ownership retained, no suspension; after → suspended durably
        // with the ownership period ended; the retry observes OwnershipLost exactly as a
        // recovering caller would.
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
        yield* select("ledger:suspend:before");
        expectInjectedFailure(yield* suspendOnce.pipe(Effect.exit), "ledger:suspend:before");
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe("running");
        expect(yield* ownershipRows).toHaveLength(1);
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

        // markUnknown: before → state unchanged; after → the unknown mark is durable; the
        // retry is an idempotent no-op.
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
        yield* select("ledger:mark-unknown:before");
        expectInjectedFailure(
          yield* markUnknownOnce.pipe(Effect.exit),
          "ledger:mark-unknown:before",
        );
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.unknown_reason).toBeNull();
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

        // recordUnknownResolution: before → no intent; after → the intent is durable while
        // the lane stays blocked; the covering resolution's wake transition commits
        // atomically with its intent.
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
        yield* select("ledger:unknown-resolution:before");
        expectInjectedFailure(
          yield* resolveOnce("call-fp-c", "never").pipe(Effect.exit),
          "ledger:unknown-resolution:before",
        );
        expect(yield* resolutionRows).toEqual([]);
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
    ));

  it("leaves a recovery-classifiable state at every S2 ledger failpoint", () =>
    withConversationStorage("wp1-failpoints-s2", (storage) =>
      Effect.gen(function* () {
        const { select, failingLedger, withSql } = yield* makeFailpointHarness(storage);
        const reservationRows = withSql(
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
        const settlementMarkers = withSql(
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT parent_submission_id, child_submission_id, child_outcome
              FROM effect_agent_child_settlements
            `;
          }),
        );
        const parentMarkers = (submissionId: string) =>
          withSql(
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;
              return yield* sql<Record<string, unknown>>`
                SELECT state, suspended_reason_json
                FROM effect_agent_submissions
                WHERE submission_id = ${submissionId}
              `;
            }),
          );

        const parentLane = "conversation-s2-failpoints";
        const childLane = "conversation-s2-failpoints-child";
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
                conversationId: conversation(parentLane),
                producerId: TEST_PRODUCER,
              }),
            );
            if (Option.isNone(claim)) return yield* Effect.die("missing parent claim");
            return { parent, child, parentClaim: claim.value };
          }),
        );

        // reserveChildBudget: before → no row, nothing to repair; after → the reservation is
        // durable ('reserved') even though the caller never saw it; the retry replays it.
        const allocation = { turns: 2 };
        const allocationDigest = yield* digestJson(allocation).pipe(
          Effect.provide(BrowserCrypto.layer),
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
        yield* select("ledger:child-reservation:before");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:child-reservation:before",
        );
        expect(yield* reservationRows).toEqual([]);
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

        // attachChildToReservation: before → no child recorded; after → the attachment is
        // durable; the retry is an idempotent no-op.
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
        yield* select("ledger:child-attach:before");
        expectInjectedFailure(yield* attachOnce.pipe(Effect.exit), "ledger:child-attach:before");
        expect((yield* reservationRows)[0]?.child_submission_id).toBeNull();
        yield* select("ledger:child-attach:after");
        expectInjectedFailure(yield* attachOnce.pipe(Effect.exit), "ledger:child-attach:after");
        expect((yield* reservationRows)[0]?.child_submission_id).toBe(child.submissionId);
        yield* select(undefined);
        const replayedAttach = yield* attachOnce;
        expect(replayedAttach.childSubmissionId).toBe(child.submissionId);

        // beginChildBudgetRelease: before → status reserved with no frozen accounting; after →
        // releasePending with the frozen decision; the retry is an idempotent no-op.
        const accounting = { consumed: { turns: 1 }, released: { turns: 1 } };
        const beginOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.beginChildBudgetRelease(
              BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
            );
          }),
        );
        yield* select("ledger:child-release-pending:before");
        expectInjectedFailure(
          yield* beginOnce.pipe(Effect.exit),
          "ledger:child-release-pending:before",
        );
        expect((yield* reservationRows)[0]?.status).toBe("reserved");
        expect((yield* reservationRows)[0]?.accounting_json).toBeNull();
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

        // releaseChildBudget: before → still releasePending; after → released durably; the
        // retry replays the released row: the unused allocation never returns twice.
        const releaseOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.releaseChildBudget(
              ReleaseChildBudgetRequest.make({ reservationId }),
            );
          }),
        );
        yield* select("ledger:child-release:before");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:child-release:before");
        expect((yield* reservationRows)[0]?.status).toBe("releasePending");
        expect((yield* reservationRows)[0]?.released_at).toBeNull();
        yield* select("ledger:child-release:after");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:child-release:after");
        expect((yield* reservationRows)[0]?.status).toBe("released");
        expect((yield* reservationRows)[0]?.released_at).not.toBeNull();
        yield* select(undefined);
        const replayedRelease = yield* releaseOnce;
        expect(replayedRelease.status).toBe("released");

        // recordChildSettled: before → the parent stays suspended AND no notification marker
        // exists; after → the wake transition and the durable cross-store marker are one
        // atomic durable step even though the caller never saw it; the retry answers
        // not-waiting.
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
                conversationId: conversation(childLane),
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
        yield* select("ledger:child-settled:before");
        expectInjectedFailure(yield* notifyOnce.pipe(Effect.exit), "ledger:child-settled:before");
        expect((yield* parentMarkers(parent.submissionId))[0]?.state).toBe("suspended");
        expect(yield* settlementMarkers).toEqual([]);
        yield* select("ledger:child-settled:after");
        expectInjectedFailure(yield* notifyOnce.pipe(Effect.exit), "ledger:child-settled:after");
        const wokenMarkers = yield* parentMarkers(parent.submissionId);
        expect(wokenMarkers[0]?.state).toBe("input-applied");
        expect(wokenMarkers[0]?.suspended_reason_json).toBeNull();
        const recordedMarkers = yield* settlementMarkers;
        expect(recordedMarkers).toHaveLength(1);
        expect(recordedMarkers[0]?.parent_submission_id).toBe(parent.submissionId);
        expect(recordedMarkers[0]?.child_submission_id).toBe(child.submissionId);
        expect(recordedMarkers[0]?.child_outcome).toBe("completed");
        yield* select(undefined);
        const replayedNotification = yield* notifyOnce;
        expect(replayedNotification).toBe("not-waiting");
      }),
    ));
});
