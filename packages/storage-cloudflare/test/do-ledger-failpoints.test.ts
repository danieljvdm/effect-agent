import {
  DoStorageFailpointError,
  type DoStorageFailpointLocation,
} from "@effect-agent/storage-cloudflare/do-storage-error";
import { ledgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Cause, Effect, Exit, Option, Ref, Schema, type Crypto } from "effect";
import { digestJson } from "effect-agent/digest";
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
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  ReleaseChildBudgetRequest,
  ReleaseOwnershipRequest,
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
} from "effect-agent/submission-ledger";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
  admission,
  thread,
  OTHER_PRODUCER,
  sequence,
  settlementReservation,
  TEST_PRODUCER,
  toolCall,
  withThreadStorage,
} from "./harness.ts";

/**
 * Lost acknowledgements at durable ledger boundaries must converge on retry.
 * The separate eviction cases cover host aborts after actual storage writes.
 */

const S2_FAILPOINT_RESERVATION = Schema.decodeSync(ChildReservationId)(
  "child-reservation:run-s2fp:call-1",
);

const isLedgerError = Schema.is(LedgerError);
const isDoStorageFailpointError = Schema.is(DoStorageFailpointError);

const expectInjectedFailure = <A>(
  exit: Exit.Exit<A, unknown>,
  location: DoStorageFailpointLocation,
) => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isFailure(exit)) {
    const error = Cause.squash(exit.cause);

    expect(error).toBeInstanceOf(LedgerError);
    if (isLedgerError(error)) {
      expect(error.cause).toBeInstanceOf(DoStorageFailpointError);
      if (isDoStorageFailpointError(error.cause)) {
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
  it("recovers ledger identity and ownership after lost acknowledgements", () =>
    withThreadStorage("wp1-failpoints-base", (storage) =>
      Effect.gen(function* () {
        const { select, failingLedger, withSql } = yield* makeFailpointHarness(storage);

        const submissionStates = withSql(
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

        const threadRows = withSql(
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

        const markReadyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          }),
        );

        yield* select(undefined);
        yield* markReadyOnce;

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
        expect(orphanedOwnership[0]?.producer_epoch).toBe(1);
        expect(yield* threadRows).toEqual([{ thread_id: lane, producer_epoch: 1 }]);
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

        yield* select(undefined);
        yield* markInputOnce;

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
    ));

  it("preserves join, approval and unknown-operation evidence after lost acknowledgements", () =>
    withThreadStorage("wp1-failpoints-p5", (storage) =>
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
              ORDER BY thread_id, queue_sequence
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

        yield* select(undefined);
        yield* markJoinedOnce;

        const revertOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.revertJoining(
              RevertJoiningRequest.make({ submissionId: queuedSecond.submissionId }),
            );
          }),
        );

        yield* select(undefined);
        yield* revertOnce;

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
    ));

  it("preserves child accounting and wake identity after lost acknowledgements", () =>
    withThreadStorage("wp1-failpoints-s2", (storage) =>
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

        yield* select(undefined);
        const replayedAttach = yield* attachOnce;

        expect(replayedAttach.childSubmissionId).toBe(child.submissionId);

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
