import {
  AbortCommand,
  AdmissionConflict,
  AppendConflict,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  CanonicalSettlementRepair,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimJoiningRequest,
  ClaimRequest,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationNotMaterialized,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  ConversationStoreError,
  ConversationTailRequest,
  digestJson,
  EMPTY_TAIL_DIGEST,
  FenceRejected,
  FencedAppendRequest,
  IdempotencyKey,
  JoinedToHost,
  LedgerError,
  LoadCheckpointRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  RecoverySnapshotRequest,
  ResumeSuspensionRequest,
  ScanConversationNonterminalRequest,
  RenewOwnershipRequest,
  SettlementConflict,
  SettlementFinalization,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  submissionInputRecordId,
  SuspendRequest,
  WaitingChild,
  WaitingForChildSuspension,
  type Claim,
  type PersistedJson,
} from "@effect-agent/session";
import { BrowserCrypto } from "@effect/platform-browser";
import { runInDurableObject } from "cloudflare:test";
import { Crypto, Effect, Layer, Option, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  ConversationPortTransport,
  ledgerLayer,
  layer as storeLayer,
  PortTransportError,
  portTransportFailure,
  routedConversationStoreLayer,
  routedSubmissionLedgerLayer,
} from "../src/index.ts";
import { batch, inputRecord } from "./canonical-fixtures.ts";
import {
  admission,
  conversation,
  conversationStub,
  id,
  sequence,
  epoch,
  settlementReservation,
  TEST_PRINCIPAL,
  TEST_PRODUCER,
  toolCall,
  withConversationStorage,
} from "./harness.ts";

const submissionId = (value: string) => id(MarkReadyRequest.fields.submissionId, value);
const ownershipToken = (value: string) => id(RenewOwnershipRequest.fields.ownershipToken, value);
const idempotencyKey = (value: string) => id(IdempotencyKey, value);

/**
 * Shared mutable control for the test transport: `fault` simulates an unreachable owning
 * Object (workerd overload, deploy-in-progress), `calls` counts every delivery attempt so
 * fail-fast and local-delegation tests can prove the transport was never consulted.
 */
interface TransportControl {
  calls: number;
  fault: string | undefined;
}

const control = (): TransportControl => ({ calls: 0, fault: undefined });

/**
 * The production-shaped test transport (D-P6-3): the Object identity rule is
 * `idFromName(conversationId)`, and the owner's `portCall` RPC method executes the envelope
 * against ITS local facets (see test/worker.ts). Everything thrown by the stub surfaces as
 * `PortTransportError` — the transport never fabricates an answer.
 */
const transportLayer = (state: TransportControl) =>
  Layer.succeed(ConversationPortTransport)({
    call: (conversationId, request) =>
      Effect.suspend(() => {
        state.calls += 1;
        if (state.fault !== undefined) {
          return Effect.fail(
            PortTransportError.make({
              target: conversationId,
              message: state.fault,
              retryable: true,
            }),
          );
        }
        return Effect.tryPromise({
          try: () => conversationStub(conversationId).portCall(request),
          catch: (cause) => portTransportFailure(conversationId, cause),
        });
      }),
  });

/**
 * Run one Effect against the ROUTED port Layers inside the named Conversation's own Durable
 * Object: local facets over this Object's real SQLite storage, wrapped by the WP2 routing
 * decorators over the namespace transport. The Object name IS the local Conversation
 * identity (the DC identity rule), and names are minted uniquely per test because the
 * 0.21.x pool shares Durable Object storage across tests within a run.
 */
const withRoutedPorts = <A, E>(
  objectName: string,
  state: TransportControl,
  build: Effect.Effect<A, E, SubmissionLedger | ConversationStore | Crypto.Crypto>,
): Promise<A> =>
  runInDurableObject(conversationStub(objectName), (_instance, doState) =>
    Effect.runPromise(
      build.pipe(
        Effect.provide(
          Layer.mergeAll(
            routedSubmissionLedgerLayer({ localConversationId: conversation(objectName) }).pipe(
              Layer.provide(
                Layer.mergeAll(ledgerLayer({ storage: doState.storage }), transportLayer(state)),
              ),
            ),
            routedConversationStoreLayer({ localConversationId: conversation(objectName) }).pipe(
              Layer.provide(
                Layer.mergeAll(
                  storeLayer({ storage: doState.storage, observationPollInterval: 1 }),
                  transportLayer(state),
                ),
              ),
            ),
            BrowserCrypto.layer,
          ),
        ),
      ),
    ),
  );

/** Admit, ready, and claim one local lane; returns the admission and the live claim. */
const claimedLocalLane = Effect.fn("RoutingTest.claimedLocalLane")(function* (
  conversationId: string,
  key: string,
  input: PersistedJson,
) {
  const ledger = yield* SubmissionLedger;
  const admitted = yield* ledger.admit(yield* admission(conversationId, key, input));
  yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
  const claimed = yield* ledger.claim(
    ClaimRequest.make({ conversationId: conversation(conversationId), producerId: TEST_PRODUCER }),
  );
  expect(Option.isSome(claimed)).toBe(true);
  const claim: Claim = Option.getOrThrow(claimed);
  return { admitted, claim };
});

describe("cross-DO port routing", () => {
  it("routes the admission subset to the owning Conversation Object with error-tag fidelity", async () => {
    const parentConv = "wp2-admission-parent";
    const childConv = "wp2-admission-child";
    const state = control();

    const established = await withRoutedPorts(
      parentConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const byKey = SubmissionLookupByKey.make({
          conversationId: conversation(childConv),
          principal: TEST_PRINCIPAL,
          idempotencyKey: idempotencyKey("wp2-child-key"),
        });

        // The authoritative owner proves absence before any admission (SUB-031).
        const before = yield* ledger.resolveAdmission(byKey);
        expect(before._tag).toBe("NotAdmitted");

        const request = yield* admission(childConv, "wp2-child-key", { task: "research" });
        const admitted = yield* ledger.admit(request);
        expect(admitted.replayed).toBe(false);
        expect(admitted.state).toBe("admitted");
        // D-P6-5: the minted identity carries its owning Conversation after the first ":".
        expect(admitted.submissionId.endsWith(`:${childConv}`)).toBe(true);

        // An identical routed replay resumes instead of duplicating (DUR-001).
        const replay = yield* ledger.admit(request);
        expect(replay.replayed).toBe(true);
        expect(replay.submissionId).toBe(admitted.submissionId);
        expect(replay.receiptId).toBe(admitted.receiptId);

        // A divergent payload under the same key re-decodes as the SAME tagged conflict the
        // owning Object's local facet raised, fields intact (error-tag fidelity).
        const divergent = yield* admission(childConv, "wp2-child-key", { task: "different" });
        const conflict = yield* ledger.admit(divergent).pipe(Effect.flip);
        expect(conflict).toBeInstanceOf(AdmissionConflict);
        if (conflict instanceof AdmissionConflict) {
          expect(conflict.conversationId).toBe(childConv);
          expect(conflict.existingInputDigest).toBe(request.inputDigest);
          expect(conflict.attemptedInputDigest).toBe(divergent.inputDigest);
        }

        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
        const second = yield* ledger.admit(
          yield* admission(childConv, "wp2-child-key-2", { task: "second" }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: second.submissionId }));

        const byIdRow = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: admitted.submissionId }),
        );
        expect(Option.isSome(byIdRow)).toBe(true);
        if (Option.isSome(byIdRow)) {
          expect(byIdRow.value.state).toBe("ready");
          expect(byIdRow.value.conversationId).toBe(childConv);
        }

        const byKeyRow = yield* ledger.lookup(byKey);
        expect(Option.isSome(byKeyRow)).toBe(true);

        const after = yield* ledger.resolveAdmission(byKey);
        expect(after._tag).toBe("Admitted");
        if (after._tag === "Admitted") {
          expect(after.submission.submissionId).toBe(admitted.submissionId);
        }

        // A missing foreign key still answers through the owner — absence there is proof.
        const unknown = yield* ledger.lookup(
          SubmissionLookupByKey.make({
            conversationId: conversation(childConv),
            principal: TEST_PRINCIPAL,
            idempotencyKey: idempotencyKey("wp2-never-admitted"),
          }),
        );
        expect(Option.isNone(unknown)).toBe(true);

        // This Object's OWN ledger holds nothing: the child row lives in the owning Object.
        const localScan = yield* ledger.scanNonterminal.pipe(Stream.runCollect);
        expect(localScan).toEqual([]);

        const routedScan = yield* ledger
          .scanConversationNonterminal(
            ScanConversationNonterminalRequest.make({ conversationId: conversation(childConv) }),
          )
          .pipe(Stream.runCollect);
        expect(routedScan.map((row) => row.submissionId)).toEqual([
          admitted.submissionId,
          second.submissionId,
        ]);
        const routedSuffix = yield* ledger
          .scanConversationNonterminal(
            ScanConversationNonterminalRequest.make({
              conversationId: conversation(childConv),
              afterQueueSequence: admitted.queueSequence,
            }),
          )
          .pipe(Stream.runCollect);
        expect(routedSuffix.map((row) => row.submissionId)).toEqual([second.submissionId]);

        return admitted;
      }),
    );
    expect(state.calls).toBeGreaterThan(0);

    // Authoritative verification inside the owning Object's local facet.
    await withConversationStorage(childConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const row = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: established.submissionId }),
        );
        expect(Option.isSome(row)).toBe(true);
        if (Option.isSome(row)) {
          expect(row.value.state).toBe("ready");
          expect(row.value.conversationId).toBe(childConv);
          expect(row.value.receiptId).toBe(established.receiptId);
        }
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );
  });

  it("routes requestAbort with SettlementConflict, JoinedToHost, and intent fidelity", async () => {
    const callerConv = "wp2-abort-caller";
    const targetConv = "wp2-abort-target";
    const state = control();

    // Prepare the owning Object's lanes locally: one settled lane, one live lane, and one
    // lane joined to a host Run.
    const prepared = await withConversationStorage(targetConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const host = yield* ledger.admit(
          yield* admission(targetConv, "wp2-abort-host", { role: "host" }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: host.submissionId }));
        const hostClaim = Option.getOrThrow(
          yield* ledger.claim(
            ClaimRequest.make({
              conversationId: conversation(targetConv),
              producerId: TEST_PRODUCER,
            }),
          ),
        );

        const queued = yield* ledger.admit(
          yield* admission(targetConv, "wp2-abort-joined", { role: "queued" }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: queued.submissionId }));
        const joining = yield* ledger.claimJoining(
          ClaimJoiningRequest.make({
            conversationId: conversation(targetConv),
            hostSubmissionId: host.submissionId,
            ownershipToken: hostClaim.ownershipToken,
            maxCount: 1,
          }),
        );
        expect(joining.map((claim) => claim.submissionId)).toEqual([queued.submissionId]);
        yield* ledger.markJoined(
          MarkJoinedRequest.make({
            submissionId: queued.submissionId,
            ownershipToken: hostClaim.ownershipToken,
            recordId: submissionInputRecordId(queued.submissionId),
            sequence: sequence(1),
          }),
        );

        // Settle the host lane so the routed abort of a settled Submission conflicts typed.
        const reservation = yield* settlementReservation(
          host,
          hostClaim.ownershipToken,
          "completed",
        );
        yield* ledger.reserveSettlement(reservation);
        yield* ledger.finalizeSettlement(
          SettlementFinalization.make({
            submissionId: host.submissionId,
            settlementId: reservation.settlementId,
          }),
        );

        const live = yield* ledger.admit(
          yield* admission(targetConv, "wp2-abort-live", { role: "live" }),
        );
        return { host, joined: queued, live, reservation };
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );

    await withRoutedPorts(
      callerConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        // Terminal outcomes are immutable across Objects: the conflict re-decodes verbatim.
        const settledConflict = yield* ledger
          .requestAbort(
            AbortCommand.make({
              submissionId: prepared.host.submissionId,
              author: "wp2-operator",
              reason: "too late",
            }),
          )
          .pipe(Effect.flip);
        expect(settledConflict).toBeInstanceOf(SettlementConflict);
        if (settledConflict instanceof SettlementConflict) {
          expect(settledConflict.existingOutcome).toBe("completed");
          expect(settledConflict.submissionId).toBe(prepared.host.submissionId);
        }

        // Canonical repair is lane-local: this caller cannot prove that its supplied envelope
        // came from the target Object's ConversationStore, so the router refuses to forward it.
        const foreignRepair = yield* ledger
          .repairSettlementFromCanonical(
            CanonicalSettlementRepair.make({
              submissionId: prepared.host.submissionId,
              record: prepared.reservation.record,
              recordDigest: prepared.reservation.recordDigest,
            }),
          )
          .pipe(Effect.flip);
        expect(foreignRepair).toBeInstanceOf(LedgerError);
        expect(foreignRepair.message).toContain("not route-capable");

        // A joined Submission settles with its host: the redirect crosses Objects typed.
        const joinedRedirect = yield* ledger
          .requestAbort(
            AbortCommand.make({
              submissionId: prepared.joined.submissionId,
              author: "wp2-operator",
              reason: "wrong lane",
            }),
          )
          .pipe(Effect.flip);
        expect(joinedRedirect).toBeInstanceOf(JoinedToHost);
        if (joinedRedirect instanceof JoinedToHost) {
          expect(joinedRedirect.hostSubmissionId).toBe(prepared.host.submissionId);
        }

        // A live lane records the intent durably and idempotently through the route.
        const intent = yield* ledger.requestAbort(
          AbortCommand.make({
            submissionId: prepared.live.submissionId,
            author: "wp2-operator",
            reason: "no longer needed",
          }),
        );
        expect(intent.submissionId).toBe(prepared.live.submissionId);
        expect(intent.author).toBe("wp2-operator");
        expect(intent.reason).toBe("no longer needed");
        const replay = yield* ledger.requestAbort(
          AbortCommand.make({
            submissionId: prepared.live.submissionId,
            author: "wp2-other-author",
            reason: "duplicate request",
          }),
        );
        // Idempotent per submission: the FIRST recorded intent replays unchanged (DUR-012).
        expect(replay.author).toBe("wp2-operator");
        expect(replay.requestedAt).toStrictEqual(intent.requestedAt);
      }),
    );

    // The intent row lives durably in the owning Object.
    await withConversationStorage(targetConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const snapshot = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: prepared.live.submissionId }),
        );
        expect(snapshot.abortIntent?.reason).toBe("no longer needed");
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );
  });

  it("resumes a waitingForChild parent across Objects only through routed canonical authority", async () => {
    const parentConv = "wp2-wake-parent";
    const childConv = "wp2-wake-child";
    const state = control();

    // The child Object settles one child lane and leaves a second child unsettled.
    const children = await withConversationStorage(childConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const settled = yield* claimedLocalLane(childConv, "wp2-settled-child", { part: 1 });
        const reservation = yield* settlementReservation(
          settled.admitted,
          settled.claim.ownershipToken,
          "completed",
        );
        yield* ledger.reserveSettlement(reservation);
        yield* ledger.finalizeSettlement(
          SettlementFinalization.make({
            submissionId: settled.admitted.submissionId,
            settlementId: reservation.settlementId,
          }),
        );
        const pending = yield* ledger.admit(
          yield* admission(childConv, "wp2-pending-child", { part: 2 }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: pending.submissionId }));
        return { settled: settled.admitted, pending };
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );

    const waitingReason = WaitingForChildSuspension.make({
      children: [
        WaitingChild.make({
          toolCallId: toolCall("wp2-call-1"),
          childSubmissionId: children.settled.submissionId,
        }),
        WaitingChild.make({
          toolCallId: toolCall("wp2-call-2"),
          childSubmissionId: children.pending.submissionId,
        }),
      ],
    });

    // The parent Object suspends its lane waiting on BOTH children (they live elsewhere, so
    // neither is locally provable and no marker exists yet).
    const parent = await withConversationStorage(parentConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const lane = yield* claimedLocalLane(parentConv, "wp2-waiting-parent", {
          plan: "delegate",
        });
        const outcome = yield* ledger.suspend(
          SuspendRequest.make({
            submissionId: lane.admitted.submissionId,
            ownershipToken: lane.claim.ownershipToken,
            reason: waitingReason,
          }),
        );
        expect(outcome).toBe("suspended");
        return lane.admitted;
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );

    // The child's Object notifies the parent's Object over the routed port. One settled
    // child of two: still-waiting.
    await withRoutedPorts(
      childConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const first = yield* ledger.recordChildSettled(
          ChildSettledNotification.make({
            parentSubmissionId: parent.submissionId,
            childSubmissionId: children.settled.submissionId,
          }),
        );
        expect(first).toBe("still-waiting");
      }),
    );
    expect(state.calls).toBeGreaterThan(0);

    // Settle the second child in its own Object, then record full operational coverage.
    await withConversationStorage(childConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const claimed = Option.getOrThrow(
          yield* ledger.claim(
            ClaimRequest.make({
              conversationId: conversation(childConv),
              producerId: TEST_PRODUCER,
            }),
          ),
        );
        expect(claimed.submissionId).toBe(children.pending.submissionId);
        const reservation = yield* settlementReservation(
          children.pending,
          claimed.ownershipToken,
          "completed",
        );
        yield* ledger.reserveSettlement(reservation);
        yield* ledger.finalizeSettlement(
          SettlementFinalization.make({
            submissionId: children.pending.submissionId,
            settlementId: reservation.settlementId,
          }),
        );
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );

    await withRoutedPorts(
      childConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const second = yield* ledger.recordChildSettled(
          ChildSettledNotification.make({
            parentSubmissionId: parent.submissionId,
            childSubmissionId: children.pending.submissionId,
          }),
        );
        expect(second).toBe("still-waiting");

        const blocked = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: parent.submissionId }),
        );
        expect(Option.isSome(blocked)).toBe(true);
        if (Option.isSome(blocked)) {
          expect(blocked.value.state).toBe("suspended");
        }

        const resumed = yield* ledger.resumeSuspension(
          ResumeSuspensionRequest.make({
            submissionId: parent.submissionId,
            expectedReason: waitingReason,
          }),
        );
        expect(resumed).toBe("resumed");

        // At-least-once redelivery answers not-waiting idempotently after explicit resume.
        const redelivered = yield* ledger.recordChildSettled(
          ChildSettledNotification.make({
            parentSubmissionId: parent.submissionId,
            childSubmissionId: children.pending.submissionId,
          }),
        );
        expect(redelivered).toBe("not-waiting");

        // The routed lookup observes the resumed parent lane in ITS Object.
        const woken = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: parent.submissionId }),
        );
        expect(Option.isSome(woken)).toBe(true);
        if (Option.isSome(woken)) {
          expect(woken.value.state).toBe("input-applied");
        }
      }),
    );
  });

  it("prearms a foreign parent while the child owner still has a preterminal projection", async () => {
    const parentConv = "wp2-prearm-parent";
    const childConv = "wp2-prearm-child";
    const state = control();

    const child = await withConversationStorage(childConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const admitted = yield* ledger.admit(
          yield* admission(childConv, "wp2-prearm-child-key", { task: "repair projection" }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
        return admitted;
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );
    const waitingReason = WaitingForChildSuspension.make({
      children: [
        WaitingChild.make({
          toolCallId: toolCall("wp2-prearm-call"),
          childSubmissionId: child.submissionId,
        }),
      ],
    });
    const parent = await withConversationStorage(parentConv, (storage) =>
      claimedLocalLane(parentConv, "wp2-prearm-parent-key", { task: "wait" }).pipe(
        Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer]),
      ),
    );

    // This is the cross-Object prefix used after the canonical child Settlement append but
    // before its owning ledger projection is repaired. The parent Object has no local child row,
    // so it durably prearms coverage even though the child owner's projection is still ready.
    await withRoutedPorts(
      childConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const outcome = yield* ledger.recordChildSettled(
          ChildSettledNotification.make({
            parentSubmissionId: parent.admitted.submissionId,
            childSubmissionId: child.submissionId,
          }),
        );
        expect(outcome).toBe("not-waiting");
        const staleChild = Option.getOrThrow(
          yield* ledger.lookup(SubmissionLookupById.make({ submissionId: child.submissionId })),
        );
        expect(staleChild.state).toBe("ready");
      }),
    );

    await withConversationStorage(parentConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const outcome = yield* ledger.suspend(
          SuspendRequest.make({
            submissionId: parent.admitted.submissionId,
            ownershipToken: parent.claim.ownershipToken,
            reason: waitingReason,
          }),
        );
        expect(outcome).toBe("resume-immediately");
        yield* ledger.renewOwnership(
          RenewOwnershipRequest.make({
            submissionId: parent.admitted.submissionId,
            ownershipToken: parent.claim.ownershipToken,
          }),
        );
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );
  });

  it("routes the conversation store subset with error-tag fidelity", async () => {
    const callerConv = "wp2-store-caller";
    const targetConv = "wp2-store-target";
    const state = control();

    await withRoutedPorts(
      callerConv,
      state,
      Effect.gen(function* () {
        const store = yield* ConversationStore;

        // The owner proves non-materialization typed across the route.
        const missing = yield* store
          .inspectTail(ConversationTailRequest.make({ conversationId: conversation(targetConv) }))
          .pipe(Effect.flip);
        expect(missing).toBeInstanceOf(ConversationNotMaterialized);
        if (missing instanceof ConversationNotMaterialized) {
          expect(missing.conversationId).toBe(targetConv);
        }

        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId: conversation(targetConv),
            producerEpoch: epoch(1),
          }),
        );

        const appended = yield* store.append(
          FencedAppendRequest.make({
            conversationId: conversation(targetConv),
            batch: batch("wp2-routed-batch", [
              inputRecord("wp2-routed-record-1", "first routed input"),
              inputRecord("wp2-routed-record-2", "second routed input"),
            ]),
            expectedTailSequence: sequence(0),
            expectedTailDigest: EMPTY_TAIL_DIGEST,
            producerEpoch: epoch(1),
          }),
        );
        expect(appended.firstSequence).toBe(1);
        expect(appended.lastSequence).toBe(2);
        expect(appended.replayed).toBe(false);

        const page = yield* store
          .read(ConversationRead.make({ conversationId: conversation(targetConv), limit: 10 }))
          .pipe(Stream.runCollect);
        expect(page.map((envelope) => envelope.record.recordId)).toEqual([
          "wp2-routed-record-1",
          "wp2-routed-record-2",
        ]);

        const tail = yield* store.inspectTail(
          ConversationTailRequest.make({ conversationId: conversation(targetConv) }),
        );
        expect(tail.tailSequence).toBe(2);
        expect(tail.tailDigest).toBe(appended.tailDigest);
        expect(tail.producerEpoch).toBe(1);

        const exported = yield* store.export(
          ConversationExportRequest.make({ conversationId: conversation(targetConv) }),
        );
        expect(exported.conversationId).toBe(targetConv);
        expect(exported.tailSequence).toBe(2);
        expect(exported.records).toHaveLength(2);

        // A stale expected tail re-decodes as AppendConflict with the resume hint intact.
        const staleTail = yield* store
          .append(
            FencedAppendRequest.make({
              conversationId: conversation(targetConv),
              batch: batch("wp2-stale-batch", [inputRecord("wp2-stale-record", "stale")]),
              expectedTailSequence: sequence(0),
              expectedTailDigest: EMPTY_TAIL_DIGEST,
              producerEpoch: epoch(1),
            }),
          )
          .pipe(Effect.flip);
        expect(staleTail).toBeInstanceOf(AppendConflict);
        if (staleTail instanceof AppendConflict) {
          expect(staleTail.reason).toBe("tail");
          expect(staleTail.actualTailSequence).toBe(2);
          expect(staleTail.actualTailDigest).toBe(appended.tailDigest);
        }

        // A superseded producer epoch re-decodes as FenceRejected with both epochs.
        const fenced = yield* store
          .append(
            FencedAppendRequest.make({
              conversationId: conversation(targetConv),
              batch: batch("wp2-fenced-batch", [inputRecord("wp2-fenced-record", "fenced")]),
              expectedTailSequence: tail.tailSequence,
              expectedTailDigest: tail.tailDigest,
              producerEpoch: epoch(9),
            }),
          )
          .pipe(Effect.flip);
        expect(fenced).toBeInstanceOf(FenceRejected);
        if (fenced instanceof FenceRejected) {
          expect(fenced.actualEpoch).toBe(1);
          expect(fenced.attemptedEpoch).toBe(9);
        }

        const staleMaterialize = yield* store
          .materialize(
            ConversationMaterialization.make({
              conversationId: conversation(targetConv),
              producerEpoch: epoch(0),
            }),
          )
          .pipe(Effect.flip);
        expect(staleMaterialize).toBeInstanceOf(FenceRejected);
      }),
    );
    expect(state.calls).toBeGreaterThan(0);

    // The canonical rows live in the owning Object's private database.
    await withConversationStorage(targetConv, (storage) =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        const local = yield* store
          .read(ConversationRead.make({ conversationId: conversation(targetConv), limit: 10 }))
          .pipe(Stream.runCollect);
        expect(local).toHaveLength(2);
      }).pipe(Effect.provide(storeLayer({ storage, observationPollInterval: 1 }))),
    );
  });

  it("fails fast typed for foreign operations outside the closed route-capable subset", async () => {
    const localConv = "wp2-cross-local";
    const foreignConv = "wp2-cross-foreign";
    const state = control();
    const foreignSid = submissionId(`00000000-0000-7000-8000-000000000000:${foreignConv}`);

    await withRoutedPorts(
      localConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const store = yield* ConversationStore;

        const expectCrossLedger = <A, E>(effect: Effect.Effect<A, E>) =>
          effect.pipe(
            Effect.flip,
            Effect.map((error) => {
              expect(error).toBeInstanceOf(LedgerError);
              if (error instanceof LedgerError) {
                expect(error.message).toContain("not route-capable");
              }
            }),
          );

        yield* expectCrossLedger(
          ledger.claim(
            ClaimRequest.make({
              conversationId: conversation(foreignConv),
              producerId: TEST_PRODUCER,
            }),
          ),
        );
        yield* expectCrossLedger(
          ledger.claimJoining(
            ClaimJoiningRequest.make({
              conversationId: conversation(foreignConv),
              hostSubmissionId: foreignSid,
              ownershipToken: ownershipToken("wp2-cross-token"),
              maxCount: 1,
            }),
          ),
        );
        yield* expectCrossLedger(
          ledger.renewOwnership(
            RenewOwnershipRequest.make({
              submissionId: foreignSid,
              ownershipToken: ownershipToken("wp2-cross-token"),
            }),
          ),
        );
        yield* expectCrossLedger(
          ledger.suspend(
            SuspendRequest.make({
              submissionId: foreignSid,
              ownershipToken: ownershipToken("wp2-cross-token"),
              reason: ApprovalPendingSuspension.make({ toolCallIds: [toolCall("wp2-cross")] }),
            }),
          ),
        );
        yield* expectCrossLedger(
          ledger.reserveChildBudget(
            ChildBudgetReservationRequest.make({
              reservationId: id(ChildReservationId, "wp2-cross-reservation"),
              parentSubmissionId: foreignSid,
              parentToolCallId: toolCall("wp2-cross"),
              ownershipToken: ownershipToken("wp2-cross-token"),
              allocation: { budget: 1 },
              allocationDigest: yield* digestJson({ budget: 1 }),
            }),
          ),
        );
        yield* expectCrossLedger(
          ledger.loadRecoverySnapshot(RecoverySnapshotRequest.make({ submissionId: foreignSid })),
        );

        const observeFailure = yield* store
          .observe(ConversationObservation.make({ conversationId: conversation(foreignConv) }))
          .pipe(Stream.runCollect, Effect.flip);
        expect(observeFailure).toBeInstanceOf(ConversationStoreError);
        if (observeFailure instanceof ConversationStoreError) {
          expect(observeFailure.message).toContain("not route-capable");
        }

        const checkpointFailure = yield* store
          .loadCheckpoint(LoadCheckpointRequest.make({ conversationId: conversation(foreignConv) }))
          .pipe(Effect.flip);
        expect(checkpointFailure).toBeInstanceOf(ConversationStoreError);
        if (checkpointFailure instanceof ConversationStoreError) {
          expect(checkpointFailure.message).toContain("not route-capable");
        }
      }),
    );

    // Fail-fast means fail BEFORE the transport: no delivery was ever attempted.
    expect(state.calls).toBe(0);
  });

  it("refuses a Submission identity beyond the routable bound typed, without routing", async () => {
    const localConv = "wp2-bound-local";
    const state = control();

    await withRoutedPorts(
      localConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const oversized = submissionId("a".repeat(1_100));
        const failure = yield* ledger
          .markReady(MarkReadyRequest.make({ submissionId: oversized }))
          .pipe(Effect.flip);
        expect(failure).toBeInstanceOf(LedgerError);
        if (failure instanceof LedgerError) {
          expect(failure.message).toContain("1024");
        }
      }),
    );
    expect(state.calls).toBe(0);
  });

  it("delegates this-conversation and opaque-identity operations to the local facet without the transport", async () => {
    const localConv = "wp2-local-delegation";
    const state = control();
    // A transport that would fail EVERY delivery proves by construction that local work
    // never touches it.
    state.fault = "poisoned transport: local operations must not route";

    await withRoutedPorts(
      localConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const store = yield* ConversationStore;

        const lane = yield* claimedLocalLane(localConv, "wp2-local-key", { work: "local" });
        expect(lane.admitted.submissionId.endsWith(`:${localConv}`)).toBe(true);

        const found = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: lane.admitted.submissionId }),
        );
        expect(Option.isSome(found)).toBe(true);

        const resolution = yield* ledger.resolveAdmission(
          SubmissionLookupByKey.make({
            conversationId: conversation(localConv),
            principal: TEST_PRINCIPAL,
            idempotencyKey: idempotencyKey("wp2-local-key"),
          }),
        );
        expect(resolution._tag).toBe("Admitted");

        // Identities without the DC-minted shape fall back to the local authority.
        const opaque = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: submissionId("submission-opaque") }),
        );
        expect(Option.isNone(opaque)).toBe(true);
        const nonUuidHead = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: submissionId("not-a-uuid:some-conv") }),
        );
        expect(Option.isNone(nonUuidHead)).toBe(true);

        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId: conversation(localConv),
            producerEpoch: lane.claim.producerEpoch,
          }),
        );
        yield* store.append(
          FencedAppendRequest.make({
            conversationId: conversation(localConv),
            batch: batch("wp2-local-batch", [inputRecord("wp2-local-record", "local input")]),
            expectedTailSequence: sequence(0),
            expectedTailDigest: EMPTY_TAIL_DIGEST,
            producerEpoch: lane.claim.producerEpoch,
          }),
        );
        const page = yield* store
          .read(ConversationRead.make({ conversationId: conversation(localConv), limit: 10 }))
          .pipe(Stream.runCollect);
        expect(page).toHaveLength(1);
        const tail = yield* store.inspectTail(
          ConversationTailRequest.make({ conversationId: conversation(localConv) }),
        );
        expect(tail.tailSequence).toBe(1);
        yield* store.export(
          ConversationExportRequest.make({ conversationId: conversation(localConv) }),
        );
      }),
    );
    expect(state.calls).toBe(0);
  });

  it("SUB-031: a transport fault answers Indeterminate and never permits a second admission", async () => {
    const parentConv = "wp2-sub031-parent";
    const childConv = "wp2-sub031-child";
    const state = control();
    const byKey = SubmissionLookupByKey.make({
      conversationId: conversation(childConv),
      principal: TEST_PRINCIPAL,
      idempotencyKey: idempotencyKey("wp2-sub031-key"),
    });

    // Establish the one child while the transport is healthy.
    const established = await withRoutedPorts(
      parentConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        return yield* ledger.admit(
          yield* admission(childConv, "wp2-sub031-key", { task: "establish once" }),
        );
      }),
    );

    // The owning Object becomes unreachable mid-recovery (the subagent:after-reserve shape).
    await withRoutedPorts(
      parentConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        state.fault = "injected transport fault: owning Object unreachable";

        // An unreachable authority proves NOTHING: the answer is Indeterminate — never
        // NotAdmitted (which alone would permit a second admission).
        const resolution = yield* ledger.resolveAdmission(byKey);
        expect(resolution._tag).toBe("Indeterminate");
        if (resolution._tag === "Indeterminate") {
          expect(resolution.reason).toContain("unreachable");
        }

        // The coordinator's SUB-031 discipline never admits on Indeterminate; and even a
        // buggy caller that TRIED to admit through the faulted transport gets a typed
        // failure, not a duplicate child.
        const admitFailure = yield* ledger
          .admit(yield* admission(childConv, "wp2-sub031-key", { task: "establish once" }))
          .pipe(Effect.flip);
        expect(admitFailure).toBeInstanceOf(LedgerError);

        // Transport heals: the same key converges on the ORIGINAL admission.
        state.fault = undefined;
        const healed = yield* ledger.resolveAdmission(byKey);
        expect(healed._tag).toBe("Admitted");
        if (healed._tag === "Admitted") {
          expect(healed.submission.submissionId).toBe(established.submissionId);
        }
        const replay = yield* ledger.admit(
          yield* admission(childConv, "wp2-sub031-key", { task: "establish once" }),
        );
        expect(replay.replayed).toBe(true);
        expect(replay.submissionId).toBe(established.submissionId);
      }),
    );

    // Exactly ONE admission row exists in the owning Object (never a second admission).
    await withConversationStorage(childConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const rows = yield* ledger.scanNonterminal.pipe(Stream.runCollect);
        expect(rows.map((row) => row.submissionId)).toEqual([established.submissionId]);
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );
  });

  it("enriches a local parent's recovery snapshot with routed child lane state", async () => {
    const parentConv = "wp2-enrich-parent";
    const childConv = "wp2-enrich-child";
    const state = control();

    await withRoutedPorts(
      parentConv,
      state,
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        const lane = yield* claimedLocalLane(parentConv, "wp2-enrich-key", { plan: "delegate" });

        // The child is admitted in ITS Object through the routed port and stays unsettled:
        // neither a local row nor a settlement marker exists in the parent's Object.
        const child = yield* ledger.admit(
          yield* admission(childConv, "wp2-enrich-child-key", { task: "research" }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: child.submissionId }));

        const allocation: PersistedJson = { maxTurns: 3 };
        const allocationDigest = yield* digestJson(allocation);
        yield* ledger.reserveChildBudget(
          ChildBudgetReservationRequest.make({
            reservationId: id(ChildReservationId, "wp2-enrich-reservation"),
            parentSubmissionId: lane.admitted.submissionId,
            parentToolCallId: toolCall("wp2-enrich-call"),
            ownershipToken: lane.claim.ownershipToken,
            allocation,
            allocationDigest,
          }),
        );
        yield* ledger.attachChildToReservation(
          AttachChildToReservationRequest.make({
            reservationId: id(ChildReservationId, "wp2-enrich-reservation"),
            ownershipToken: lane.claim.ownershipToken,
            childSubmissionId: child.submissionId,
          }),
        );

        // The routed snapshot falls back to a per-child routed lookup for the attached child
        // that is neither local nor marker-settled (plan §1.3).
        const snapshot = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: lane.admitted.submissionId }),
        );
        expect(snapshot.childAttachments).toHaveLength(1);
        expect(snapshot.childAttachments[0].toolCallId).toBe("wp2-enrich-call");
        expect(snapshot.childAttachments[0].childSubmissionId).toBe(child.submissionId);
        expect(snapshot.childAttachments[0].childState).toBe("ready");
        expect(snapshot.childAttachments[0].childOutcome).toBeUndefined();

        // A transport failure surfaces typed so the alarm pass retries (never a silently
        // impoverished snapshot).
        state.fault = "injected transport fault during recovery enrichment";
        const failure = yield* ledger
          .loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: lane.admitted.submissionId }),
          )
          .pipe(Effect.flip);
        expect(failure).toBeInstanceOf(LedgerError);
        state.fault = undefined;
      }),
    );
  });
});
