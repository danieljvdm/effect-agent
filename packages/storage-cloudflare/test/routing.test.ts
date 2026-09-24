import { doMessageDeliveryStoreLayer } from "@effect-agent/storage-cloudflare/do-message-delivery-store";
import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import { ledgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import {
  storageConfigLayer,
  layer as storeLayer,
} from "@effect-agent/storage-cloudflare/do-thread-store";
import { PortResponse } from "@effect-agent/storage-cloudflare/port-protocol";
import {
  ThreadPortTransport,
  PortTransportError,
  portTransportFailure,
  routedThreadStoreLayer,
  routedMessageDeliveryStoreLayer,
  routedSubmissionLedgerLayer,
} from "@effect-agent/storage-cloudflare/port-routing";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { runInDurableObject } from "cloudflare:test";
import type { Crypto } from "effect";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import { digestJson, EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import { MessageDeliveryStore, readPending } from "effect-agent/message-delivery";
import { type PersistedJson } from "effect-agent/records";
import {
  AttachChildToReservationRequest,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimRequest,
  IdempotencyKey,
  LedgerError,
  MarkReadyRequest,
  RecoverySnapshotRequest,
  RenewOwnershipRequest,
  SettlementFinalization,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  SuspendRequest,
  WaitingChild,
  WaitingForChildSuspension,
  type Claim,
} from "effect-agent/submission-ledger";
import { makeMessageDeliveryFixture } from "effect-agent/testing/message-delivery-store-conformance";
import {
  ThreadIdentityRequest,
  ThreadMaterialization,
  ThreadStore,
  ThreadStoreError,
} from "effect-agent/thread-store";
import { describe, expect, it } from "vite-plus/test";

import {
  admission,
  thread,
  threadStub,
  id,
  epoch,
  settlementReservation,
  TEST_PRINCIPAL,
  TEST_PRODUCER,
  toolCall,
  withThreadStorage,
} from "./harness.ts";

const submissionId = (value: string) => id(MarkReadyRequest.fields.submissionId, value);
const ownershipToken = (value: string) => id(RenewOwnershipRequest.fields.ownershipToken, value);
const idempotencyKey = (value: string) => id(IdempotencyKey, value);

const isLedgerError = Schema.is(LedgerError);
const isThreadStoreError = Schema.is(ThreadStoreError);

/**
 * Shared mutable control for the test transport: `fault` simulates an unreachable owning
 * Object (workerd overload, deploy-in-progress), `calls` counts every delivery attempt so
 * fail-fast and local-delegation tests can prove the transport was never consulted.
 */
interface TransportControl {
  calls: number;
  fault: string | undefined;
  transformResponse?: (response: unknown) => unknown;
}

const control = (): TransportControl => ({ calls: 0, fault: undefined });

/**
 * The production-shaped test transport (D-P6-3): the Object identity rule is
 * `idFromName(threadId)`, and the owner's `portCall` RPC method executes the envelope
 * against ITS local facets (see test/worker.ts). Everything thrown by the stub surfaces as
 * `PortTransportError` — the transport never fabricates an answer.
 */
const transportLayer = (state: TransportControl) =>
  Layer.succeed(ThreadPortTransport)({
    call: (threadId, request) =>
      Effect.suspend(() => {
        state.calls += 1;
        if (state.fault !== undefined) {
          return Effect.fail(
            PortTransportError.make({
              target: threadId,
              message: state.fault,
              retryable: true,
            }),
          );
        }

        return Effect.tryPromise({
          try: () => threadStub(threadId).portCall(request),
          catch: (cause) => portTransportFailure(threadId, cause),
        }).pipe(Effect.map((response) => state.transformResponse?.(response) ?? response));
      }),
  });

/**
 * Run one Effect against the ROUTED port Layers inside the named Thread's own Durable
 * Object: local facets over this Object's real SQLite storage, wrapped by the WP2 routing
 * decorators over the namespace transport. The Object name IS the local Thread
 * identity (the DC identity rule), and names are minted uniquely per test because the
 * 0.21.x pool shares Durable Object storage across tests within a run.
 */
const withRoutedPorts = <A, E>(
  objectName: string,
  state: TransportControl,
  build: Effect.Effect<A, E, SubmissionLedger | ThreadStore | MessageDeliveryStore | Crypto.Crypto>,
  includeCheckpoints = true,
): Promise<A> =>
  runInDurableObject(threadStub(objectName), (_instance, doState) =>
    Effect.runPromise(
      build.pipe(
        Effect.provide(
          Layer.mergeAll(
            routedMessageDeliveryStoreLayer({
              ownsThread: (target) => target === thread(objectName),
            }).pipe(
              Layer.provide(
                doMessageDeliveryStoreLayer().pipe(
                  Layer.provide([
                    storageConfigLayer({ storage: doState.storage }),
                    SqliteClient.layer({ storage: doState.storage }),
                    DoStorageFailpoint.layer,
                  ]),
                ),
              ),
              Layer.provide(transportLayer(state)),
            ),
            routedSubmissionLedgerLayer({
              ownsThread: (target) => target === thread(objectName),
            }).pipe(
              Layer.provide(
                Layer.mergeAll(ledgerLayer({ storage: doState.storage }), transportLayer(state)),
              ),
            ),
            routedThreadStoreLayer({ ownsThread: (target) => target === thread(objectName) }).pipe(
              Layer.updateService(ThreadStore, (store) =>
                includeCheckpoints
                  ? store
                  : {
                      materialize: store.materialize,
                      append: store.append,
                      read: store.read,
                      observe: store.observe,
                      export: store.export,
                      inspectTail: store.inspectTail,
                      readIdentity: store.readIdentity,
                    },
              ),
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
  threadId: string,
  key: string,
  input: PersistedJson,
) {
  const ledger = yield* SubmissionLedger;
  const admitted = yield* ledger.admit(yield* admission(threadId, key, input));

  yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));

  const claimed = yield* ledger.claim(
    ClaimRequest.make({ threadId: thread(threadId), producerId: TEST_PRODUCER }),
  );

  expect(Option.isSome(claimed)).toBe(true);
  const claim: Claim = Option.getOrThrow(claimed);

  return { admitted, claim };
});

describe("cross-DO port routing", () => {
  // Regression: https://github.com/danieljvdm/effect-agent/commit/6a4f4f870

  // Regression: https://github.com/danieljvdm/effect-agent/commit/6a4f4f870
  it("preserves identity absence and rejects invalid owner replies typed", () => {
    const state = control();

    return withRoutedPorts(
      "identity-failure-reader",
      state,
      Effect.gen(function* () {
        const store = yield* ThreadStore;
        const threadId = thread("identity-failure-owner");
        const request = ThreadIdentityRequest.make({ threadId });

        expect(yield* store.readIdentity(request).pipe(Effect.flip)).toMatchObject({
          _tag: "ThreadNotMaterialized",
          threadId,
        });
        yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch: epoch(3) }));
        const empty = yield* store.readIdentity(request);

        expect(empty).toMatchObject({
          threadId,
          tailSequence: 0,
          tailDigest: EMPTY_TAIL_DIGEST,
          producerEpoch: 3,
          records: [],
        });

        const invalidReplies = [
          { _tag: "StoreReadIdentityResult", identity: { ...empty, threadId: "another-owner" } },
        ];

        for (const result of invalidReplies) {
          state.transformResponse = (response) => {
            expect(Schema.decodeUnknownSync(PortResponse)(response)._tag).toBe("PortSucceeded");

            return { _tag: "PortSucceeded", result };
          };
          expect(yield* store.readIdentity(request).pipe(Effect.flip)).toBeInstanceOf(
            ThreadStoreError,
          );
        }
        delete state.transformResponse;
        state.fault = "owner unavailable";
        const unavailable = yield* store.readIdentity(request).pipe(Effect.flip);

        expect(unavailable).toBeInstanceOf(ThreadStoreError);
        if (isThreadStoreError(unavailable)) {
          expect(unavailable.cause).toBeInstanceOf(PortTransportError);
        }
        state.fault = undefined;
      }),
    );
  });

  it("routes current retained obligations to their source owner before materialization", async () => {
    const state = control();
    const owner = "wp2-pending-owner";

    const record = await withRoutedPorts(
      owner,
      state,
      Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const record = yield* makeMessageDeliveryFixture("future-delivery", owner);

        yield* store.insert(record);

        return yield* store.change(record.key, {
          _tag: "Defer",
          expectedVersion: 1,
          nowMillis: 0,
          untilMillis: 100,
        });
      }),
    );

    await withRoutedPorts(
      "wp2-pending-caller",
      state,
      Effect.gen(function* () {
        expect(yield* readPending({ ownerThreadId: record.key.ownerThreadId, limit: 1 })).toEqual([
          record,
        ]);
        expect(
          yield* readPending({ ownerThreadId: thread("wp2-pending-caller"), limit: 1 }),
        ).toEqual([]);
      }),
    );
  });

  it("wakes a waitingForChild parent across Objects through routed recordChildSettled", async () => {
    const parentConv = "wp2-wake-parent";
    const childConv = "wp2-wake-child";
    const state = control();

    // The child Object settles one child lane and leaves a second child unsettled.
    const children = await withThreadStorage(childConv, (storage) =>
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

    // The parent Object suspends its lane waiting on BOTH children (they live elsewhere, so
    // neither is locally provable and no marker exists yet).
    const parent = await withThreadStorage(parentConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        const lane = yield* claimedLocalLane(parentConv, "wp2-waiting-parent", {
          plan: "delegate",
        });

        const outcome = yield* ledger.suspend(
          SuspendRequest.make({
            submissionId: lane.admitted.submissionId,
            ownershipToken: lane.claim.ownershipToken,
            reason: WaitingForChildSuspension.make({
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
            }),
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

    // Settle the second child in its own Object, then notify again: woken, exactly once.
    await withThreadStorage(childConv, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        const claimed = Option.getOrThrow(
          yield* ledger.claim(
            ClaimRequest.make({
              threadId: thread(childConv),
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

        expect(second).toBe("woken");

        // At-least-once redelivery answers not-waiting idempotently after the wake.
        const redelivered = yield* ledger.recordChildSettled(
          ChildSettledNotification.make({
            parentSubmissionId: parent.submissionId,
            childSubmissionId: children.pending.submissionId,
          }),
        );

        expect(redelivered).toBe("not-waiting");

        // The routed lookup observes the woken parent lane in ITS Object.
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

        const expectCrossLedger = <A, E>(effect: Effect.Effect<A, E>) =>
          effect.pipe(
            Effect.flip,
            Effect.map((error) => {
              expect(error).toBeInstanceOf(LedgerError);
              if (isLedgerError(error)) {
                expect(error.message).toContain("not route-capable");
              }
            }),
          );

        yield* expectCrossLedger(
          ledger.claim(
            ClaimRequest.make({
              threadId: thread(foreignConv),
              producerId: TEST_PRODUCER,
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
      }),
    );

    // Fail-fast means fail BEFORE the transport: no delivery was ever attempted.
    expect(state.calls).toBe(0);
  });

  it("SUB-031: a transport fault answers Indeterminate and never permits a second admission", async () => {
    const parentConv = "wp2-sub031-parent";
    const childConv = "wp2-sub031-child";
    const state = control();

    const byKey = SubmissionLookupByKey.make({
      threadId: thread(childConv),
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
    await withThreadStorage(childConv, (storage) =>
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
