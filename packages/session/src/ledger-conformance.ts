import { AgentId, ConversationId, ReceiptId, SubmissionId } from "@effect-agent/core";
import { Clock, Crypto, DateTime, Duration, Effect, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";

import { digestJson, type DigestError } from "./digest.ts";
import {
  AbortCommand,
  AdmissionConflict,
  AdmissionRequest,
  AdmissionResult,
  ClaimRequest,
  IdempotencyKey,
  MarkInputAppliedRequest,
  MarkReadyRequest,
  OwnershipLost,
  Principal,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  SettlementConflict,
  SettlementFinalization,
  SettlementReservation,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  submissionInputRecordId,
  submissionSettlementId,
  submissionSettlementRecordId,
  type OwnershipToken,
  type SubmissionLedgerFailure,
} from "./ledger.ts";
import {
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  SubmissionSettled,
  type PersistedJson,
  type SettlementOutcome,
} from "./records.ts";

/** A SubmissionLedger contract invariant that an adapter under test violated. */
export class SubmissionLedgerConformanceViolation extends Schema.TaggedErrorClass<SubmissionLedgerConformanceViolation>()(
  "SubmissionLedgerConformanceViolation",
  {
    caseName: Schema.String,
    message: Schema.String,
  },
) {}

export type SubmissionLedgerConformanceFailure =
  | SubmissionLedgerFailure
  | DigestError
  | SubmissionLedgerConformanceViolation;

/**
 * One adapter-neutral SubmissionLedger contract case. Each case owns disjoint Conversation
 * lanes, so a suite may run every case against one shared ledger instance or against a fresh
 * ledger per case. Cases drive lease expiry through `TestClock`, so they must run inside the
 * `@effect/vitest` test environment (`it.effect`), and they compute real content digests, so the
 * host suite must provide `Crypto.Crypto`.
 */
export interface SubmissionLedgerConformanceCase {
  readonly name: string;
  readonly run: Effect.Effect<
    void,
    SubmissionLedgerConformanceFailure,
    SubmissionLedger | Crypto.Crypto
  >;
}

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeSubmissionId = Schema.decodeSync(SubmissionId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeSequence = Schema.decodeSync(CanonicalSequence);

const CONFORMANCE_PRINCIPAL = Schema.decodeSync(Principal)("principal-ledger-conformance");
const OTHER_PRINCIPAL = Schema.decodeSync(Principal)("principal-ledger-conformance-other");
const CONFORMANCE_AGENT = Schema.decodeSync(AgentId)("agent-ledger-conformance");
const CONFORMANCE_DEPLOYMENT = Schema.decodeSync(DeploymentId)("deployment-ledger-conformance");
const CONFORMANCE_DEFINITION_DIGEST = Schema.decodeSync(Digest)("d".repeat(64));
const CONFORMANCE_DIGESTS = DefinitionDigests.make({
  agent: CONFORMANCE_DEFINITION_DIGEST,
  model: CONFORMANCE_DEFINITION_DIGEST,
  tools: CONFORMANCE_DEFINITION_DIGEST,
});
const CONFORMANCE_CREATED_AT = DateTime.toUtc(DateTime.makeUnsafe(1));
const PRODUCER_A = Schema.decodeSync(ProducerId)("producer-ledger-conformance-a");
const PRODUCER_B = Schema.decodeSync(ProducerId)("producer-ledger-conformance-b");

const sameInstant = (left: DateTime.Utc, right: DateTime.Utc): boolean =>
  DateTime.toEpochMillis(left) === DateTime.toEpochMillis(right);

const admissionRequest = Effect.fn("SubmissionLedgerConformance.admissionRequest")(function* (
  conversationId: ConversationId,
  idempotencyKey: string,
  input: PersistedJson,
) {
  const inputDigest = yield* digestJson(input);
  return AdmissionRequest.make({
    conversationId,
    principal: CONFORMANCE_PRINCIPAL,
    idempotencyKey: decodeIdempotencyKey(idempotencyKey),
    agentId: CONFORMANCE_AGENT,
    agentDigests: CONFORMANCE_DIGESTS,
    deploymentId: CONFORMANCE_DEPLOYMENT,
    inputPayload: input,
    inputDigest,
  });
});

const admitReady = Effect.fn("SubmissionLedgerConformance.admitReady")(function* (
  conversationId: ConversationId,
  idempotencyKey: string,
  input: PersistedJson,
) {
  const ledger = yield* SubmissionLedger;
  const admitted = yield* ledger.admit(
    yield* admissionRequest(conversationId, idempotencyKey, input),
  );
  yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
  return admitted;
});

const claimLane = Effect.fn("SubmissionLedgerConformance.claimLane")(function* (
  conversationId: ConversationId,
  producerId: ProducerId,
) {
  const ledger = yield* SubmissionLedger;
  return yield* ledger.claim(ClaimRequest.make({ conversationId, producerId }));
});

const lookupById = Effect.fn("SubmissionLedgerConformance.lookupById")(function* (
  submissionId: SubmissionId,
) {
  const ledger = yield* SubmissionLedger;
  return yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
});

const recoverySnapshot = Effect.fn("SubmissionLedgerConformance.recoverySnapshot")(function* (
  submissionId: SubmissionId,
) {
  const ledger = yield* SubmissionLedger;
  return yield* ledger.loadRecoverySnapshot(RecoverySnapshotRequest.make({ submissionId }));
});

interface ReservationOptions {
  readonly submissionId: SubmissionId;
  readonly ownershipToken: OwnershipToken;
  readonly receiptId: ReceiptId;
  readonly outcome: SettlementOutcome;
  readonly result?: PersistedJson;
}

/**
 * Builds a complete, deterministic settlement reservation: the exact canonical envelope that
 * would be appended (DUR-011) plus the digest of its canonical JSON encoding. Two calls with the
 * same options produce byte-identical content, so replays are honest reservation replays.
 */
const settlementReservation = Effect.fn("SubmissionLedgerConformance.settlementReservation")(
  function* (options: ReservationOptions) {
    const settlementId = submissionSettlementId(options.submissionId);
    const record = RecordEnvelope.make({
      recordId: submissionSettlementRecordId(options.submissionId),
      family: "conversation",
      schemaVersion: 1,
      createdAt: CONFORMANCE_CREATED_AT,
      deploymentId: CONFORMANCE_DEPLOYMENT,
      payload: SubmissionSettled.make({
        submissionId: options.submissionId,
        settlementId,
        receiptId: options.receiptId,
        outcome: options.outcome,
        ...(options.result === undefined ? {} : { result: options.result }),
      }),
    });
    const encoded = yield* Schema.encodeEffect(RecordEnvelope)(record).pipe(Effect.orDie);
    const recordDigest = yield* digestJson(encoded);
    return SettlementReservation.make({
      submissionId: options.submissionId,
      ownershipToken: options.ownershipToken,
      settlementId,
      outcome: options.outcome,
      record,
      recordDigest,
    });
  },
);

const settleClaimed = Effect.fn("SubmissionLedgerConformance.settleClaimed")(function* (
  admitted: AdmissionResult,
  ownershipToken: OwnershipToken,
) {
  const ledger = yield* SubmissionLedger;
  const reservation = yield* settlementReservation({
    submissionId: admitted.submissionId,
    ownershipToken,
    receiptId: admitted.receiptId,
    outcome: "completed",
  });
  yield* ledger.reserveSettlement(reservation);
  return yield* ledger.finalizeSettlement(
    SettlementFinalization.make({
      submissionId: admitted.submissionId,
      settlementId: submissionSettlementId(admitted.submissionId),
    }),
  );
});

/** Advances the TestClock one millisecond past the given lease boundary. */
const advancePastLease = Effect.fn("SubmissionLedgerConformance.advancePastLease")(function* (
  leaseExpiresAt: DateTime.Utc,
) {
  const nowMillis = yield* Clock.currentTimeMillis;
  const waitMillis = Math.max(0, DateTime.toEpochMillis(leaseExpiresAt) - nowMillis) + 1;
  yield* TestClock.adjust(Duration.millis(waitMillis));
});

const conformanceCase = (
  name: string,
  build: (assert: {
    readonly ensure: (
      condition: boolean,
      message: string,
    ) => Effect.Effect<void, SubmissionLedgerConformanceViolation>;
    readonly expectFailure: <A, E, R>(
      description: string,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<E, SubmissionLedgerConformanceViolation, R>;
    readonly expectSome: <A>(
      description: string,
      option: Option.Option<A>,
    ) => Effect.Effect<A, SubmissionLedgerConformanceViolation>;
  }) => Effect.Effect<void, SubmissionLedgerConformanceFailure, SubmissionLedger | Crypto.Crypto>,
): SubmissionLedgerConformanceCase => ({
  name,
  run: build({
    ensure: (condition, message) =>
      condition
        ? Effect.void
        : Effect.fail(SubmissionLedgerConformanceViolation.make({ caseName: name, message })),
    expectFailure: (description, effect) =>
      Effect.flip(effect).pipe(
        Effect.mapError(() =>
          SubmissionLedgerConformanceViolation.make({
            caseName: name,
            message: `Expected failure but the operation succeeded: ${description}`,
          }),
        ),
      ),
    expectSome: (description, option) =>
      Option.isSome(option)
        ? Effect.succeed(option.value)
        : Effect.fail(
            SubmissionLedgerConformanceViolation.make({
              caseName: name,
              message: `Expected a value but found none: ${description}`,
            }),
          ),
  }).pipe(Effect.withSpan(`SubmissionLedgerConformance.${name}`)),
});

const admissionIdempotency = conformanceCase(
  "replays identical admissions and rejects conflicting input digests",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-admission");
      const ledger = yield* SubmissionLedger;

      const request = yield* admissionRequest(conversationId, "admission-key-1", {
        city: "Lisbon",
      });
      const first = yield* ledger.admit(request);
      yield* ensure(!first.replayed, "The first admission of a key must not report replayed");
      yield* ensure(first.state === "admitted", "A fresh admission must start in state admitted");

      const afterAdmit = yield* expectSome(
        "lookup immediately after admission",
        yield* lookupById(first.submissionId),
      );
      yield* ensure(
        afterAdmit.inputDigest === request.inputDigest && afterAdmit.state === "admitted",
        "Admission must be readable with strong consistency immediately after the write",
      );

      const replayed = yield* ledger.admit(request);
      yield* ensure(
        replayed.replayed &&
          replayed.submissionId === first.submissionId &&
          replayed.receiptId === first.receiptId &&
          replayed.queueSequence === first.queueSequence,
        "An identical admission must replay the original identities with replayed set",
      );

      const conflicting = yield* admissionRequest(conversationId, "admission-key-1", {
        city: "Porto",
      });
      const conflict = yield* expectFailure(
        "an admission reusing the key with different canonical input",
        ledger.admit(conflicting),
      );
      yield* ensure(
        conflict instanceof AdmissionConflict &&
          conflict.existingInputDigest === request.inputDigest &&
          conflict.attemptedInputDigest === conflicting.inputDigest,
        "A same-key admission with a different digest must fail with both digests reported",
      );

      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: first.submissionId }));
      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: first.submissionId }));
      const ready = yield* expectSome(
        "lookup after readiness",
        yield* lookupById(first.submissionId),
      );
      yield* ensure(
        ready.state === "ready" && ready.readyAt !== undefined,
        "markReady must be idempotent and record readiness exactly once",
      );

      const retryAfterReady = yield* ledger.admit(request);
      yield* ensure(
        retryAfterReady.replayed && retryAfterReady.receiptId === first.receiptId,
        "A client retry after readiness must return the original Receipt",
      );

      const second = yield* ledger.admit(
        yield* admissionRequest(conversationId, "admission-key-2", { city: "Faro" }),
      );
      yield* ensure(
        second.submissionId !== first.submissionId && second.queueSequence !== first.queueSequence,
        "A different key on the same lane must mint fresh identities and a fresh queue sequence",
      );
    }),
);

const concurrentAdmissionFifo = conformanceCase(
  "allocates distinct FIFO queue sequences under concurrent admission and claims in order",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-fifo-concurrent");
      const ledger = yield* SubmissionLedger;

      const requests = [
        yield* admissionRequest(conversationId, "fifo-key-0", { step: 0 }),
        yield* admissionRequest(conversationId, "fifo-key-1", { step: 1 }),
        yield* admissionRequest(conversationId, "fifo-key-2", { step: 2 }),
      ];
      const admitted = yield* Effect.all(
        requests.map((request) => ledger.admit(request)),
        { concurrency: "unbounded" },
      );

      const duplicate = yield* admissionRequest(conversationId, "fifo-key-dup", { step: 3 });
      const duplicated = yield* Effect.all([ledger.admit(duplicate), ledger.admit(duplicate)], {
        concurrency: "unbounded",
      });
      yield* ensure(
        duplicated[0].submissionId === duplicated[1].submissionId &&
          duplicated[0].receiptId === duplicated[1].receiptId &&
          duplicated[0].queueSequence === duplicated[1].queueSequence,
        "Concurrent duplicate admissions must resolve to one Submission and one Receipt",
      );
      yield* ensure(
        duplicated.filter((result) => !result.replayed).length === 1,
        "Exactly one of two concurrent duplicate admissions must create the Submission",
      );

      const lane = [...admitted, duplicated[0]];
      yield* ensure(
        new Set(lane.map((result) => result.queueSequence)).size === lane.length,
        "Concurrent admissions on one lane must allocate distinct queue sequences",
      );
      yield* ensure(
        new Set(lane.map((result) => result.submissionId)).size === lane.length,
        "Concurrent admissions on one lane must mint distinct Submission identities",
      );

      yield* Effect.forEach(
        lane,
        (result) => ledger.markReady(MarkReadyRequest.make({ submissionId: result.submissionId })),
        { discard: true },
      );

      const expectedOrder = [...lane].sort((a, b) => a.queueSequence - b.queueSequence);
      for (const expected of expectedOrder) {
        const claim = yield* expectSome(
          `a claim while ${expected.submissionId} heads the lane`,
          yield* claimLane(conversationId, PRODUCER_A),
        );
        yield* ensure(
          claim.submissionId === expected.submissionId,
          "Claims must deliver Submissions in ascending queue-sequence order",
        );
        yield* settleClaimed(expected, claim.ownershipToken);
      }
      yield* ensure(
        Option.isNone(yield* claimLane(conversationId, PRODUCER_A)),
        "A fully settled lane must produce no claim",
      );
    }),
);

const fifoHeadClaim = conformanceCase(
  "claims only the lowest unsettled head and fences the lane epoch forward",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const emptyLane = decodeConversationId("ledger-conformance-empty");
      yield* ensure(
        Option.isNone(yield* claimLane(emptyLane, PRODUCER_A)),
        "Claiming a lane with no admitted work must return none",
      );

      const conversationId = decodeConversationId("ledger-conformance-head");
      const first = yield* admitReady(conversationId, "head-key-1", { order: 1 });
      const second = yield* admitReady(conversationId, "head-key-2", { order: 2 });
      yield* ensure(
        second.queueSequence > first.queueSequence,
        "Sequential admissions must allocate increasing queue sequences",
      );

      const headClaim = yield* expectSome(
        "the first claim on the lane",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      yield* ensure(
        headClaim.submissionId === first.submissionId,
        "A claim must take the lowest unsettled queue sequence, never later work",
      );
      const running = yield* expectSome(
        "lookup of the claimed head",
        yield* lookupById(first.submissionId),
      );
      yield* ensure(
        running.state === "running",
        "Claiming a ready head must transition it to running with strong read visibility",
      );
      yield* ensure(
        Option.isNone(yield* claimLane(conversationId, PRODUCER_B)),
        "A lane whose head lease is live under another owner must produce no claim",
      );

      yield* settleClaimed(first, headClaim.ownershipToken);
      const nextClaim = yield* expectSome(
        "the claim after the head settled",
        yield* claimLane(conversationId, PRODUCER_B),
      );
      yield* ensure(
        nextClaim.submissionId === second.submissionId,
        "Settling the head must make exactly the next queue sequence claimable",
      );
      yield* ensure(
        nextClaim.producerEpoch > headClaim.producerEpoch,
        "Every successful claim must bump the Conversation producer epoch",
      );
      yield* settleClaimed(second, nextClaim.ownershipToken);
      yield* ensure(
        Option.isNone(yield* claimLane(conversationId, PRODUCER_A)),
        "A drained lane must produce no claim",
      );
    }),
);

const leaseExpiryReclaim = conformanceCase(
  "reclaims an expired lease with a higher epoch and fences the stale token",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-lease");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "lease-key-1", { work: "lease" });

      const firstClaim = yield* expectSome(
        "the initial claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      const renewal = yield* ledger.renewOwnership(
        RenewOwnershipRequest.make({
          submissionId: admitted.submissionId,
          ownershipToken: firstClaim.ownershipToken,
        }),
      );
      yield* ensure(
        DateTime.toEpochMillis(renewal.leaseExpiresAt) >=
          DateTime.toEpochMillis(firstClaim.leaseExpiresAt),
        "Renewal must never shorten the ownership lease",
      );
      const liveToken = renewal.ownershipToken;
      yield* ensure(
        Option.isNone(yield* claimLane(conversationId, PRODUCER_B)),
        "A renewed live lease must keep the lane blocked for other owners",
      );

      yield* advancePastLease(renewal.leaseExpiresAt);
      const reclaim = yield* expectSome(
        "the reclaim after lease expiry",
        yield* claimLane(conversationId, PRODUCER_B),
      );
      yield* ensure(
        reclaim.submissionId === admitted.submissionId &&
          reclaim.attemptId !== firstClaim.attemptId &&
          reclaim.ownershipToken !== liveToken,
        "Reclaiming an expired lease must start a fresh Attempt with a fresh token",
      );
      yield* ensure(
        reclaim.producerEpoch > firstClaim.producerEpoch,
        "Reclaiming an expired lease must bump the producer epoch past the stale Attempt",
      );

      const staleRenew = yield* expectFailure(
        "renewing with the superseded token",
        ledger.renewOwnership(
          RenewOwnershipRequest.make({
            submissionId: admitted.submissionId,
            ownershipToken: liveToken,
          }),
        ),
      );
      yield* ensure(
        staleRenew instanceof OwnershipLost && staleRenew.actualEpoch === reclaim.producerEpoch,
        "A superseded token must fail renewal with the current epoch reported",
      );
      const staleRelease = yield* expectFailure(
        "releasing with the superseded token",
        ledger.releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: admitted.submissionId,
            ownershipToken: liveToken,
          }),
        ),
      );
      yield* ensure(
        staleRelease instanceof OwnershipLost,
        "A superseded token must not release the lane",
      );
      const staleMark = yield* expectFailure(
        "marking input applied with the superseded token",
        ledger.markInputApplied(
          MarkInputAppliedRequest.make({
            submissionId: admitted.submissionId,
            ownershipToken: liveToken,
            recordId: submissionInputRecordId(admitted.submissionId),
            sequence: decodeSequence(1),
          }),
        ),
      );
      yield* ensure(
        staleMark instanceof OwnershipLost,
        "A superseded token must not mark canonical input applied",
      );
      const staleReservation = yield* settlementReservation({
        submissionId: admitted.submissionId,
        ownershipToken: liveToken,
        receiptId: admitted.receiptId,
        outcome: "completed",
      });
      const staleReserve = yield* expectFailure(
        "reserving a settlement with the superseded token",
        ledger.reserveSettlement(staleReservation),
      );
      yield* ensure(
        staleReserve instanceof OwnershipLost,
        "A superseded token must not reserve a settlement",
      );
      const afterStale = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        afterStale.reservation === undefined,
        "A fenced settlement reservation must leave no reservation behind",
      );

      yield* ledger.renewOwnership(
        RenewOwnershipRequest.make({
          submissionId: admitted.submissionId,
          ownershipToken: reclaim.ownershipToken,
        }),
      );

      const renewalLane = decodeConversationId("ledger-conformance-renewal");
      const renewalWork = yield* admitReady(renewalLane, "renewal-key-1", { work: "renewal" });
      const renewalClaim = yield* expectSome(
        "the claim on the renewal lane",
        yield* claimLane(renewalLane, PRODUCER_A),
      );
      yield* advancePastLease(renewalClaim.leaseExpiresAt);
      const lateRenewal = yield* ledger.renewOwnership(
        RenewOwnershipRequest.make({
          submissionId: renewalWork.submissionId,
          ownershipToken: renewalClaim.ownershipToken,
        }),
      );
      yield* ensure(
        DateTime.toEpochMillis(lateRenewal.leaseExpiresAt) >
          DateTime.toEpochMillis(renewalClaim.leaseExpiresAt),
        "Renewal after expiry must succeed and extend the lease when nobody claimed in between",
      );
    }),
);

const releaseMakesHeadClaimable = conformanceCase(
  "releases ownership gracefully so the head is immediately claimable",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-release");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "release-key-1", { work: "release" });

      const firstClaim = yield* expectSome(
        "the initial claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      yield* ledger.releaseOwnership(
        ReleaseOwnershipRequest.make({
          submissionId: admitted.submissionId,
          ownershipToken: firstClaim.ownershipToken,
        }),
      );

      const releasedRenew = yield* expectFailure(
        "renewing a released token",
        ledger.renewOwnership(
          RenewOwnershipRequest.make({
            submissionId: admitted.submissionId,
            ownershipToken: firstClaim.ownershipToken,
          }),
        ),
      );
      yield* ensure(
        releasedRenew instanceof OwnershipLost,
        "A released token must no longer renew the lease",
      );

      const reclaim = yield* expectSome(
        "the claim immediately after release",
        yield* claimLane(conversationId, PRODUCER_B),
      );
      yield* ensure(
        reclaim.submissionId === admitted.submissionId &&
          reclaim.producerEpoch > firstClaim.producerEpoch,
        "A released nonterminal head must be claimable immediately with a higher epoch",
      );
      const doubleRelease = yield* expectFailure(
        "releasing with the pre-release token after a new claim",
        ledger.releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: admitted.submissionId,
            ownershipToken: firstClaim.ownershipToken,
          }),
        ),
      );
      yield* ensure(
        doubleRelease instanceof OwnershipLost,
        "A superseded token must not release the new Attempt's ownership",
      );
      yield* settleClaimed(admitted, reclaim.ownershipToken);
    }),
);

const inputAppliedIdempotency = conformanceCase(
  "marks canonical input applied idempotently under the owning token",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-input");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "input-key-1", { work: "input" });
      const claim = yield* expectSome(
        "the claim before input apply",
        yield* claimLane(conversationId, PRODUCER_A),
      );

      const marker = MarkInputAppliedRequest.make({
        submissionId: admitted.submissionId,
        ownershipToken: claim.ownershipToken,
        recordId: submissionInputRecordId(admitted.submissionId),
        sequence: decodeSequence(2),
      });
      yield* ledger.markInputApplied(marker);
      const applied = yield* expectSome(
        "lookup after marking input applied",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        applied.state === "input-applied",
        "Marking input applied must transition the Submission to input-applied",
      );

      yield* ledger.markInputApplied(marker);
      const snapshot = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        snapshot.inputApplied !== undefined &&
          snapshot.inputApplied.recordId === marker.recordId &&
          snapshot.inputApplied.sequence === marker.sequence &&
          snapshot.submission.state === "input-applied",
        "Repeating the identical input-applied marker must be a no-op with the marker retained",
      );
    }),
);

const settlementLifecycle = conformanceCase(
  "reserves and finalizes exactly one settlement with idempotent replays",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-settle");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "settle-key-1", { work: "settle" });
      const claim = yield* expectSome(
        "the claim before terminalization",
        yield* claimLane(conversationId, PRODUCER_A),
      );

      const reservation = yield* settlementReservation({
        submissionId: admitted.submissionId,
        ownershipToken: claim.ownershipToken,
        receiptId: admitted.receiptId,
        outcome: "completed",
        result: { answer: 42 },
      });
      const reserved = yield* ledger.reserveSettlement(reservation);
      yield* ensure(
        !reserved.replayed &&
          reserved.settlementId === reservation.settlementId &&
          reserved.outcome === "completed" &&
          reserved.recordDigest === reservation.recordDigest,
        "The first reservation must commit the exact reserved record without replay",
      );
      const terminalizing = yield* expectSome(
        "lookup after reservation",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        terminalizing.state === "terminalizing",
        "Reserving a settlement must transition the Submission to terminalizing",
      );

      const replayedReservation = yield* ledger.reserveSettlement(reservation);
      yield* ensure(
        replayedReservation.replayed &&
          replayedReservation.recordDigest === reservation.recordDigest,
        "An identical reservation must replay with the stored exact record",
      );

      const finalization = SettlementFinalization.make({
        submissionId: admitted.submissionId,
        settlementId: reservation.settlementId,
      });
      const settlement = yield* ledger.finalizeSettlement(finalization);
      yield* ensure(
        settlement.submissionId === admitted.submissionId &&
          settlement.settlementId === reservation.settlementId &&
          settlement.receiptId === admitted.receiptId &&
          settlement.outcome === "completed",
        "Finalization must return the Settlement bound to the admission Receipt",
      );
      const settled = yield* expectSome(
        "lookup after finalization",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        settled.state === "settled" && settled.settledOutcome === "completed",
        "Finalization must settle the Submission with its recorded outcome",
      );

      yield* TestClock.adjust("1 second");
      const retried = yield* ledger.finalizeSettlement(finalization);
      yield* ensure(
        retried.settlementId === settlement.settlementId &&
          retried.receiptId === settlement.receiptId &&
          retried.outcome === settlement.outcome &&
          sameInstant(retried.settledAt, settlement.settledAt),
        "Retrying finalization after a lost acknowledgment must return the same Settlement",
      );
      yield* ensure(
        Option.isNone(yield* claimLane(conversationId, PRODUCER_B)),
        "A settled lane with no further work must produce no claim",
      );
    }),
);

const settlementConflicts = conformanceCase(
  "rejects conflicting settlement reservations and finalizations",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-settle-conflict");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "conflict-key-1", { work: "conflict" });
      const claim = yield* expectSome(
        "the claim before terminalization",
        yield* claimLane(conversationId, PRODUCER_A),
      );

      const reservation = yield* settlementReservation({
        submissionId: admitted.submissionId,
        ownershipToken: claim.ownershipToken,
        receiptId: admitted.receiptId,
        outcome: "completed",
        result: { attempt: 1 },
      });
      yield* ledger.reserveSettlement(reservation);

      const conflictingOutcome = yield* settlementReservation({
        submissionId: admitted.submissionId,
        ownershipToken: claim.ownershipToken,
        receiptId: admitted.receiptId,
        outcome: "failed",
        result: { attempt: 1 },
      });
      const outcomeConflict = yield* expectFailure(
        "reserving a different outcome for the same Submission",
        ledger.reserveSettlement(conflictingOutcome),
      );
      yield* ensure(
        outcomeConflict instanceof SettlementConflict &&
          outcomeConflict.existingOutcome === "completed",
        "A second reservation with a different outcome must conflict with the recorded outcome",
      );

      const conflictingContent = yield* settlementReservation({
        submissionId: admitted.submissionId,
        ownershipToken: claim.ownershipToken,
        receiptId: admitted.receiptId,
        outcome: "completed",
        result: { attempt: 2 },
      });
      const contentConflict = yield* expectFailure(
        "reserving the same outcome with different canonical content",
        ledger.reserveSettlement(conflictingContent),
      );
      yield* ensure(
        contentConflict instanceof SettlementConflict &&
          contentConflict.existingOutcome === "completed",
        "A second reservation with different content must conflict even when outcomes match",
      );

      const wrongFinalization = yield* expectFailure(
        "finalizing with a settlement identity that was never reserved",
        ledger.finalizeSettlement(
          SettlementFinalization.make({
            submissionId: admitted.submissionId,
            settlementId: submissionSettlementId(
              decodeSubmissionId(`${admitted.submissionId}-other`),
            ),
          }),
        ),
      );
      yield* ensure(
        wrongFinalization instanceof SettlementConflict &&
          wrongFinalization.existingOutcome === "completed",
        "Finalization disagreeing with the reserved settlement must conflict",
      );

      const settlement = yield* ledger.finalizeSettlement(
        SettlementFinalization.make({
          submissionId: admitted.submissionId,
          settlementId: reservation.settlementId,
        }),
      );
      yield* ensure(
        settlement.outcome === "completed",
        "The originally reserved outcome must remain the one that settles",
      );
    }),
);

const abortIdempotency = conformanceCase(
  "records abort intent idempotently and refuses to abort settled work",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-abort");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* ledger.admit(
        yield* admissionRequest(conversationId, "abort-key-1", { work: "abort" }),
      );

      const intent = yield* ledger.requestAbort(
        AbortCommand.make({
          submissionId: admitted.submissionId,
          author: "conformance-operator",
          reason: "first abort request",
        }),
      );
      yield* TestClock.adjust("1 second");
      const repeated = yield* ledger.requestAbort(
        AbortCommand.make({
          submissionId: admitted.submissionId,
          author: "conformance-operator",
          reason: "second abort request",
        }),
      );
      yield* ensure(
        repeated.reason === intent.reason &&
          repeated.author === intent.author &&
          sameInstant(repeated.requestedAt, intent.requestedAt),
        "Repeating an abort command must return the recorded intent unchanged",
      );
      const snapshot = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        snapshot.abortIntent !== undefined && snapshot.abortIntent.reason === intent.reason,
        "The recovery snapshot must expose the recorded abort intent",
      );

      const settledLane = decodeConversationId("ledger-conformance-abort-settled");
      const settledWork = yield* admitReady(settledLane, "abort-key-2", { work: "settled" });
      const claim = yield* expectSome(
        "the claim on the settled lane",
        yield* claimLane(settledLane, PRODUCER_A),
      );
      yield* settleClaimed(settledWork, claim.ownershipToken);
      const terminalAbort = yield* expectFailure(
        "aborting a settled Submission",
        ledger.requestAbort(
          AbortCommand.make({
            submissionId: settledWork.submissionId,
            author: "conformance-operator",
            reason: "too late",
          }),
        ),
      );
      yield* ensure(
        terminalAbort instanceof SettlementConflict &&
          terminalAbort.existingOutcome === "completed",
        "Abort must never rewrite a terminal outcome",
      );
    }),
);

const scanNonterminalWorklist = conformanceCase(
  "scans exactly the unsettled work in lane order",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const laneA = decodeConversationId("ledger-conformance-scan-a");
      const laneB = decodeConversationId("ledger-conformance-scan-b");
      const ledger = yield* SubmissionLedger;

      const first = yield* admitReady(laneA, "scan-key-1", { step: 1 });
      const second = yield* admitReady(laneA, "scan-key-2", { step: 2 });
      const third = yield* admitReady(laneA, "scan-key-3", { step: 3 });
      const settledElsewhere = yield* admitReady(laneB, "scan-key-4", { step: 4 });

      const firstClaim = yield* expectSome(
        "the claim on the first head",
        yield* claimLane(laneA, PRODUCER_A),
      );
      yield* settleClaimed(first, firstClaim.ownershipToken);
      yield* expectSome("the claim on the second head", yield* claimLane(laneA, PRODUCER_A));
      const otherClaim = yield* expectSome(
        "the claim on the other lane",
        yield* claimLane(laneB, PRODUCER_A),
      );
      yield* settleClaimed(settledElsewhere, otherClaim.ownershipToken);

      const scanned = yield* ledger.scanNonterminal.pipe(Stream.runCollect);
      yield* ensure(
        scanned.every((snapshot) => snapshot.state !== "settled"),
        "scanNonterminal must never emit settled Submissions",
      );
      const mine = scanned.filter(
        (snapshot) => snapshot.conversationId === laneA || snapshot.conversationId === laneB,
      );
      yield* ensure(
        mine.length === 2 &&
          mine.at(0)?.submissionId === second.submissionId &&
          mine.at(1)?.submissionId === third.submissionId,
        "scanNonterminal must emit exactly the unsettled Submissions in queue-sequence order",
      );
      yield* ensure(
        mine.at(0)?.state === "running" && mine.at(1)?.state === "ready",
        "scanNonterminal snapshots must carry the current Submission states",
      );
      const start = scanned.findIndex((snapshot) => snapshot.submissionId === second.submissionId);
      yield* ensure(
        start >= 0 && scanned.at(start + 1)?.submissionId === third.submissionId,
        "One lane's unsettled Submissions must be contiguous in (conversation, sequence) order",
      );
    }),
);

const lookupByIdAndKey = conformanceCase(
  "looks up submissions by identity and scoped idempotency key with strong reads",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-lookup");
      const ledger = yield* SubmissionLedger;
      const request = yield* admissionRequest(conversationId, "lookup-key-1", { city: "Kyoto" });
      const admitted = yield* ledger.admit(request);

      const byId = yield* expectSome(
        "lookup by Submission identity",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        byId.submissionId === admitted.submissionId &&
          byId.conversationId === conversationId &&
          byId.queueSequence === admitted.queueSequence &&
          byId.principal === request.principal &&
          byId.idempotencyKey === request.idempotencyKey &&
          byId.agentId === request.agentId &&
          byId.deploymentId === request.deploymentId &&
          byId.inputDigest === request.inputDigest &&
          byId.receiptId === admitted.receiptId &&
          byId.state === "admitted" &&
          byId.settledOutcome === undefined &&
          byId.readyAt === undefined,
        "Lookup by identity must return the full admission snapshot",
      );

      const byKey = yield* expectSome(
        "lookup by scoped idempotency key",
        yield* ledger.lookup(
          SubmissionLookupByKey.make({
            conversationId,
            principal: request.principal,
            idempotencyKey: request.idempotencyKey,
          }),
        ),
      );
      yield* ensure(
        byKey.submissionId === admitted.submissionId,
        "Lookup by key must resolve to the same Submission as lookup by identity",
      );

      yield* ensure(
        Option.isNone(
          yield* lookupById(decodeSubmissionId("submission-ledger-conformance-missing")),
        ),
        "Lookup of an unknown Submission identity must return none",
      );
      yield* ensure(
        Option.isNone(
          yield* ledger.lookup(
            SubmissionLookupByKey.make({
              conversationId,
              principal: request.principal,
              idempotencyKey: decodeIdempotencyKey("lookup-key-missing"),
            }),
          ),
        ),
        "Lookup of an unknown idempotency key must return none",
      );
      yield* ensure(
        Option.isNone(
          yield* ledger.lookup(
            SubmissionLookupByKey.make({
              conversationId,
              principal: OTHER_PRINCIPAL,
              idempotencyKey: request.idempotencyKey,
            }),
          ),
        ),
        "Idempotency keys must be scoped to their principal",
      );

      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
      const readyByKey = yield* expectSome(
        "lookup by key after readiness",
        yield* ledger.lookup(
          SubmissionLookupByKey.make({
            conversationId,
            principal: request.principal,
            idempotencyKey: request.idempotencyKey,
          }),
        ),
      );
      yield* ensure(
        readyByKey.state === "ready" && readyByKey.readyAt !== undefined,
        "Lookups must observe prior writes with strong consistency",
      );
    }),
);

const recoverySnapshotConsistency = conformanceCase(
  "loads a strongly consistent recovery snapshot without exposing the live token",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-recovery");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "recovery-key-1", { work: "recovery" });

      const initial = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        initial.submission.state === "ready" &&
          initial.ownership === undefined &&
          initial.inputApplied === undefined &&
          initial.reservation === undefined &&
          initial.abortIntent === undefined,
        "A ready, unclaimed Submission must have no ownership, marker, reservation, or intent",
      );

      const claim = yield* expectSome(
        "the claim before snapshotting",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      const owned = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        owned.ownership !== undefined &&
          owned.ownership.attemptId === claim.attemptId &&
          owned.ownership.producerEpoch === claim.producerEpoch &&
          owned.ownership.ownerProducerId === PRODUCER_A &&
          sameInstant(owned.ownership.leaseExpiresAt, claim.leaseExpiresAt),
        "The recovery snapshot must expose the live Attempt's identity, owner, epoch, and lease",
      );

      const marker = MarkInputAppliedRequest.make({
        submissionId: admitted.submissionId,
        ownershipToken: claim.ownershipToken,
        recordId: submissionInputRecordId(admitted.submissionId),
        sequence: decodeSequence(3),
      });
      yield* ledger.markInputApplied(marker);
      yield* ledger.requestAbort(
        AbortCommand.make({
          submissionId: admitted.submissionId,
          author: "conformance-operator",
          reason: "abort during recovery case",
        }),
      );
      const reservation = yield* settlementReservation({
        submissionId: admitted.submissionId,
        ownershipToken: claim.ownershipToken,
        receiptId: admitted.receiptId,
        outcome: "aborted",
      });
      yield* ledger.reserveSettlement(reservation);

      const reserved = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        reserved.inputApplied !== undefined &&
          reserved.inputApplied.recordId === marker.recordId &&
          reserved.inputApplied.sequence === marker.sequence,
        "The recovery snapshot must expose the applied-input marker",
      );
      yield* ensure(
        reserved.abortIntent !== undefined &&
          reserved.abortIntent.reason === "abort during recovery case",
        "The recovery snapshot must expose the abort intent",
      );
      yield* ensure(
        reserved.reservation !== undefined &&
          !reserved.reservation.finalized &&
          reserved.reservation.settlementId === reservation.settlementId &&
          reserved.reservation.outcome === "aborted" &&
          reserved.reservation.recordDigest === reservation.recordDigest,
        "The recovery snapshot must expose the unfinalized reservation with its exact record",
      );

      yield* ledger.finalizeSettlement(
        SettlementFinalization.make({
          submissionId: admitted.submissionId,
          settlementId: reservation.settlementId,
        }),
      );
      const settled = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        settled.submission.state === "settled" &&
          settled.submission.settledOutcome === "aborted" &&
          settled.reservation !== undefined &&
          settled.reservation.finalized,
        "After finalization the snapshot must show the settled state and finalized reservation",
      );
    }),
);

/**
 * The shared, adapter-parameterized SubmissionLedger contract suite (STORE-010). Every durable
 * ledger adapter test suite must execute each case against its own ledger provisioning, inside
 * the `@effect/vitest` test environment (TestClock) and with `Crypto.Crypto` provided.
 */
export const submissionLedgerConformanceCases: ReadonlyArray<SubmissionLedgerConformanceCase> = [
  admissionIdempotency,
  concurrentAdmissionFifo,
  fifoHeadClaim,
  leaseExpiryReclaim,
  releaseMakesHeadClaimable,
  inputAppliedIdempotency,
  settlementLifecycle,
  settlementConflicts,
  abortIdempotency,
  scanNonterminalWorklist,
  lookupByIdAndKey,
  recoverySnapshotConsistency,
];
