import { AgentId, ConversationId, ReceiptId, SubmissionId, ToolCallId } from "@effect-agent/core";
import { Clock, Crypto, DateTime, Duration, Effect, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";

import { digestJson, type DigestError } from "./digest.ts";
import {
  AbortCommand,
  AdmissionConflict,
  AdmissionRequest,
  AdmissionResult,
  ApprovalConflict,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  CanonicalSettlementRepair,
  ChildBudgetReservationRequest,
  ChildReservationConflict,
  ChildReservationId,
  ChildSettledNotification,
  ClaimJoiningRequest,
  ClaimRequest,
  IdempotencyKey,
  JoinedToHost,
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  OwnershipToken,
  ParentLinkage,
  Principal,
  RecoverySnapshotRequest,
  ResumeSuspensionRequest,
  ReleaseChildBudgetRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  ResolutionSafeToRetry,
  RevertJoiningRequest,
  SettlementConflict,
  SettlementFinalization,
  SettlementReservation,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  SuspendRequest,
  UnknownResolutionCommand,
  UnknownResolutionConflict,
  WaitingChild,
  WaitingForChildSuspension,
  submissionInputRecordId,
  submissionSettlementId,
  submissionSettlementRecordId,
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
export class SubmissionLedgerConformanceViolation extends Schema.TaggedError<SubmissionLedgerConformanceViolation>()(
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
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodeChildReservationId = Schema.decodeSync(ChildReservationId);

/** A token that never owned any lane, for ownership-fencing assertions. */
const BOGUS_TOKEN = Schema.decodeSync(OwnershipToken)("ownership-ledger-conformance-bogus");

const sameJson = (left: unknown, right: unknown): boolean =>
  JSON.stringify(left) === JSON.stringify(right);

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
  parentLinkage?: ParentLinkage,
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
    ...(parentLinkage === undefined ? {} : { parentLinkage }),
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

const crossPrincipalAdmissionScoping = conformanceCase(
  "scopes idempotency keys to their principal: a second principal reusing a key mints a distinct Submission",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      // SEC-002 (security-operations §2): "Client idempotency keys are scoped so one principal
      // cannot discover or collide with another principal's submission." This is the COLLIDE
      // half — a second principal reusing an existing (conversation, idempotency key) must not
      // replay, overwrite, or conflict with the first principal's Submission, and must not learn
      // its identity. (The DISCOVER half — scoped lookup/resolveAdmission — is covered by
      // `lookupByIdAndKey` and `resolveAdmissionAuthority`.)
      const conversationId = decodeConversationId("ledger-conformance-cross-principal");
      const ledger = yield* SubmissionLedger;
      const idempotencyKey = decodeIdempotencyKey("shared-key-1");

      const mineRequest = yield* admissionRequest(conversationId, "shared-key-1", {
        owner: "mine",
      });
      const mine = yield* ledger.admit(mineRequest);

      // A DIFFERENT principal admits the SAME (conversation, key) with different canonical input.
      // It is neither a replay nor an AdmissionConflict: keys are principal-scoped, so this mints
      // a fresh, distinct Submission with its own identities and queue position.
      const theirsRequest = AdmissionRequest.make({
        conversationId,
        principal: OTHER_PRINCIPAL,
        idempotencyKey,
        agentId: CONFORMANCE_AGENT,
        agentDigests: CONFORMANCE_DIGESTS,
        deploymentId: CONFORMANCE_DEPLOYMENT,
        inputPayload: { owner: "theirs" },
        inputDigest: yield* digestJson({ owner: "theirs" }),
      });
      const theirs = yield* ledger.admit(theirsRequest);
      yield* ensure(
        !theirs.replayed &&
          theirs.submissionId !== mine.submissionId &&
          theirs.receiptId !== mine.receiptId &&
          theirs.queueSequence !== mine.queueSequence,
        "A second principal reusing a key must mint a fresh, distinct Submission — never a replay or collision",
      );

      // The first principal's own replay is unaffected by the second principal's admission.
      const mineReplay = yield* ledger.admit(mineRequest);
      yield* ensure(
        mineReplay.replayed && mineReplay.submissionId === mine.submissionId,
        "The original principal's replay must still resolve to its own Submission after the other principal admitted",
      );

      // Each principal's scoped lookup resolves ONLY its own Submission — no cross-principal
      // discovery through the shared key.
      const mineByKey = yield* expectSome(
        "the first principal's scoped lookup",
        yield* ledger.lookup(
          SubmissionLookupByKey.make({
            conversationId,
            principal: CONFORMANCE_PRINCIPAL,
            idempotencyKey,
          }),
        ),
      );
      const theirsByKey = yield* expectSome(
        "the second principal's scoped lookup",
        yield* ledger.lookup(
          SubmissionLookupByKey.make({
            conversationId,
            principal: OTHER_PRINCIPAL,
            idempotencyKey,
          }),
        ),
      );
      yield* ensure(
        mineByKey.submissionId === mine.submissionId &&
          theirsByKey.submissionId === theirs.submissionId &&
          mineByKey.submissionId !== theirsByKey.submissionId,
        "Each principal's scoped lookup must resolve only its own Submission under the shared key",
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

const canonicalSettlementRepair = conformanceCase(
  "repairs missing ledger settlement state from the exact canonical record",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-canonical-repair");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "canonical-repair-key", {
        work: "canonical-repair",
      });
      const claim = yield* expectSome(
        "the claim before the stale suspension",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      const suspended = yield* ledger.suspend(
        SuspendRequest.make({
          submissionId: admitted.submissionId,
          ownershipToken: claim.ownershipToken,
          reason: ApprovalPendingSuspension.make({
            toolCallIds: [decodeToolCallId("call-canonical-repair-stale-suspension")],
          }),
        }),
      );
      yield* ensure(
        suspended === "suspended",
        "The repair fixture must begin with stale nonterminal suspension state",
      );
      const canonical = yield* settlementReservation({
        submissionId: admitted.submissionId,
        ownershipToken: BOGUS_TOKEN,
        receiptId: admitted.receiptId,
        outcome: "failed",
        result: { errorTag: "RecoveredCanonicalFailure" },
      });
      const request = CanonicalSettlementRepair.make({
        submissionId: admitted.submissionId,
        record: canonical.record,
        recordDigest: canonical.recordDigest,
      });
      const repaired = yield* ledger.repairSettlementFromCanonical(request);
      yield* ensure(
        repaired.submissionId === admitted.submissionId &&
          repaired.settlementId === canonical.settlementId &&
          repaired.receiptId === admitted.receiptId &&
          repaired.outcome === "failed",
        "Canonical repair must reconstruct the exact Settlement without a prior reservation",
      );
      const repairedSnapshot = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        repairedSnapshot.submission.state === "settled" &&
          repairedSnapshot.submission.settledOutcome === "failed" &&
          repairedSnapshot.reservation?.recordDigest === canonical.recordDigest &&
          repairedSnapshot.suspension === undefined,
        "Canonical repair must atomically reconstruct reservation columns, settle the row, and clear stale suspension state",
      );
      yield* TestClock.adjust("1 second");
      const replayed = yield* ledger.repairSettlementFromCanonical(request);
      yield* ensure(
        sameInstant(replayed.settledAt, repaired.settledAt),
        "Replaying the same canonical repair must preserve the original Settlement timestamp",
      );

      const unknownConversation = decodeConversationId(
        "ledger-conformance-canonical-repair-unknown",
      );
      const unknown = yield* admitReady(unknownConversation, "canonical-repair-unknown-key", {
        work: "canonical-repair-unknown",
      });
      yield* expectSome(
        "the claim before the unknown mark",
        yield* claimLane(unknownConversation, PRODUCER_A),
      );
      yield* ledger.markUnknown(
        MarkUnknownRequest.make({
          submissionId: unknown.submissionId,
          toolCallIds: [decodeToolCallId("call-canonical-repair-unknown")],
          reason: "the external effect may have completed",
        }),
      );
      const unknownBeforeRepair = yield* recoverySnapshot(unknown.submissionId);
      yield* ensure(
        unknownBeforeRepair.submission.state === "unknown",
        "The unknown repair fixture must begin in the blocked Unknown Outcome state",
      );
      const unknownCanonical = yield* settlementReservation({
        submissionId: unknown.submissionId,
        ownershipToken: BOGUS_TOKEN,
        receiptId: unknown.receiptId,
        outcome: "completed",
      });
      yield* ledger.repairSettlementFromCanonical(
        CanonicalSettlementRepair.make({
          submissionId: unknown.submissionId,
          record: unknownCanonical.record,
          recordDigest: unknownCanonical.recordDigest,
        }),
      );
      const unknownAfterRepair = yield* recoverySnapshot(unknown.submissionId);
      yield* ensure(
        unknownAfterRepair.submission.state === "settled" &&
          unknownAfterRepair.submission.settledOutcome === "completed",
        "Canonical repair must replace blocked Unknown operational state with the exact canonical settlement",
      );

      const tamperConversation = decodeConversationId("ledger-conformance-canonical-repair-tamper");
      const tamper = yield* admitReady(tamperConversation, "canonical-repair-tamper-key", {
        work: "canonical-repair-tamper",
      });
      const tamperCanonical = yield* settlementReservation({
        submissionId: tamper.submissionId,
        ownershipToken: BOGUS_TOKEN,
        receiptId: tamper.receiptId,
        outcome: "completed",
      });
      const invalid = yield* expectFailure(
        "repairing from a record whose supplied digest is not canonical",
        ledger.repairSettlementFromCanonical(
          CanonicalSettlementRepair.make({
            submissionId: tamper.submissionId,
            record: tamperCanonical.record,
            recordDigest: CONFORMANCE_DEFINITION_DIGEST,
          }),
        ),
      );
      yield* ensure(
        invalid instanceof LedgerError,
        "A tampered canonical repair must fail as a typed LedgerError",
      );
      const encodedCanonicalRecord = yield* Schema.encodeEffect(RecordEnvelope)(
        tamperCanonical.record,
      ).pipe(Effect.orDie);
      const wrongFamilyRecord = { ...encodedCanonicalRecord, family: "artifact" };
      const wrongFamilyDigest = yield* digestJson(wrongFamilyRecord);
      const wrongFamily = yield* expectFailure(
        "repairing from a wrong-family record whose supplied digest matches its exact envelope",
        ledger.repairSettlementFromCanonical(
          CanonicalSettlementRepair.make({
            submissionId: tamper.submissionId,
            record: wrongFamilyRecord,
            recordDigest: wrongFamilyDigest,
          }),
        ),
      );
      yield* ensure(
        wrongFamily instanceof LedgerError,
        "A valid digest cannot make a foreign record family canonical settlement authority",
      );
      const untouched = yield* expectSome(
        "the tampered repair target remains admitted",
        yield* lookupById(tamper.submissionId),
      );
      yield* ensure(
        untouched.state === "ready" && untouched.settledOutcome === undefined,
        "Canonical validation must fail before any ledger mutation",
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

const joiningPrefixClaim = conformanceCase(
  "claims a contiguous joining prefix and stops at a gap",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-joining-prefix");
      const ledger = yield* SubmissionLedger;

      const host = yield* admitReady(conversationId, "join-prefix-host", { work: "host" });
      const second = yield* admitReady(conversationId, "join-prefix-2", { queued: 2 });
      const third = yield* admitReady(conversationId, "join-prefix-3", { queued: 3 });
      // Admitted but never marked ready: the gap that breaks the contiguous prefix.
      const fourth = yield* ledger.admit(
        yield* admissionRequest(conversationId, "join-prefix-4", { queued: 4 }),
      );
      const fifth = yield* admitReady(conversationId, "join-prefix-5", { queued: 5 });

      const hostClaim = yield* expectSome(
        "the host claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      yield* ensure(
        hostClaim.submissionId === host.submissionId,
        "The host must head the lane before joining later work",
      );

      const foreign = yield* expectFailure(
        "claiming joining work without owning the host lane",
        ledger.claimJoining(
          ClaimJoiningRequest.make({
            conversationId,
            hostSubmissionId: host.submissionId,
            ownershipToken: BOGUS_TOKEN,
            maxCount: 8,
          }),
        ),
      );
      yield* ensure(
        foreign instanceof OwnershipLost,
        "claimJoining must be fenced by the host's ownership token",
      );

      const first = yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId,
          hostSubmissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          maxCount: 1,
        }),
      );
      yield* ensure(
        first.length === 1 &&
          first[0].submissionId === second.submissionId &&
          first[0].queueSequence === second.queueSequence &&
          sameJson(first[0].inputPayload, { queued: 2 }),
        "maxCount must bound the claim to exactly the next queued Submission with its input",
      );

      const rest = yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId,
          hostSubmissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          maxCount: 8,
        }),
      );
      yield* ensure(
        rest.length === 1 && rest[0].submissionId === third.submissionId,
        "A repeated claim must skip already-joining rows and stop at the admitted gap",
      );

      const blocked = yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId,
          hostSubmissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          maxCount: 8,
        }),
      );
      yield* ensure(
        blocked.length === 0,
        "An admitted-not-ready row must break the contiguous prefix and hide later ready work",
      );

      const secondState = yield* expectSome(
        "lookup of a claimed joining Submission",
        yield* lookupById(second.submissionId),
      );
      const fourthState = yield* expectSome(
        "lookup of the gap Submission",
        yield* lookupById(fourth.submissionId),
      );
      const fifthState = yield* expectSome(
        "lookup of the ready Submission after the gap",
        yield* lookupById(fifth.submissionId),
      );
      yield* ensure(
        secondState.state === "joining" &&
          fourthState.state === "admitted" &&
          fifthState.state === "ready",
        "Claiming must transition exactly the claimed prefix to joining",
      );

      const hostSnapshot = yield* recoverySnapshot(host.submissionId);
      yield* ensure(
        hostSnapshot.joins.length === 2 &&
          hostSnapshot.joins[0].submissionId === second.submissionId &&
          hostSnapshot.joins[0].state === "joining" &&
          hostSnapshot.joins[0].hostSubmissionId === host.submissionId &&
          hostSnapshot.joins[1].submissionId === third.submissionId,
        "The host recovery snapshot must expose the claimed prefix in queue order",
      );
      const joinedSide = yield* recoverySnapshot(second.submissionId);
      yield* ensure(
        joinedSide.hostSubmissionId === host.submissionId,
        "A joining Submission's recovery snapshot must expose its host linkage",
      );

      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: fourth.submissionId }));
      const afterGap = yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId,
          hostSubmissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          maxCount: 8,
        }),
      );
      yield* ensure(
        afterGap.length === 2 &&
          afterGap[0].submissionId === fourth.submissionId &&
          afterGap[1].submissionId === fifth.submissionId,
        "Once the gap closes, the claim must extend past already-claimed rows in queue order",
      );
    }),
);

const revertJoiningReturnsToReady = conformanceCase(
  "revertJoining returns exactly the pre-append claims to ready",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-revert-joining");
      const ledger = yield* SubmissionLedger;

      const host = yield* admitReady(conversationId, "revert-host", { work: "host" });
      const second = yield* admitReady(conversationId, "revert-2", { queued: 2 });
      const third = yield* admitReady(conversationId, "revert-3", { queued: 3 });
      const hostClaim = yield* expectSome(
        "the host claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );

      const claims = yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId,
          hostSubmissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          maxCount: 8,
        }),
      );
      yield* ensure(claims.length === 2, "Both queued Submissions must join the host's prefix");

      // third's canonical input is appended; second's never is.
      yield* ledger.markJoined(
        MarkJoinedRequest.make({
          submissionId: third.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          recordId: submissionInputRecordId(third.submissionId),
          sequence: decodeSequence(7),
        }),
      );

      yield* ledger.revertJoining(RevertJoiningRequest.make({ submissionId: second.submissionId }));
      const reverted = yield* expectSome(
        "lookup after revert",
        yield* lookupById(second.submissionId),
      );
      yield* ensure(
        reverted.state === "ready",
        "Reverting a pre-append joining Submission must return it to ready",
      );
      const revertedSnapshot = yield* recoverySnapshot(second.submissionId);
      yield* ensure(
        revertedSnapshot.hostSubmissionId === undefined,
        "Reverting must clear the host linkage",
      );
      const hostSnapshot = yield* recoverySnapshot(host.submissionId);
      yield* ensure(
        hostSnapshot.joins.length === 1 &&
          hostSnapshot.joins[0].submissionId === third.submissionId &&
          hostSnapshot.joins[0].state === "joined",
        "The reverted Submission must leave the host's join view",
      );

      yield* ledger.revertJoining(RevertJoiningRequest.make({ submissionId: second.submissionId }));
      const stillReady = yield* expectSome(
        "lookup after repeating the revert",
        yield* lookupById(second.submissionId),
      );
      yield* ensure(stillReady.state === "ready", "Repeating revertJoining must be a no-op");

      // A post-append joined Submission must NOT revert (DUR-016: it reattaches instead).
      yield* ledger.revertJoining(RevertJoiningRequest.make({ submissionId: third.submissionId }));
      const joined = yield* expectSome(
        "lookup of the joined Submission after an attempted revert",
        yield* lookupById(third.submissionId),
      );
      yield* ensure(
        joined.state === "joined",
        "revertJoining must be a no-op for an already-joined Submission",
      );

      const reclaimed = yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId,
          hostSubmissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          maxCount: 8,
        }),
      );
      yield* ensure(
        reclaimed.length === 1 && reclaimed[0].submissionId === second.submissionId,
        "A reverted Submission must be claimable again exactly once",
      );
    }),
);

const markJoinedIdempotency = conformanceCase(
  "markJoined is idempotent and repairable from history",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-mark-joined");
      const ledger = yield* SubmissionLedger;

      const host = yield* admitReady(conversationId, "mark-joined-host", { work: "host" });
      const second = yield* admitReady(conversationId, "mark-joined-2", { queued: 2 });
      const third = yield* admitReady(conversationId, "mark-joined-3", { queued: 3 });
      const hostClaim = yield* expectSome(
        "the host claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId,
          hostSubmissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          maxCount: 8,
        }),
      );

      const marker = MarkJoinedRequest.make({
        submissionId: second.submissionId,
        ownershipToken: hostClaim.ownershipToken,
        recordId: submissionInputRecordId(second.submissionId),
        sequence: decodeSequence(4),
      });
      yield* ledger.markJoined(marker);
      const joined = yield* expectSome(
        "lookup after marking joined",
        yield* lookupById(second.submissionId),
      );
      yield* ensure(
        joined.state === "joined",
        "Marking the canonical input position must transition joining to joined",
      );

      yield* ledger.markJoined(marker);
      const snapshot = yield* recoverySnapshot(second.submissionId);
      yield* ensure(
        snapshot.submission.state === "joined" &&
          snapshot.hostSubmissionId === host.submissionId &&
          snapshot.inputApplied !== undefined &&
          snapshot.inputApplied.recordId === marker.recordId &&
          snapshot.inputApplied.sequence === marker.sequence,
        "Repeating the identical join marker must be a no-op with the marker retained",
      );

      const divergent = yield* expectFailure(
        "re-marking with a different canonical position",
        ledger.markJoined(
          MarkJoinedRequest.make({
            submissionId: second.submissionId,
            ownershipToken: hostClaim.ownershipToken,
            recordId: marker.recordId,
            sequence: decodeSequence(5),
          }),
        ),
      );
      yield* ensure(
        !(divergent instanceof OwnershipLost),
        "A divergent join marker must fail as a ledger integrity error, not ownership loss",
      );

      const stale = yield* expectFailure(
        "marking joined without owning the host lane",
        ledger.markJoined(
          MarkJoinedRequest.make({
            submissionId: third.submissionId,
            ownershipToken: BOGUS_TOKEN,
            recordId: submissionInputRecordId(third.submissionId),
            sequence: decodeSequence(6),
          }),
        ),
      );
      yield* ensure(
        stale instanceof OwnershipLost,
        "markJoined must be fenced by the host's ownership",
      );

      // Repairable from history: the host Attempt dies and a later host Attempt repairs the
      // lost marker under its fresh ownership token (DUR-016).
      yield* ledger.releaseOwnership(
        ReleaseOwnershipRequest.make({
          submissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
        }),
      );
      const reclaim = yield* expectSome(
        "the host reclaim after release",
        yield* claimLane(conversationId, PRODUCER_B),
      );
      yield* ensure(
        reclaim.submissionId === host.submissionId &&
          reclaim.producerEpoch > hostClaim.producerEpoch,
        "The host must be reclaimable while its joined work is pending",
      );
      const superseded = yield* expectFailure(
        "repairing the marker with the superseded host token",
        ledger.markJoined(
          MarkJoinedRequest.make({
            submissionId: third.submissionId,
            ownershipToken: hostClaim.ownershipToken,
            recordId: submissionInputRecordId(third.submissionId),
            sequence: decodeSequence(6),
          }),
        ),
      );
      yield* ensure(
        superseded instanceof OwnershipLost,
        "A superseded host token must not repair a join marker",
      );
      yield* ledger.markJoined(
        MarkJoinedRequest.make({
          submissionId: third.submissionId,
          ownershipToken: reclaim.ownershipToken,
          recordId: submissionInputRecordId(third.submissionId),
          sequence: decodeSequence(6),
        }),
      );
      const repaired = yield* expectSome(
        "lookup after the repair",
        yield* lookupById(third.submissionId),
      );
      yield* ensure(
        repaired.state === "joined",
        "A later host Attempt must repair the lost join marker from history",
      );
    }),
);

const claimNeverGrantsBlockedHead = conformanceCase(
  "claim never grants a suspended, unknown, joining, or joined head",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;

      // joining head: the host settled while a claimed-but-unappended join was pending.
      const joiningLane = decodeConversationId("ledger-conformance-head-joining");
      const joiningHost = yield* admitReady(joiningLane, "head-joining-host", { work: "host" });
      const joiningSub = yield* admitReady(joiningLane, "head-joining-2", { queued: 2 });
      yield* admitReady(joiningLane, "head-joining-3", { queued: 3 });
      const joiningHostClaim = yield* expectSome(
        "the joining lane's host claim",
        yield* claimLane(joiningLane, PRODUCER_A),
      );
      yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId: joiningLane,
          hostSubmissionId: joiningHost.submissionId,
          ownershipToken: joiningHostClaim.ownershipToken,
          maxCount: 1,
        }),
      );
      yield* settleClaimed(joiningHost, joiningHostClaim.ownershipToken);
      yield* ensure(
        Option.isNone(yield* claimLane(joiningLane, PRODUCER_B)),
        "A joining head must block the lane; later ready work is never skipped past it",
      );
      yield* ledger.revertJoining(
        RevertJoiningRequest.make({ submissionId: joiningSub.submissionId }),
      );
      const afterRevert = yield* expectSome(
        "the claim after reverting the joining head",
        yield* claimLane(joiningLane, PRODUCER_B),
      );
      yield* ensure(
        afterRevert.submissionId === joiningSub.submissionId,
        "Reverting the joining head must make it claimable in FIFO order",
      );

      // joined head: the host settled between its finalization and the joined settlement.
      const joinedLane = decodeConversationId("ledger-conformance-head-joined");
      const joinedHost = yield* admitReady(joinedLane, "head-joined-host", { work: "host" });
      const joinedSub = yield* admitReady(joinedLane, "head-joined-2", { queued: 2 });
      const joinedHostClaim = yield* expectSome(
        "the joined lane's host claim",
        yield* claimLane(joinedLane, PRODUCER_A),
      );
      yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId: joinedLane,
          hostSubmissionId: joinedHost.submissionId,
          ownershipToken: joinedHostClaim.ownershipToken,
          maxCount: 1,
        }),
      );
      yield* ledger.markJoined(
        MarkJoinedRequest.make({
          submissionId: joinedSub.submissionId,
          ownershipToken: joinedHostClaim.ownershipToken,
          recordId: submissionInputRecordId(joinedSub.submissionId),
          sequence: decodeSequence(3),
        }),
      );
      const joinedAbort = yield* expectFailure(
        "aborting a joined Submission",
        ledger.requestAbort(
          AbortCommand.make({
            submissionId: joinedSub.submissionId,
            author: "conformance-operator",
            reason: "joined abort target",
          }),
        ),
      );
      yield* ensure(
        joinedAbort instanceof JoinedToHost &&
          joinedAbort.hostSubmissionId === joinedHost.submissionId,
        "Abort of a joined Submission must fail with the host linkage — the abort target is the host",
      );
      yield* settleClaimed(joinedHost, joinedHostClaim.ownershipToken);
      yield* ensure(
        Option.isNone(yield* claimLane(joinedLane, PRODUCER_B)),
        "A joined head must block the lane; it settles with the host, never with a worker claim",
      );

      // suspended head: durable approval waiting consumes no worker permit.
      const suspendedLane = decodeConversationId("ledger-conformance-head-suspended");
      const suspendedHost = yield* admitReady(suspendedLane, "head-suspended-host", {
        work: "host",
      });
      const suspendedClaim = yield* expectSome(
        "the suspended lane's claim",
        yield* claimLane(suspendedLane, PRODUCER_A),
      );
      const gatedCall = decodeToolCallId("call-head-suspended");
      const suspendOutcome = yield* ledger.suspend(
        SuspendRequest.make({
          submissionId: suspendedHost.submissionId,
          ownershipToken: suspendedClaim.ownershipToken,
          reason: ApprovalPendingSuspension.make({ toolCallIds: [gatedCall] }),
        }),
      );
      yield* ensure(suspendOutcome === "suspended", "An undecided approval must suspend durably");
      yield* ensure(
        Option.isNone(yield* claimLane(suspendedLane, PRODUCER_B)),
        "A suspended head must produce no claim",
      );
      yield* ledger.recordApprovalDecision(
        ApprovalDecisionCommand.make({
          submissionId: suspendedHost.submissionId,
          toolCallId: gatedCall,
          decision: "approved",
          resolver: "conformance-approver",
          reason: "unblock the suspended head",
        }),
      );
      yield* ensure(
        Option.isNone(yield* claimLane(suspendedLane, PRODUCER_B)),
        "An operational approval decision alone must not wake a suspended lane",
      );
      const resumed = yield* ledger.resumeSuspension(
        ResumeSuspensionRequest.make({
          submissionId: suspendedHost.submissionId,
          expectedReason: ApprovalPendingSuspension.make({ toolCallIds: [gatedCall] }),
        }),
      );
      yield* ensure(resumed === "resumed", "Exact covered approval evidence must resume the lane");
      const wokenClaim = yield* expectSome(
        "the claim after the covering decision",
        yield* claimLane(suspendedLane, PRODUCER_B),
      );
      yield* ensure(
        wokenClaim.submissionId === suspendedHost.submissionId &&
          wokenClaim.producerEpoch > suspendedClaim.producerEpoch,
        "A covering decision must wake the lane for a fresh fenced Attempt",
      );

      // unknown head: the DUR-017 blocked lane outlives lease expiry.
      const unknownLane = decodeConversationId("ledger-conformance-head-unknown");
      const unknownHost = yield* admitReady(unknownLane, "head-unknown-host", { work: "host" });
      const unknownClaim = yield* expectSome(
        "the unknown lane's claim",
        yield* claimLane(unknownLane, PRODUCER_A),
      );
      const unknownCall = decodeToolCallId("call-head-unknown");
      yield* ledger.markUnknown(
        MarkUnknownRequest.make({
          submissionId: unknownHost.submissionId,
          toolCallIds: [unknownCall],
          reason: "an ordinary call may have executed",
        }),
      );
      yield* advancePastLease(unknownClaim.leaseExpiresAt);
      yield* ensure(
        Option.isNone(yield* claimLane(unknownLane, PRODUCER_B)),
        "An unknown head must stay blocked even after the stale lease expires",
      );
      yield* ledger.recordUnknownResolution(
        UnknownResolutionCommand.make({
          submissionId: unknownHost.submissionId,
          toolCallId: unknownCall,
          author: "conformance-operator",
          reason: "supplier store shows no effect",
          resolution: ResolutionNeverHappened.make(),
        }),
      );
      const reopened = yield* expectSome(
        "the claim after the covering resolution",
        yield* claimLane(unknownLane, PRODUCER_B),
      );
      yield* ensure(
        reopened.submissionId === unknownHost.submissionId,
        "A covering resolution must reopen the blocked lane",
      );
    }),
);

const approvalDecisionIdempotency = conformanceCase(
  "approval decisions are idempotent and conflict on divergence",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-approval");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "approval-key-1", { work: "approve" });
      const claim = yield* expectSome(
        "the claim before decisions",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      const callA = decodeToolCallId("call-approval-a");
      const callB = decodeToolCallId("call-approval-b");

      const first = yield* ledger.recordApprovalDecision(
        ApprovalDecisionCommand.make({
          submissionId: admitted.submissionId,
          toolCallId: callA,
          decision: "approved",
          resolver: "conformance-approver",
          reason: "policy allows the booking",
        }),
      );
      yield* ensure(
        first.decision === "approved" && first.toolCallId === callA,
        "The first decision must record the intent",
      );

      yield* TestClock.adjust("1 second");
      const replayed = yield* ledger.recordApprovalDecision(
        ApprovalDecisionCommand.make({
          submissionId: admitted.submissionId,
          toolCallId: callA,
          decision: "approved",
          resolver: "conformance-approver-second",
          reason: "a different reason text",
        }),
      );
      yield* ensure(
        replayed.reason === first.reason &&
          replayed.resolver === first.resolver &&
          sameInstant(replayed.decidedAt, first.decidedAt),
        "Repeating the same decision must replay the recorded intent unchanged",
      );

      const conflict = yield* expectFailure(
        "re-deciding the same call divergently",
        ledger.recordApprovalDecision(
          ApprovalDecisionCommand.make({
            submissionId: admitted.submissionId,
            toolCallId: callA,
            decision: "denied",
            resolver: "conformance-approver",
            reason: "changed my mind",
          }),
        ),
      );
      yield* ensure(
        conflict instanceof ApprovalConflict &&
          conflict.toolCallId === callA &&
          conflict.existingDecision === "approved",
        "A divergent re-decision must conflict with the recorded decision",
      );

      yield* ledger.recordApprovalDecision(
        ApprovalDecisionCommand.make({
          submissionId: admitted.submissionId,
          toolCallId: callB,
          decision: "denied",
          resolver: "conformance-approver",
          reason: "policy denies the cancellation",
        }),
      );
      const snapshot = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        snapshot.approvalDecisions.length === 2 &&
          snapshot.approvalDecisions.some(
            (intent) => intent.toolCallId === callA && intent.decision === "approved",
          ) &&
          snapshot.approvalDecisions.some(
            (intent) => intent.toolCallId === callB && intent.decision === "denied",
          ),
        "The recovery snapshot must expose every recorded decision intent",
      );

      yield* settleClaimed(admitted, claim.ownershipToken);
      const late = yield* expectFailure(
        "deciding an approval for a settled Submission",
        ledger.recordApprovalDecision(
          ApprovalDecisionCommand.make({
            submissionId: admitted.submissionId,
            toolCallId: decodeToolCallId("call-approval-late"),
            decision: "approved",
            resolver: "conformance-approver",
            reason: "too late",
          }),
        ),
      );
      yield* ensure(
        late instanceof SettlementConflict && late.existingOutcome === "completed",
        "A decision must never land on a settled Submission",
      );
    }),
);

const unknownResolutionLifecycle = conformanceCase(
  "unknown resolutions reopen the lane only when no open call remains",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-unknown");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "unknown-key-1", { work: "unknown" });
      const claim = yield* expectSome(
        "the claim before the unknown marking",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      const call1 = decodeToolCallId("call-unknown-1");
      const call2 = decodeToolCallId("call-unknown-2");
      const call3 = decodeToolCallId("call-unknown-3");

      const marking = MarkUnknownRequest.make({
        submissionId: admitted.submissionId,
        toolCallIds: [call1, call2],
        reason: "the worker died during two supplier calls",
      });
      yield* ledger.markUnknown(marking);
      yield* ledger.markUnknown(marking);
      const marked = yield* expectSome(
        "lookup after marking unknown",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        marked.state === "unknown",
        "Marking unknown must be idempotent and block the lane",
      );

      yield* advancePastLease(claim.leaseExpiresAt);
      yield* ensure(
        Option.isNone(yield* claimLane(conversationId, PRODUCER_B)),
        "An unknown lane must consume no worker permit",
      );

      const first = yield* ledger.recordUnknownResolution(
        UnknownResolutionCommand.make({
          submissionId: admitted.submissionId,
          toolCallId: call1,
          author: "conformance-operator",
          reason: "supplier store shows no booking",
          resolution: ResolutionNeverHappened.make(),
        }),
      );
      const partiallyResolved = yield* expectSome(
        "lookup after a partial resolution",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        partiallyResolved.state === "unknown" &&
          Option.isNone(yield* claimLane(conversationId, PRODUCER_B)),
        "A partial resolution must keep the lane blocked while calls remain open",
      );

      yield* TestClock.adjust("1 second");
      const divergent = yield* expectFailure(
        "re-resolving the same call divergently",
        ledger.recordUnknownResolution(
          UnknownResolutionCommand.make({
            submissionId: admitted.submissionId,
            toolCallId: call1,
            author: "conformance-operator",
            reason: "changed my mind",
            resolution: ResolutionSafeToRetry.make(),
          }),
        ),
      );
      yield* ensure(
        divergent instanceof UnknownResolutionConflict && divergent.toolCallId === call1,
        "A divergent re-resolution must conflict",
      );
      const replayed = yield* ledger.recordUnknownResolution(
        UnknownResolutionCommand.make({
          submissionId: admitted.submissionId,
          toolCallId: call1,
          author: "conformance-operator-second",
          reason: "a different reason text",
          resolution: ResolutionNeverHappened.make(),
        }),
      );
      yield* ensure(
        sameInstant(replayed.resolvedAt, first.resolvedAt) && replayed.reason === first.reason,
        "Repeating the same resolution must replay the recorded intent unchanged",
      );

      // A later marking extends the open set; the lane must not reopen until it is covered too.
      yield* ledger.markUnknown(
        MarkUnknownRequest.make({
          submissionId: admitted.submissionId,
          toolCallIds: [call3],
          reason: "a second wave of uncertainty",
        }),
      );
      yield* ledger.recordUnknownResolution(
        UnknownResolutionCommand.make({
          submissionId: admitted.submissionId,
          toolCallId: call2,
          author: "conformance-operator",
          reason: "supplier confirmed the booking",
          resolution: ResolutionCompletedWithResult.make({
            result: { bookingRef: "booking-1" },
            isFailure: false,
          }),
        }),
      );
      const stillBlocked = yield* expectSome(
        "lookup while the extended set stays open",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        stillBlocked.state === "unknown" &&
          Option.isNone(yield* claimLane(conversationId, PRODUCER_B)),
        "The lane must stay blocked while any marked call lacks a resolution",
      );

      yield* ledger.recordUnknownResolution(
        UnknownResolutionCommand.make({
          submissionId: admitted.submissionId,
          toolCallId: call3,
          author: "conformance-operator",
          reason: "idempotency key covers a repeat",
          resolution: ResolutionSafeToRetry.make(),
        }),
      );
      const reopenedState = yield* expectSome(
        "lookup after the covering resolution",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        reopenedState.state === "input-applied",
        "Covering every marked call must reopen the lane as input-applied",
      );
      const snapshot = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        snapshot.unknownResolutions.length === 3,
        "The recovery snapshot must expose every recorded resolution intent",
      );
      const reclaim = yield* expectSome(
        "the claim after the lane reopened",
        yield* claimLane(conversationId, PRODUCER_B),
      );
      yield* ensure(
        reclaim.submissionId === admitted.submissionId &&
          reclaim.producerEpoch > claim.producerEpoch,
        "The reopened lane must grant a fresh fenced Attempt",
      );

      yield* settleClaimed(admitted, reclaim.ownershipToken);
      const late = yield* expectFailure(
        "resolving an unknown call for a settled Submission",
        ledger.recordUnknownResolution(
          UnknownResolutionCommand.make({
            submissionId: admitted.submissionId,
            toolCallId: decodeToolCallId("call-unknown-late"),
            author: "conformance-operator",
            reason: "too late",
            resolution: ResolutionNeverHappened.make(),
          }),
        ),
      );
      yield* ensure(
        late instanceof SettlementConflict,
        "A resolution must never land on a settled Submission",
      );
    }),
);

const suspendResumesImmediatelyWhenDecided = conformanceCase(
  "suspend with an already-present decision resumes immediately",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-suspend");
      const ledger = yield* SubmissionLedger;
      const admitted = yield* admitReady(conversationId, "suspend-key-1", { work: "suspend" });
      const claim = yield* expectSome(
        "the claim before suspension",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      const callA = decodeToolCallId("call-suspend-a");
      const callB = decodeToolCallId("call-suspend-b");

      const foreign = yield* expectFailure(
        "suspending without owning the lane",
        ledger.suspend(
          SuspendRequest.make({
            submissionId: admitted.submissionId,
            ownershipToken: BOGUS_TOKEN,
            reason: ApprovalPendingSuspension.make({ toolCallIds: [callA] }),
          }),
        ),
      );
      yield* ensure(foreign instanceof OwnershipLost, "suspend must be fenced by the owning token");

      // The decision raced ahead of the suspend transaction (plan §2.6).
      yield* ledger.recordApprovalDecision(
        ApprovalDecisionCommand.make({
          submissionId: admitted.submissionId,
          toolCallId: callA,
          decision: "approved",
          resolver: "conformance-approver",
          reason: "decided before the suspend committed",
        }),
      );
      const immediate = yield* ledger.suspend(
        SuspendRequest.make({
          submissionId: admitted.submissionId,
          ownershipToken: claim.ownershipToken,
          reason: ApprovalPendingSuspension.make({ toolCallIds: [callA] }),
        }),
      );
      yield* ensure(
        immediate === "resume-immediately",
        "A fully-decided suspension reason must resume immediately",
      );
      const notSuspended = yield* expectSome(
        "lookup after the immediate resume",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        notSuspended.state === "running",
        "An immediate resume must not transition the Submission to suspended",
      );
      // The ownership period must survive an immediate resume: the caller keeps working.
      yield* ledger.renewOwnership(
        RenewOwnershipRequest.make({
          submissionId: admitted.submissionId,
          ownershipToken: claim.ownershipToken,
        }),
      );

      const suspended = yield* ledger.suspend(
        SuspendRequest.make({
          submissionId: admitted.submissionId,
          ownershipToken: claim.ownershipToken,
          reason: ApprovalPendingSuspension.make({ toolCallIds: [callA, callB] }),
        }),
      );
      yield* ensure(
        suspended === "suspended",
        "An undecided call in the reason must suspend durably",
      );
      const suspendedState = yield* expectSome(
        "lookup after the suspension",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(suspendedState.state === "suspended", "The suspension must be durable");
      const suspendedSnapshot = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        suspendedSnapshot.suspension !== undefined &&
          suspendedSnapshot.suspension.reason._tag === "ApprovalPending" &&
          suspendedSnapshot.suspension.reason.toolCallIds.length === 2 &&
          suspendedSnapshot.approvalDecisions.length === 1,
        "The recovery snapshot must expose the suspension reason and prior decisions",
      );
      const ended = yield* expectFailure(
        "renewing after the suspension ended the ownership period",
        ledger.renewOwnership(
          RenewOwnershipRequest.make({
            submissionId: admitted.submissionId,
            ownershipToken: claim.ownershipToken,
          }),
        ),
      );
      yield* ensure(
        ended instanceof OwnershipLost,
        "Suspension must end the ownership period without settling",
      );

      yield* ledger.recordApprovalDecision(
        ApprovalDecisionCommand.make({
          submissionId: admitted.submissionId,
          toolCallId: callB,
          decision: "denied",
          resolver: "conformance-approver",
          reason: "the covering decision",
        }),
      );
      const inert = yield* expectSome(
        "lookup after the operational covering decision",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        inert.state === "suspended",
        "A covering decision intent alone must not clear suspension",
      );
      const resumed = yield* ledger.resumeSuspension(
        ResumeSuspensionRequest.make({
          submissionId: admitted.submissionId,
          expectedReason: ApprovalPendingSuspension.make({ toolCallIds: [callA, callB] }),
        }),
      );
      yield* ensure(resumed === "resumed", "The exact covered reason must resume the lane");
      const woken = yield* expectSome(
        "lookup after the covering decision",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        woken.state === "input-applied",
        "The covering decision must wake the suspended lane",
      );
      const wokenSnapshot = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        wokenSnapshot.suspension === undefined,
        "Waking must clear the durable suspension",
      );

      const reclaim = yield* expectSome(
        "the claim after waking",
        yield* claimLane(conversationId, PRODUCER_B),
      );
      yield* settleClaimed(admitted, reclaim.ownershipToken);
      const terminal = yield* expectFailure(
        "suspending a settled Submission",
        ledger.suspend(
          SuspendRequest.make({
            submissionId: admitted.submissionId,
            ownershipToken: reclaim.ownershipToken,
            reason: ApprovalPendingSuspension.make({ toolCallIds: [callA] }),
          }),
        ),
      );
      yield* ensure(
        terminal instanceof SettlementConflict && terminal.existingOutcome === "completed",
        "Suspension must never land on a settled Submission",
      );
    }),
);

const joinedSettlementLinkageAuthority = conformanceCase(
  "a joined Submission's settlement reservation is authorized by host linkage",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-joined-settlement");
      const ledger = yield* SubmissionLedger;

      const host = yield* admitReady(conversationId, "joined-settle-host", { work: "host" });
      const queued = yield* admitReady(conversationId, "joined-settle-2", { queued: 2 });
      const hostClaim = yield* expectSome(
        "the host claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      const claims = yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId,
          hostSubmissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          maxCount: 1,
        }),
      );
      yield* ensure(
        claims.length === 1 && claims[0]?.submissionId === queued.submissionId,
        "The queued Submission must join the host's contiguous prefix",
      );

      // A merely-`joining` Submission is still revertible: its reservation stays fenced by
      // lane ownership like any other row.
      const joiningReservation = yield* settlementReservation({
        submissionId: queued.submissionId,
        ownershipToken: BOGUS_TOKEN,
        receiptId: queued.receiptId,
        outcome: "completed",
      });
      const fenced = yield* expectFailure(
        "reserving a joining Submission's settlement without lane ownership",
        ledger.reserveSettlement(joiningReservation),
      );
      yield* ensure(
        fenced instanceof OwnershipLost,
        "A joining Submission's settlement reservation must stay ownership-fenced",
      );

      yield* ledger.markJoined(
        MarkJoinedRequest.make({
          submissionId: queued.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          recordId: submissionInputRecordId(queued.submissionId),
          sequence: decodeSequence(7),
        }),
      );
      // A `joined` lane is never worker-claimable, so no ownership token can exist for it:
      // the recorded host linkage authorizes the reservation and the presented token is not
      // consulted (plan §2.5 — the coordinator's joined-settlement loop and the
      // SettleJoinedWithHost recovery executor both rely on this).
      const joinedReservation = yield* settlementReservation({
        submissionId: queued.submissionId,
        ownershipToken: BOGUS_TOKEN,
        receiptId: queued.receiptId,
        outcome: "completed",
      });
      yield* ledger.reserveSettlement(joinedReservation);
      const settlement = yield* ledger.finalizeSettlement(
        SettlementFinalization.make({
          submissionId: queued.submissionId,
          settlementId: submissionSettlementId(queued.submissionId),
        }),
      );
      yield* ensure(
        settlement.outcome === "completed",
        "The joined settlement must finalize with the reserved outcome",
      );
      const settled = yield* expectSome(
        "lookup after the joined settlement",
        yield* lookupById(queued.submissionId),
      );
      yield* ensure(
        settled.state === "settled" && settled.settledOutcome === "completed",
        "The joined Submission must settle terminally",
      );
    }),
);

const childReservationIdempotency = conformanceCase(
  "replays an identical child reservation and rejects a divergent allocation digest",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-child-reserve");
      const ledger = yield* SubmissionLedger;
      const parent = yield* admitReady(conversationId, "child-reserve-parent", { work: "parent" });
      const claim = yield* expectSome(
        "the parent claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );

      const allocation = { turns: 4, toolCalls: 8 };
      const allocationDigest = yield* digestJson(allocation);
      const requestFields = {
        reservationId: decodeChildReservationId("child-reservation:run-reserve:call-1"),
        parentSubmissionId: parent.submissionId,
        parentToolCallId: decodeToolCallId("call-child-reserve"),
        ownershipToken: claim.ownershipToken,
        allocation,
        allocationDigest,
      };
      const request = ChildBudgetReservationRequest.make(requestFields);
      const first = yield* ledger.reserveChildBudget(request);
      yield* ensure(
        !first.replayed &&
          first.reservation.status === "reserved" &&
          first.reservation.allocationDigest === allocationDigest &&
          first.reservation.childSubmissionId === undefined &&
          sameJson(first.reservation.allocation, allocation),
        "The first reservation must create a reserved row carrying the exact allocation",
      );

      yield* TestClock.adjust("1 second");
      const replayed = yield* ledger.reserveChildBudget(request);
      yield* ensure(
        replayed.replayed &&
          replayed.reservation.status === "reserved" &&
          sameJson(replayed.reservation.allocation, allocation) &&
          sameInstant(replayed.reservation.reservedAt, first.reservation.reservedAt),
        "An identical reservation must replay the stored row unchanged",
      );

      const divergentAllocation = { turns: 64, toolCalls: 8 };
      const divergent = yield* expectFailure(
        "a divergent allocation for the same reservation id",
        ledger.reserveChildBudget(
          ChildBudgetReservationRequest.make({
            ...requestFields,
            allocation: divergentAllocation,
            allocationDigest: yield* digestJson(divergentAllocation),
          }),
        ),
      );
      yield* ensure(
        divergent instanceof ChildReservationConflict && divergent.status === "reserved",
        "A divergent allocation must conflict with the recorded reservation",
      );

      const secondId = yield* expectFailure(
        "a second reservation id for the same parent Tool Call",
        ledger.reserveChildBudget(
          ChildBudgetReservationRequest.make({
            ...requestFields,
            reservationId: decodeChildReservationId("child-reservation:run-reserve:call-1-other"),
          }),
        ),
      );
      yield* ensure(
        secondId instanceof ChildReservationConflict,
        "One parent Tool Call must never own two reservations",
      );

      const snapshot = yield* recoverySnapshot(parent.submissionId);
      yield* ensure(
        snapshot.childReservations.length === 1 &&
          snapshot.childReservations[0].reservationId === request.reservationId &&
          snapshot.childReservations[0].status === "reserved" &&
          snapshot.childAttachments.length === 0,
        "The parent recovery snapshot must expose the reservation before any attachment",
      );
    }),
);

const childReservationFencing = conformanceCase(
  "a stale parent token cannot transition a child reservation",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-child-fence");
      const childLane = decodeConversationId("ledger-conformance-child-fence-child");
      const ledger = yield* SubmissionLedger;
      const parent = yield* admitReady(conversationId, "child-fence-parent", { work: "parent" });
      const child = yield* admitReady(childLane, "child-fence-child", { work: "child" });
      const firstClaim = yield* expectSome(
        "the first parent claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );

      const allocation = { turns: 2 };
      const allocationDigest = yield* digestJson(allocation);
      const requestFields = {
        reservationId: decodeChildReservationId("child-reservation:run-fence:call-1"),
        parentSubmissionId: parent.submissionId,
        parentToolCallId: decodeToolCallId("call-child-fence"),
        ownershipToken: BOGUS_TOKEN,
        allocation,
        allocationDigest,
      };
      const request = ChildBudgetReservationRequest.make(requestFields);
      const foreign = yield* expectFailure(
        "creating a reservation without owning the parent lane",
        ledger.reserveChildBudget(request),
      );
      yield* ensure(
        foreign instanceof OwnershipLost,
        "Reservation creation must be fenced by the parent's live ownership token",
      );
      const afterForeign = yield* recoverySnapshot(parent.submissionId);
      yield* ensure(
        afterForeign.childReservations.length === 0,
        "A fenced reservation attempt must leave no row behind",
      );

      yield* ledger.reserveChildBudget(
        ChildBudgetReservationRequest.make({
          ...requestFields,
          ownershipToken: firstClaim.ownershipToken,
        }),
      );

      // The parent Attempt ends and a replacement claims the lane: the old token is fenced.
      yield* ledger.releaseOwnership(
        ReleaseOwnershipRequest.make({
          submissionId: parent.submissionId,
          ownershipToken: firstClaim.ownershipToken,
        }),
      );
      const reclaim = yield* expectSome(
        "the replacement parent claim",
        yield* claimLane(conversationId, PRODUCER_B),
      );
      yield* ensure(
        reclaim.producerEpoch > firstClaim.producerEpoch,
        "The replacement claim must fence the stale parent Attempt",
      );

      const staleAttach = yield* expectFailure(
        "attaching a child with the superseded parent token",
        ledger.attachChildToReservation(
          AttachChildToReservationRequest.make({
            reservationId: request.reservationId,
            ownershipToken: firstClaim.ownershipToken,
            childSubmissionId: child.submissionId,
          }),
        ),
      );
      yield* ensure(
        staleAttach instanceof OwnershipLost,
        "A superseded parent token must not attach a child",
      );

      const staleCreate = yield* expectFailure(
        "creating a second-call reservation with the superseded parent token",
        ledger.reserveChildBudget(
          ChildBudgetReservationRequest.make({
            ...requestFields,
            reservationId: decodeChildReservationId("child-reservation:run-fence:call-2"),
            parentToolCallId: decodeToolCallId("call-child-fence-second"),
            ownershipToken: firstClaim.ownershipToken,
          }),
        ),
      );
      yield* ensure(
        staleCreate instanceof OwnershipLost,
        "A superseded parent token must not create new reservation state",
      );

      // An identical replay creates nothing, so it short-circuits before the fence exactly
      // like reserveSettlement: a recovering caller reads the recorded row.
      const staleReplay = yield* ledger.reserveChildBudget(
        ChildBudgetReservationRequest.make({
          ...requestFields,
          ownershipToken: firstClaim.ownershipToken,
        }),
      );
      yield* ensure(
        staleReplay.replayed && staleReplay.reservation.status === "reserved",
        "An identical reservation replay must return the recorded row even from a stale caller",
      );

      const attached = yield* ledger.attachChildToReservation(
        AttachChildToReservationRequest.make({
          reservationId: request.reservationId,
          ownershipToken: reclaim.ownershipToken,
          childSubmissionId: child.submissionId,
        }),
      );
      yield* ensure(
        attached.childSubmissionId === child.submissionId,
        "The live replacement token must attach the child",
      );
    }),
);

const attachChildIdempotency = conformanceCase(
  "attachChildToReservation is idempotent and rejects a divergent child",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-child-attach");
      const childLane = decodeConversationId("ledger-conformance-child-attach-child");
      const ledger = yield* SubmissionLedger;
      const parent = yield* admitReady(conversationId, "child-attach-parent", { work: "parent" });
      const child = yield* admitReady(childLane, "child-attach-child", { queued: 1 });
      const otherChild = yield* admitReady(childLane, "child-attach-other", { queued: 2 });
      const claim = yield* expectSome(
        "the parent claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );

      const allocation = { turns: 3 };
      const reservationId = decodeChildReservationId("child-reservation:run-attach:call-1");
      yield* ledger.reserveChildBudget(
        ChildBudgetReservationRequest.make({
          reservationId,
          parentSubmissionId: parent.submissionId,
          parentToolCallId: decodeToolCallId("call-child-attach"),
          ownershipToken: claim.ownershipToken,
          allocation,
          allocationDigest: yield* digestJson(allocation),
        }),
      );

      const unknownReservation = yield* expectFailure(
        "attaching to a reservation that was never created",
        ledger.attachChildToReservation(
          AttachChildToReservationRequest.make({
            reservationId: decodeChildReservationId("child-reservation:run-attach:missing"),
            ownershipToken: claim.ownershipToken,
            childSubmissionId: child.submissionId,
          }),
        ),
      );
      yield* ensure(
        unknownReservation instanceof LedgerError,
        "Attaching to an unknown reservation must fail as a ledger error",
      );

      const attached = yield* ledger.attachChildToReservation(
        AttachChildToReservationRequest.make({
          reservationId,
          ownershipToken: claim.ownershipToken,
          childSubmissionId: child.submissionId,
        }),
      );
      yield* ensure(
        attached.childSubmissionId === child.submissionId && attached.status === "reserved",
        "Attaching must record the child on the reservation row",
      );

      const replayed = yield* ledger.attachChildToReservation(
        AttachChildToReservationRequest.make({
          reservationId,
          ownershipToken: claim.ownershipToken,
          childSubmissionId: child.submissionId,
        }),
      );
      yield* ensure(
        replayed.childSubmissionId === child.submissionId,
        "Repeating the identical attachment must be a no-op",
      );

      const divergent = yield* expectFailure(
        "attaching a different child to the same reservation",
        ledger.attachChildToReservation(
          AttachChildToReservationRequest.make({
            reservationId,
            ownershipToken: claim.ownershipToken,
            childSubmissionId: otherChild.submissionId,
          }),
        ),
      );
      yield* ensure(
        divergent instanceof ChildReservationConflict,
        "A divergent child attachment must conflict with the recorded child",
      );

      const snapshot = yield* recoverySnapshot(parent.submissionId);
      yield* ensure(
        snapshot.childAttachments.length === 1 &&
          snapshot.childAttachments[0].childSubmissionId === child.submissionId &&
          snapshot.childAttachments[0].toolCallId === decodeToolCallId("call-child-attach") &&
          snapshot.childAttachments[0].childState === "ready" &&
          snapshot.childAttachments[0].childOutcome === undefined,
        "The parent recovery snapshot must expose the attachment with the child's lane state",
      );
    }),
);

const beginReleaseFreezesAccountingOnce = conformanceCase(
  "beginRelease freezes the accounting decision exactly once",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-child-freeze");
      const ledger = yield* SubmissionLedger;
      const parent = yield* admitReady(conversationId, "child-freeze-parent", { work: "parent" });
      const claim = yield* expectSome(
        "the parent claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );

      const allocation = { turns: 4 };
      const reservationId = decodeChildReservationId("child-reservation:run-freeze:call-1");
      yield* ledger.reserveChildBudget(
        ChildBudgetReservationRequest.make({
          reservationId,
          parentSubmissionId: parent.submissionId,
          parentToolCallId: decodeToolCallId("call-child-freeze"),
          ownershipToken: claim.ownershipToken,
          allocation,
          allocationDigest: yield* digestJson(allocation),
        }),
      );

      const accounting = { consumed: { turns: 1 }, released: { turns: 3 } };
      const frozen = yield* ledger.beginChildBudgetRelease(
        BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
      );
      yield* ensure(
        frozen.status === "releasePending" &&
          sameJson(frozen.accounting, accounting) &&
          frozen.releaseBeganAt !== undefined,
        "The first beginRelease must freeze the accounting and move to releasePending",
      );

      yield* TestClock.adjust("1 second");
      const replayed = yield* ledger.beginChildBudgetRelease(
        BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
      );
      yield* ensure(
        replayed.status === "releasePending" &&
          replayed.releaseBeganAt !== undefined &&
          frozen.releaseBeganAt !== undefined &&
          sameInstant(replayed.releaseBeganAt, frozen.releaseBeganAt),
        "Replaying the identical accounting must be a no-op with the frozen decision retained",
      );

      const divergent = yield* expectFailure(
        "freezing a different accounting decision",
        ledger.beginChildBudgetRelease(
          BeginChildBudgetReleaseRequest.make({
            reservationId,
            accounting: { consumed: { turns: 4 }, released: { turns: 0 } },
          }),
        ),
      );
      yield* ensure(
        divergent instanceof ChildReservationConflict && divergent.status === "releasePending",
        "A divergent accounting freeze must conflict with the frozen decision",
      );

      yield* ledger.releaseChildBudget(ReleaseChildBudgetRequest.make({ reservationId }));
      const afterRelease = yield* ledger.beginChildBudgetRelease(
        BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
      );
      yield* ensure(
        afterRelease.status === "released" && sameJson(afterRelease.accounting, accounting),
        "Replaying the identical accounting after release must return the released row",
      );
      const divergentAfterRelease = yield* expectFailure(
        "freezing a different accounting decision after release",
        ledger.beginChildBudgetRelease(
          BeginChildBudgetReleaseRequest.make({
            reservationId,
            accounting: { consumed: { turns: 2 }, released: { turns: 2 } },
          }),
        ),
      );
      yield* ensure(
        divergentAfterRelease instanceof ChildReservationConflict &&
          divergentAfterRelease.status === "released",
        "The frozen decision must stay immutable after release",
      );
    }),
);

const releaseAppliedExactlyOnce = conformanceCase(
  "release returns unused allocation exactly once",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-child-release");
      const ledger = yield* SubmissionLedger;
      const parent = yield* admitReady(conversationId, "child-release-parent", { work: "parent" });
      const claim = yield* expectSome(
        "the parent claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );

      const allocation = { turns: 4 };
      const reservationId = decodeChildReservationId("child-reservation:run-release:call-1");
      yield* ledger.reserveChildBudget(
        ChildBudgetReservationRequest.make({
          reservationId,
          parentSubmissionId: parent.submissionId,
          parentToolCallId: decodeToolCallId("call-child-release"),
          ownershipToken: claim.ownershipToken,
          allocation,
          allocationDigest: yield* digestJson(allocation),
        }),
      );

      const early = yield* expectFailure(
        "releasing before the accounting decision is frozen",
        ledger.releaseChildBudget(ReleaseChildBudgetRequest.make({ reservationId })),
      );
      yield* ensure(
        early instanceof ChildReservationConflict && early.status === "reserved",
        "Release must never skip the releasePending freeze",
      );

      yield* ledger.beginChildBudgetRelease(
        BeginChildBudgetReleaseRequest.make({
          reservationId,
          accounting: { consumed: {}, released: { turns: 4 } },
        }),
      );
      const released = yield* ledger.releaseChildBudget(
        ReleaseChildBudgetRequest.make({ reservationId }),
      );
      yield* ensure(
        released.status === "released" && released.releasedAt !== undefined,
        "Release must transition releasePending to released",
      );

      yield* TestClock.adjust("1 second");
      const replayed = yield* ledger.releaseChildBudget(
        ReleaseChildBudgetRequest.make({ reservationId }),
      );
      yield* ensure(
        replayed.status === "released" &&
          replayed.releasedAt !== undefined &&
          released.releasedAt !== undefined &&
          sameInstant(replayed.releasedAt, released.releasedAt),
        "Replaying the release must return the stored row unchanged — never applied twice",
      );

      const snapshot = yield* recoverySnapshot(parent.submissionId);
      yield* ensure(
        snapshot.childReservations.length === 1 &&
          snapshot.childReservations[0].status === "released",
        "The recovery snapshot must expose the released reservation",
      );
    }),
);

const recordChildSettledWake = conformanceCase(
  "child notifications accept canonical terminalizing prefixes but stay inert until exact suspension resume",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const parentLane = decodeConversationId("ledger-conformance-child-wake");
      const childLaneA = decodeConversationId("ledger-conformance-child-wake-a");
      const childLaneB = decodeConversationId("ledger-conformance-child-wake-b");
      const ledger = yield* SubmissionLedger;

      const parent = yield* admitReady(parentLane, "child-wake-parent", { work: "parent" });
      const childA = yield* admitReady(childLaneA, "child-wake-a", { child: "a" });
      const childB = yield* admitReady(childLaneB, "child-wake-b", { child: "b" });
      const parentClaim = yield* expectSome(
        "the parent claim",
        yield* claimLane(parentLane, PRODUCER_A),
      );

      const waitingReason = WaitingForChildSuspension.make({
        children: [
          WaitingChild.make({
            toolCallId: decodeToolCallId("call-wake-a"),
            childSubmissionId: childA.submissionId,
          }),
          WaitingChild.make({
            toolCallId: decodeToolCallId("call-wake-b"),
            childSubmissionId: childB.submissionId,
          }),
        ],
      });
      const suspended = yield* ledger.suspend(
        SuspendRequest.make({
          submissionId: parent.submissionId,
          ownershipToken: parentClaim.ownershipToken,
          reason: waitingReason,
        }),
      );
      yield* ensure(
        suspended === "suspended",
        "Unsettled children must suspend the parent durably",
      );
      const ended = yield* expectFailure(
        "renewing after the waitingForChild suspension ended the ownership period",
        ledger.renewOwnership(
          RenewOwnershipRequest.make({
            submissionId: parent.submissionId,
            ownershipToken: parentClaim.ownershipToken,
          }),
        ),
      );
      yield* ensure(
        ended instanceof OwnershipLost,
        "waitingForChild must end the ownership period without settling (SUB-030)",
      );
      yield* ensure(
        Option.isNone(yield* claimLane(parentLane, PRODUCER_B)),
        "A waitingForChild head must produce no claim and consume no worker permit",
      );
      const mismatchedResume = yield* expectFailure(
        "resuming with a reason that omits a waiting child",
        ledger.resumeSuspension(
          ResumeSuspensionRequest.make({
            submissionId: parent.submissionId,
            expectedReason: WaitingForChildSuspension.make({
              children: [waitingReason.children[0]],
            }),
          }),
        ),
      );
      yield* ensure(
        mismatchedResume instanceof LedgerError &&
          Option.isNone(yield* claimLane(parentLane, PRODUCER_B)),
        "A divergent expected reason must fail without making the lane runnable",
      );

      const childClaimA = yield* expectSome(
        "the first child claim",
        yield* claimLane(childLaneA, PRODUCER_A),
      );
      const childReservationA = yield* settlementReservation({
        submissionId: childA.submissionId,
        ownershipToken: childClaimA.ownershipToken,
        receiptId: childA.receiptId,
        outcome: "completed",
      });
      yield* ledger.reserveSettlement(childReservationA);
      const partial = yield* ledger.recordChildSettled(
        ChildSettledNotification.make({
          parentSubmissionId: parent.submissionId,
          childSubmissionId: childA.submissionId,
        }),
      );
      yield* ensure(
        partial === "still-waiting",
        "A settlement notification must not wake the parent while a listed child is unsettled",
      );
      yield* ledger.finalizeSettlement(
        SettlementFinalization.make({
          submissionId: childA.submissionId,
          settlementId: childReservationA.settlementId,
        }),
      );
      const stillSuspended = yield* expectSome(
        "lookup while one child is outstanding",
        yield* lookupById(parent.submissionId),
      );
      yield* ensure(
        stillSuspended.state === "suspended" &&
          Option.isNone(yield* claimLane(parentLane, PRODUCER_B)),
        "The parent lane must stay suspended until every listed child settled",
      );

      const childClaimB = yield* expectSome(
        "the second child claim",
        yield* claimLane(childLaneB, PRODUCER_A),
      );
      const childReservationB = yield* settlementReservation({
        submissionId: childB.submissionId,
        ownershipToken: childClaimB.ownershipToken,
        receiptId: childB.receiptId,
        outcome: "completed",
      });
      yield* ledger.reserveSettlement(childReservationB);
      const covered = yield* ledger.recordChildSettled(
        ChildSettledNotification.make({
          parentSubmissionId: parent.submissionId,
          childSubmissionId: childB.submissionId,
        }),
      );
      yield* ensure(
        covered === "still-waiting",
        "Operational child coverage alone must not wake the parent",
      );
      yield* ensure(
        Option.isNone(yield* claimLane(parentLane, PRODUCER_B)),
        "A fully covered lane stays suspended until canonical evidence authorizes resume",
      );
      const resumed = yield* ledger.resumeSuspension(
        ResumeSuspensionRequest.make({
          submissionId: parent.submissionId,
          expectedReason: waitingReason,
        }),
      );
      yield* ensure(
        resumed === "resumed",
        "The exact canonical-evidence-authorized child reason must resume once covered",
      );
      yield* ledger.finalizeSettlement(
        SettlementFinalization.make({
          submissionId: childB.submissionId,
          settlementId: childReservationB.settlementId,
        }),
      );
      const awake = yield* expectSome(
        "lookup after the covering settlement",
        yield* lookupById(parent.submissionId),
      );
      yield* ensure(
        awake.state === "input-applied",
        "The woken parent must transition suspended(WaitingForChild) to input-applied",
      );
      const wokenSnapshot = yield* recoverySnapshot(parent.submissionId);
      yield* ensure(
        wokenSnapshot.suspension === undefined,
        "Waking must clear the durable suspension",
      );

      const replayedNotification = yield* ledger.recordChildSettled(
        ChildSettledNotification.make({
          parentSubmissionId: parent.submissionId,
          childSubmissionId: childA.submissionId,
        }),
      );
      yield* ensure(
        replayedNotification === "not-waiting",
        "Replaying a notification after the wake must be an idempotent no-op",
      );

      const reclaim = yield* expectSome(
        "the parent claim after waking",
        yield* claimLane(parentLane, PRODUCER_B),
      );
      yield* ensure(
        reclaim.submissionId === parent.submissionId &&
          reclaim.producerEpoch > parentClaim.producerEpoch,
        "The woken lane must grant a fresh fenced Attempt",
      );
      yield* settleClaimed(parent, reclaim.ownershipToken);
      const afterSettlement = yield* ledger.recordChildSettled(
        ChildSettledNotification.make({
          parentSubmissionId: parent.submissionId,
          childSubmissionId: childB.submissionId,
        }),
      );
      yield* ensure(
        afterSettlement === "not-waiting",
        "A notification for a settled parent must answer not-waiting",
      );
    }),
);

const suspendResumesImmediatelyForSettledChildren = conformanceCase(
  "suspend returns resume-immediately when children already settled",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const parentLane = decodeConversationId("ledger-conformance-child-raced");
      const childLaneA = decodeConversationId("ledger-conformance-child-raced-a");
      const childLaneB = decodeConversationId("ledger-conformance-child-raced-b");
      const ledger = yield* SubmissionLedger;

      // The child settles BEFORE the parent's suspend transaction commits (spec §12 step 10
      // race): the suspend must observe the settlement and resume immediately.
      const settledChild = yield* admitReady(childLaneA, "child-raced-a", { child: "a" });
      const settledClaim = yield* expectSome(
        "the settled child's claim",
        yield* claimLane(childLaneA, PRODUCER_A),
      );
      yield* settleClaimed(settledChild, settledClaim.ownershipToken);
      const pendingChild = yield* admitReady(childLaneB, "child-raced-b", { child: "b" });

      const parent = yield* admitReady(parentLane, "child-raced-parent", { work: "parent" });
      const parentClaim = yield* expectSome(
        "the parent claim",
        yield* claimLane(parentLane, PRODUCER_A),
      );

      const immediate = yield* ledger.suspend(
        SuspendRequest.make({
          submissionId: parent.submissionId,
          ownershipToken: parentClaim.ownershipToken,
          reason: WaitingForChildSuspension.make({
            children: [
              WaitingChild.make({
                toolCallId: decodeToolCallId("call-raced-a"),
                childSubmissionId: settledChild.submissionId,
              }),
            ],
          }),
        }),
      );
      yield* ensure(
        immediate === "resume-immediately",
        "A suspend listing only settled children must resume immediately",
      );
      const stillRunning = yield* expectSome(
        "lookup after the immediate resume",
        yield* lookupById(parent.submissionId),
      );
      yield* ensure(
        stillRunning.state === "running",
        "An immediate resume must not transition the parent to suspended",
      );
      // The ownership period must survive an immediate resume: the caller keeps working.
      yield* ledger.renewOwnership(
        RenewOwnershipRequest.make({
          submissionId: parent.submissionId,
          ownershipToken: parentClaim.ownershipToken,
        }),
      );

      const suspended = yield* ledger.suspend(
        SuspendRequest.make({
          submissionId: parent.submissionId,
          ownershipToken: parentClaim.ownershipToken,
          reason: WaitingForChildSuspension.make({
            children: [
              WaitingChild.make({
                toolCallId: decodeToolCallId("call-raced-a"),
                childSubmissionId: settledChild.submissionId,
              }),
              WaitingChild.make({
                toolCallId: decodeToolCallId("call-raced-b"),
                childSubmissionId: pendingChild.submissionId,
              }),
            ],
          }),
        }),
      );
      yield* ensure(
        suspended === "suspended",
        "One unsettled listed child must suspend the parent durably",
      );
      const snapshot = yield* recoverySnapshot(parent.submissionId);
      yield* ensure(
        snapshot.suspension !== undefined &&
          snapshot.suspension.reason._tag === "WaitingForChild" &&
          snapshot.suspension.reason.children.length === 2,
        "The recovery snapshot must expose the WaitingForChild reason with its children",
      );
    }),
);

const admissionParentLinkage = conformanceCase(
  "admit records and replays parent linkage",
  ({ ensure, expectFailure, expectSome }) =>
    Effect.gen(function* () {
      const parentLane = decodeConversationId("ledger-conformance-linkage");
      const childLane = decodeConversationId("ledger-conformance-linkage-child");
      const ledger = yield* SubmissionLedger;
      const parent = yield* admitReady(parentLane, "linkage-parent", { work: "parent" });

      const linkage = ParentLinkage.make({
        parentSubmissionId: parent.submissionId,
        parentToolCallId: decodeToolCallId("call-linkage"),
      });
      const request = yield* admissionRequest(
        childLane,
        "linkage-child-key",
        { task: "research" },
        linkage,
      );
      const admitted = yield* ledger.admit(request);
      yield* ensure(!admitted.replayed, "The first linked admission must create the child");

      const byId = yield* expectSome(
        "lookup of the linked child",
        yield* lookupById(admitted.submissionId),
      );
      yield* ensure(
        byId.parentLinkage !== undefined &&
          byId.parentLinkage.parentSubmissionId === parent.submissionId &&
          byId.parentLinkage.parentToolCallId === linkage.parentToolCallId,
        "The child snapshot must expose its immutable parent linkage",
      );
      const childSnapshot = yield* recoverySnapshot(admitted.submissionId);
      yield* ensure(
        childSnapshot.parentLinkage !== undefined &&
          childSnapshot.parentLinkage.parentSubmissionId === parent.submissionId,
        "The child recovery snapshot must expose its parent linkage",
      );

      const replayed = yield* ledger.admit(request);
      yield* ensure(
        replayed.replayed &&
          replayed.submissionId === admitted.submissionId &&
          replayed.receiptId === admitted.receiptId,
        "An identical linked admission must replay the original identities (SUB-016)",
      );

      const divergentLinkage = yield* expectFailure(
        "replaying the admission with a different parent Tool Call",
        ledger.admit(
          yield* admissionRequest(
            childLane,
            "linkage-child-key",
            { task: "research" },
            ParentLinkage.make({
              parentSubmissionId: parent.submissionId,
              parentToolCallId: decodeToolCallId("call-linkage-other"),
            }),
          ),
        ),
      );
      yield* ensure(
        divergentLinkage instanceof AdmissionConflict,
        "A divergent parent linkage must conflict even when the input digest matches",
      );

      const droppedLinkage = yield* expectFailure(
        "replaying the admission without its parent linkage",
        ledger.admit(yield* admissionRequest(childLane, "linkage-child-key", { task: "research" })),
      );
      yield* ensure(
        droppedLinkage instanceof AdmissionConflict,
        "Dropping the recorded linkage on replay must conflict",
      );

      const plain = yield* ledger.admit(
        yield* admissionRequest(childLane, "linkage-plain-key", { task: "plain" }),
      );
      const addedLinkage = yield* expectFailure(
        "replaying an unlinked admission with a parent linkage",
        ledger.admit(
          yield* admissionRequest(childLane, "linkage-plain-key", { task: "plain" }, linkage),
        ),
      );
      yield* ensure(
        addedLinkage instanceof AdmissionConflict,
        "Adding a linkage to an unlinked admission on replay must conflict",
      );
      const plainSnapshot = yield* expectSome(
        "lookup of the unlinked Submission",
        yield* lookupById(plain.submissionId),
      );
      yield* ensure(
        plainSnapshot.parentLinkage === undefined,
        "An unlinked Submission must expose no parent linkage",
      );
    }),
);

const resolveAdmissionAuthority = conformanceCase(
  "resolveAdmission distinguishes notAdmitted from admitted authoritatively",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      const conversationId = decodeConversationId("ledger-conformance-resolve");
      const ledger = yield* SubmissionLedger;
      const key = SubmissionLookupByKey.make({
        conversationId,
        principal: CONFORMANCE_PRINCIPAL,
        idempotencyKey: decodeIdempotencyKey("resolve-key-1"),
      });

      const before = yield* ledger.resolveAdmission(key);
      yield* ensure(
        before._tag === "NotAdmitted",
        "The authoritative store must prove absence before admission (SUB-031)",
      );

      const admitted = yield* ledger.admit(
        yield* admissionRequest(conversationId, "resolve-key-1", { work: "resolve" }),
      );
      const after = yield* ledger.resolveAdmission(key);
      yield* ensure(
        after._tag === "Admitted" &&
          after.submission.submissionId === admitted.submissionId &&
          after.submission.receiptId === admitted.receiptId &&
          after.submission.state === "admitted",
        "An admitted key must resolve to the full authoritative snapshot",
      );

      const foreign = yield* ledger.resolveAdmission(
        SubmissionLookupByKey.make({
          conversationId,
          principal: OTHER_PRINCIPAL,
          idempotencyKey: decodeIdempotencyKey("resolve-key-1"),
        }),
      );
      yield* ensure(
        foreign._tag === "NotAdmitted",
        "Admission resolution must stay scoped to the requesting principal",
      );

      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
      const claim = yield* expectSome(
        "the claim before terminal resolution",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      yield* settleClaimed(admitted, claim.ownershipToken);
      const settled = yield* ledger.resolveAdmission(key);
      yield* ensure(
        settled._tag === "Admitted" &&
          settled.submission.state === "settled" &&
          settled.submission.settledOutcome === "completed",
        "A settled Submission still resolves as admitted — terminality never becomes absence",
      );
    }),
);

const queuedAbortSettlementAuthority = conformanceCase(
  "reserveSettlement authorizes an aborted, unowned queued settlement by its durable intent",
  ({ ensure, expectFailure }) =>
    Effect.gen(function* () {
      // P7 §7(c): an aborted, never-claimed, still-queued `ready` Submission has no live
      // ownership to fence against, so its durable abort intent authorizes exactly its
      // ABORTED settlement — recovery settles it without waiting for it to head the lane.
      // The authorization is outcome- and state-narrow and stays fail-closed otherwise.
      const conversationId = decodeConversationId("ledger-conformance-queued-abort");
      const ledger = yield* SubmissionLedger;

      const head = yield* admitReady(conversationId, "queued-abort-head", { work: "head" });
      const second = yield* admitReady(conversationId, "queued-abort-2", { queued: 2 });
      const third = yield* admitReady(conversationId, "queued-abort-3", { queued: 3 });

      yield* ledger.requestAbort(
        AbortCommand.make({
          submissionId: second.submissionId,
          author: "operator",
          reason: "cancelled while queued",
        }),
      );

      // Fail-closed control: a queued row WITHOUT an abort intent never accepts an unowned
      // reservation, aborted or not.
      const unaborted = yield* expectFailure(
        "reserving an aborted settlement for a queued row without an abort intent",
        settlementReservation({
          submissionId: third.submissionId,
          ownershipToken: BOGUS_TOKEN,
          receiptId: third.receiptId,
          outcome: "aborted",
        }).pipe(Effect.flatMap((reservation) => ledger.reserveSettlement(reservation))),
      );
      yield* ensure(
        unaborted instanceof OwnershipLost,
        "A queued reservation without a durable abort intent must stay fenced (OwnershipLost)",
      );

      // Fail-closed control: the durable intent authorizes ONLY the aborted outcome.
      const wrongOutcome = yield* expectFailure(
        "reserving a completed settlement for the aborted queued row",
        settlementReservation({
          submissionId: second.submissionId,
          ownershipToken: BOGUS_TOKEN,
          receiptId: second.receiptId,
          outcome: "completed",
          result: { fabricated: true },
        }).pipe(Effect.flatMap((reservation) => ledger.reserveSettlement(reservation))),
      );
      yield* ensure(
        wrongOutcome instanceof OwnershipLost,
        "An abort intent must never authorize a non-aborted settlement outcome",
      );

      const reservation = yield* settlementReservation({
        submissionId: second.submissionId,
        ownershipToken: BOGUS_TOKEN,
        receiptId: second.receiptId,
        outcome: "aborted",
      });
      const reserved = yield* ledger.reserveSettlement(reservation);
      yield* ensure(
        reserved.replayed === false && reserved.outcome === "aborted",
        "The abort intent must authorize the aborted reservation without lane ownership",
      );
      // The crash replay (`terminalizing` + committed reservation) is equally authorized.
      const replayed = yield* ledger.reserveSettlement(reservation);
      yield* ensure(
        replayed.replayed === true,
        "Replaying the identical aborted reservation must short-circuit idempotently",
      );
      yield* ledger.finalizeSettlement(
        SettlementFinalization.make({
          submissionId: second.submissionId,
          settlementId: submissionSettlementId(second.submissionId),
        }),
      );

      const settled = yield* recoverySnapshot(second.submissionId);
      yield* ensure(
        settled.submission.state === "settled" && settled.submission.settledOutcome === "aborted",
        "The aborted queued Submission must settle while the head is still unsettled",
      );
      const headState = yield* recoverySnapshot(head.submissionId);
      yield* ensure(
        headState.submission.state === "ready",
        "Settling the aborted queued row must not disturb the unclaimed head",
      );
    }),
);

const abortedSettledRowIsNotAJoiningGap = conformanceCase(
  "claimJoining treats an aborted-settled row as a non-gap",
  ({ ensure, expectSome }) =>
    Effect.gen(function* () {
      // P7 §7(c): an aborted-settled row is a CLOSED obligation — the contiguous joining
      // prefix walks over it so later ready work still joins the host (the pre-P7 rule
      // treated every settled row as a conservative gap).
      const conversationId = decodeConversationId("ledger-conformance-aborted-non-gap");
      const ledger = yield* SubmissionLedger;

      const host = yield* admitReady(conversationId, "aborted-gap-host", { work: "host" });
      const second = yield* admitReady(conversationId, "aborted-gap-2", { queued: 2 });
      const third = yield* admitReady(conversationId, "aborted-gap-3", { queued: 3 });

      yield* ledger.requestAbort(
        AbortCommand.make({
          submissionId: second.submissionId,
          author: "operator",
          reason: "cancelled while queued",
        }),
      );
      const reservation = yield* settlementReservation({
        submissionId: second.submissionId,
        ownershipToken: BOGUS_TOKEN,
        receiptId: second.receiptId,
        outcome: "aborted",
      });
      yield* ledger.reserveSettlement(reservation);
      yield* ledger.finalizeSettlement(
        SettlementFinalization.make({
          submissionId: second.submissionId,
          settlementId: submissionSettlementId(second.submissionId),
        }),
      );

      const hostClaim = yield* expectSome(
        "the host claim",
        yield* claimLane(conversationId, PRODUCER_A),
      );
      yield* ensure(
        hostClaim.submissionId === host.submissionId,
        "The host must head the lane (the aborted-settled row is out of the queue)",
      );
      const claims = yield* ledger.claimJoining(
        ClaimJoiningRequest.make({
          conversationId,
          hostSubmissionId: host.submissionId,
          ownershipToken: hostClaim.ownershipToken,
          maxCount: 8,
        }),
      );
      yield* ensure(
        claims.length === 1 && claims[0].submissionId === third.submissionId,
        "The joining prefix must skip the aborted-settled row and claim the later ready work",
      );
      const settled = yield* recoverySnapshot(second.submissionId);
      yield* ensure(
        settled.submission.state === "settled" && settled.submission.settledOutcome === "aborted",
        "Walking the prefix must never disturb the aborted-settled row",
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
  crossPrincipalAdmissionScoping,
  concurrentAdmissionFifo,
  fifoHeadClaim,
  leaseExpiryReclaim,
  releaseMakesHeadClaimable,
  inputAppliedIdempotency,
  settlementLifecycle,
  settlementConflicts,
  canonicalSettlementRepair,
  abortIdempotency,
  scanNonterminalWorklist,
  lookupByIdAndKey,
  recoverySnapshotConsistency,
  joiningPrefixClaim,
  revertJoiningReturnsToReady,
  markJoinedIdempotency,
  claimNeverGrantsBlockedHead,
  approvalDecisionIdempotency,
  unknownResolutionLifecycle,
  suspendResumesImmediatelyWhenDecided,
  joinedSettlementLinkageAuthority,
  childReservationIdempotency,
  childReservationFencing,
  attachChildIdempotency,
  beginReleaseFreezesAccountingOnce,
  releaseAppliedExactlyOnce,
  recordChildSettledWake,
  suspendResumesImmediatelyForSettledChildren,
  admissionParentLinkage,
  resolveAdmissionAuthority,
  queuedAbortSettlementAuthority,
  abortedSettledRowIsNotAJoiningGap,
];
