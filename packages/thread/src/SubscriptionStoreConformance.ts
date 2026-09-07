import { AgentId, ThreadId, ReceiptId, SubmissionId } from "@effect-agent/core/Identifiers";
import { Clock, Effect, Result, Schema } from "effect";
import { TestClock } from "effect/testing";

import { Receipt } from "./DurableAgentRuntime.ts";
import { DefinitionDigests, Digest } from "./Records.ts";
import { IdempotencyKey, Principal, QueueSequence } from "./SubmissionLedger.ts";
import {
  type AcceptedEvent,
  defaultSubscriptionLimits,
  type SourcePartition,
  type SubscriptionDelivery,
  type SubscriptionRecord,
  SubscriptionStore,
  type SubscriptionStoreFailure,
} from "./Subscription.ts";

export class SubscriptionStoreConformanceViolation extends Schema.TaggedError<SubscriptionStoreConformanceViolation>()(
  "SubscriptionStoreConformanceViolation",
  { caseName: Schema.String, message: Schema.String },
) {}

export interface SubscriptionStoreConformanceCase {
  readonly name: string;
  readonly run: Effect.Effect<
    void,
    SubscriptionStoreFailure | SubscriptionStoreConformanceViolation,
    SubscriptionStore
  >;
}

export const subscriptionConformancePartition: SourcePartition = {
  tenantId: "subscription-conformance",
  address: "source-address",
};

const principal = Schema.decodeSync(Principal)("subscription-conformance-principal");
const agentId = Schema.decodeSync(AgentId)("subscription-conformance-agent");
const threadId = Schema.decodeSync(ThreadId)("subscription-conformance-thread");
const digest = (character: string) => Schema.decodeSync(Digest)(character.repeat(64));

const definitions = DefinitionDigests.make({
  agent: digest("a"),
  model: digest("b"),
  tools: digest("c"),
});

const record = (
  name: string,
  mode: "once" | "continuous" = "once",
  ownerId = "owner",
): SubscriptionRecord => ({
  schemaVersion: 1,
  key: { partition: subscriptionConformancePartition, ownerId, subscriptionId: name },
  creationFingerprint: digest(name.charCodeAt(0) % 2 === 0 ? "d" : "e"),
  createdBy: principal,
  createdAtMillis: 1,
  ordinal: 0,
  configurationRevision: 1,
  configurationFingerprint: digest(name.charCodeAt(0) % 2 === 0 ? "d" : "e"),
  creationConfiguration: {
    source: { name: "trusted", version: "1" },
    matchingKey: "match",
    parameters: { name },
    context: { name },
    mode,
    expiresAtMillis: 100_000,
    destination: { _tag: "ExistingThread", threadId },
    deliveryPrincipal: principal,
    agentId,
    definitions,
  },
  configuration: {
    source: { name: "trusted", version: "1" },
    matchingKey: "match",
    parameters: { name },
    context: { name },
    mode,
    expiresAtMillis: 100_000,
    destination: { _tag: "ExistingThread", threadId },
    deliveryPrincipal: principal,
    agentId,
    definitions,
  },
  state: "active",
  recovery: null,
});

const event = (name: string, payloadDigest = digest("f")): AcceptedEvent => ({
  schemaVersion: 1,
  partition: subscriptionConformancePartition,
  eventId: name,
  source: { name: "trusted", version: "1" },
  matchingKey: "match",
  payload: { name },
  payloadDigest,
  acceptedAtMillis: 10,
  cutoff: 0,
  cursor: 0,
  routingComplete: false,
  routingFailure: null,
  nextAttemptAtMillis: 10,
});

const delivery = (
  subscription: SubscriptionRecord,
  accepted: AcceptedEvent,
  suffix = "0",
): SubscriptionDelivery => {
  const deliveryId = digest(suffix === "0" ? "1" : "2");

  return {
    schemaVersion: 1,
    key: { subscription: subscription.key, eventId: accepted.eventId },
    deliveryId,
    source: accepted.source,
    subscriptionFingerprint: subscription.configurationFingerprint,
    configurationRevision: subscription.configurationRevision,
    configuration: subscription.configuration,
    eventDigest: accepted.payloadDigest,
    threadId,
    admissionKey: Schema.decodeSync(IdempotencyKey)(`subscription:${deliveryId}`),
    selectedAtMillis: suffix === "0" ? 20 : 21,
    state: "selected",
    envelope: null,
    envelopeDigest: null,
    retry: {
      generation: 0,
      attempts: 0,
      automaticAttempts: 0,
      parked: false,
      nextAttemptAtMillis: 20,
      lastAttemptAtMillis: null,
      lastFailure: null,
    },
    receipt: null,
    refusal: null,
  };
};

const preparedInput = (selected: SubscriptionDelivery) => ({
  schemaVersion: 1 as const,
  threadId: selected.threadId,
  deliveryPrincipal: principal,
  agentId,
  definitions,
  input: { event: selected.key.eventId },
  inputDigest: digest("3"),
  admissionKey: selected.admissionKey,
  authorization: { policyId: "policy", decisionId: "decision" },
});

const receipt = Receipt.make({
  receiptId: Schema.decodeSync(ReceiptId)("subscription-conformance-receipt"),
  submissionId: Schema.decodeSync(SubmissionId)("subscription-conformance-submission"),
  threadId,
  queueSequence: Schema.decodeSync(QueueSequence)(1),
});

const conformanceCase = (
  name: string,
  body: (
    ensure: (
      condition: boolean,
      message: string,
    ) => Effect.Effect<void, SubscriptionStoreConformanceViolation>,
  ) => Effect.Effect<
    void,
    SubscriptionStoreFailure | SubscriptionStoreConformanceViolation,
    SubscriptionStore
  >,
): SubscriptionStoreConformanceCase => ({
  name,
  run: body((condition, message) =>
    condition
      ? Effect.void
      : Effect.fail(SubscriptionStoreConformanceViolation.make({ caseName: name, message })),
  ),
});

const cutoffAndIntake = conformanceCase(
  "orders eligibility cutoffs and preserves duplicate intake",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;
      const before = yield* store.register(record("before"), defaultSubscriptionLimits);
      const accepted = yield* store.accept(event("cutoff"), defaultSubscriptionLimits);
      const after = yield* store.register(record("after"), defaultSubscriptionLimits);

      yield* ensure(
        before.ordinal < accepted.cutoff && after.ordinal > accepted.cutoff,
        "sequence did not order registration and intake",
      );
      const candidates = yield* store.candidates(accepted, 10);

      yield* ensure(
        candidates.length === 1 && candidates[0].key.subscriptionId === "before",
        "cutoff admitted a later registration",
      );

      const replayed = yield* store.accept(
        { ...event("cutoff"), acceptedAtMillis: 99 },
        defaultSubscriptionLimits,
      );

      yield* ensure(
        replayed.cutoff === accepted.cutoff &&
          replayed.acceptedAtMillis === accepted.acceptedAtMillis,
        "duplicate intake did not return retained progress",
      );

      const conflict = yield* Effect.result(
        store.accept(event("cutoff", digest("9")), defaultSubscriptionLimits),
      );

      yield* ensure(
        Result.isFailure(conflict) &&
          conflict.failure._tag === "SubscriptionError" &&
          conflict.failure.reason === "conflict",
        "conflicting event identity was accepted",
      );
    }),
);

const onceAndCapacity = conformanceCase(
  "consumes once atomically and rejects capacity before progress",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;
      const once = yield* store.register(record("once"), defaultSubscriptionLimits);
      const first = yield* store.accept(event("first"), defaultSubscriptionLimits);
      const second = yield* store.accept(event("second"), defaultSubscriptionLimits);

      yield* store.select(
        first,
        [delivery(once, first)],
        once.ordinal,
        true,
        20,
        defaultSubscriptionLimits,
      );
      yield* store.select(
        second,
        [delivery(once, second)],
        once.ordinal,
        true,
        20,
        defaultSubscriptionLimits,
      );
      yield* ensure(
        (yield* store.get(once.key))?.state === "consumed",
        "once registration remained active",
      );
      yield* ensure(
        (yield* store.delivery({ subscription: once.key, eventId: first.eventId })) !== null,
        "winning delivery was lost",
      );
      yield* ensure(
        (yield* store.delivery({ subscription: once.key, eventId: second.eventId })) === null,
        "second event consumed once registration again",
      );

      const left = yield* store.register(
        record("left", "continuous", "capacity"),
        defaultSubscriptionLimits,
      );

      const right = yield* store.register(
        record("right", "continuous", "capacity"),
        defaultSubscriptionLimits,
      );

      const bounded = yield* store.accept(event("bounded"), defaultSubscriptionLimits);
      const limits = { ...defaultSubscriptionLimits, maxDeliveries: 2, maxDeliveriesPerOwner: 1 };

      const failure = yield* Effect.result(
        store.select(
          bounded,
          [delivery(left, bounded), delivery(right, bounded, "1")],
          right.ordinal,
          true,
          20,
          limits,
        ),
      );

      yield* ensure(
        Result.isFailure(failure) &&
          failure.failure._tag === "SubscriptionError" &&
          failure.failure.reason === "capacity",
        "delivery quota did not reject batch",
      );
      yield* ensure(
        (yield* store.event(bounded.eventId))?.cursor === 0,
        "capacity failure advanced event cursor",
      );
    }),
);

const preparationLifecycle = conformanceCase(
  "refuses cancelled selection and preserves prepared work",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;
      const cancelled = yield* store.register(record("cancelled"), defaultSubscriptionLimits);

      const cancelledEvent = yield* store.accept(
        event("cancelled-event"),
        defaultSubscriptionLimits,
      );

      const cancelledDelivery = delivery(cancelled, cancelledEvent);

      yield* store.select(
        cancelledEvent,
        [cancelledDelivery],
        cancelled.ordinal,
        true,
        20,
        defaultSubscriptionLimits,
      );
      yield* store.cancel(cancelled.key);

      const refused = yield* store.changeDelivery(
        cancelledDelivery.key,
        cancelledDelivery.deliveryId,
        {
          _tag: "Prepare",
          envelope: preparedInput(cancelledDelivery),
          envelopeDigest: digest("4"),
          nowMillis: 30,
        },
      );

      yield* ensure(
        refused.state === "refused" && refused.refusal?.code === "cancelled",
        "cancelled selection prepared",
      );

      const durable = yield* store.register(record("durable"), defaultSubscriptionLimits);
      const durableEvent = yield* store.accept(event("durable-event"), defaultSubscriptionLimits);
      const selected = delivery(durable, durableEvent);

      yield* store.select(
        durableEvent,
        [selected],
        durable.ordinal,
        true,
        20,
        defaultSubscriptionLimits,
      );

      const prepared = yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Prepare",
        envelope: preparedInput(selected),
        envelopeDigest: digest("5"),
        nowMillis: 30,
      });

      const retry = {
        generation: 0,
        automaticAttempts: 0,
        parked: false,
        attempts: 2,
        nextAttemptAtMillis: 60,
        lastAttemptAtMillis: 35,
        lastFailure: "transport" as const,
      };

      yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Retry",
        retry,
        nowMillis: 35,
      });
      for (const staleRetry of [
        { ...retry, attempts: 1, nextAttemptAtMillis: 70 },
        { ...retry, nextAttemptAtMillis: 70 },
        { ...retry, attempts: 3, nextAttemptAtMillis: 50 },
      ]) {
        const retained = yield* store.changeDelivery(selected.key, selected.deliveryId, {
          _tag: "Retry",
          retry: staleRetry,
          nowMillis: 36,
        });

        yield* ensure(
          retained.retry.attempts === 2 && retained.retry.nextAttemptAtMillis === 60,
          "stale retry overwrote newer retry state",
        );
      }
      yield* store.cancel(durable.key);

      const completed = yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Complete",
        receipt,
        nowMillis: 40,
      });

      yield* ensure(
        prepared.state === "prepared" && completed.state === "delivered",
        "prepared work did not survive cancellation",
      );

      const stale = yield* Effect.result(
        store.changeDelivery(selected.key, digest("8"), {
          _tag: "Complete",
          receipt,
          nowMillis: 41,
        }),
      );

      yield* ensure(
        Result.isFailure(stale) &&
          stale.failure._tag === "SubscriptionError" &&
          stale.failure.code === "stale-delivery",
        "stale outcome was not fenced",
      );
    }),
);

const catchUpAndCursors = conformanceCase(
  "limits catch-up to one watch and persists recovery cursors",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;
      const accepted = yield* store.accept(event("observed"), defaultSubscriptionLimits);
      const watch = yield* store.register(record("watch"), defaultSubscriptionLimits);
      const selected = delivery(watch, accepted);

      yield* store.catchUp(accepted, selected, 20, defaultSubscriptionLimits);
      yield* store.catchUp(
        accepted,
        { ...selected, selectedAtMillis: 999, retry: { ...selected.retry, attempts: 2 } },
        30,
        defaultSubscriptionLimits,
      );
      yield* ensure(
        (yield* store.get(watch.key))?.state === "consumed",
        "catch-up did not consume watch",
      );
      yield* store.advanceScanCursors({ events: "e", deliveries: "d", recovery: 7 });
      const cursors = yield* store.readScanCursors;

      yield* ensure(
        cursors.events === "e" && cursors.deliveries === "d" && cursors.recovery === 7,
        "scan cursors were not durable",
      );
      yield* ensure(
        (yield* store.nextDeadline) === 0,
        "nonzero scan cursor did not keep recovery armed",
      );
    }),
);

const replayUnderTighterLimits = conformanceCase(
  "preserves retained identities when limits tighten",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;
      const created = yield* store.register(record("retained"), defaultSubscriptionLimits);
      const accepted = yield* store.accept(event("retained"), defaultSubscriptionLimits);

      const limits = {
        ...defaultSubscriptionLimits,
        maxRegistrations: 1,
        maxEvents: 1,
        maxPayloadBytes: 1,
        maxContextBytes: 1,
        maxLifetimeMillis: 1,
      };

      const replayed = yield* store.register(record("retained"), limits);
      const repeated = yield* store.accept(event("retained"), limits);

      yield* ensure(replayed.ordinal === created.ordinal, "registration replay lost identity");
      yield* ensure(repeated.cutoff === accepted.cutoff, "intake replay lost cutoff");
      const newRegistration = yield* Effect.result(store.register(record("new"), limits));
      const newEvent = yield* Effect.result(store.accept(event("new"), limits));

      yield* ensure(Result.isFailure(newRegistration), "new registration ignored tightened limits");
      yield* ensure(Result.isFailure(newEvent), "new intake ignored tightened limits");
    }),
);

const revisionsAndRetention = conformanceCase(
  "preserves historical selection, CAS revisions, and unsettled evidence during retention",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;
      const now = yield* Clock.currentTimeMillis;
      const policy = { replayHorizonMillis: 10_000, completedRetentionMillis: 0, maxTombstones: 8 };
      const limits = { ...defaultSubscriptionLimits, retention: policy };
      const indefinite = record("revision", "continuous");

      const original = yield* store.register(
        {
          ...indefinite,
          configuration: { ...indefinite.configuration, expiresAtMillis: null },
        },
        limits,
      );

      yield* ensure(
        original.configuration.expiresAtMillis === null,
        "UntilCancelled must remain explicit",
      );
      yield* ensure(
        (yield* store.nextDeadline) === null,
        "UntilCancelled cannot create an expiry wake",
      );

      const accepted = yield* store.accept(
        { ...event("retained"), occurredAtMillis: now, acceptedAtMillis: now },
        limits,
      );

      const selected = {
        ...delivery(original, accepted),
        configuration: original.configuration,
        configurationRevision: 1,
      };

      yield* store.select(
        accepted,
        [selected],
        original.ordinal,
        true,
        now + limits.maxLifetimeMillis + 1,
        limits,
      );
      yield* ensure(
        (yield* store.delivery(selected.key)) !== null,
        "UntilCancelled cannot expire at the ordinary maximum lifetime",
      );

      const updated = yield* store.change(original.key, 1, {
        _tag: "Update",
        configuration: { ...original.configuration, context: { edited: true } },
        configurationFingerprint: digest("9"),
        recovery: null,
      });

      yield* ensure(
        updated.configurationRevision === 2 && updated.creationConfiguration !== undefined,
        "Update must preserve creation evidence and advance revision",
      );
      const stale = yield* store.change(original.key, 1, { _tag: "Pause" }).pipe(Effect.result);

      yield* ensure(Result.isFailure(stale), "Stale management must conflict");
      yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Prepare",
        envelope: preparedInput(selected),
        envelopeDigest: digest("4"),
        nowMillis: now,
      });
      yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Complete",
        receipt,
        nowMillis: now,
      });
      yield* store.compact(now, policy, 8);
      yield* ensure(
        (yield* store.delivery(selected.key)) !== null,
        "Admission is not settlement; delivery evidence must remain",
      );
      yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "ObserveSettlement",
        receipt,
        settled: true,
        nowMillis: now,
        nextAttemptAtMillis: now,
      });
      const recovering = yield* store.register(record("recovering-retention", "once"), limits);

      yield* store.deferRecovery(recovering.key, recovering.configurationRevision, {
        attempts: 1,
        nextAttemptAtMillis: null,
        lastFailure: "source-unavailable",
      });
      yield* store.compact(now, policy, 8);
      yield* ensure(
        (yield* store.delivery(selected.key)) !== null,
        "Permanent recovery evidence must protect retained source facts",
      );
      yield* store.deferRecovery(recovering.key, recovering.configurationRevision, null);
      yield* store.compact(now, policy, 8);
      yield* ensure(
        (yield* store.delivery(selected.key)) === null,
        "Settled delivery must release retained capacity",
      );
      yield* ensure(
        (yield* store.event(accepted.eventId))?.tombstone === true,
        "Deduplication tombstone must outlive completed payload",
      );
      const replay = yield* store.accept(accepted, limits);

      yield* ensure(
        Result.isFailure(
          yield* store
            .accept({ ...accepted, occurredAtMillis: now + 1 }, limits)
            .pipe(Effect.result),
        ),
        "An event identity cannot replace its retained source timestamp",
      );

      yield* ensure(
        Result.isFailure(
          yield* store.compact(now, { ...policy, replayHorizonMillis: 1 }, 8).pipe(Effect.result),
        ),
        "Compaction cannot change the persisted replay horizon",
      );
      yield* ensure(
        Result.isFailure(
          yield* store
            .accept(event("without-policy"), defaultSubscriptionLimits)
            .pipe(Effect.result),
        ),
        "Fresh intake cannot disable established retention",
      );
      yield* ensure(
        replay.cutoff === accepted.cutoff && replay.tombstone === true,
        "Duplicate intake cannot reopen routing after compaction",
      );
    }),
);

const sustainedRetention = conformanceCase(
  "processes more than 1000 distinct events within fixed retained quotas",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;
      const policy = { replayHorizonMillis: 100, completedRetentionMillis: 0, maxTombstones: 8 };

      const limits = {
        ...defaultSubscriptionLimits,
        maxEvents: 4,
        maxDeliveries: 4,
        maxDeliveriesPerOwner: 4,
        retention: policy,
      };

      const registration = yield* store.register(record("sustained", "continuous"), limits);

      for (let index = 0; index < 1005; index++) {
        const now = yield* Clock.currentTimeMillis;

        const accepted = yield* store.accept(
          { ...event(`retained-${index}`), occurredAtMillis: now, acceptedAtMillis: now },
          limits,
        );

        const deliveryId = Schema.decodeSync(Digest)(index.toString(16).padStart(64, "0"));

        const selected = {
          ...delivery(registration, accepted),
          deliveryId,
          admissionKey: Schema.decodeSync(IdempotencyKey)(`subscription:${deliveryId}`),
        };

        yield* store.select(accepted, [selected], registration.ordinal, true, now, limits);
        yield* store.changeDelivery(selected.key, selected.deliveryId, {
          _tag: "Prepare",
          envelope: preparedInput(selected),
          envelopeDigest: digest("4"),
          nowMillis: now,
        });
        yield* store.changeDelivery(selected.key, selected.deliveryId, {
          _tag: "Complete",
          receipt,
          nowMillis: now,
        });
        yield* store.changeDelivery(selected.key, selected.deliveryId, {
          _tag: "ObserveSettlement",
          receipt,
          settled: true,
          nowMillis: now,
          nextAttemptAtMillis: now,
        });
        yield* store.compact(now, policy, 8);
        yield* ensure(
          (yield* store.accept(accepted, limits)).tombstone === true,
          "Duplicate acknowledgement must preserve its tombstone",
        );
        yield* TestClock.adjust(20);
      }
      yield* ensure(
        (yield* store.event("retained-0")) === null,
        "Expired identity must be reclaimed",
      );

      const expired = yield* store
        .accept({ ...event("retained-0"), occurredAtMillis: 0 }, limits)
        .pipe(Effect.result);

      yield* ensure(
        Result.isFailure(expired),
        "Reclaimed identity must not be admitted after its replay horizon",
      );
    }),
);

const recoveryRevisionFence = conformanceCase(
  "fences source recovery across pause, resume and configuration replacement",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;

      const initial = yield* store.register(
        {
          ...record("recovery-revisions"),
          recovery: { attempts: 0, nextAttemptAtMillis: 10, lastFailure: null },
        },
        defaultSubscriptionLimits,
      );

      const paused = yield* store.change(initial.key, 1, { _tag: "Pause" });

      yield* store.deferRecovery(initial.key, 1, null);
      yield* ensure(
        (yield* store.get(initial.key))?.recovery?.nextAttemptAtMillis === 10,
        "late provider completion erased paused recovery",
      );

      const resumed = yield* store.change(initial.key, paused.configurationRevision, {
        _tag: "Resume",
      });

      yield* ensure(
        resumed.recovery?.nextAttemptAtMillis === 10,
        "resume lost the recovery obligation",
      );

      const updated = yield* store.change(initial.key, resumed.configurationRevision, {
        _tag: "Update",
        configuration: { ...initial.configuration, source: { name: "replacement", version: "2" } },
        configurationFingerprint: digest("9"),
        recovery: { attempts: 0, nextAttemptAtMillis: 50, lastFailure: null },
      });

      yield* store.deferRecovery(initial.key, resumed.configurationRevision, {
        attempts: 9,
        nextAttemptAtMillis: null,
        lastFailure: "obsolete-source",
      });
      const retained = yield* store.get(initial.key);

      yield* ensure(
        retained?.recovery?.nextAttemptAtMillis === 50 &&
          retained.configurationRevision === updated.configurationRevision,
        "obsolete provider completion overwrote replacement recovery",
      );
      yield* ensure(
        retained?.creationFingerprint === initial.creationFingerprint &&
          retained.creationConfiguration.source.name === initial.configuration.source.name,
        "revision rewrote creation identity",
      );
    }),
);

const retryGenerationFence = conformanceCase(
  "rearms only one delivery generation and retains late receipt authority",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;

      const subscription = yield* store.register(
        record("retry-generation", "continuous"),
        defaultSubscriptionLimits,
      );

      const accepted = yield* store.accept(event("retry-generation"), defaultSubscriptionLimits);
      const selected = { ...delivery(subscription, accepted), observeSettlement: true };

      yield* store.select(
        accepted,
        [selected],
        subscription.ordinal,
        true,
        20,
        defaultSubscriptionLimits,
      );
      yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Prepare",
        envelope: preparedInput(selected),
        envelopeDigest: digest("4"),
        nowMillis: 30,
      });

      const parked = yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Retry",
        nowMillis: 40,
        retry: {
          ...selected.retry,
          attempts: 8,
          automaticAttempts: 8,
          parked: true,
          nextAttemptAtMillis: 50,
          lastAttemptAtMillis: 40,
          lastFailure: "ambiguous",
        },
      });

      yield* ensure(
        (yield* store.pendingDeliveries(100, "", 10)).length === 0,
        "parked admission remained due for settlement observation",
      );
      yield* ensure(
        (yield* store.nextDeadline) === null,
        "parked admission retained a busy-loop deadline",
      );

      const recovered = yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Recover",
        expectedGeneration: parked.retry.generation,
        nowMillis: 50,
      });

      const stale = yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Retry",
        nowMillis: 60,
        retry: { ...parked.retry, attempts: 9, nextAttemptAtMillis: 70 },
      });

      yield* ensure(
        !stale.retry.parked && stale.retry.generation === recovered.retry.generation,
        "stale retry undid recovery",
      );

      const completed = yield* store.changeDelivery(selected.key, selected.deliveryId, {
        _tag: "Complete",
        receipt,
        nowMillis: 70,
      });

      yield* ensure(
        completed.receipt?.receiptId === receipt.receiptId &&
          completed.envelopeDigest === digest("4"),
        "late receipt or immutable envelope was lost",
      );
    }),
);

const retentionFairness = conformanceCase(
  "advances bounded maintenance beyond protected oldest events",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* SubscriptionStore;
      const policy = { replayHorizonMillis: 1_000, completedRetentionMillis: 0, maxTombstones: 8 };
      const limits = { ...defaultSubscriptionLimits, retention: policy };

      for (let index = 0; index < 6; index++) {
        const accepted = yield* store.accept(
          { ...event(`protected-${index}`), occurredAtMillis: 0, acceptedAtMillis: 0 },
          limits,
        );

        if (index === 5) yield* store.select(accepted, [], 0, true, 0, limits);
      }
      yield* TestClock.setTime(10);
      for (let pass = 0; pass < 8; pass++) yield* store.compact(10, policy, 1);
      yield* ensure(
        (yield* store.event("protected-5"))?.tombstone === true,
        "protected first pages starved a reclaimable event",
      );
      yield* ensure(
        (yield* store.event("protected-0"))?.tombstone !== true,
        "unfinished event was reclaimed",
      );
      yield* TestClock.setTime(1_001);
      for (let pass = 0; pass < 8; pass++) yield* store.compact(1_001, policy, 1);
      yield* ensure(
        (yield* store.event("protected-5")) === null,
        "idle maintenance did not expire tombstone",
      );

      const rejected = yield* store
        .accept({ ...event("protected-5"), occurredAtMillis: 0 }, limits)
        .pipe(Effect.result);

      // The caller cannot manufacture a recent occurrence to resurrect a pruned identity.
      yield* ensure(Result.isFailure(rejected), "retention state became inconsistent");
    }),
);

export const subscriptionStoreConformanceCases: ReadonlyArray<SubscriptionStoreConformanceCase> = [
  recoveryRevisionFence,
  retryGenerationFence,
  retentionFairness,
  sustainedRetention,
  revisionsAndRetention,
  cutoffAndIntake,
  onceAndCapacity,
  preparationLifecycle,
  catchUpAndCursors,
  replayUnderTighterLimits,
];
