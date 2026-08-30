import { AgentId, ConversationId, ReceiptId, SubmissionId } from "@effect-agent/core";
import { Effect, Result, Schema } from "effect";

import { Receipt } from "./durable-runtime.ts";
import { IdempotencyKey, Principal, QueueSequence } from "./ledger.ts";
import { DefinitionDigests, Digest } from "./records.ts";
import {
  type AcceptedEvent,
  defaultSubscriptionLimits,
  type SourcePartition,
  type SubscriptionDelivery,
  type SubscriptionRecord,
  SubscriptionStore,
  type SubscriptionStoreFailure,
} from "./subscription.ts";

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
const conversationId = Schema.decodeSync(ConversationId)("subscription-conformance-conversation");
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
  configuration: {
    source: { name: "trusted", version: "1" },
    matchingKey: "match",
    parameters: { name },
    context: { name },
    mode,
    expiresAtMillis: 100_000,
    destination: { _tag: "ExistingConversation", conversationId },
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
    subscriptionFingerprint: subscription.creationFingerprint,
    eventDigest: accepted.payloadDigest,
    conversationId,
    admissionKey: Schema.decodeSync(IdempotencyKey)(`subscription:${deliveryId}`),
    selectedAtMillis: suffix === "0" ? 20 : 21,
    state: "selected",
    envelope: null,
    envelopeDigest: null,
    retry: { attempts: 0, nextAttemptAtMillis: 20, lastAttemptAtMillis: null, lastFailure: null },
    receipt: null,
    refusal: null,
  };
};

const preparedInput = (selected: SubscriptionDelivery) => ({
  schemaVersion: 1 as const,
  conversationId: selected.conversationId,
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
  conversationId,
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

export const subscriptionStoreConformanceCases: ReadonlyArray<SubscriptionStoreConformanceCase> = [
  cutoffAndIntake,
  onceAndCapacity,
  preparationLifecycle,
  catchUpAndCursors,
  replayUnderTighterLimits,
];
