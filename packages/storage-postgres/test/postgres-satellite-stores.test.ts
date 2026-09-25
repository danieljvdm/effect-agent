import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Result, Schema } from "effect";
import { MessageDeliveryStore } from "effect-agent/message-delivery";
import { Digest } from "effect-agent/records";
import { ScheduleRecord, ScheduleStore } from "effect-agent/schedule";
import {
  defaultSubscriptionLimits,
  SubscriptionConfiguration,
  SubscriptionDelivery,
  SubscriptionRecord,
  SubscriptionStore,
} from "effect-agent/subscription";
import {
  makeMessageDeliveryFixture,
  messageDeliveryStoreConformanceCases,
} from "effect-agent/testing/message-delivery-store-conformance";
import { scheduleStoreConformanceCases } from "effect-agent/testing/schedule-store-conformance";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "effect-agent/testing/subscription-store-conformance";
import { TestClock } from "effect/testing";

import { storage, withTemporaryDatabase } from "./harness.ts";

describe("PostgresScheduleStore", () => {
  it.effect("counts capacity without parsing arbitrary schedule inputs in PostgreSQL", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const store = yield* ScheduleStore;
        const text = "nul:\u0000 lone:\ud800 slash:\\u0000 emoji:😀";
        const { envelope } = yield* makeMessageDeliveryFixture("schedule", "sender", text);

        const record = yield* Schema.decodeEffect(ScheduleRecord)({
          schemaVersion: 1,
          owner: { tenantId: "unicode", ownerId: "owner" },
          scheduleId: "unicode",
          creationFingerprint: envelope.inputDigest,
          createdBy: envelope.deliveryPrincipal,
          createdAtMillis: 1,
          updatedAtMillis: 1,
          configurationRevision: 1,
          version: 1,
          configuration: {
            timing: { _tag: "At", atMillis: 100 },
            destination: { _tag: "ExistingThread", threadId: envelope.threadId },
            deliveryPrincipal: envelope.deliveryPrincipal,
            agentId: envelope.agentId,
            definitions: envelope.definitions,
            input: envelope.input,
            inputDigest: envelope.inputDigest,
          },
          state: "active",
          nextAtMillis: 100,
          pending: null,
          lastReceipt: null,
          lastRefusal: null,
          lastSkippedRange: null,
        });

        yield* store.insert(record, 1);

        const later = yield* Schema.decodeEffect(ScheduleRecord)({
          ...record,
          scheduleId: "later",
        });

        const full = yield* store.insert(later, 1).pipe(Effect.result);

        expect(Result.isFailure(full) && full.failure._tag).toBe("ScheduleCapacityError");
        yield* store.insert(later, 2);
        expect((yield* store.get(record))?.configuration.input).toEqual({ text });
      }).pipe(Effect.provide(Layer.mergeAll(storage(url).scheduleStore, NodeCrypto.layer))),
    ),
  );

  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((url) =>
        conformanceCase.run.pipe(Effect.provide(storage(url).scheduleStore)),
      ),
    );
  }
});

describe("PostgresSubscriptionStore", () => {
  it.effect(
    "reads pending deadlines without parsing arbitrary subscription payloads in PostgreSQL",
    () =>
      withTemporaryDatabase((url) =>
        Effect.gen(function* () {
          const store = yield* SubscriptionStore;
          const text = "nul:\u0000 lone:\ud800 slash:\\u0000 emoji:😀";
          const { envelope } = yield* makeMessageDeliveryFixture("subscription", "sender", text);

          const configuration = yield* Schema.decodeEffect(SubscriptionConfiguration)({
            source: { name: "trusted", version: "1" },
            matchingKey: "match",
            parameters: { text },
            context: { text },
            mode: "continuous",
            expiresAtMillis: 100_000,
            destination: { _tag: "ExistingThread", threadId: envelope.threadId },
            deliveryPrincipal: envelope.deliveryPrincipal,
            agentId: envelope.agentId,
            definitions: envelope.definitions,
          });

          const record = yield* Schema.decodeEffect(SubscriptionRecord)({
            schemaVersion: 1,
            key: {
              partition: subscriptionConformancePartition,
              ownerId: "owner",
              subscriptionId: "unicode",
            },
            creationFingerprint: envelope.inputDigest,
            configurationRevision: 1,
            configurationFingerprint: envelope.inputDigest,
            creationConfiguration: configuration,
            createdBy: envelope.deliveryPrincipal,
            createdAtMillis: 1,
            ordinal: 0,
            configuration,
            state: "active",
            recovery: null,
          });

          const registered = yield* store.register(record, defaultSubscriptionLimits);

          const event = yield* store.accept(
            {
              schemaVersion: 1,
              partition: subscriptionConformancePartition,
              eventId: "unicode-event",
              source: configuration.source,
              matchingKey: configuration.matchingKey,
              payload: { text },
              payloadDigest: envelope.inputDigest,
              acceptedAtMillis: 10,
              cutoff: 0,
              cursor: 0,
              routingComplete: false,
              routingFailure: null,
              nextAttemptAtMillis: 10,
            },
            defaultSubscriptionLimits,
          );

          const delivery = yield* Schema.decodeEffect(SubscriptionDelivery)({
            schemaVersion: 1,
            key: { subscription: registered.key, eventId: event.eventId },
            deliveryId: envelope.inputDigest,
            subscriptionFingerprint: registered.configurationFingerprint,
            configurationRevision: registered.configurationRevision,
            configuration,
            eventDigest: event.payloadDigest,
            source: event.source,
            threadId: envelope.threadId,
            admissionKey: `subscription:${envelope.inputDigest}`,
            selectedAtMillis: 20,
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
          });

          yield* store.select(event, [delivery], event.cutoff, true, 20, defaultSubscriptionLimits);
          expect(yield* store.pendingDeliveries(20, "", 10)).toEqual([delivery.key]);
          expect(yield* store.nextDeadline).toBe(20);
          expect((yield* store.delivery(delivery.key))?.configuration.parameters).toEqual({ text });
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              storage(url).subscriptionStore(subscriptionConformancePartition),
              NodeCrypto.layer,
            ),
          ),
        ),
      ),
  );

  it.effect("initializes retention tables and a shared partition concurrently", () =>
    withTemporaryDatabase((url) =>
      Effect.all(
        Array.from({ length: 2 }, () =>
          Effect.gen(function* () {
            const store = yield* SubscriptionStore;

            expect(yield* store.nextDeadline).toBeNull();
          }).pipe(Effect.provide(storage(url).subscriptionStore(subscriptionConformancePartition))),
        ),
        { concurrency: 2 },
      ),
    ),
  );

  it.effect("retains epoch timestamps and long replay horizons across reopen", () =>
    withTemporaryDatabase((url) => {
      const nowMillis = 1_800_000_000_000;

      const retention = {
        replayHorizonMillis: 30 * 24 * 60 * 60 * 1_000,
        completedRetentionMillis: 0,
        maxTombstones: 8,
      };

      const limits = { ...defaultSubscriptionLimits, retention };

      const storeLayer = storage(url).subscriptionStore(subscriptionConformancePartition);

      return Effect.gen(function* () {
        yield* TestClock.setTime(nowMillis);
        yield* Effect.gen(function* () {
          const store = yield* SubscriptionStore;

          const accepted = yield* store.accept(
            {
              schemaVersion: 1,
              partition: subscriptionConformancePartition,
              eventId: "retained-epoch",
              source: { name: "trusted", version: "1" },
              matchingKey: "match",
              payload: { value: "retained" },
              payloadDigest: yield* Schema.decodeEffect(Digest)("a".repeat(64)),
              occurredAtMillis: nowMillis,
              acceptedAtMillis: nowMillis,
              cutoff: 0,
              cursor: 0,
              routingComplete: false,
              routingFailure: null,
              nextAttemptAtMillis: nowMillis,
            },
            limits,
          );

          yield* store.select(accepted, [], accepted.cutoff, true, nowMillis, limits);
          expect(yield* store.compact(nowMillis, retention, 8)).toBe(1);
        }).pipe(Effect.provide(storeLayer));

        yield* Effect.gen(function* () {
          const store = yield* SubscriptionStore;

          expect(yield* store.event("retained-epoch")).toMatchObject({
            acceptedAtMillis: nowMillis,
            occurredAtMillis: nowMillis,
            tombstone: true,
            payload: null,
          });
          expect(yield* store.nextDeadline).toBe(nowMillis + 60_000);
          yield* TestClock.setTime(nowMillis + retention.replayHorizonMillis);
          expect(
            yield* store.compact(nowMillis + retention.replayHorizonMillis, retention, 8),
          ).toBe(1);
          expect(yield* store.event("retained-epoch")).toBeNull();
          expect(yield* store.nextDeadline).toBeNull();
        }).pipe(Effect.provide(storeLayer));
      });
    }),
  );

  for (const conformanceCase of subscriptionStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((url) =>
        conformanceCase.run.pipe(
          Effect.provide(storage(url).subscriptionStore(subscriptionConformancePartition)),
        ),
      ),
    );
  }
});

describe("PostgresMessageDeliveryStore", () => {
  it.effect(
    "keeps arbitrary JSON payloads readable while counting the owner's later admissions",
    () =>
      withTemporaryDatabase((url) =>
        Effect.gen(function* () {
          const store = yield* MessageDeliveryStore;
          const text = "nul:\u0000 lone:\ud800 slash:\\u0000 emoji:😀";
          const first = yield* makeMessageDeliveryFixture("unicode", "sender", text);

          yield* store.insert(first);
          yield* store.insert(yield* makeMessageDeliveryFixture("later", "sender"));
          expect((yield* store.get(first.key))?.envelope.input).toEqual({ text });
          expect(
            (yield* store.list({ ownerThreadId: first.key.ownerThreadId, limit: 10 })).items,
          ).toHaveLength(2);
        }).pipe(
          Effect.provide(Layer.mergeAll(storage(url).messageDeliveryStore(), NodeCrypto.layer)),
        ),
      ),
  );

  for (const conformanceCase of messageDeliveryStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((url) =>
        conformanceCase.run.pipe(
          Effect.provide(Layer.mergeAll(storage(url).messageDeliveryStore(), NodeCrypto.layer)),
        ),
      ),
    );
  }
});
