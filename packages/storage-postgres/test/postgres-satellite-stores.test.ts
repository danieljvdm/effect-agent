import * as PostgresMessageDeliveryStore from "@effect-agent/storage-postgres/postgres-message-delivery-store";
import * as PostgresScheduleStore from "@effect-agent/storage-postgres/postgres-schedule-store";
import {
  PostgresStorageConfig,
  PostgresStorageConfigValue,
} from "@effect-agent/storage-postgres/postgres-storage-config";
import { PostgresStorageFailpoint } from "@effect-agent/storage-postgres/postgres-storage-failpoint";
import * as PostgresSubscriptionStore from "@effect-agent/storage-postgres/postgres-subscription-store";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
import { Digest } from "effect-agent/records";
import { defaultSubscriptionLimits, SubscriptionStore } from "effect-agent/subscription";
import { messageDeliveryStoreConformanceCases } from "effect-agent/testing/message-delivery-store-conformance";
import { scheduleStoreConformanceCases } from "effect-agent/testing/schedule-store-conformance";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "effect-agent/testing/subscription-store-conformance";
import { TestClock } from "effect/testing";

import { clientLayer, withTemporaryDatabase } from "./harness.ts";

const storageServices = (url: string) =>
  Layer.mergeAll(
    Layer.succeed(PostgresStorageConfig)(
      PostgresStorageConfigValue.make({
        observationPollInterval: 1,
        lockTimeout: 5_000,
        ownershipLeaseDuration: 30_000,
        verifyOnOpen: false,
        schema: "public",
      }),
    ),
    PostgresStorageFailpoint.layer,
    clientLayer(url),
  );

describe("PostgresScheduleStore", () => {
  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((url) =>
        conformanceCase.run.pipe(
          Effect.provide(PostgresScheduleStore.layer.pipe(Layer.provide(storageServices(url)))),
        ),
      ),
    );
  }
});

describe("PostgresSubscriptionStore", () => {
  it.effect("retains epoch timestamps and long replay horizons across reopen", () =>
    withTemporaryDatabase((url) => {
      const nowMillis = 1_800_000_000_000;

      const retention = {
        replayHorizonMillis: 30 * 24 * 60 * 60 * 1_000,
        completedRetentionMillis: 0,
        maxTombstones: 8,
      };

      const limits = { ...defaultSubscriptionLimits, retention };

      const storeLayer = PostgresSubscriptionStore.layer(subscriptionConformancePartition).pipe(
        Layer.provide(storageServices(url)),
      );

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
              payloadDigest: Schema.decodeSync(Digest)("a".repeat(64)),
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
          Effect.provide(
            PostgresSubscriptionStore.layer(subscriptionConformancePartition).pipe(
              Layer.provide(storageServices(url)),
            ),
          ),
        ),
      ),
    );
  }
});

describe("PostgresMessageDeliveryStore", () => {
  for (const conformanceCase of messageDeliveryStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((url) =>
        conformanceCase.run.pipe(
          Effect.provide(
            Layer.mergeAll(
              PostgresMessageDeliveryStore.layer().pipe(Layer.provide(storageServices(url))),
              NodeCrypto.layer,
            ),
          ),
        ),
      ),
    );
  }
});
