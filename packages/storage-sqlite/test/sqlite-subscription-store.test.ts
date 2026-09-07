import {
  SqliteStorageConfig,
  SqliteStorageConfigValue,
} from "@effect-agent/storage-sqlite/SqliteStorageConfig";
import { SqliteStorageFailpoint } from "@effect-agent/storage-sqlite/SqliteStorageFailpoint";
import { subscriptionStoreLayer } from "@effect-agent/storage-sqlite/SqliteSubscriptionStore";
import { Digest } from "@effect-agent/thread/Records";
import {
  AcceptedEvent,
  defaultSubscriptionLimits,
  subscriptionDeliveryKeyString,
  SubscriptionStore,
  SubscriptionFailpoint,
  SubscriptionFailpointError,
} from "@effect-agent/thread/Subscription";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "@effect-agent/thread/testing/SubscriptionStoreConformance";
import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import { Effect, FileSystem, Layer, Schema } from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

const testLayer = (filename: string) =>
  subscriptionStoreLayer(subscriptionConformancePartition).pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(SqliteStorageConfig)(
          SqliteStorageConfigValue.make({
            observationPollInterval: 1,
            busyTimeout: 5_000,
            ownershipLeaseDuration: 30_000,
            verifyOnOpen: false,
          }),
        ),
        SqliteStorageFailpoint.layer,
        SqliteClient.layer({ filename }),
      ),
    ),
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-subscription-sqlite-",
      });

      return yield* use(`${directory}/subscriptions.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("SqliteSubscriptionStore", () => {
  for (const testCase of subscriptionStoreConformanceCases) {
    it.effect(
      testCase.name,
      () =>
        withTemporaryDatabase((filename) => testCase.run.pipe(Effect.provide(testLayer(filename)))),
      // Shared conformance includes sustained durable writes on CI's slower filesystem.
      30_000,
    );
  }
});

it.effect(
  "persists bounded retention progress across faults and reopen, preserving corrupt evidence",
  () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const partition = subscriptionConformancePartition;

        const brokenKey = {
          subscription: { partition, ownerId: "owner", subscriptionId: "broken" },
          eventId: "malformed",
        };

        const policy = {
          replayHorizonMillis: 10_000,
          completedRetentionMillis: 0,
          maxTombstones: 8,
        };

        const limits = { ...defaultSubscriptionLimits, retention: policy };
        let armed: string | undefined;

        const failpoints = Layer.succeed(SubscriptionFailpoint)({
          hit: (point) =>
            point === armed ? SubscriptionFailpointError.make({ point }) : Effect.void,
        });

        const dependencies = Layer.mergeAll(
          Layer.succeed(SqliteStorageConfig)(
            SqliteStorageConfigValue.make({
              observationPollInterval: 1,
              busyTimeout: 5_000,
              ownershipLeaseDuration: 30_000,
              verifyOnOpen: false,
            }),
          ),
          SqliteStorageFailpoint.layer,
          SqliteClient.layer({ filename }),
        );

        const reopen = <A, E>(
          effect: Effect.Effect<A, E, SubscriptionStore | SqlClientService.SqlClient>,
        ) =>
          effect.pipe(
            Effect.provide(
              subscriptionStoreLayer(partition).pipe(
                Layer.provideMerge(dependencies),
                Layer.provide(failpoints),
              ),
            ),
          );

        yield* TestClock.setTime(1_000);
        yield* reopen(
          Effect.gen(function* () {
            const store = yield* SubscriptionStore;
            const sql = yield* SqlClientService.SqlClient;

            for (const eventId of ["a-corrupt", "b-mismatch", "c-reclaim"]) {
              const accepted = yield* store.accept(
                AcceptedEvent.make({
                  schemaVersion: 1,
                  partition,
                  eventId,
                  source: { name: "host", version: "1" },
                  matchingKey: "entity",
                  payload: null,
                  payloadDigest: Schema.decodeSync(Digest)("a".repeat(64)),
                  occurredAtMillis: 1_000,
                  acceptedAtMillis: 1_000,
                  cutoff: 0,
                  cursor: 0,
                  routingComplete: false,
                  routingFailure: null,
                  nextAttemptAtMillis: 1_000,
                }),
                limits,
              );

              yield* store.select(accepted, [], 0, true, 1_000, limits);
            }
            yield* sql`UPDATE effect_agent_subscription_events SET record_json='{}' WHERE event_id='a-corrupt'`;
            yield* sql`UPDATE effect_agent_subscription_events SET record_json=(SELECT record_json FROM effect_agent_subscription_events WHERE event_id='c-reclaim') WHERE event_id='b-mismatch'`;
            yield* sql`INSERT INTO effect_agent_subscription_deliveries
              (tenant_id, source_address, owner_id, subscription_id, event_id, delivery_key, state, next_attempt_at_millis, record_json)
              VALUES (${partition.tenantId}, ${partition.address}, ${brokenKey.subscription.ownerId}, ${brokenKey.subscription.subscriptionId}, ${brokenKey.eventId}, ${subscriptionDeliveryKeyString(brokenKey)}, 'selected', 0, '{')`;
          }),
        );
        armed = "subscription:compact:before";
        expect(
          (yield* reopen(
            Effect.flatMap(SubscriptionStore, (store) => store.compact(1_000, policy, 1)),
          ).pipe(Effect.flip))._tag,
        ).toBe("SubscriptionFailpointError");
        armed = "subscription:compact:after";
        expect(
          (yield* reopen(
            Effect.flatMap(SubscriptionStore, (store) => store.compact(1_000, policy, 1)),
          ).pipe(Effect.flip))._tag,
        ).toBe("SubscriptionFailpointError");
        armed = undefined;
        yield* reopen(
          Effect.gen(function* () {
            const store = yield* SubscriptionStore;
            const sql = yield* SqlClientService.SqlClient;

            expect(yield* sql`SELECT event_cursor FROM effect_agent_event_retention`).toEqual([
              { event_cursor: "a-corrupt" },
            ]);
            expect(yield* store.compact(1_000, policy, 1)).toBe(0);
            expect(yield* store.compact(1_000, policy, 1)).toBe(1);
            expect((yield* store.event("c-reclaim"))?.tombstone).toBe(true);
            expect(
              yield* sql`SELECT record_json FROM effect_agent_subscription_events WHERE event_id='a-corrupt'`,
            ).toEqual([{ record_json: "{}" }]);
            for (const state of ["selected", "delivered"]) {
              yield* sql`UPDATE effect_agent_subscription_deliveries SET state=${state} WHERE event_id=${brokenKey.eventId}`;
              expect(yield* store.pendingDeliveries(1_000, "", 1)).toEqual([brokenKey]);
              expect(yield* store.nextDeadline).toBe(0);
              yield* store.advanceScanCursors({ events: "", deliveries: "", recovery: 0 });
            }
            expect(yield* store.delivery(brokenKey).pipe(Effect.flip)).toMatchObject({
              reason: "corrupt",
            });
            expect(
              yield* sql`SELECT record_json FROM effect_agent_subscription_deliveries WHERE event_id=${brokenKey.eventId}`,
            ).toEqual([{ record_json: "{" }]);
            // Remove only the injected fixture after proving it survived compaction and reopen.
            yield* sql`DELETE FROM effect_agent_subscription_deliveries WHERE event_id=${brokenKey.eventId}`;
            yield* store.advanceScanCursors({ events: "", deliveries: "", recovery: 0 });
            expect(yield* store.nextDeadline).toBe(61_000);
          }),
        );
      }),
    ),
);
