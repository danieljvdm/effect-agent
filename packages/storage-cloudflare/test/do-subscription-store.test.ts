import {
  DoSubscriptionTransaction,
  doSubscriptionStoreLayer,
} from "@effect-agent/storage-cloudflare/DoSubscriptionStore";
import { Digest } from "@effect-agent/thread/Records";
import {
  AcceptedEvent,
  defaultSubscriptionLimits,
  SubscriptionFailpoint,
  SubscriptionFailpointError,
  SubscriptionError,
  SubscriptionStore,
} from "@effect-agent/thread/Subscription";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "@effect-agent/thread/testing/SubscriptionStoreConformance";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer, Schema } from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import { withScheduleStorage } from "./harness.ts";

let objectCounter = 0;

describe("Durable Object SubscriptionStore conformance", () => {
  for (const testCase of subscriptionStoreConformanceCases) {
    // oxlint-disable-next-line vitest/valid-title -- exported contract cases own their names
    it(
      String(testCase.name),
      () =>
        expect(
          withScheduleStorage(`subscription-store-${objectCounter++}`, (storage) =>
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              const transaction = DoSubscriptionTransaction.of({
                run: (body) =>
                  sql
                    .withTransaction(body(() => Effect.void))
                    .pipe(
                      Effect.catchTag("SqlError", () =>
                        SubscriptionError.make({ reason: "storage", code: "test-transaction" }),
                      ),
                    ),
              });

              const dependencies = Layer.merge(
                Layer.succeed(SqlClientService.SqlClient)(sql),
                Layer.succeed(DoSubscriptionTransaction)(transaction),
              );

              yield* testCase.run.pipe(
                Effect.provide(
                  doSubscriptionStoreLayer(subscriptionConformancePartition).pipe(
                    Layer.provide(dependencies),
                  ),
                ),
              );
            }).pipe(
              Effect.provide(Layer.merge(SqliteClient.layer({ storage }), TestClock.layer())),
            ),
          ),
        ).resolves.toBeUndefined(),
      30_000, // Real transactional conformance also runs beside other Workerd suites in ready.
    );
  }

  it("rejects an unsupported subscription version without mutation", () =>
    expect(
      withScheduleStorage("subscription-store-unsupported-version", (storage) =>
        Effect.gen(function* () {
          const sql = yield* SqlClientService.SqlClient;

          yield* sql`
            CREATE TABLE effect_agent_subscription_store_state (
              singleton INTEGER PRIMARY KEY NOT NULL,
              storage_version INTEGER NOT NULL,
              alarm_generation INTEGER NOT NULL
            )
          `.withoutTransform;
          for (const table of [
            "effect_agent_subscription_sequences",
            "effect_agent_subscriptions",
            "effect_agent_subscription_events",
            "effect_agent_subscription_deliveries",
          ]) {
            yield* sql.unsafe(`CREATE TABLE ${table} (sentinel TEXT)`);
          }
          yield* sql`
            INSERT INTO effect_agent_subscription_store_state (
              singleton, storage_version, alarm_generation
            ) VALUES (1, 1, 9)
          `.withoutTransform;

          const transaction = DoSubscriptionTransaction.of({
            run: (body) =>
              sql
                .withTransaction(body(() => Effect.void))
                .pipe(
                  Effect.catchTag("SqlError", () =>
                    SubscriptionError.make({ reason: "storage", code: "test-transaction" }),
                  ),
                ),
          });

          const dependencies = Layer.merge(
            Layer.succeed(SqlClientService.SqlClient)(sql),
            Layer.succeed(DoSubscriptionTransaction)(transaction),
          );

          const failure = yield* SubscriptionStore.pipe(
            Effect.provide(
              doSubscriptionStoreLayer(subscriptionConformancePartition).pipe(
                Layer.provide(dependencies),
              ),
            ),
            Effect.flip,
          );

          expect(failure).toMatchObject({
            _tag: "SubscriptionError",
            reason: "corrupt",
            code: "incompatible subscription storage version 1; expected 3",
          });

          const state = yield* sql<Record<string, unknown>>`
            SELECT storage_version, alarm_generation
            FROM effect_agent_subscription_store_state
          `;

          expect(state).toEqual([{ storage_version: 1, alarm_generation: 9 }]);
        }).pipe(Effect.provide(Layer.merge(SqliteClient.layer({ storage }), TestClock.layer()))),
      ),
    ).resolves.toBeUndefined());
});

it("persists bounded retention progress across faults and reopen, preserving corrupt evidence", () =>
  withScheduleStorage("retention-reopen", (storage) =>
    Effect.gen(function* () {
      const partition = subscriptionConformancePartition;
      const policy = { replayHorizonMillis: 10_000, completedRetentionMillis: 0, maxTombstones: 8 };
      const limits = { ...defaultSubscriptionLimits, retention: policy };
      let armed: string | undefined;

      const failpoints = Layer.succeed(SubscriptionFailpoint)({
        hit: (point) =>
          point === armed ? SubscriptionFailpointError.make({ point }) : Effect.void,
      });

      const dependencies = Layer.effect(
        DoSubscriptionTransaction,
        Effect.map(SqlClientService.SqlClient, (sql) => ({
          run: (body) =>
            sql
              .withTransaction(body(() => Effect.void))
              .pipe(
                Effect.catchTag("SqlError", () =>
                  SubscriptionError.make({ reason: "storage", code: "test-transaction" }),
                ),
              ),
        })),
      ).pipe(Layer.provideMerge(SqliteClient.layer({ storage })));

      const reopen = <A, E>(
        effect: Effect.Effect<A, E, SubscriptionStore | SqlClientService.SqlClient>,
      ) =>
        effect.pipe(
          Effect.provide(
            doSubscriptionStoreLayer(partition).pipe(
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
          expect(yield* store.nextDeadline).toBe(61_000);
        }),
      );
    }).pipe(Effect.provide(TestClock.layer())),
  ));
