import * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Result, Schema, String } from "effect";
import { ActivityProcessorStore } from "effect-agent/activity-store";
import { SubscriptionStore } from "effect-agent/subscription";
import { subscriptionConformancePartition } from "effect-agent/testing/subscription-store-conformance";
import { ThreadMaterialization, ThreadStore } from "effect-agent/thread-store";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { withTemporaryDatabase } from "./harness.ts";

it.effect("opens Activity independently, then composes every port over one pool", () =>
  withTemporaryDatabase((url) =>
    Effect.gen(function* () {
      const options = { schema: "select" };
      const sql = yield* SqlClient.SqlClient;

      const journalExists = sql`SELECT to_regclass('"select".effect_agent_storage_version') IS NOT NULL AS present`;

      expect(yield* journalExists).toEqual([{ present: false }]);
      yield* ActivityProcessorStore.pipe(
        Effect.provide(PostgresStorage.activityStoreLayer(options)),
      );
      expect(yield* journalExists).toEqual([{ present: false }]);

      yield* Effect.gen(function* () {
        const store = yield* ThreadStore;
        const subscriptions = yield* SubscriptionStore;

        expect(yield* subscriptions.nextDeadline).toBeNull();
        yield* store.materialize(
          yield* Schema.decodeEffect(ThreadMaterialization)({
            threadId: "native-composition",
            producerEpoch: 1,
          }),
        );

        expect(
          yield* sql`SELECT COUNT(*) AS connection_count FROM ${sql("pgStatActivity")} WHERE datname = current_database()`,
        ).toEqual([{ connectionCount: 1n }]);
        expect(yield* sql`SELECT current_schema() AS current_schema`).toEqual([
          { currentSchema: "public" },
        ]);
        expect(yield* sql`SELECT thread_id FROM "select".effect_agent_threads`).toEqual([
          { threadId: "native-composition" },
        ]);
        expect(yield* journalExists).toEqual([{ present: true }]);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            PostgresStorage.layerWith(options),
            PostgresStorage.scheduleStoreLayer(options),
            PostgresStorage.messageDeliveryStoreLayer(options),
            PostgresStorage.subscriptionStoreLayer(subscriptionConformancePartition, options),
            PostgresStorage.activityStoreLayer(options),
          ),
        ),
      );
    }).pipe(
      Effect.provide([
        PgClient.layer({
          url: Redacted.make(url),
          maxConnections: 1,
          transformQueryNames: String.camelToSnake,
          transformResultNames: String.snakeToCamel,
        }),
        NodeCrypto.layer,
      ]),
    ),
  ),
);

// Regression introduced in 6df51d36: nested storage must fail before reserving another connection.
it.live(
  "rejects a store mutation inside the shared client's transaction without changing state",
  () =>
    withTemporaryDatabase((url) => {
      return Effect.gen(function* () {
        const store = yield* ThreadStore;
        const sql = yield* SqlClient.SqlClient;

        const materialization = yield* Schema.decodeEffect(ThreadMaterialization)({
          threadId: "nested-transaction",
          producerEpoch: 1,
        });

        const result = yield* sql
          .withTransaction(store.materialize(materialization))
          .pipe(Effect.timeout("1 second"), Effect.result);

        const rows = yield* sql`
          SELECT thread_id FROM effect_agent_threads WHERE thread_id = ${materialization.threadId}
        `;

        expect(rows).toEqual([]);
        expect(Result.isFailure(result) && result.failure).toMatchObject({
          _tag: "ThreadStoreError",
        });
      }).pipe(
        Effect.provide(PostgresStorage.layer),
        Effect.provide([
          PgClient.layer({ url: Redacted.make(url), maxConnections: 1 }),
          NodeCrypto.layer,
        ]),
      );
    }),
);

it.effect("rejects invalid subscription partitions before using the native client", () =>
  Effect.gen(function* () {
    const result = yield* SubscriptionStore.pipe(
      Effect.provide(PostgresStorage.subscriptionStoreLayer({ tenantId: "", address: "" })),
      Effect.result,
    );

    expect(Result.isFailure(result) && result.failure).toMatchObject({
      _tag: "SubscriptionError",
      reason: "validation",
      code: "partition",
    });
  }).pipe(Effect.provide(PgClient.layer({ port: 1 }))),
);
