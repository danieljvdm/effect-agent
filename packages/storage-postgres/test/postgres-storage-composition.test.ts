import * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Result, Schema } from "effect";
import { ActivityProcessorStore } from "effect-agent/activity-store";
import { SubscriptionStore } from "effect-agent/subscription";
import { subscriptionConformancePartition } from "effect-agent/testing/subscription-store-conformance";
import { ThreadStore } from "effect-agent/thread-store";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { withTemporaryDatabase } from "./harness.ts";

it.effect("opens Activity independently, then composes every port over one pool", () =>
  withTemporaryDatabase((url) =>
    Effect.gen(function* () {
      const storage = PostgresStorage.make({
        client: { url: Redacted.make(url), maxConnections: 1 },
      });

      const journalExists = Effect.flatMap(
        SqlClient.SqlClient,
        (sql) => sql`SELECT to_regclass('effect_agent_storage_version') IS NOT NULL AS present`,
      );

      expect(yield* journalExists.pipe(Effect.provide(storage.clientLayer))).toEqual([
        { present: false },
      ]);
      yield* ActivityProcessorStore.pipe(Effect.provide(storage.activityStore));
      expect(yield* journalExists.pipe(Effect.provide(storage.clientLayer))).toEqual([
        { present: false },
      ]);

      yield* Effect.gen(function* () {
        yield* ThreadStore;
        const subscriptions = yield* SubscriptionStore;

        expect(yield* subscriptions.nextDeadline).toBeNull();
        const sql = yield* SqlClient.SqlClient;

        const connections = yield* Schema.decodeUnknownEffect(
          Schema.Array(Schema.Struct({ count: Schema.Int })),
        )(
          yield* sql`SELECT COUNT(*) AS count FROM pg_stat_activity WHERE datname = current_database()`,
        );

        expect(connections).toEqual([{ count: 1 }]);
        expect(yield* journalExists).toEqual([{ present: true }]);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            storage.clientLayer,
            storage.threadStore,
            storage.submissionLedger,
            storage.scheduleStore,
            storage.messageDeliveryStore(),
            storage.subscriptionStore(subscriptionConformancePartition),
            storage.activityStore,
          ),
        ),
      );
    }),
  ),
);

it.effect("rejects invalid subscription partitions before acquiring a client", () =>
  Effect.gen(function* () {
    const storage = PostgresStorage.make({ client: { port: 1 } });

    const result = yield* SubscriptionStore.pipe(
      Effect.provide(storage.subscriptionStore({ tenantId: "", address: "" })),
      Effect.result,
    );

    expect(Result.isFailure(result) && result.failure).toMatchObject({
      _tag: "SubscriptionError",
      reason: "validation",
      code: "partition",
    });
  }),
);
