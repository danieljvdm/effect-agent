import { Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { AdmissionRequest, SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import {
  AcceptedEvent,
  SubscriptionStore,
  defaultSubscriptionLimits,
} from "@effect-agent/thread/Subscription";
import { Effect, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { expect } from "vite-plus/test";

import { storageV2Fixture } from "./storage-v2.ts";

export const fixturePartition = { tenantId: "fixture-tenant", address: "fixture-source" };
export type FixtureStore = "thread" | "schedule" | "subscription" | "sqlite";

const owns = (name: string, store: FixtureStore) =>
  store === "sqlite" ||
  (store === "schedule"
    ? name.startsWith("effect_agent_schedule")
    : store === "subscription"
      ? name.startsWith("effect_agent_subscription")
      : !name.startsWith("effect_agent_schedule") && !name.startsWith("effect_agent_subscription"));

export const restoreV2 = Effect.fn("StorageUpgradeFixture.restore")(function* (
  store: FixtureStore,
) {
  const sql = yield* SqlClient.SqlClient;

  yield* sql.withTransaction(
    Effect.gen(function* () {
      if (store === "sqlite") yield* sql`PRAGMA defer_foreign_keys = ON`;
      for (const { name, sql: ddl } of storageV2Fixture.ddl)
        if (owns(name, store)) yield* sql.unsafe(ddl);
      for (const [table, values] of Object.entries(storageV2Fixture.data).sort(
        ([left], [right]) => {
          const order = [
            "effect_agent_threads",
            "effect_agent_submissions",
            "effect_agent_canonical_batches",
            "effect_agent_canonical_records",
          ];

          return (
            (order.includes(left) ? order.indexOf(left) : 10) -
            (order.includes(right) ? order.indexOf(right) : 10)
          );
        },
      )) {
        if (!owns(table, store)) continue;
        for (const row of values) yield* sql`INSERT INTO ${sql(table)} ${sql.insert(row)}`;
      }
      if (store === "sqlite") yield* sql`PRAGMA user_version = 7`;
      if (store === "thread") {
        yield* sql`CREATE TABLE effect_agent_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)`;
        yield* sql`INSERT INTO effect_agent_meta VALUES ('storage_version', '2')`;
        yield* sql`CREATE TABLE effect_agent_child_settlements (parent_submission_id TEXT NOT NULL, child_submission_id TEXT NOT NULL, child_outcome TEXT, recorded_at TEXT NOT NULL, PRIMARY KEY (parent_submission_id,child_submission_id))`;
        yield* sql`INSERT INTO effect_agent_child_settlements VALUES ('parent', 'child', NULL, '1970-01-01T00:00:01.000Z')`;
      }
      if (store === "schedule") {
        yield* sql`CREATE TABLE effect_agent_schedule_store_state (singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton=1), storage_version INTEGER NOT NULL, alarm_generation INTEGER NOT NULL)`;
        yield* sql`INSERT INTO effect_agent_schedule_store_state VALUES (1,2,17)`;
      }
      if (store === "subscription") {
        yield* sql`CREATE TABLE effect_agent_subscription_store_state (singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton=1), storage_version INTEGER NOT NULL, alarm_generation INTEGER NOT NULL)`;
        yield* sql`INSERT INTO effect_agent_subscription_store_state VALUES (1,2,23)`;
      }
    }),
  );
});

export const snapshotStore = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const definitions = yield* sql<{
    name: string;
    sql: string;
  }>`SELECT name,sql FROM sqlite_master WHERE name LIKE 'effect_agent_%' AND sql IS NOT NULL ORDER BY name`;

  const contents: Record<string, ReadonlyArray<object>> = {};

  for (const row of definitions)
    if (row.sql.startsWith("CREATE TABLE"))
      contents[row.name] = yield* sql`SELECT * FROM ${sql(row.name)} ORDER BY rowid`;

  return { definitions, contents };
});

const json = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

export const assertPreserved = Effect.fn("StorageUpgradeFixture.assertPreserved")(function* (
  store: FixtureStore,
) {
  const sql = yield* SqlClient.SqlClient;

  for (const [table, oldRows] of Object.entries(storageV2Fixture.data)) {
    if (!owns(table, store)) continue;
    const current = yield* sql<Record<string, unknown>>`SELECT * FROM ${sql(table)} ORDER BY rowid`;

    expect(current).toHaveLength(oldRows.length);
    for (const [index, old] of oldRows.entries()) {
      const row = current[index];

      if (table === "effect_agent_submissions") {
        const { admission_group, admission_fence_json, ...retained } = row;

        expect(admission_group).toBeNull();
        expect(admission_fence_json).toBeNull();
        expect(retained).toEqual(old);
      } else if (
        table === "effect_agent_schedules" ||
        table === "effect_agent_subscriptions" ||
        table === "effect_agent_subscription_deliveries"
      ) {
        const { record_json, recovery_present, ...indexed } = row;
        const { record_json: oldJson, ...oldIndexed } = old;

        expect(indexed).toEqual(oldIndexed);
        const before = json(oldJson);
        const after = json(record_json);

        if (table === "effect_agent_schedules") {
          const pending = Schema.decodeUnknownSync(
            Schema.Struct({
              envelope: Schema.Unknown,
              retry: Schema.Record(Schema.String, Schema.Unknown),
            }),
          )(before.pending);

          expect(after).toEqual({
            ...before,
            pending: {
              ...pending,
              retry: {
                ...pending.retry,
                generation: 0,
                automaticAttempts: pending.retry.attempts,
                parked: false,
              },
            },
          });
        } else if (table === "effect_agent_subscriptions") {
          expect(after).toEqual({
            ...before,
            configurationRevision: 1,
            configurationFingerprint: before.creationFingerprint,
            creationConfiguration: before.configuration,
          });
          expect(recovery_present).toBe(before.recovery === null ? 0 : 1);
        } else {
          const retry = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown))(
            before.retry,
          );

          const { configurationRevision, configuration, retry: currentRetry, ...retained } = after;

          expect(configurationRevision).toBe(1);
          expect(configuration).toBeDefined();
          expect(currentRetry).toEqual({
            ...retry,
            generation: 0,
            automaticAttempts: retry.attempts,
            parked: false,
          });
          const { retry: _retry, ...original } = before;

          expect(retained).toEqual(original);
          expect(after).not.toHaveProperty("settledAtMillis");
          expect(after).not.toHaveProperty("completedAtMillis");
        }
      } else if (table === "effect_agent_subscription_events") {
        const { tombstone, ...retained } = row;

        expect(tombstone).toBe(0);
        expect(retained).toEqual(old);
      } else expect(row).toEqual(old);
    }
  }
});

export const assertReceiptReplay = Effect.gen(function* () {
  const ledger = yield* SubmissionLedger;

  for (const value of storageV2Fixture.requests) {
    const request = yield* Schema.decodeUnknownEffect(AdmissionRequest)(value);

    const before = storageV2Fixture.data.effect_agent_submissions.find(
      (row) => row.thread_id === request.threadId && row.idempotency_key === request.idempotencyKey,
    );

    if (before === undefined) return yield* Effect.die("Missing original admission fixture");

    const result = yield* ledger.admit(request);

    expect(result).toMatchObject({
      replayed: true,
      receiptId: before.receipt_id,
      submissionId: before.submission_id,
      queueSequence: before.queue_sequence,
    });
  }
});

export const assertSubscriptionReplay = Effect.gen(function* () {
  const store = yield* SubscriptionStore;

  const old = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(AcceptedEvent))(
    storageV2Fixture.data.effect_agent_subscription_events[0].record_json,
  );

  const replay = yield* store.accept(
    { ...old, occurredAtMillis: 1, acceptedAtMillis: 999 },
    defaultSubscriptionLimits,
  );

  expect(replay).toEqual(old);
  expect(replay.occurredAtMillis).toBeUndefined();

  const changed = yield* store
    .accept(
      {
        ...old,
        payloadDigest: Schema.decodeSync(AcceptedEvent.fields.payloadDigest)("f".repeat(64)),
        occurredAtMillis: 1,
      },
      defaultSubscriptionLimits,
    )
    .pipe(Effect.result);

  expect(changed._tag).toBe("Failure");
  for (const subscriptionId of ["prepared", "delivered"]) {
    const key = {
      subscription: { partition: fixturePartition, ownerId: "fixture-owner", subscriptionId },
      eventId: subscriptionId,
    };

    const before = yield* store.delivery(key);

    if (before === null) return yield* Effect.die("Missing legacy delivery");

    const receipt = yield* Schema.decodeUnknownEffect(Receipt)(
      storageV2Fixture.receipts[subscriptionId === "prepared" ? 0 : 1],
    );

    const complete = yield* store.changeDelivery(key, before.deliveryId, {
      _tag: "Complete",
      receipt,
      nowMillis: 100,
    });

    expect(complete.receipt).toEqual(receipt);
    expect(complete.envelope).toEqual(before.envelope);
    expect(complete.envelopeDigest).toBe(before.envelopeDigest);
    expect(complete.retry).toEqual(before.retry);
    if (subscriptionId === "delivered") expect(complete).toEqual(before);
    expect(
      yield* store.changeDelivery(key, before.deliveryId, {
        _tag: "Complete",
        receipt,
        nowMillis: 200,
      }),
    ).toEqual(complete);
  }
});
