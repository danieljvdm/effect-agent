import { Effect, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { digestJson } from "./Digest.ts";
import { v2Columns } from "./internal/storage-v2-layout.ts";
import { V2Delivery, V2Schedule, V2Subscription } from "./internal/storage-v2.ts";
import { DefinitionDigests } from "./Records.ts";
import { ScheduleRecord, ScheduleFailpoint } from "./Schedule.ts";
import { scheduleDeadline } from "./ScheduleTransition.ts";
import {
  SubscriptionConfiguration,
  SubscriptionFailpoint,
  SubscriptionRecord,
  SubscriptionDelivery,
  PreparedInput,
  subscriptionDeliveryKeyString,
} from "./Subscription.ts";

/** Adapter initialization preserves this diagnosis; no payloads are included in diagnostics. */
export class StorageUpgradeError extends Schema.TaggedError<StorageUpgradeError>()(
  "StorageUpgradeError",
  {
    table: Schema.String,
    rowKey: Schema.String,
    reason: Schema.Literals(["corrupt", "unsupported", "storage"]),
    message: Schema.String,
  },
) {}

const invalid = (table: string, rowKey: string, message: string) =>
  StorageUpgradeError.make({ table, rowKey, reason: "corrupt", message });

const decode = <A, I>(schema: Schema.Codec<A, I>, input: unknown, table: string, rowKey: string) =>
  Schema.decodeUnknownEffect(schema)(input, { onExcessProperty: "error" }).pipe(
    Effect.mapError(() =>
      invalid(
        table,
        rowKey,
        "The stored value does not match the unpatched beta49/beta50 contract. Restore or repair this row using its original writer; no upgrade was committed.",
      ),
    ),
  );

const json = <A, I>(schema: Schema.Codec<A, I>, input: string, table: string, rowKey: string) =>
  decode(Schema.fromJsonString(schema), input, table, rowKey);

const encode = <A, I>(schema: Schema.Codec<A, I>, input: A, table: string, rowKey: string) =>
  Schema.encodeEffect(Schema.fromJsonString(schema))(input).pipe(
    Effect.mapError(() =>
      invalid(table, rowKey, "The translated row does not match the current contract."),
    ),
  );

const digest = (value: Schema.Json, table: string, rowKey: string) =>
  digestJson(value).pipe(
    Effect.mapError(() => invalid(table, rowKey, "Unable to verify stored digest.")),
  );

const JsonRow = Schema.Struct({ record_json: Schema.String });

const rows = <A, I>(schema: Schema.Codec<A, I>, input: unknown, table: string) =>
  // SQL rows may contain the indexed columns in addition to the requested payload.
  Schema.decodeUnknownEffect(Schema.Array(schema))(input).pipe(
    Effect.mapError(() => invalid(table, "rows", "Invalid SQL row shape.")),
  );

const checkColumns = Effect.fn("SqlStorageV2Upgrade.checkColumns")(function* (
  tables: ReadonlyArray<keyof typeof v2Columns>,
) {
  const sql = yield* SqlClient.SqlClient;

  for (const table of tables) {
    const columns = yield* rows(
      Schema.Struct({ name: Schema.String }),
      yield* sql.unsafe(`PRAGMA table_info(${table})`),
      table,
    );

    if (JSON.stringify(columns.map((column) => column.name)) !== JSON.stringify(v2Columns[table]))
      return yield* invalid(
        table,
        "schema",
        "The table does not match the supported unpatched v2 layout. Keep the original writer available to inspect this store; no upgrade was committed.",
      );
  }
});

/** Native SQL only; callers own one transaction, recheck the version inside it, and advance it last. */
export const upgradeV2Schedules = Effect.fn("SqlStorageV2Upgrade.schedules")(function* (
  maxBytes: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const { hit } = yield* ScheduleFailpoint;

  yield* checkColumns(["effect_agent_schedules"]);
  const table = "effect_agent_schedules";
  let after: number | undefined;

  while (true) {
    const stored = yield* rows(
      Schema.Struct({
        storage_rowid: Schema.Int,
        ...JsonRow.fields,
        tenant_id: Schema.String,
        owner_id: Schema.String,
        schedule_id: Schema.String,
        deadline_at_millis: Schema.NullOr(Schema.Number),
      }),
      yield* after === undefined
        ? sql`SELECT rowid AS storage_rowid, * FROM effect_agent_schedules ORDER BY rowid LIMIT 16`
        : sql`SELECT rowid AS storage_rowid, * FROM effect_agent_schedules WHERE rowid > ${after} ORDER BY rowid LIMIT 16`,
      table,
    );

    if (stored.length === 0) break;
    for (const row of stored) {
      after = row.storage_rowid;
      const key = `${row.tenant_id}/${row.owner_id}/${row.schedule_id}`;
      const old = yield* json(V2Schedule, row.record_json, table, key);

      const record = yield* decode(
        ScheduleRecord,
        {
          ...old,
          pending:
            old.pending === null
              ? null
              : {
                  envelope: old.pending.envelope,
                  retry: {
                    ...old.pending.retry,
                    generation: 0,
                    automaticAttempts: old.pending.retry.attempts,
                    parked: false,
                  },
                },
        },
        table,
        key,
      );

      if (
        record.owner.tenantId !== row.tenant_id ||
        record.owner.ownerId !== row.owner_id ||
        record.scheduleId !== row.schedule_id ||
        scheduleDeadline(record) !== row.deadline_at_millis
      )
        return yield* invalid(
          table,
          key,
          "Schedule identity or deadline disagrees with its index.",
        );
      if (record.pending !== null) {
        const value = yield* encode(ScheduleRecord, record, table, key);

        if (new TextEncoder().encode(value).byteLength > maxBytes)
          return yield* invalid(
            table,
            key,
            "Translated schedule exceeds the adapter value bound; original data was retained.",
          );
        yield* hit("upgrade:before-mutation");
        yield* sql`UPDATE effect_agent_schedules SET record_json=${value} WHERE tenant_id=${row.tenant_id} AND owner_id=${row.owner_id} AND schedule_id=${row.schedule_id}`;
        yield* hit("upgrade:after-mutation");
      }
    }
  }
});

/** v2 registrations were immutable: revision 1 and the delivery snapshot have one possible value. */
export const upgradeV2Subscriptions = Effect.fn("SqlStorageV2Upgrade.subscriptions")(function* (
  maxBytes: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const { hit } = yield* SubscriptionFailpoint;

  yield* checkColumns([
    "effect_agent_subscriptions",
    "effect_agent_subscription_events",
    "effect_agent_subscription_deliveries",
    "effect_agent_subscription_sequences",
  ]);
  const registrationTable = "effect_agent_subscriptions";
  const eventTable = "effect_agent_subscription_events";
  const deliveryTable = "effect_agent_subscription_deliveries";

  // Rebuild only the registration table to remove the old NOT NULL expiry constraint.
  // The original rows, keys, ordinals and expiry values are copied before the swap.
  const mutate = <A, E2>(body: Effect.Effect<A, E2>) =>
    Effect.gen(function* () {
      yield* hit("upgrade:before-mutation");
      const result = yield* body;

      yield* hit("upgrade:after-mutation");

      return result;
    });

  yield* mutate(sql`CREATE TABLE effect_agent_subscriptions_v3 (
    tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, owner_id TEXT NOT NULL, subscription_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL, source_name TEXT NOT NULL, source_version TEXT NOT NULL, matching_key TEXT NOT NULL,
    state TEXT NOT NULL, expires_at_millis INTEGER, recovery_at_millis INTEGER, recovery_present INTEGER NOT NULL DEFAULT 0, record_json TEXT NOT NULL,
    PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id), UNIQUE (tenant_id, source_address, ordinal))`);

  let registrationAfter: number | undefined;

  while (true) {
    const registrations = yield* rows(
      Schema.Struct({
        storage_rowid: Schema.Int,
        ...JsonRow.fields,
        tenant_id: Schema.String,
        source_address: Schema.String,
        owner_id: Schema.String,
        subscription_id: Schema.String,
        ordinal: Schema.Natural,
        source_name: Schema.String,
        source_version: Schema.String,
        matching_key: Schema.String,
        state: Schema.String,
        expires_at_millis: Schema.Number,
        recovery_at_millis: Schema.NullOr(Schema.Number),
      }),
      yield* registrationAfter === undefined
        ? sql`SELECT rowid AS storage_rowid, * FROM effect_agent_subscriptions ORDER BY rowid LIMIT 16`
        : sql`SELECT rowid AS storage_rowid, * FROM effect_agent_subscriptions WHERE rowid > ${registrationAfter} ORDER BY rowid LIMIT 16`,
      registrationTable,
    );

    if (registrations.length === 0) break;

    const registrationKey = (key: SubscriptionRecord["key"]) =>
      JSON.stringify([
        key.partition.tenantId,
        key.partition.address,
        key.ownerId,
        key.subscriptionId,
      ]);

    for (const row of registrations) {
      registrationAfter = row.storage_rowid;

      const key = JSON.stringify([
        row.tenant_id,
        row.source_address,
        row.owner_id,
        row.subscription_id,
      ]);

      const old = yield* json(V2Subscription, row.record_json, registrationTable, key);

      const record = yield* decode(
        SubscriptionRecord,
        {
          ...old,
          configurationRevision: 1,
          configurationFingerprint: old.creationFingerprint,
          creationConfiguration: old.configuration,
        },
        registrationTable,
        key,
      );

      if (
        registrationKey(record.key) !== key ||
        record.ordinal !== row.ordinal ||
        record.configuration.source.name !== row.source_name ||
        record.configuration.source.version !== row.source_version ||
        record.configuration.matchingKey !== row.matching_key ||
        record.state !== row.state ||
        record.configuration.expiresAtMillis !== row.expires_at_millis ||
        (record.recovery?.nextAttemptAtMillis ?? null) !== row.recovery_at_millis
      )
        return yield* invalid(
          registrationTable,
          key,
          "Subscription identity or operational indexes disagree with its record.",
        );
      if (
        (yield* digest(
          {
            key: record.key,
            createdBy: record.createdBy,
            configuration: yield* Schema.encodeEffect(SubscriptionConfiguration)(old.configuration),
          },
          registrationTable,
          key,
        )) !== record.creationFingerprint
      )
        return yield* invalid(
          registrationTable,
          key,
          "The immutable v2 configuration does not match its fingerprint; a delivery snapshot cannot safely be inferred.",
        );
      const value = yield* encode(SubscriptionRecord, record, registrationTable, key);

      if (new TextEncoder().encode(value).byteLength > maxBytes)
        return yield* invalid(
          registrationTable,
          key,
          "Adding the creation configuration exceeds the adapter value bound; original data was retained.",
        );
      yield* mutate(
        sql`INSERT INTO effect_agent_subscriptions_v3 VALUES (${row.tenant_id}, ${row.source_address}, ${row.owner_id}, ${row.subscription_id}, ${row.ordinal}, ${row.source_name}, ${row.source_version}, ${row.matching_key}, ${row.state}, ${row.expires_at_millis}, ${row.recovery_at_millis}, ${record.recovery === null ? 0 : 1}, ${value})`,
      );
    }
  }
  let deliveryAfter: number | undefined;

  while (true) {
    const deliveries = yield* rows(
      Schema.Struct({
        storage_rowid: Schema.Int,
        ...JsonRow.fields,
        tenant_id: Schema.String,
        source_address: Schema.String,
        owner_id: Schema.String,
        subscription_id: Schema.String,
        event_id: Schema.String,
        delivery_key: Schema.String,
        state: Schema.String,
        next_attempt_at_millis: Schema.Number,
      }),
      yield* deliveryAfter === undefined
        ? sql`SELECT rowid AS storage_rowid, * FROM effect_agent_subscription_deliveries ORDER BY rowid LIMIT 16`
        : sql`SELECT rowid AS storage_rowid, * FROM effect_agent_subscription_deliveries WHERE rowid > ${deliveryAfter} ORDER BY rowid LIMIT 16`,
      deliveryTable,
    );

    if (deliveries.length === 0) break;
    for (const row of deliveries) {
      deliveryAfter = row.storage_rowid;
      const key = row.delivery_key;
      const old = yield* json(V2Delivery, row.record_json, deliveryTable, key);

      const registrations = yield* rows(
        JsonRow,
        yield* sql`SELECT record_json FROM effect_agent_subscriptions_v3 WHERE tenant_id=${row.tenant_id} AND source_address=${row.source_address} AND owner_id=${row.owner_id} AND subscription_id=${row.subscription_id}`,
        registrationTable,
      );

      // Indexed immutable identity is sufficient here. Event payloads are not transformed or scanned.
      const events = yield* rows(
        Schema.Struct({
          event_id: Schema.String,
          source_name: Schema.String,
          source_version: Schema.String,
          payload_digest: Schema.String,
        }),
        yield* sql`SELECT event_id, source_name, source_version, payload_digest FROM effect_agent_subscription_events WHERE tenant_id=${row.tenant_id} AND source_address=${row.source_address} AND event_id=${row.event_id}`,
        eventTable,
      );

      if (registrations.length !== 1 || events.length !== 1)
        return yield* invalid(
          deliveryTable,
          key,
          "Missing original registration or event. Restore the referenced row from the original store; the delivery configuration cannot be reconstructed safely.",
        );

      const registration = yield* json(
        SubscriptionRecord,
        registrations[0].record_json,
        registrationTable,
        key,
      );

      const event = events[0];

      const record = yield* decode(
        SubscriptionDelivery,
        {
          ...old,
          configurationRevision: 1,
          configuration: registration.configuration,
          retry: {
            ...old.retry,
            generation: 0,
            automaticAttempts: old.retry.attempts,
            parked: false,
          },
        },
        deliveryTable,
        key,
      );

      const expectedId = yield* digest(
        { schemaVersion: 1, subscription: registration.key, eventId: event.event_id },
        deliveryTable,
        key,
      );

      const destination = registration.configuration.destination;

      if (
        record.key.subscription.partition.tenantId !== row.tenant_id ||
        record.key.subscription.partition.address !== row.source_address ||
        record.key.subscription.ownerId !== row.owner_id ||
        record.key.subscription.subscriptionId !== row.subscription_id ||
        record.key.eventId !== row.event_id ||
        subscriptionDeliveryKeyString(record.key) !== key ||
        record.state !== row.state ||
        record.retry.nextAttemptAtMillis !== row.next_attempt_at_millis ||
        record.subscriptionFingerprint !== registration.creationFingerprint ||
        record.eventDigest !== event.payload_digest ||
        record.source.name !== event.source_name ||
        record.source.version !== event.source_version ||
        record.source.name !== registration.configuration.source.name ||
        record.source.version !== registration.configuration.source.version ||
        record.deliveryId !== expectedId ||
        record.admissionKey !== `subscription:${expectedId}` ||
        record.threadId !==
          (destination._tag === "ExistingThread"
            ? destination.threadId
            : `subscription:${expectedId}`)
      )
        return yield* invalid(
          deliveryTable,
          key,
          "Delivery identity or immutable selection evidence disagrees with its registration/event.",
        );
      if (record.envelope !== null) {
        const envelope = record.envelope;
        const encoded = yield* Schema.encodeEffect(PreparedInput)(envelope);

        if (
          envelope.threadId !== record.threadId ||
          envelope.admissionKey !== record.admissionKey ||
          envelope.deliveryPrincipal !== registration.configuration.deliveryPrincipal ||
          envelope.agentId !== registration.configuration.agentId ||
          !Schema.toEquivalence(DefinitionDigests)(
            envelope.definitions,
            registration.configuration.definitions,
          ) ||
          (yield* digest(envelope.input, deliveryTable, key)) !== envelope.inputDigest ||
          (yield* digest(
            {
              deliveryId: record.deliveryId,
              subscriptionFingerprint: record.subscriptionFingerprint,
              eventDigest: record.eventDigest,
              envelope: encoded,
            },
            deliveryTable,
            key,
          )) !== record.envelopeDigest
        )
          return yield* invalid(
            deliveryTable,
            key,
            "Prepared envelope identity or digest is inconsistent. Its admission may already have happened; no replacement input or receipt can be invented.",
          );
      }
      const value = yield* encode(SubscriptionDelivery, record, deliveryTable, key);

      if (new TextEncoder().encode(value).byteLength > maxBytes)
        return yield* invalid(
          deliveryTable,
          key,
          "Adding the immutable configuration exceeds the adapter value bound; original data was retained.",
        );
      yield* mutate(
        sql`UPDATE effect_agent_subscription_deliveries SET record_json=${value} WHERE tenant_id=${row.tenant_id} AND source_address=${row.source_address} AND owner_id=${row.owner_id} AND subscription_id=${row.subscription_id} AND event_id=${row.event_id}`,
      );
    }
  }
  yield* mutate(sql`DROP TABLE effect_agent_subscriptions`);
  yield* mutate(
    sql`ALTER TABLE effect_agent_subscriptions_v3 RENAME TO effect_agent_subscriptions`,
  );
  yield* mutate(
    sql`CREATE INDEX effect_agent_subscriptions_owner ON effect_agent_subscriptions (tenant_id, source_address, owner_id, ordinal)`,
  );
  yield* mutate(
    sql`CREATE INDEX effect_agent_subscriptions_candidates ON effect_agent_subscriptions (tenant_id, source_address, source_name, source_version, matching_key, ordinal)`,
  );
  yield* mutate(
    sql`CREATE INDEX effect_agent_subscriptions_recovery ON effect_agent_subscriptions (tenant_id, source_address, recovery_at_millis, ordinal) WHERE recovery_at_millis IS NOT NULL`,
  );
  yield* mutate(
    sql`ALTER TABLE effect_agent_subscription_events ADD COLUMN tombstone INTEGER NOT NULL DEFAULT 0`,
  );
});

/** Check the shared layout without reading journal, receipt, lease or settlement payloads. */
export const checkV2ThreadLayout = Effect.fn("SqlStorageV2Upgrade.threadLayout")(function* () {
  yield* checkColumns([
    "effect_agent_threads",
    "effect_agent_canonical_batches",
    "effect_agent_canonical_records",
    "effect_agent_checkpoints",
    "effect_agent_submissions",
    "effect_agent_submission_ownership",
    "effect_agent_attempts",
    "effect_agent_settlement_reservations",
    "effect_agent_abort_intents",
    "effect_agent_approval_decisions",
    "effect_agent_unknown_resolutions",
    "effect_agent_child_reservations",
  ]);
});
