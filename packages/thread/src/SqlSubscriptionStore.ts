import { Clock, Context, Effect, Result, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { Digest } from "./Records.ts";
import {
  AcceptedEvent,
  DeliveryChange,
  SubscriptionChange,
  SubscriptionRetentionPolicy,
  type SourcePartition,
  SubscriptionDelivery,
  SubscriptionDeliveryKey,
  SubscriptionError,
  SubscriptionFailpoint,
  type SubscriptionFailpointError,
  SubscriptionKey,
  SubscriptionLimits,
  SubscriptionName,
  SubscriptionRecord,
  SubscriptionScanCursors,
  SubscriptionStore,
  subscriptionDeliveryKeyString,
} from "./Subscription.ts";
import {
  applySubscriptionDeliveryChange,
  applySubscriptionChange,
  validateEventRetention,
  sameAcceptedEventIdentity,
  sameSourcePartition,
  subscriptionCanSelect,
  subscriptionDeliveryCanSelect,
} from "./SubscriptionTransition.ts";

const CountRow = Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) });
const SequenceRow = Schema.Struct({ sequence: Schema.Natural });

const ScanRow = Schema.Struct({
  event_scan_cursor: Schema.String,
  delivery_scan_cursor: Schema.String,
  recovery_scan_cursor: Schema.Natural,
});

const error = (reason: SubscriptionError["reason"], code: string) =>
  SubscriptionError.make({ reason, code });

const unavailable = (operation: string) => error("storage", operation);
const corrupt = (operation: string) => error("corrupt", operation);
const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;

const validate = <A, I>(schema: Schema.Codec<A, I>, value: unknown, code: string) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => error("validation", code)));

const encode = <A, I>(schema: Schema.Codec<A, I>, value: A, code: string) =>
  Schema.encodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError(() => corrupt(code)),
  );

const decode = <A, I>(schema: Schema.Codec<A, I>, value: string, code: string) =>
  Schema.decodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError(() => corrupt(code)),
  );

const decodeRows = <A, I>(schema: Schema.Codec<A, I>, rows: unknown, code: string) =>
  Schema.decodeUnknownEffect(Schema.Array(schema))(rows).pipe(Effect.mapError(() => corrupt(code)));

const sameDeliveryIdentity = (left: SubscriptionDelivery, right: SubscriptionDelivery): boolean =>
  subscriptionDeliveryKeyString(left.key) === subscriptionDeliveryKeyString(right.key) &&
  left.deliveryId === right.deliveryId &&
  left.source.name === right.source.name &&
  left.source.version === right.source.version &&
  left.threadId === right.threadId &&
  left.admissionKey === right.admissionKey &&
  left.subscriptionFingerprint === right.subscriptionFingerprint &&
  left.eventDigest === right.eventDigest;

/** Adapter-specific storage limits. */
export interface SqlSubscriptionStoreOptions {
  /** Stored JSON decoder ceiling in UTF-16 code units; admission byte limits remain separate. */
  readonly maxStoredJsonLength: number;
}

/**
 * Adapter-owned atomic transaction on the store's SqlClient, including any native alarm update.
 * Return only after commit; a failed body must leave its writes uncommitted. Before-mutation
 * failpoints run inside the body, and after-mutation failpoints run after this operation returns.
 */
export class SqlSubscriptionTransaction extends Context.Service<
  SqlSubscriptionTransaction,
  {
    readonly run: <A>(
      body: Effect.Effect<A, SubscriptionError | SubscriptionFailpointError>,
    ) => Effect.Effect<A, SubscriptionError | SubscriptionFailpointError>;
  }
>()("@effect-agent/thread/SqlSubscriptionTransaction") {}

/**
 * Shared SQLite subscription operations over an existing SqlClient. The adapter validates the
 * partition, initializes its tables, and provides SqlSubscriptionTransaction at construction.
 * The returned methods capture the SQL client, transaction service, and failpoint handler.
 * Cloudflare's transaction also updates its native alarm before committing.
 */
export const makeSqlSubscriptionStore = Effect.fn("SqlSubscriptionStore.make")(function* (
  partition: SourcePartition,
  options: SqlSubscriptionStoreOptions,
): Effect.fn.Return<
  SubscriptionStore["Service"],
  SubscriptionError,
  SqlClientService.SqlClient | SqlSubscriptionTransaction
> {
  const sql = yield* SqlClientService.SqlClient;
  const failpoint = yield* SubscriptionFailpoint;
  const transactions = yield* SqlSubscriptionTransaction;
  const StoredJson = Schema.String.check(Schema.isMaxLength(options.maxStoredJsonLength));

  const JsonRow = Schema.Struct({ record_json: StoredJson });

  const RegistrationRow = Schema.Struct({
    owner_id: Schema.String,
    subscription_id: Schema.String,
    ordinal: Schema.Natural,
    source_name: Schema.String,
    source_version: Schema.String,
    matching_key: Schema.String,
    state: SubscriptionRecord.fields.state,
    expires_at_millis: Schema.NullOr(Schema.Number),
    recovery_at_millis: Schema.NullOr(Schema.Number),
    recovery_present: Schema.Literals([0, 1]),
    record_json: StoredJson,
  });

  const EventRow = Schema.Struct({
    event_id: Schema.String,
    source_name: Schema.String,
    source_version: Schema.String,
    matching_key: Schema.String,
    payload_digest: Digest,
    cutoff: Schema.Natural,
    cursor: Schema.Natural,
    routing_complete: Schema.Number,
    tombstone: Schema.Literals([0, 1]),
    next_attempt_at_millis: Schema.Number,
    record_json: StoredJson,
  });

  const DeliveryRow = Schema.Struct({
    owner_id: Schema.String,
    subscription_id: Schema.String,
    event_id: Schema.String,
    delivery_key: Schema.String,
    state: SubscriptionDelivery.fields.state,
    next_attempt_at_millis: Schema.Number,
    record_json: StoredJson,
  });

  yield* sql`CREATE TABLE IF NOT EXISTS effect_agent_event_retention (
    tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, replay_horizon_millis INTEGER NOT NULL,
    next_maintenance_at_millis INTEGER, tombstone_count INTEGER NOT NULL DEFAULT 0, event_cursor TEXT NOT NULL DEFAULT '', delivery_cursor TEXT NOT NULL DEFAULT '', PRIMARY KEY (tenant_id, source_address)
  )`.pipe(Effect.mapError(() => unavailable("initialize event retention")));

  yield* sql`
    INSERT INTO effect_agent_subscription_sequences (
      tenant_id, source_address, sequence, event_scan_cursor, delivery_scan_cursor, recovery_scan_cursor
    ) VALUES (${partition.tenantId}, ${partition.address}, 0, '', '', 0) ON CONFLICT DO NOTHING
  `.pipe(Effect.mapError(() => unavailable("initialize subscription partition")));

  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_events_retention_order ON effect_agent_subscription_events
    (tenant_id, source_address, tombstone, event_id)
  `.pipe(Effect.mapError(() => unavailable("index event retention")));
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_subscriptions_recovery_reference ON effect_agent_subscriptions
    (tenant_id, source_address, source_name, source_version, matching_key, recovery_present)
    WHERE recovery_present=1
  `.pipe(Effect.mapError(() => unavailable("index recovery retention")));
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_deliveries_event ON effect_agent_subscription_deliveries
    (tenant_id, source_address, event_id, state)
  `.pipe(Effect.mapError(() => unavailable("index delivery retention")));

  const query = <A extends object>(
    effect: Effect.Effect<ReadonlyArray<A>, SqlError>,
    code: string,
  ) => effect.pipe(Effect.mapError(() => unavailable(code)));

  const requirePartition = (candidate: SourcePartition, code: string) =>
    sameSourcePartition(candidate, partition)
      ? Effect.void
      : Effect.fail(error("validation", code));

  const requireKey = Effect.fn("SqlSubscriptionStore.requireKey")(function* (
    input: SubscriptionKey,
    code: string,
  ) {
    const key = yield* validate(SubscriptionKey, input, code);

    yield* requirePartition(key.partition, code);

    return key;
  });

  const readRegistration = Effect.fn("SqlSubscriptionStore.readRegistration")(function* (
    key: SubscriptionKey,
    code: string,
  ) {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT owner_id, subscription_id, ordinal, source_name, source_version, matching_key, state,
        expires_at_millis, recovery_at_millis, recovery_present, record_json FROM effect_agent_subscriptions
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
        AND owner_id=${key.ownerId} AND subscription_id=${key.subscriptionId}
    `,
      code,
    );

    const decodedRows = yield* decodeRows(RegistrationRow, rows, code);

    if (decodedRows.length > 1) return yield* corrupt(code);
    const row = decodedRows[0];

    if (row === undefined) return null;
    const record = yield* decode(SubscriptionRecord, row.record_json, code);

    if (
      !sameSourcePartition(record.key.partition, partition) ||
      record.key.ownerId !== row.owner_id ||
      record.key.subscriptionId !== row.subscription_id ||
      record.ordinal !== row.ordinal ||
      record.configuration.source.name !== row.source_name ||
      record.configuration.source.version !== row.source_version ||
      record.configuration.matchingKey !== row.matching_key ||
      record.state !== row.state ||
      record.configuration.expiresAtMillis !== row.expires_at_millis ||
      (record.recovery?.nextAttemptAtMillis ?? null) !== row.recovery_at_millis ||
      (record.recovery === null ? 0 : 1) !== row.recovery_present
    )
      return yield* corrupt(`${code}-projection`);

    return record;
  });

  const readEvent = Effect.fn("SqlSubscriptionStore.readEvent")(function* (
    eventId: string,
    code: string,
  ) {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT event_id, source_name, source_version, matching_key, payload_digest, cutoff, cursor,
        routing_complete, tombstone, next_attempt_at_millis, record_json FROM effect_agent_subscription_events
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND event_id=${eventId}
    `,
      code,
    );

    const decodedRows = yield* decodeRows(EventRow, rows, code);

    if (decodedRows.length > 1) return yield* corrupt(code);
    const row = decodedRows[0];

    if (row === undefined) return null;
    const event = yield* decode(AcceptedEvent, row.record_json, code);

    if (
      !sameSourcePartition(event.partition, partition) ||
      event.eventId !== row.event_id ||
      event.source.name !== row.source_name ||
      event.source.version !== row.source_version ||
      event.matchingKey !== row.matching_key ||
      event.payloadDigest !== row.payload_digest ||
      event.cutoff !== row.cutoff ||
      event.cursor !== row.cursor ||
      (event.routingComplete ? 1 : 0) !== row.routing_complete ||
      (event.tombstone === true ? 1 : 0) !== row.tombstone ||
      event.nextAttemptAtMillis !== row.next_attempt_at_millis
    )
      return yield* corrupt(`${code}-projection`);

    return event;
  });

  const readDelivery = Effect.fn("SqlSubscriptionStore.readDelivery")(function* (
    key: SubscriptionDeliveryKey,
    code: string,
  ) {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT owner_id, subscription_id, event_id, delivery_key, state, next_attempt_at_millis, record_json
      FROM effect_agent_subscription_deliveries
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
        AND owner_id=${key.subscription.ownerId} AND subscription_id=${key.subscription.subscriptionId}
        AND event_id=${key.eventId}
    `,
      code,
    );

    const decodedRows = yield* decodeRows(DeliveryRow, rows, code);

    if (decodedRows.length > 1) return yield* corrupt(code);
    const row = decodedRows[0];

    if (row === undefined) return null;
    const delivery = yield* decode(SubscriptionDelivery, row.record_json, code);

    if (
      !sameSourcePartition(delivery.key.subscription.partition, partition) ||
      delivery.key.subscription.ownerId !== row.owner_id ||
      delivery.key.subscription.subscriptionId !== row.subscription_id ||
      delivery.key.eventId !== row.event_id ||
      subscriptionDeliveryKeyString(delivery.key) !== row.delivery_key ||
      delivery.state !== row.state ||
      delivery.retry.nextAttemptAtMillis !== row.next_attempt_at_millis
    )
      return yield* corrupt(`${code}-projection`);

    return delivery;
  });

  const count = Effect.fn("SqlSubscriptionStore.count")(function* (
    statement: Effect.Effect<ReadonlyArray<Record<string, unknown>>, SqlError>,
    code: string,
  ) {
    const rows = yield* query(statement, code);
    const decoded = yield* decodeRows(CountRow, rows, code);

    if (decoded.length !== 1) return yield* corrupt(code);

    return decoded[0].count;
  });

  const nextSequence = Effect.fn("SqlSubscriptionStore.nextSequence")(function* () {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      UPDATE effect_agent_subscription_sequences SET sequence=sequence+1
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
      RETURNING sequence
    `,
      "advance subscription sequence",
    );

    const decoded = yield* decodeRows(SequenceRow, rows, "advance subscription sequence");

    if (decoded.length !== 1) return yield* corrupt("subscription sequence");

    return decoded[0].sequence;
  });

  const writeRegistration = Effect.fn("SqlSubscriptionStore.writeRegistration")(function* (
    record: SubscriptionRecord,
  ) {
    const json = yield* encode(SubscriptionRecord, record, "encode subscription");

    yield* query(
      sql<Record<string, unknown>>`
      UPDATE effect_agent_subscriptions SET ordinal=${record.ordinal}, source_name=${record.configuration.source.name},
        source_version=${record.configuration.source.version}, matching_key=${record.configuration.matchingKey}, state=${record.state}, expires_at_millis=${record.configuration.expiresAtMillis},
        recovery_at_millis=${record.recovery?.nextAttemptAtMillis ?? null}, recovery_present=${record.recovery === null ? 0 : 1}, record_json=${json}
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
        AND owner_id=${record.key.ownerId} AND subscription_id=${record.key.subscriptionId}
    `,
      "write subscription",
    );
  });

  const writeEvent = Effect.fn("SqlSubscriptionStore.writeEvent")(function* (event: AcceptedEvent) {
    const json = yield* encode(AcceptedEvent, event, "encode event");

    yield* query(
      sql<Record<string, unknown>>`
      UPDATE effect_agent_subscription_events SET cursor=${event.cursor}, routing_complete=${event.routingComplete ? 1 : 0}, tombstone=${event.tombstone === true ? 1 : 0},
        next_attempt_at_millis=${event.nextAttemptAtMillis}, record_json=${json}
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND event_id=${event.eventId}
    `,
      "write event",
    );
  });

  const writeDelivery = Effect.fn("SqlSubscriptionStore.writeDelivery")(function* (
    delivery: SubscriptionDelivery,
  ) {
    const json = yield* encode(SubscriptionDelivery, delivery, "encode delivery");

    yield* query(
      sql<Record<string, unknown>>`
      UPDATE effect_agent_subscription_deliveries SET state=${delivery.state},
        next_attempt_at_millis=${delivery.retry.nextAttemptAtMillis}, record_json=${json}
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
        AND owner_id=${delivery.key.subscription.ownerId} AND subscription_id=${delivery.key.subscription.subscriptionId}
        AND event_id=${delivery.key.eventId}
    `,
      "write delivery",
    );
  });

  const register: SubscriptionStore["Service"]["register"] = Effect.fn(
    "SqlSubscriptionStore.register",
  )(function* (input, inputLimits) {
    const record = yield* validate(SubscriptionRecord, input, "register-record");
    const limits = yield* validate(SubscriptionLimits, inputLimits, "register-limits");

    yield* requirePartition(record.key.partition, "register-partition");

    const result = yield* transactions.run(
      Effect.gen(function* () {
        const existing = yield* readRegistration(record.key, "register-existing");

        if (existing !== null) {
          if (existing.creationFingerprint !== record.creationFingerprint)
            return yield* error("conflict", "registration-identity");

          return { value: existing, changed: false } as const;
        }
        if (bytes(record.configuration.context) > limits.maxContextBytes)
          return yield* error("capacity", "context-bytes");
        if (bytes(record.configuration.parameters) > limits.maxPayloadBytes)
          return yield* error("capacity", "parameters-bytes");
        if (
          record.configuration.expiresAtMillis !== null &&
          record.configuration.expiresAtMillis - record.createdAtMillis > limits.maxLifetimeMillis
        )
          return yield* error("capacity", "lifetime");
        if (
          (yield* count(
            sql<
              Record<string, unknown>
            >`SELECT COUNT(*) AS count FROM effect_agent_subscriptions WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}`,
            "count registrations",
          )) >= limits.maxRegistrations
        )
          return yield* error("capacity", "registrations");
        if (
          (yield* count(
            sql<
              Record<string, unknown>
            >`SELECT COUNT(*) AS count FROM effect_agent_subscriptions WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND owner_id=${record.key.ownerId}`,
            "count owner registrations",
          )) >= limits.maxRegistrationsPerOwner
        )
          return yield* error("capacity", "owner-registrations");
        const assigned = { ...record, ordinal: yield* nextSequence() };
        const json = yield* encode(SubscriptionRecord, assigned, "encode registration");

        yield* failpoint.hit("subscription:register:before");
        yield* query(
          sql<Record<string, unknown>>`
        INSERT INTO effect_agent_subscriptions (tenant_id, source_address, owner_id, subscription_id, ordinal,
          source_name, source_version, matching_key, state, expires_at_millis, recovery_at_millis, recovery_present, record_json)
        VALUES (${partition.tenantId}, ${partition.address}, ${assigned.key.ownerId}, ${assigned.key.subscriptionId}, ${assigned.ordinal},
          ${assigned.configuration.source.name}, ${assigned.configuration.source.version}, ${assigned.configuration.matchingKey}, ${assigned.state},
          ${assigned.configuration.expiresAtMillis}, ${assigned.recovery?.nextAttemptAtMillis ?? null}, ${assigned.recovery === null ? 0 : 1}, ${json})
      `,
          "insert registration",
        );

        return { value: assigned, changed: true } as const;
      }),
    );

    if (result.changed) yield* failpoint.hit("subscription:register:after");

    return result.value;
  });

  const get: SubscriptionStore["Service"]["get"] = Effect.fn("SqlSubscriptionStore.get")(
    function* (input) {
      return yield* readRegistration(yield* requireKey(input, "get-key"), "get subscription");
    },
  );

  const list: SubscriptionStore["Service"]["list"] = Effect.fn("SqlSubscriptionStore.list")(
    function* (ownerId, after, limit) {
      const rows = yield* query(
        sql<Record<string, unknown>>`
      SELECT owner_id, subscription_id, ordinal FROM effect_agent_subscriptions WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
        AND owner_id=${ownerId} AND ordinal>${after} ORDER BY ordinal LIMIT ${limit}
    `,
        "list subscriptions",
      );

      const decoded = yield* decodeRows(
        Schema.Struct({
          owner_id: Schema.String,
          subscription_id: Schema.String,
          ordinal: Schema.Natural,
        }),
        rows,
        "list subscriptions",
      );

      return yield* Effect.forEach(
        decoded,
        Effect.fn("SqlSubscriptionStore.listRecord")(function* (row) {
          const record = yield* readRegistration(
            { partition, ownerId: row.owner_id, subscriptionId: row.subscription_id },
            "list subscription",
          );

          if (record === null || record.ordinal !== row.ordinal)
            return yield* corrupt("list subscription projection");

          return record;
        }),
      );
    },
  );

  const change: SubscriptionStore["Service"]["change"] = Effect.fn("SubscriptionStore.change")(
    function* (input, expectedRevision, inputChange) {
      const key = yield* requireKey(input, "change-key");
      const change = yield* validate(SubscriptionChange, inputChange, "change");

      yield* validate(Schema.Int.check(Schema.isGreaterThan(0)), expectedRevision, "revision");

      const updated = yield* transactions.run(
        Effect.gen(function* () {
          const existing = yield* readRegistration(key, "change-registration");

          if (existing === null) return yield* error("not-found", "subscription");
          yield* failpoint.hit("subscription:change:before");

          const revised = yield* Effect.fromResult(
            applySubscriptionChange(existing, expectedRevision, change),
          );

          const updated = { ...revised, ordinal: yield* nextSequence() };

          yield* writeRegistration(updated);

          return updated;
        }),
      );

      yield* failpoint.hit("subscription:change:after");

      return updated;
    },
  );

  const cancel: SubscriptionStore["Service"]["cancel"] = Effect.fn("SqlSubscriptionStore.cancel")(
    function* (input, expectedRevision) {
      const key = yield* requireKey(input, "cancel-key");

      const result = yield* transactions.run(
        Effect.gen(function* () {
          const current = yield* readRegistration(key, "cancel subscription");

          if (current === null) return yield* error("not-found", "subscription");
          if (
            expectedRevision !== undefined &&
            expectedRevision !== current.configurationRevision &&
            !(
              current.state === "cancelled" &&
              expectedRevision + 1 === current.configurationRevision
            )
          )
            return yield* SubscriptionError.make({
              reason: "conflict",
              code: "configuration-revision",
              currentRevision: current.configurationRevision,
              currentState: current.state,
            });
          if (current.state === "cancelled") return { value: current, changed: false } as const;

          const updated = {
            ...current,
            configurationRevision: current.configurationRevision + 1,
            state: "cancelled" as const,
            recovery: null,
          };

          yield* failpoint.hit("subscription:cancel:before");
          yield* writeRegistration(updated);

          return { value: updated, changed: true } as const;
        }),
      );

      if (result.changed) yield* failpoint.hit("subscription:cancel:after");

      return result.value;
    },
  );

  const accept: SubscriptionStore["Service"]["accept"] = Effect.fn("SqlSubscriptionStore.accept")(
    function* (input, inputLimits) {
      const event = yield* validate(AcceptedEvent, input, "accept-event");
      const limits = yield* validate(SubscriptionLimits, inputLimits, "accept-limits");

      yield* requirePartition(event.partition, "accept-partition");

      const result = yield* transactions.run(
        Effect.gen(function* () {
          const existing = yield* readEvent(event.eventId, "accept event");

          if (existing !== null) {
            if (!sameAcceptedEventIdentity(existing, event))
              return yield* error("conflict", "event-identity");

            return { value: existing, changed: false } as const;
          }
          yield* Effect.fromResult(
            validateEventRetention(event, limits, yield* Clock.currentTimeMillis),
          );

          const retainedPolicies = yield* query(
            sql<
              Record<string, unknown>
            >`SELECT replay_horizon_millis FROM effect_agent_event_retention WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}`,
            "retained horizon",
          );

          if (
            retainedPolicies.length > 0 &&
            retainedPolicies[0]?.replay_horizon_millis !== limits.retention?.replayHorizonMillis
          )
            return yield* error("conflict", "retention-horizon");
          if (limits.retention !== undefined) {
            yield* query(
              sql<
                Record<string, unknown>
              >`INSERT INTO effect_agent_event_retention (tenant_id,source_address,replay_horizon_millis,next_maintenance_at_millis)
            VALUES (${partition.tenantId},${partition.address},${limits.retention.replayHorizonMillis},${event.acceptedAtMillis}) ON CONFLICT DO NOTHING`,
              "retain event horizon",
            );

            const policies = yield* query(
              sql<
                Record<string, unknown>
              >`SELECT replay_horizon_millis FROM effect_agent_event_retention WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}`,
              "retained event horizon",
            );

            if (policies[0]?.replay_horizon_millis !== limits.retention.replayHorizonMillis)
              return yield* error("conflict", "retention-horizon");
          }

          if (bytes(event.payload) > limits.maxPayloadBytes)
            return yield* error("capacity", "payload-bytes");
          if (
            (yield* count(
              sql<
                Record<string, unknown>
              >`SELECT COUNT(*) AS count FROM effect_agent_subscription_events WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND tombstone=0`,
              "count events",
            )) >= limits.maxEvents
          )
            return yield* error("capacity", "events");

          const accepted: AcceptedEvent = {
            ...event,
            cutoff: yield* nextSequence(),
            cursor: 0,
            routingComplete: false,
            routingFailure: null,
          };

          const json = yield* encode(AcceptedEvent, accepted, "encode accepted event");

          yield* failpoint.hit("subscription:accept:before");
          yield* query(
            sql<Record<string, unknown>>`
        INSERT INTO effect_agent_subscription_events (tenant_id, source_address, event_id, source_name, source_version,
          matching_key, payload_digest, cutoff, cursor, routing_complete, next_attempt_at_millis, record_json)
        VALUES (${partition.tenantId}, ${partition.address}, ${accepted.eventId}, ${accepted.source.name}, ${accepted.source.version},
          ${accepted.matchingKey}, ${accepted.payloadDigest}, ${accepted.cutoff}, ${accepted.cursor}, 0, ${accepted.nextAttemptAtMillis}, ${json})
      `,
            "insert event",
          );

          return { value: accepted, changed: true } as const;
        }),
      );

      if (result.changed) yield* failpoint.hit("subscription:accept:after");

      return result.value;
    },
  );

  const event: SubscriptionStore["Service"]["event"] = Effect.fn("SqlSubscriptionStore.event")(
    (eventId) => readEvent(eventId, "get event"),
  );

  const pendingEvents: SubscriptionStore["Service"]["pendingEvents"] = Effect.fn(
    "SqlSubscriptionStore.pendingEvents",
  )(function* (nowMillis, after, limit) {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT event_id FROM effect_agent_subscription_events WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
        AND routing_complete=0 AND next_attempt_at_millis<=${nowMillis} AND event_id>${after} ORDER BY event_id LIMIT ${limit}
    `,
      "pending events",
    );

    return yield* decodeRows(
      Schema.Struct({ event_id: Schema.String }),
      rows,
      "pending event keys",
    ).pipe(Effect.map((items) => items.map((item) => item.event_id)));
  });

  const candidates: SubscriptionStore["Service"]["candidates"] = Effect.fn(
    "SqlSubscriptionStore.candidates",
  )(function* (input, limit) {
    const supplied = yield* validate(AcceptedEvent, input, "candidates-event");

    yield* requirePartition(supplied.partition, "candidates-partition");
    const stored = yield* readEvent(supplied.eventId, "candidates event");

    if (stored === null) return yield* error("not-found", "event");
    if (!sameAcceptedEventIdentity(stored, supplied)) return yield* error("conflict", "event");

    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT owner_id, subscription_id, ordinal FROM effect_agent_subscriptions WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
        AND source_name=${stored.source.name} AND source_version=${stored.source.version} AND matching_key=${stored.matchingKey}
        AND ordinal>${stored.cursor} AND ordinal<=${stored.cutoff} ORDER BY ordinal LIMIT ${limit}
    `,
      "subscription candidates",
    );

    const decoded = yield* decodeRows(
      Schema.Struct({
        owner_id: Schema.String,
        subscription_id: Schema.String,
        ordinal: Schema.Natural,
      }),
      rows,
      "subscription candidates",
    );

    return yield* Effect.forEach(
      decoded,
      Effect.fn("SqlSubscriptionStore.candidateRecord")(function* (row) {
        const record = yield* readRegistration(
          { partition, ownerId: row.owner_id, subscriptionId: row.subscription_id },
          "subscription candidate",
        );

        if (record === null || record.ordinal !== row.ordinal)
          return yield* corrupt("subscription candidate projection");

        return record;
      }),
    );
  });

  const insertDelivery = Effect.fn("SqlSubscriptionStore.insertDelivery")(function* (
    delivery: SubscriptionDelivery,
  ) {
    const json = yield* encode(SubscriptionDelivery, delivery, "encode selected delivery");

    yield* query(
      sql<Record<string, unknown>>`
      INSERT INTO effect_agent_subscription_deliveries (tenant_id, source_address, owner_id, subscription_id, event_id,
        delivery_key, state, next_attempt_at_millis, record_json)
      VALUES (${partition.tenantId}, ${partition.address}, ${delivery.key.subscription.ownerId}, ${delivery.key.subscription.subscriptionId},
        ${delivery.key.eventId}, ${subscriptionDeliveryKeyString(delivery.key)}, ${delivery.state}, ${delivery.retry.nextAttemptAtMillis}, ${json})
    `,
      "insert delivery",
    );
  });

  const select: SubscriptionStore["Service"]["select"] = Effect.fn("SqlSubscriptionStore.select")(
    function* (inputEvent, inputDeliveries, cursor, complete, nowMillis, inputLimits) {
      const supplied = yield* validate(AcceptedEvent, inputEvent, "select-event");

      const deliveries = yield* validate(
        Schema.Array(SubscriptionDelivery),
        inputDeliveries,
        "select-deliveries",
      );

      const limits = yield* validate(SubscriptionLimits, inputLimits, "select-limits");

      yield* requirePartition(supplied.partition, "select-partition");
      for (const candidate of deliveries)
        yield* requirePartition(candidate.key.subscription.partition, "select-delivery-partition");

      const changed = yield* transactions.run(
        Effect.gen(function* () {
          const accepted = yield* readEvent(supplied.eventId, "select event");

          if (accepted === null) return yield* error("not-found", "event");
          if (!sameAcceptedEventIdentity(accepted, supplied) || accepted.cursor !== supplied.cursor)
            return yield* error("conflict", "event-cursor");
          if (accepted.routingComplete) return false;
          if (!Number.isSafeInteger(cursor) || cursor < accepted.cursor || cursor > accepted.cutoff)
            return yield* error("validation", "cursor");
          yield* failpoint.hit("subscription:select:before");
          const effectiveNowMillis = Math.max(nowMillis, yield* Clock.currentTimeMillis);

          const additions: Array<{ delivery: SubscriptionDelivery; record: SubscriptionRecord }> =
            [];

          for (const delivery of deliveries) {
            const record = yield* readRegistration(
              delivery.key.subscription,
              "select registration",
            );

            if (record === null) return yield* error("not-found", "subscription");
            if (
              !subscriptionDeliveryCanSelect(delivery, record, accepted) ||
              delivery.key.eventId !== accepted.eventId ||
              delivery.source.name !== accepted.source.name ||
              delivery.source.version !== accepted.source.version ||
              record.ordinal <= accepted.cursor ||
              record.ordinal > cursor
            )
              return yield* error("conflict", "selection");
            const existing = yield* readDelivery(delivery.key, "select existing delivery");

            if (existing !== null) {
              if (!sameDeliveryIdentity(existing, delivery))
                return yield* error("conflict", "delivery-identity");
              continue;
            }
            if (!subscriptionCanSelect(record, accepted, effectiveNowMillis, false)) continue;
            additions.push({ delivery, record });
          }

          const total = yield* count(
            sql<
              Record<string, unknown>
            >`SELECT COUNT(*) AS count FROM effect_agent_subscription_deliveries WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}`,
            "count deliveries",
          );

          if (total + additions.length > limits.maxDeliveries)
            return yield* error("capacity", "deliveries");
          for (const ownerId of new Set(additions.map(({ record }) => record.key.ownerId))) {
            const existing = yield* count(
              sql<
                Record<string, unknown>
              >`SELECT COUNT(*) AS count FROM effect_agent_subscription_deliveries WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND owner_id=${ownerId}`,
              "count owner deliveries",
            );

            if (
              existing + additions.filter(({ record }) => record.key.ownerId === ownerId).length >
              limits.maxDeliveriesPerOwner
            )
              return yield* error("capacity", "owner-deliveries");
          }
          for (const addition of additions) {
            yield* insertDelivery(addition.delivery);
            if (addition.record.configuration.mode === "once")
              yield* writeRegistration({ ...addition.record, state: "consumed", recovery: null });
          }
          yield* writeEvent({
            ...accepted,
            cursor,
            routingComplete: complete,
            routingFailure: null,
          });

          return true;
        }),
      );

      if (changed) yield* failpoint.hit("subscription:select:after");
    },
  );

  const catchUp: SubscriptionStore["Service"]["catchUp"] = Effect.fn(
    "SqlSubscriptionStore.catchUp",
  )(function* (inputEvent, inputDelivery, nowMillis, inputLimits) {
    const supplied = yield* validate(AcceptedEvent, inputEvent, "catch-up-event");
    const delivery = yield* validate(SubscriptionDelivery, inputDelivery, "catch-up-delivery");
    const limits = yield* validate(SubscriptionLimits, inputLimits, "catch-up-limits");

    yield* requirePartition(supplied.partition, "catch-up-partition");
    yield* requirePartition(delivery.key.subscription.partition, "catch-up-delivery-partition");

    const changed = yield* transactions.run(
      Effect.gen(function* () {
        const accepted = yield* readEvent(supplied.eventId, "catch-up event");
        const record = yield* readRegistration(delivery.key.subscription, "catch-up subscription");

        if (accepted === null || record === null)
          return yield* error("not-found", accepted === null ? "event" : "subscription");
        if (
          !sameAcceptedEventIdentity(accepted, supplied) ||
          !subscriptionDeliveryCanSelect(delivery, record, accepted) ||
          delivery.key.eventId !== accepted.eventId ||
          delivery.source.name !== accepted.source.name ||
          delivery.source.version !== accepted.source.version ||
          record.configuration.mode !== "once"
        )
          return yield* error("conflict", "catch-up-identity");
        const existing = yield* readDelivery(delivery.key, "catch-up existing delivery");

        if (existing !== null) {
          if (!sameDeliveryIdentity(existing, delivery))
            return yield* error("conflict", "delivery-identity");

          return false;
        }
        yield* failpoint.hit("subscription:catch-up:before");
        const effectiveNowMillis = Math.max(nowMillis, yield* Clock.currentTimeMillis);

        if (!subscriptionCanSelect(record, accepted, effectiveNowMillis, true))
          return yield* error("conflict", "catch-up-eligibility");
        if (
          (yield* count(
            sql<
              Record<string, unknown>
            >`SELECT COUNT(*) AS count FROM effect_agent_subscription_deliveries WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}`,
            "count deliveries",
          )) >= limits.maxDeliveries
        )
          return yield* error("capacity", "deliveries");
        if (
          (yield* count(
            sql<
              Record<string, unknown>
            >`SELECT COUNT(*) AS count FROM effect_agent_subscription_deliveries WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND owner_id=${record.key.ownerId}`,
            "count owner deliveries",
          )) >= limits.maxDeliveriesPerOwner
        )
          return yield* error("capacity", "owner-deliveries");
        yield* insertDelivery(delivery);
        yield* writeRegistration({ ...record, state: "consumed", recovery: null });

        return true;
      }),
    );

    if (changed) yield* failpoint.hit("subscription:catch-up:after");
  });

  const deferEvent: SubscriptionStore["Service"]["deferEvent"] = Effect.fn(
    "SqlSubscriptionStore.deferEvent",
  )(function* (eventId, nextAttemptAtMillis, code) {
    const routingFailure =
      code === undefined
        ? "routing-failed"
        : yield* validate(SubscriptionName, code, "routing-failure");

    yield* transactions.run(
      Effect.gen(function* () {
        const accepted = yield* readEvent(eventId, "defer event");

        if (accepted === null) return yield* error("not-found", "event");
        yield* failpoint.hit("subscription:defer-event:before");
        yield* writeEvent({ ...accepted, nextAttemptAtMillis, routingFailure });
      }),
    );
    yield* failpoint.hit("subscription:defer-event:after");
  });

  const delivery: SubscriptionStore["Service"]["delivery"] = Effect.fn(
    "SqlSubscriptionStore.delivery",
  )(function* (input) {
    const key = yield* validate(SubscriptionDeliveryKey, input, "delivery-key");

    yield* requirePartition(key.subscription.partition, "delivery-partition");

    return yield* readDelivery(key, "get delivery");
  });

  const pendingDeliveries: SubscriptionStore["Service"]["pendingDeliveries"] = Effect.fn(
    "SqlSubscriptionStore.pendingDeliveries",
  )(function* (nowMillis, after, limit) {
    // Malformed bodies remain selectable for isolated decoding. Keep the same CASE guard
    // in both deadline queries so corruption cannot prevent cursor commits or alarm repair.
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT owner_id, subscription_id, event_id FROM effect_agent_subscription_deliveries
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND CASE WHEN json_valid(record_json) THEN
          ((state NOT IN ('delivered','refused') AND COALESCE(json_extract(record_json, '$.retry.parked'), 0)=0) OR (state='delivered' AND json_extract(record_json, '$.observeSettlement')=1))
          ELSE state<>'refused' END
        AND next_attempt_at_millis<=${nowMillis} AND delivery_key>${after} ORDER BY delivery_key LIMIT ${limit}
    `,
      "pending deliveries",
    );

    const rowSchema = Schema.Struct({
      owner_id: Schema.String,
      subscription_id: Schema.String,
      event_id: Schema.String,
    });

    return yield* decodeRows(rowSchema, rows, "pending delivery keys").pipe(
      Effect.map((items) =>
        items.map((item) => ({
          subscription: { partition, ownerId: item.owner_id, subscriptionId: item.subscription_id },
          eventId: item.event_id,
        })),
      ),
    );
  });

  const listDeliveries: SubscriptionStore["Service"]["listDeliveries"] = Effect.fn(
    "SqlSubscriptionStore.listDeliveries",
  )(function* (input, after, limit) {
    const key = yield* requireKey(input, "list-deliveries-key");

    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT record_json FROM effect_agent_subscription_deliveries WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
        AND owner_id=${key.ownerId} AND subscription_id=${key.subscriptionId} AND delivery_key>${after} ORDER BY delivery_key LIMIT ${limit}
    `,
      "list deliveries",
    );

    const decoded = yield* decodeRows(JsonRow, rows, "list deliveries");

    return yield* Effect.forEach(decoded, (row) =>
      decode(SubscriptionDelivery, row.record_json, "list delivery"),
    );
  });

  const changeDelivery: SubscriptionStore["Service"]["changeDelivery"] = Effect.fn(
    "SqlSubscriptionStore.changeDelivery",
  )(function* (inputKey, inputDeliveryId, inputChange) {
    const key = yield* validate(SubscriptionDeliveryKey, inputKey, "change-delivery-key");
    const deliveryId = yield* validate(Digest, inputDeliveryId, "change-delivery-id");
    const change = yield* validate(DeliveryChange, inputChange, "change-delivery-change");

    yield* requirePartition(key.subscription.partition, "change-delivery-partition");

    const result = yield* transactions.run(
      Effect.gen(function* () {
        const existing = yield* readDelivery(key, "change delivery");
        const record = yield* readRegistration(key.subscription, "change delivery subscription");

        if (existing === null || record === null)
          return yield* error("not-found", existing === null ? "delivery" : "subscription");
        yield* failpoint.hit(`subscription:delivery-${change._tag.toLowerCase()}:before`);

        const effectiveChange =
          change._tag === "Prepare"
            ? { ...change, nowMillis: Math.max(change.nowMillis, yield* Clock.currentTimeMillis) }
            : change;

        const transition = applySubscriptionDeliveryChange(
          existing,
          record,
          deliveryId,
          effectiveChange,
        );

        if (Result.isFailure(transition)) return yield* transition.failure;
        if (transition.success === existing) return { value: existing, changed: false } as const;
        yield* writeDelivery(transition.success);

        return { value: transition.success, changed: true } as const;
      }),
    );

    if (result.changed)
      yield* failpoint.hit(`subscription:delivery-${change._tag.toLowerCase()}:after`);

    return result.value;
  });

  const recovering: SubscriptionStore["Service"]["recovering"] = Effect.fn(
    "SqlSubscriptionStore.recovering",
  )(function* (nowMillis, after, limit) {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT owner_id, subscription_id, ordinal FROM effect_agent_subscriptions
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND state='active'
        AND recovery_at_millis IS NOT NULL AND recovery_at_millis<=${nowMillis} AND ordinal>${after}
      ORDER BY ordinal LIMIT ${limit}
    `,
      "recovering subscriptions",
    );

    const rowSchema = Schema.Struct({
      owner_id: Schema.String,
      subscription_id: Schema.String,
      ordinal: Schema.Natural,
    });

    return yield* decodeRows(rowSchema, rows, "recovering subscription keys").pipe(
      Effect.map((items) =>
        items.map((item) => ({
          key: { partition, ownerId: item.owner_id, subscriptionId: item.subscription_id },
          ordinal: item.ordinal,
        })),
      ),
    );
  });

  const deferRecovery: SubscriptionStore["Service"]["deferRecovery"] = Effect.fn(
    "SqlSubscriptionStore.deferRecovery",
  )(function* (input, expectedRevision, recovery) {
    const key = yield* requireKey(input, "defer-recovery-key");

    yield* transactions.run(
      Effect.gen(function* () {
        const record = yield* readRegistration(key, "defer recovery");

        if (record === null) return yield* error("not-found", "subscription");
        if (record.configurationRevision !== expectedRevision) return;
        yield* failpoint.hit("subscription:defer-recovery:before");
        yield* writeRegistration({
          ...record,
          recovery: record.state === "active" || record.state === "paused" ? recovery : null,
        });
      }),
    );
    yield* failpoint.hit("subscription:defer-recovery:after");
  });

  const readScanCursors: SubscriptionStore["Service"]["readScanCursors"] = Effect.gen(function* () {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT event_scan_cursor, delivery_scan_cursor, recovery_scan_cursor
      FROM effect_agent_subscription_sequences
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
    `,
      "read subscription scan cursors",
    );

    const decoded = yield* decodeRows(ScanRow, rows, "read subscription scan cursors");

    if (decoded.length !== 1) return yield* corrupt("subscription scan cursors");

    return {
      events: decoded[0].event_scan_cursor,
      deliveries: decoded[0].delivery_scan_cursor,
      recovery: decoded[0].recovery_scan_cursor,
    };
  });

  const advanceScanCursors: SubscriptionStore["Service"]["advanceScanCursors"] = Effect.fn(
    "SqlSubscriptionStore.advanceScanCursors",
  )(function* (input) {
    const cursors = yield* validate(SubscriptionScanCursors, input, "scan-cursors");

    yield* transactions.run(
      Effect.gen(function* () {
        yield* failpoint.hit("subscription:advance-scan-cursors:before");
        yield* query(
          sql<Record<string, unknown>>`
        UPDATE effect_agent_subscription_sequences
        SET event_scan_cursor=${cursors.events}, delivery_scan_cursor=${cursors.deliveries}, recovery_scan_cursor=${cursors.recovery}
        WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
      `,
          "advance subscription scan cursors",
        );
      }),
    );
    yield* failpoint.hit("subscription:advance-scan-cursors:after");
  });

  const indexedDeadline = query(
    sql<Record<string, unknown>>`
    SELECT MIN(deadline) AS deadline FROM (
      SELECT next_attempt_at_millis AS deadline FROM effect_agent_subscription_events
        WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND routing_complete=0
      UNION ALL SELECT next_attempt_at_millis FROM effect_agent_subscription_deliveries
        WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND CASE WHEN json_valid(record_json) THEN
          ((state NOT IN ('delivered','refused') AND COALESCE(json_extract(record_json, '$.retry.parked'), 0)=0) OR (state='delivered' AND json_extract(record_json, '$.observeSettlement')=1))
          ELSE state<>'refused' END
      UNION ALL SELECT next_maintenance_at_millis FROM effect_agent_event_retention
          WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
      UNION ALL SELECT recovery_at_millis FROM effect_agent_subscriptions
        WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND state='active' AND recovery_at_millis IS NOT NULL
    )
  `,
    "next subscription deadline",
  ).pipe(
    Effect.flatMap((rows) =>
      decodeRows(
        Schema.Struct({ deadline: Schema.NullOr(Schema.Number) }),
        rows,
        "next subscription deadline",
      ),
    ),
    Effect.flatMap((rows) =>
      rows.length === 1
        ? Effect.succeed(rows[0].deadline)
        : Effect.fail(corrupt("next subscription deadline")),
    ),
  );

  const nextDeadline = Effect.gen(function* () {
    const cursors = yield* readScanCursors;

    if (cursors.events !== "" || cursors.deliveries !== "" || cursors.recovery !== 0) return 0;

    return yield* indexedDeadline;
  });

  const compact: SubscriptionStore["Service"]["compact"] = Effect.fn(
    "SqlSubscriptionStore.compact",
  )(function* (nowMillis, inputPolicy, requestedLimit) {
    nowMillis = Math.min(nowMillis, yield* Clock.currentTimeMillis);
    const policy = yield* validate(SubscriptionRetentionPolicy, inputPolicy, "retention-policy");

    const limit = yield* validate(
      Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
      requestedLimit,
      "maintenance-limit",
    );

    const removed = yield* transactions.run(
      Effect.gen(function* () {
        yield* failpoint.hit("subscription:compact:before");
        yield* query(
          sql<
            Record<string, unknown>
          >`INSERT INTO effect_agent_event_retention (tenant_id, source_address, replay_horizon_millis) VALUES (${partition.tenantId}, ${partition.address}, ${policy.replayHorizonMillis}) ON CONFLICT DO NOTHING`,
          "initialize maintenance",
        );

        const progress = yield* query(
          sql<
            Record<string, unknown>
          >`SELECT replay_horizon_millis, tombstone_count, event_cursor, delivery_cursor FROM effect_agent_event_retention WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}`,
          "maintenance progress",
        ).pipe(
          Effect.flatMap((rows) =>
            decodeRows(
              Schema.Struct({
                replay_horizon_millis: Schema.Number,
                tombstone_count: Schema.Natural,
                event_cursor: Schema.String,
                delivery_cursor: Schema.String,
              }),
              rows,
              "maintenance progress",
            ),
          ),
        );

        const cursor = progress[0];

        if (progress.length !== 1 || cursor === undefined)
          return yield* corrupt("maintenance progress");
        if (cursor.replay_horizon_millis !== policy.replayHorizonMillis)
          return yield* error("conflict", "retention-horizon");

        const deliveryRows = yield* query(
          sql<
            Record<string, unknown>
          >`SELECT delivery_key, record_json FROM effect_agent_subscription_deliveries WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND delivery_key>${cursor.delivery_cursor} ORDER BY delivery_key LIMIT ${limit}`,
          "maintenance deliveries",
        ).pipe(
          Effect.flatMap((rows) =>
            decodeRows(
              Schema.Struct({ delivery_key: Schema.String, record_json: Schema.String }),
              rows,
              "maintenance deliveries",
            ),
          ),
        );

        const eventRows = yield* query(
          sql<
            Record<string, unknown>
          >`SELECT event_id, record_json FROM effect_agent_subscription_events WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND event_id>${cursor.event_cursor} ORDER BY event_id LIMIT ${limit}`,
          "maintenance events",
        ).pipe(
          Effect.flatMap((rows) =>
            decodeRows(
              Schema.Struct({ event_id: Schema.String, record_json: Schema.String }),
              rows,
              "maintenance events",
            ),
          ),
        );

        const protectedByRecovery = (event: AcceptedEvent) =>
          query(
            sql<
              Record<string, unknown>
            >`SELECT 1 FROM effect_agent_subscriptions WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND source_name=${event.source.name} AND source_version=${event.source.version} AND matching_key=${event.matchingKey} AND recovery_present=1 LIMIT 1`,
            "maintenance recovery reference",
          ).pipe(Effect.map((rows) => rows.length > 0));

        const cutoff = nowMillis - policy.completedRetentionMillis;

        for (const row of deliveryRows) {
          const decoded = yield* decode(
            SubscriptionDelivery,
            row.record_json,
            "maintenance delivery",
          ).pipe(Effect.result);

          if (Result.isFailure(decoded)) {
            yield* Effect.logWarning("Subscription retention preserved corrupt delivery");
            continue;
          }
          const delivery = decoded.success;

          if (
            subscriptionDeliveryKeyString(delivery.key) !== row.delivery_key ||
            !sameSourcePartition(delivery.key.subscription.partition, partition)
          ) {
            yield* Effect.logWarning(
              "Subscription retention preserved mismatched delivery identity",
            );
            continue;
          }

          if (
            delivery.state !== "refused" &&
            (delivery.state !== "delivered" || delivery.settledAtMillis === undefined)
          )
            continue;
          if (
            (delivery.settledAtMillis ?? delivery.completedAtMillis ?? delivery.selectedAtMillis) >
            cutoff
          )
            continue;

          const accepted = yield* readEvent(
            delivery.key.eventId,
            "maintenance delivery event",
          ).pipe(Effect.result);

          if (Result.isFailure(accepted)) {
            if (accepted.failure.reason !== "corrupt") return yield* accepted.failure;
            yield* Effect.logWarning("Subscription retention preserved corrupt event");
            continue;
          }
          const event = accepted.success;

          if (
            event === null ||
            !event.routingComplete ||
            event.occurredAtMillis === undefined ||
            event.acceptedAtMillis > cutoff ||
            (yield* protectedByRecovery(event))
          )
            continue;
          yield* query(
            sql<
              Record<string, unknown>
            >`DELETE FROM effect_agent_subscription_deliveries WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND delivery_key=${row.delivery_key}`,
            "reclaim delivery",
          );
        }
        let removed = 0;

        let tombstones = cursor.tombstone_count;

        for (const row of eventRows) {
          const decoded = yield* decode(AcceptedEvent, row.record_json, "maintenance event").pipe(
            Effect.result,
          );

          if (Result.isFailure(decoded)) {
            yield* Effect.logWarning("Subscription retention preserved corrupt event");
            continue;
          }
          const event = decoded.success;

          if (event.eventId !== row.event_id || !sameSourcePartition(event.partition, partition)) {
            yield* Effect.logWarning("Subscription retention preserved mismatched event identity");
            continue;
          }

          if (
            !event.routingComplete ||
            event.occurredAtMillis === undefined ||
            event.acceptedAtMillis > cutoff
          )
            continue;
          const expired = event.occurredAtMillis <= nowMillis - policy.replayHorizonMillis;

          if (event.tombstone === true && !expired) continue;
          if (
            (yield* query(
              sql<
                Record<string, unknown>
              >`SELECT 1 FROM effect_agent_subscription_deliveries WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND event_id=${event.eventId} LIMIT 1`,
              "maintenance delivery reference",
            )).length > 0 ||
            (yield* protectedByRecovery(event))
          )
            continue;
          if (expired) {
            yield* query(
              sql<
                Record<string, unknown>
              >`DELETE FROM effect_agent_subscription_events WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND event_id=${event.eventId}`,
              "expire event identity",
            );
            if (event.tombstone === true) tombstones--;
          } else {
            if (tombstones >= policy.maxTombstones) continue;
            yield* writeEvent({ ...event, payload: null, tombstone: true });
            tombstones++;
          }
          removed++;
        }

        const remaining =
          (yield* query(
            sql<
              Record<string, unknown>
            >`SELECT 1 FROM effect_agent_subscription_events WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} LIMIT 1`,
            "remaining maintenance",
          )).length > 0;

        yield* query(
          sql<
            Record<string, unknown>
          >`UPDATE effect_agent_event_retention SET tombstone_count=${tombstones}, event_cursor=${eventRows.length < limit ? "" : (eventRows.at(-1)?.event_id ?? "")}, delivery_cursor=${deliveryRows.length < limit ? "" : (deliveryRows.at(-1)?.delivery_key ?? "")}, next_maintenance_at_millis=${remaining ? nowMillis + 60_000 : null} WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}`,
          "advance maintenance",
        );

        return removed;
      }),
    );

    yield* failpoint.hit("subscription:compact:after");

    return removed;
  });

  return SubscriptionStore.of({
    partition,
    compact,
    register,
    get,
    list,
    cancel,
    change,
    accept,
    event,
    pendingEvents,
    candidates,
    select,
    catchUp,
    deferEvent,
    delivery,
    pendingDeliveries,
    listDeliveries,
    changeDelivery,
    recovering,
    deferRecovery,
    readScanCursors,
    advanceScanCursors,
    nextDeadline,
  });
});
