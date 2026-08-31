import type { SubscriptionFailpointError } from "@effect-agent/thread";
import {
  AcceptedEvent,
  applySubscriptionDeliveryChange,
  DeliveryChange,
  Digest,
  sameAcceptedEventIdentity,
  sameSourcePartition,
  SourcePartition,
  subscriptionCanSelect,
  subscriptionDeliveryCanSelect,
  SubscriptionDelivery,
  SubscriptionDeliveryKey,
  SubscriptionError,
  SubscriptionFailpoint,
  SubscriptionKey,
  SubscriptionLimits,
  SubscriptionName,
  SubscriptionRecord,
  SubscriptionScanCursors,
  SubscriptionStore,
  subscriptionDeliveryKeyString,
} from "@effect-agent/thread";
import { Clock, Effect, Layer, Result, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { initializeSqliteJournal } from "./sqlite-journal.ts";
import type { SqliteStorageConfig } from "./sqlite-storage-config.ts";
import type { SqliteStorageFailpoint } from "./sqlite-storage-failpoint.ts";
import type { SqliteStorageInitializationError } from "./sqlite-thread-store.ts";

const StoredJson = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const CountRow = Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) });
const SequenceRow = Schema.Struct({ sequence: Schema.Natural });
const ScanRow = Schema.Struct({
  event_scan_cursor: Schema.String,
  delivery_scan_cursor: Schema.String,
  recovery_scan_cursor: Schema.Natural,
});
const JsonRow = Schema.Struct({ record_json: StoredJson });
const RegistrationRow = Schema.Struct({
  owner_id: Schema.String,
  subscription_id: Schema.String,
  ordinal: Schema.Natural,
  source_name: Schema.String,
  source_version: Schema.String,
  matching_key: Schema.String,
  state: SubscriptionRecord.fields.state,
  expires_at_millis: Schema.Number,
  recovery_at_millis: Schema.NullOr(Schema.Number),
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

const makeSubscriptionStore = Effect.fn("SqliteSubscriptionStore.make")(function* (
  owned: SourcePartition,
) {
  const partition = yield* validate(SourcePartition, owned, "partition");
  const sql = yield* SqlClientService.SqlClient;
  const failpoint = yield* SubscriptionFailpoint;
  yield* initializeSqliteJournal();
  yield* sql`
    INSERT INTO effect_agent_subscription_sequences (
      tenant_id, source_address, sequence, event_scan_cursor, delivery_scan_cursor, recovery_scan_cursor
    ) VALUES (${partition.tenantId}, ${partition.address}, 0, '', '', 0) ON CONFLICT DO NOTHING
  `.pipe(Effect.mapError(() => unavailable("initialize subscription partition")));

  const transact = <A>(effect: Effect.Effect<A, SubscriptionError | SubscriptionFailpointError>) =>
    sql
      .withTransaction(effect)
      .pipe(Effect.catchTag("SqlError", () => Effect.fail(unavailable("transaction"))));
  const query = <A extends object>(
    effect: Effect.Effect<ReadonlyArray<A>, SqlError>,
    code: string,
  ) => effect.pipe(Effect.mapError(() => unavailable(code)));
  const requirePartition = (candidate: SourcePartition, code: string) =>
    sameSourcePartition(candidate, partition)
      ? Effect.void
      : Effect.fail(error("validation", code));
  const requireKey = Effect.fn("SqliteSubscriptionStore.requireKey")(function* (
    input: SubscriptionKey,
    code: string,
  ) {
    const key = yield* validate(SubscriptionKey, input, code);
    yield* requirePartition(key.partition, code);
    return key;
  });
  const readRegistration = Effect.fn("SqliteSubscriptionStore.readRegistration")(function* (
    key: SubscriptionKey,
    code: string,
  ) {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT owner_id, subscription_id, ordinal, source_name, source_version, matching_key, state,
        expires_at_millis, recovery_at_millis, record_json FROM effect_agent_subscriptions
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
      (record.recovery?.nextAttemptAtMillis ?? null) !== row.recovery_at_millis
    )
      return yield* corrupt(`${code}-projection`);
    return record;
  });
  const readEvent = Effect.fn("SqliteSubscriptionStore.readEvent")(function* (
    eventId: string,
    code: string,
  ) {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT event_id, source_name, source_version, matching_key, payload_digest, cutoff, cursor,
        routing_complete, next_attempt_at_millis, record_json FROM effect_agent_subscription_events
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
      event.nextAttemptAtMillis !== row.next_attempt_at_millis
    )
      return yield* corrupt(`${code}-projection`);
    return event;
  });
  const readDelivery = Effect.fn("SqliteSubscriptionStore.readDelivery")(function* (
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
  const count = Effect.fn("SqliteSubscriptionStore.count")(function* (
    statement: Effect.Effect<ReadonlyArray<Record<string, unknown>>, SqlError>,
    code: string,
  ) {
    const rows = yield* query(statement, code);
    const decoded = yield* decodeRows(CountRow, rows, code);
    if (decoded.length !== 1) return yield* corrupt(code);
    return decoded[0].count;
  });
  const nextSequence = Effect.fn("SqliteSubscriptionStore.nextSequence")(function* () {
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
  const writeRegistration = Effect.fn("SqliteSubscriptionStore.writeRegistration")(function* (
    record: SubscriptionRecord,
  ) {
    const json = yield* encode(SubscriptionRecord, record, "encode subscription");
    yield* query(
      sql<Record<string, unknown>>`
      UPDATE effect_agent_subscriptions SET state=${record.state}, expires_at_millis=${record.configuration.expiresAtMillis},
        recovery_at_millis=${record.recovery?.nextAttemptAtMillis ?? null}, record_json=${json}
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
        AND owner_id=${record.key.ownerId} AND subscription_id=${record.key.subscriptionId}
    `,
      "write subscription",
    );
  });
  const writeEvent = Effect.fn("SqliteSubscriptionStore.writeEvent")(function* (
    event: AcceptedEvent,
  ) {
    const json = yield* encode(AcceptedEvent, event, "encode event");
    yield* query(
      sql<Record<string, unknown>>`
      UPDATE effect_agent_subscription_events SET cursor=${event.cursor}, routing_complete=${event.routingComplete ? 1 : 0},
        next_attempt_at_millis=${event.nextAttemptAtMillis}, record_json=${json}
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND event_id=${event.eventId}
    `,
      "write event",
    );
  });
  const writeDelivery = Effect.fn("SqliteSubscriptionStore.writeDelivery")(function* (
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
    "SqliteSubscriptionStore.register",
  )(function* (input, inputLimits) {
    const record = yield* validate(SubscriptionRecord, input, "register-record");
    const limits = yield* validate(SubscriptionLimits, inputLimits, "register-limits");
    yield* requirePartition(record.key.partition, "register-partition");
    const result = yield* transact(
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
          record.configuration.expiresAtMillis - record.createdAtMillis >
          limits.maxLifetimeMillis
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
          source_name, source_version, matching_key, state, expires_at_millis, recovery_at_millis, record_json)
        VALUES (${partition.tenantId}, ${partition.address}, ${assigned.key.ownerId}, ${assigned.key.subscriptionId}, ${assigned.ordinal},
          ${assigned.configuration.source.name}, ${assigned.configuration.source.version}, ${assigned.configuration.matchingKey}, ${assigned.state},
          ${assigned.configuration.expiresAtMillis}, ${assigned.recovery?.nextAttemptAtMillis ?? null}, ${json})
      `,
          "insert registration",
        );
        return { value: assigned, changed: true } as const;
      }),
    );
    if (result.changed) yield* failpoint.hit("subscription:register:after");
    return result.value;
  });

  const get: SubscriptionStore["Service"]["get"] = Effect.fn("SqliteSubscriptionStore.get")(
    function* (input) {
      return yield* readRegistration(yield* requireKey(input, "get-key"), "get subscription");
    },
  );

  const list: SubscriptionStore["Service"]["list"] = Effect.fn("SqliteSubscriptionStore.list")(
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
        Effect.fn("SqliteSubscriptionStore.listRecord")(function* (row) {
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

  const cancel: SubscriptionStore["Service"]["cancel"] = Effect.fn(
    "SqliteSubscriptionStore.cancel",
  )(function* (input) {
    const key = yield* requireKey(input, "cancel-key");
    const result = yield* transact(
      Effect.gen(function* () {
        const current = yield* readRegistration(key, "cancel subscription");
        if (current === null) return yield* error("not-found", "subscription");
        if (current.state === "cancelled") return { value: current, changed: false } as const;
        const updated = { ...current, state: "cancelled" as const, recovery: null };
        yield* failpoint.hit("subscription:cancel:before");
        yield* writeRegistration(updated);
        return { value: updated, changed: true } as const;
      }),
    );
    if (result.changed) yield* failpoint.hit("subscription:cancel:after");
    return result.value;
  });

  const accept: SubscriptionStore["Service"]["accept"] = Effect.fn(
    "SqliteSubscriptionStore.accept",
  )(function* (input, inputLimits) {
    const event = yield* validate(AcceptedEvent, input, "accept-event");
    const limits = yield* validate(SubscriptionLimits, inputLimits, "accept-limits");
    yield* requirePartition(event.partition, "accept-partition");
    const result = yield* transact(
      Effect.gen(function* () {
        const existing = yield* readEvent(event.eventId, "accept event");
        if (existing !== null) {
          if (!sameAcceptedEventIdentity(existing, event))
            return yield* error("conflict", "event-identity");
          return { value: existing, changed: false } as const;
        }
        if (bytes(event.payload) > limits.maxPayloadBytes)
          return yield* error("capacity", "payload-bytes");
        if (
          (yield* count(
            sql<
              Record<string, unknown>
            >`SELECT COUNT(*) AS count FROM effect_agent_subscription_events WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}`,
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
  });

  const event: SubscriptionStore["Service"]["event"] = Effect.fn("SqliteSubscriptionStore.event")(
    (eventId) => readEvent(eventId, "get event"),
  );

  const pendingEvents: SubscriptionStore["Service"]["pendingEvents"] = Effect.fn(
    "SqliteSubscriptionStore.pendingEvents",
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
    "SqliteSubscriptionStore.candidates",
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
      Effect.fn("SqliteSubscriptionStore.candidateRecord")(function* (row) {
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

  const insertDelivery = Effect.fn("SqliteSubscriptionStore.insertDelivery")(function* (
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

  const select: SubscriptionStore["Service"]["select"] = Effect.fn(
    "SqliteSubscriptionStore.select",
  )(function* (inputEvent, inputDeliveries, cursor, complete, nowMillis, inputLimits) {
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
    const changed = yield* transact(
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
        const additions: Array<{ delivery: SubscriptionDelivery; record: SubscriptionRecord }> = [];
        for (const delivery of deliveries) {
          const record = yield* readRegistration(delivery.key.subscription, "select registration");
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
        yield* writeEvent({ ...accepted, cursor, routingComplete: complete, routingFailure: null });
        return true;
      }),
    );
    if (changed) yield* failpoint.hit("subscription:select:after");
  });

  const catchUp: SubscriptionStore["Service"]["catchUp"] = Effect.fn(
    "SqliteSubscriptionStore.catchUp",
  )(function* (inputEvent, inputDelivery, nowMillis, inputLimits) {
    const supplied = yield* validate(AcceptedEvent, inputEvent, "catch-up-event");
    const delivery = yield* validate(SubscriptionDelivery, inputDelivery, "catch-up-delivery");
    const limits = yield* validate(SubscriptionLimits, inputLimits, "catch-up-limits");
    yield* requirePartition(supplied.partition, "catch-up-partition");
    yield* requirePartition(delivery.key.subscription.partition, "catch-up-delivery-partition");
    const changed = yield* transact(
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
    "SqliteSubscriptionStore.deferEvent",
  )(function* (eventId, nextAttemptAtMillis, code) {
    const routingFailure =
      code === undefined
        ? "routing-failed"
        : yield* validate(SubscriptionName, code, "routing-failure");
    yield* transact(
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
    "SqliteSubscriptionStore.delivery",
  )(function* (input) {
    const key = yield* validate(SubscriptionDeliveryKey, input, "delivery-key");
    yield* requirePartition(key.subscription.partition, "delivery-partition");
    return yield* readDelivery(key, "get delivery");
  });

  const pendingDeliveries: SubscriptionStore["Service"]["pendingDeliveries"] = Effect.fn(
    "SqliteSubscriptionStore.pendingDeliveries",
  )(function* (nowMillis, after, limit) {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT owner_id, subscription_id, event_id FROM effect_agent_subscription_deliveries
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND state NOT IN ('delivered','refused')
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
    "SqliteSubscriptionStore.listDeliveries",
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
    "SqliteSubscriptionStore.changeDelivery",
  )(function* (inputKey, inputDeliveryId, inputChange) {
    const key = yield* validate(SubscriptionDeliveryKey, inputKey, "change-delivery-key");
    const deliveryId = yield* validate(Digest, inputDeliveryId, "change-delivery-id");
    const change = yield* validate(DeliveryChange, inputChange, "change-delivery-change");
    yield* requirePartition(key.subscription.partition, "change-delivery-partition");
    const result = yield* transact(
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
    "SqliteSubscriptionStore.recovering",
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
    "SqliteSubscriptionStore.deferRecovery",
  )(function* (input, recovery) {
    const key = yield* requireKey(input, "defer-recovery-key");
    yield* transact(
      Effect.gen(function* () {
        const record = yield* readRegistration(key, "defer recovery");
        if (record === null) return yield* error("not-found", "subscription");
        yield* failpoint.hit("subscription:defer-recovery:before");
        yield* writeRegistration({
          ...record,
          recovery: record.state === "active" ? recovery : null,
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
    "SqliteSubscriptionStore.advanceScanCursors",
  )(function* (input) {
    const cursors = yield* validate(SubscriptionScanCursors, input, "scan-cursors");
    yield* transact(
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
        WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND state NOT IN ('delivered','refused')
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

  return SubscriptionStore.of({
    partition,
    register,
    get,
    list,
    cancel,
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

export const subscriptionStoreLayer = (
  partition: SourcePartition,
): Layer.Layer<
  SubscriptionStore,
  SqliteStorageInitializationError | SubscriptionError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient
> => Layer.effect(SubscriptionStore, makeSubscriptionStore(partition));
