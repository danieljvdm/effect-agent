import {
  ScheduleCapacityError,
  ScheduleDueCursor,
  defaultSchedulingLimits,
  scheduleUsesCapacity,
  ScheduleChange,
  ScheduleConflict,
  ScheduleFailpoint,
  applyScheduleChange,
  ScheduleKey,
  ScheduleId,
  ScheduleInstant,
  ScheduleNotFound,
  ScheduleOwner,
  type SchedulePage,
  SchedulePageRequest,
  ScheduleRecord,
  ScheduleStorageError,
  ScheduleStore,
  scheduleDeadline,
} from "@effect-agent/thread";
import { Effect, Layer, Result, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { initializeSqliteJournal } from "./sqlite-journal.ts";
import type { SqliteStorageConfig } from "./sqlite-storage-config.ts";
import type { SqliteStorageFailpoint } from "./sqlite-storage-failpoint.ts";
import type { SqliteStorageInitializationError } from "./sqlite-thread-store.ts";

// Configuration and the immutable pending envelope may each carry the canonical input. Leave
// room for JSON escaping and bounded status while rejecting an unreadable oversized row.
const StoredScheduleJson = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const StoredDeadline = Schema.NullOr(ScheduleInstant);

class ScheduleRow extends Schema.Class<ScheduleRow>("@effect-agent/storage-sqlite/ScheduleRow")({
  tenant_id: ScheduleOwner.fields.tenantId,
  owner_id: ScheduleOwner.fields.ownerId,
  schedule_id: ScheduleId,
  deadline_at_millis: StoredDeadline,
  record_json: StoredScheduleJson,
}) {}

const ScheduleDueRow = Schema.Struct({
  tenant_id: ScheduleOwner.fields.tenantId,
  owner_id: ScheduleOwner.fields.ownerId,
  schedule_id: ScheduleId,
  deadline_at_millis: ScheduleInstant,
});

class ScheduleCountRow extends Schema.Class<ScheduleCountRow>(
  "@effect-agent/storage-sqlite/ScheduleCountRow",
)({
  schedule_count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

class ScheduleDeadlineRow extends Schema.Class<ScheduleDeadlineRow>(
  "@effect-agent/storage-sqlite/ScheduleDeadlineRow",
)({
  deadline_at_millis: StoredDeadline,
}) {}

const unavailable = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "unavailable" });

const corrupt = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "corrupt" });

const decodeRows = Effect.fn("SqliteScheduleStore.decodeRows")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  rows: ReadonlyArray<unknown>,
  operation: string,
): Effect.fn.Return<A, ScheduleStorageError> {
  return yield* Schema.decodeUnknownEffect(schema)(rows).pipe(
    Effect.mapError(() => corrupt(operation)),
  );
});

const decodeRecord = Effect.fn("SqliteScheduleStore.decodeRecord")(function* (
  row: ScheduleRow,
): Effect.fn.Return<ScheduleRecord, ScheduleStorageError> {
  const record = yield* Schema.decodeEffect(Schema.fromJsonString(ScheduleRecord))(
    row.record_json,
  ).pipe(Effect.mapError(() => corrupt("decode schedule")));
  if (
    record.owner.tenantId !== row.tenant_id ||
    record.owner.ownerId !== row.owner_id ||
    record.scheduleId !== row.schedule_id ||
    scheduleDeadline(record) !== row.deadline_at_millis
  ) {
    return yield* corrupt("decode schedule identity");
  }
  return record;
});

const encodeRecord = Effect.fn("SqliteScheduleStore.encodeRecord")(function* (
  record: ScheduleRecord,
): Effect.fn.Return<string, ScheduleStorageError> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(ScheduleRecord))(record).pipe(
    Effect.mapError(() => corrupt("encode schedule")),
  );
});

const decodeInput = Effect.fn("SqliteScheduleStore.decodeInput")(function* <A, I>(
  operation: string,
  schema: Schema.Codec<A, I, never>,
  value: unknown,
): Effect.fn.Return<A, ScheduleStorageError> {
  return yield* Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(() => corrupt(operation)),
  );
});

const makeScheduleStore = Effect.gen(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const scheduleFailpoint = yield* ScheduleFailpoint;

  yield* initializeSqliteJournal();

  const readRows = Effect.fn("SqliteScheduleStore.readRows")(function* (
    key: ScheduleKey,
    operation: string,
  ): Effect.fn.Return<ReadonlyArray<ScheduleRow>, ScheduleStorageError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT tenant_id, owner_id, schedule_id, deadline_at_millis, record_json
      FROM effect_agent_schedules
      WHERE tenant_id = ${key.owner.tenantId}
        AND owner_id = ${key.owner.ownerId}
        AND schedule_id = ${key.scheduleId}
    `.pipe(Effect.mapError(() => unavailable(operation)));
    return yield* decodeRows(Schema.Array(ScheduleRow), rows, operation);
  });

  const readOne = Effect.fn("SqliteScheduleStore.readOne")(function* (
    key: ScheduleKey,
    operation: string,
  ): Effect.fn.Return<ScheduleRecord | null, ScheduleStorageError> {
    const rows = yield* readRows(key, operation);
    if (rows.length === 0) return null;
    if (rows.length !== 1) return yield* corrupt(operation);
    return yield* decodeRecord(rows[0]);
  });

  const insert: ScheduleStore["Service"]["insert"] = Effect.fn("SqliteScheduleStore.insert")(
    function* (record, ownerLimit) {
      const operation = "insert schedule";
      const canonical = yield* decodeInput(operation, ScheduleRecord, record);
      const recordJson = yield* encodeRecord(canonical);
      const result = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const existing = yield* readOne(canonical, operation);
            if (existing !== null) {
              if (existing.creationFingerprint === canonical.creationFingerprint) {
                return { record: existing, inserted: false } as const;
              }
              return yield* ScheduleConflict.make({
                reason: "creation",
                key: { owner: canonical.owner, scheduleId: canonical.scheduleId },
              });
            }
            const rawCounts = yield* sql<Record<string, unknown>>`
            SELECT COUNT(*) AS schedule_count
            FROM effect_agent_schedules
            WHERE tenant_id = ${canonical.owner.tenantId}
              AND owner_id = ${canonical.owner.ownerId}
              AND (json_extract(record_json, '$.pending') IS NOT NULL OR
                (json_extract(record_json, '$.state') != 'cancelled' AND json_extract(record_json, '$.nextAtMillis') IS NOT NULL))
          `.pipe(Effect.mapError(() => unavailable(operation)));
            const counts = yield* decodeRows(Schema.Array(ScheduleCountRow), rawCounts, operation);
            if (counts.length !== 1) return yield* corrupt(operation);
            if (counts[0].schedule_count >= ownerLimit) {
              return yield* ScheduleCapacityError.make({ limit: ownerLimit });
            }
            yield* scheduleFailpoint.hit("schedule:insert:before");
            yield* sql`
            INSERT INTO effect_agent_schedules (
              tenant_id, owner_id, schedule_id, deadline_at_millis, record_json
            ) VALUES (
              ${canonical.owner.tenantId},
              ${canonical.owner.ownerId},
              ${canonical.scheduleId},
              ${scheduleDeadline(canonical)},
              ${recordJson}
            )
          `.pipe(Effect.mapError(() => unavailable(operation)));
            return { record: canonical, inserted: true } as const;
          }),
        )
        .pipe(Effect.catchTag("SqlError", () => Effect.fail(unavailable(operation))));
      if (result.inserted) yield* scheduleFailpoint.hit("schedule:insert:after");
      return result.record;
    },
  );

  const get: ScheduleStore["Service"]["get"] = Effect.fn("SqliteScheduleStore.get")(
    function* (key) {
      const decodedKey = yield* decodeInput("get schedule", ScheduleKey, key);
      return yield* readOne(decodedKey, "get schedule");
    },
  );

  const list: ScheduleStore["Service"]["list"] = Effect.fn("SqliteScheduleStore.list")(function* (
    request: SchedulePageRequest,
  ): Effect.fn.Return<SchedulePage, ScheduleStorageError> {
    const operation = "list schedules";
    const decodedRequest = yield* decodeInput(operation, SchedulePageRequest, request);
    const rows =
      decodedRequest.after === undefined
        ? yield* sql<Record<string, unknown>>`
            SELECT tenant_id, owner_id, schedule_id, deadline_at_millis, record_json
            FROM effect_agent_schedules
            WHERE tenant_id = ${decodedRequest.owner.tenantId}
              AND owner_id = ${decodedRequest.owner.ownerId}
            ORDER BY schedule_id
            LIMIT ${decodedRequest.limit + 1}
          `.pipe(Effect.mapError(() => unavailable(operation)))
        : yield* sql<Record<string, unknown>>`
            SELECT tenant_id, owner_id, schedule_id, deadline_at_millis, record_json
            FROM effect_agent_schedules
            WHERE tenant_id = ${decodedRequest.owner.tenantId}
              AND owner_id = ${decodedRequest.owner.ownerId}
              AND schedule_id > ${decodedRequest.after}
            ORDER BY schedule_id
            LIMIT ${decodedRequest.limit + 1}
          `.pipe(Effect.mapError(() => unavailable(operation)));
    const decoded = yield* decodeRows(Schema.Array(ScheduleRow), rows, operation);
    const records = yield* Effect.forEach(decoded, decodeRecord);
    const hasNext = records.length > decodedRequest.limit;
    const items = hasNext ? records.slice(0, decodedRequest.limit) : records;
    return { items, next: hasNext ? (items.at(-1)?.scheduleId ?? null) : null };
  });

  const change: ScheduleStore["Service"]["change"] = Effect.fn("SqliteScheduleStore.change")(
    function* (key, change, ownerLimit = defaultSchedulingLimits.maxSchedulesPerOwner) {
      const operation = "change schedule";
      const decodedKey = yield* decodeInput(operation, ScheduleKey, key);
      const decodedChange = yield* decodeInput(operation, ScheduleChange, change);
      const result = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const current = yield* readOne(decodedKey, operation);
            if (current === null) return yield* ScheduleNotFound.make({ key: decodedKey });
            const transition = applyScheduleChange(current, decodedChange);
            if (Result.isFailure(transition)) return yield* transition.failure;
            const next = transition.success;
            if (!scheduleUsesCapacity(current) && scheduleUsesCapacity(next)) {
              const rawCounts = yield* sql<Record<string, unknown>>`
              SELECT COUNT(*) AS schedule_count FROM effect_agent_schedules
              WHERE tenant_id = ${key.owner.tenantId} AND owner_id = ${key.owner.ownerId}
                AND (json_extract(record_json, '$.pending') IS NOT NULL OR
                  (json_extract(record_json, '$.state') != 'cancelled' AND json_extract(record_json, '$.nextAtMillis') IS NOT NULL))
            `.pipe(Effect.mapError(() => unavailable(operation)));
              const counts = yield* decodeRows(
                Schema.Array(ScheduleCountRow),
                rawCounts,
                operation,
              );
              if (counts.length !== 1) return yield* corrupt(operation);
              if (counts[0].schedule_count >= ownerLimit)
                return yield* ScheduleCapacityError.make({ limit: ownerLimit });
            }
            if (next === current) return { record: current, changed: false } as const;
            const recordJson = yield* encodeRecord(next);
            yield* scheduleFailpoint.hit(`schedule:${decodedChange._tag.toLowerCase()}:before`);
            yield* sql`
            UPDATE effect_agent_schedules
            SET deadline_at_millis = ${scheduleDeadline(next)}, record_json = ${recordJson}
            WHERE tenant_id = ${decodedKey.owner.tenantId}
              AND owner_id = ${decodedKey.owner.ownerId}
              AND schedule_id = ${decodedKey.scheduleId}
          `.pipe(Effect.mapError(() => unavailable(operation)));
            return { record: next, changed: true } as const;
          }),
        )
        .pipe(Effect.catchTag("SqlError", () => Effect.fail(unavailable(operation))));
      if (result.changed) {
        yield* scheduleFailpoint.hit(`schedule:${decodedChange._tag.toLowerCase()}:after`);
      }
      return result.record;
    },
  );

  const due: ScheduleStore["Service"]["due"] = Effect.fn("SqliteScheduleStore.due")(function* (
    nowMillis,
    limit,
    owner?: ScheduleOwner,
    after?: ScheduleDueCursor,
  ) {
    const operation = "query due schedules";
    const decodedOwner =
      owner === undefined ? undefined : yield* decodeInput(operation, ScheduleOwner, owner);
    const cursor =
      after === undefined
        ? undefined
        : yield* Schema.decodeUnknownEffect(ScheduleDueCursor)(after).pipe(
            Effect.mapError(() => corrupt(operation)),
          );
    const continuation =
      cursor === undefined
        ? sql`1 = 1`
        : sql`
      (deadline_at_millis, tenant_id, owner_id, schedule_id) >
      (${cursor.deadlineAtMillis}, ${cursor.owner.tenantId}, ${cursor.owner.ownerId}, ${cursor.scheduleId})`;
    const rows =
      decodedOwner === undefined
        ? yield* sql<Record<string, unknown>>`
            SELECT tenant_id, owner_id, schedule_id, deadline_at_millis
            FROM effect_agent_schedules
            WHERE deadline_at_millis <= ${nowMillis} AND ${continuation}
            ORDER BY deadline_at_millis, tenant_id, owner_id, schedule_id
            LIMIT ${limit}
          `.pipe(Effect.mapError(() => unavailable(operation)))
        : yield* sql<Record<string, unknown>>`
            SELECT tenant_id, owner_id, schedule_id, deadline_at_millis
            FROM effect_agent_schedules
            WHERE tenant_id = ${decodedOwner.tenantId}
              AND owner_id = ${decodedOwner.ownerId}
              AND deadline_at_millis <= ${nowMillis} AND ${continuation}
            ORDER BY deadline_at_millis, schedule_id
            LIMIT ${limit}
          `.pipe(Effect.mapError(() => unavailable(operation)));
    const decoded = yield* decodeRows(Schema.Array(ScheduleDueRow), rows, operation);
    return decoded.map((row) => ({
      owner: { tenantId: row.tenant_id, ownerId: row.owner_id },
      scheduleId: row.schedule_id,
      deadlineAtMillis: row.deadline_at_millis,
    }));
  });

  const nextDeadline: ScheduleStore["Service"]["nextDeadline"] = Effect.fn(
    "SqliteScheduleStore.nextDeadline",
  )(function* (owner?: ScheduleOwner) {
    const operation = "query next schedule deadline";
    const decodedOwner =
      owner === undefined ? undefined : yield* decodeInput(operation, ScheduleOwner, owner);
    const rows =
      decodedOwner === undefined
        ? yield* sql<Record<string, unknown>>`
          SELECT MIN(deadline_at_millis) AS deadline_at_millis
          FROM effect_agent_schedules
          WHERE deadline_at_millis IS NOT NULL
        `.pipe(Effect.mapError(() => unavailable(operation)))
        : yield* sql<Record<string, unknown>>`
          SELECT MIN(deadline_at_millis) AS deadline_at_millis
          FROM effect_agent_schedules
          WHERE tenant_id = ${decodedOwner.tenantId}
            AND owner_id = ${decodedOwner.ownerId}
            AND deadline_at_millis IS NOT NULL
        `.pipe(Effect.mapError(() => unavailable(operation)));
    const decoded = yield* decodeRows(Schema.Array(ScheduleDeadlineRow), rows, operation);
    if (decoded.length !== 1) return yield* corrupt(operation);
    return decoded[0].deadline_at_millis;
  });

  return ScheduleStore.of({ insert, get, list, change, due, nextDeadline });
});

/** SQLite implementation of the atomic ScheduleStore port. */
export const scheduleStoreLayer: Layer.Layer<
  ScheduleStore,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient
> = Layer.effect(ScheduleStore)(makeScheduleStore);
