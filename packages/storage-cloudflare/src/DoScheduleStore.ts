import {
  ScheduleCapacityError,
  ScheduleDueCursor,
  defaultSchedulingLimits,
  ScheduleChange,
  ScheduleConflict,
  ScheduleFailpoint,
  type ScheduleFailpointError,
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
} from "@effect-agent/thread/Schedule";
import {
  applyScheduleChange,
  scheduleUsesCapacity,
  scheduleDeadline,
} from "@effect-agent/thread/ScheduleTransition";
import { Context, Effect, Layer, Result, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

const CURRENT_SCHEDULE_STORE_VERSION = 2;
const MAX_STORED_SCHEDULE_BYTES = 1_900_000;

const StoredScheduleJson = Schema.String.check(Schema.isMaxLength(MAX_STORED_SCHEDULE_BYTES));
const StoredDeadline = Schema.NullOr(ScheduleInstant);

class ScheduleRow extends Schema.Class<ScheduleRow>("@effect-agent/storage-cloudflare/ScheduleRow")(
  {
    tenant_id: ScheduleOwner.fields.tenantId,
    owner_id: ScheduleOwner.fields.ownerId,
    schedule_id: ScheduleId,
    deadline_at_millis: StoredDeadline,
    record_json: StoredScheduleJson,
  },
) {}

const ScheduleDueRow = Schema.Struct({
  tenant_id: ScheduleOwner.fields.tenantId,
  owner_id: ScheduleOwner.fields.ownerId,
  schedule_id: ScheduleId,
  deadline_at_millis: ScheduleInstant,
});

class ScheduleCountRow extends Schema.Class<ScheduleCountRow>(
  "@effect-agent/storage-cloudflare/ScheduleCountRow",
)({
  schedule_count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

class ScheduleDeadlineRow extends Schema.Class<ScheduleDeadlineRow>(
  "@effect-agent/storage-cloudflare/ScheduleDeadlineRow",
)({
  deadline_at_millis: StoredDeadline,
}) {}

class ScheduleStoreStateRow extends Schema.Class<ScheduleStoreStateRow>(
  "@effect-agent/storage-cloudflare/ScheduleStoreStateRow",
)({
  storage_version: Schema.Int.check(Schema.isGreaterThan(0)),
  alarm_generation: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

class ScheduleTableNameRow extends Schema.Class<ScheduleTableNameRow>(
  "@effect-agent/storage-cloudflare/ScheduleTableNameRow",
)({
  name: Schema.String,
}) {}

export interface DoScheduleAlarmReplacement {
  readonly deadlineAtMillis: number | null;
  /** Included in every logical alarm payload so equal-time replacement stays distinguishable. */
  readonly generation: number;
}

export type DoScheduleReplaceAlarm = (
  replacement: DoScheduleAlarmReplacement,
) => Effect.Effect<void, ScheduleStorageError>;

/**
 * Platform-owned transaction boundary for schedule SQL and logical alarm mutation. The callback
 * and its `replaceAlarm` capability belong to one fiber and must not escape or fork.
 */
export class DoScheduleTransaction extends Context.Service<
  DoScheduleTransaction,
  {
    readonly run: <A, E, R>(
      body: (replaceAlarm: DoScheduleReplaceAlarm) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ScheduleStorageError, R>;
  }
>()("@effect-agent/storage-cloudflare/DoScheduleTransaction") {}

/** Platform driver operations that use the same schedule-state and alarm transaction. */
export class DoScheduleAlarmControl extends Context.Service<
  DoScheduleAlarmControl,
  {
    /** Establish a future recovery wake before cross-Object admission starts. */
    readonly prearm: (
      deadlineAtMillis: number,
    ) => Effect.Effect<void, ScheduleStorageError | ScheduleFailpointError>;
    /** Replace or cancel the wake from the object's authoritative indexed deadline. */
    readonly reconcile: Effect.Effect<void, ScheduleStorageError | ScheduleFailpointError>;
  }
>()("@effect-agent/storage-cloudflare/DoScheduleAlarmControl") {}

const unavailable = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "unavailable" });

const corrupt = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "corrupt" });

const decodeRows = Effect.fn("DoScheduleStore.decodeRows")(function* <A, I, R>(
  schema: Schema.Codec<A, I, R>,
  rows: ReadonlyArray<unknown>,
  operation: string,
): Effect.fn.Return<A, ScheduleStorageError, R> {
  return yield* Schema.decodeUnknownEffect(schema)(rows).pipe(
    Effect.mapError(() => corrupt(operation)),
  );
});

const decodeBoundary = <A, I, R>(
  schema: Schema.Codec<A, I, R>,
  value: unknown,
  operation: string,
): Effect.Effect<A, ScheduleStorageError, R> =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => corrupt(operation)));

const decodeRecord = Effect.fn("DoScheduleStore.decodeRecord")(function* (
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
    return yield* corrupt("decode schedule index");
  }

  return record;
});

const encodeRecord = Effect.fn("DoScheduleStore.encodeRecord")(function* (
  record: ScheduleRecord,
): Effect.fn.Return<string, ScheduleStorageError> {
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ScheduleRecord))(record).pipe(
    Effect.mapError(() => corrupt("encode schedule")),
  );

  return yield* Schema.decodeUnknownEffect(StoredScheduleJson)(encoded).pipe(
    Effect.mapError(() => corrupt("encode schedule bounds")),
  );
});

const initializeScheduleStore = Effect.fn("DoScheduleStore.initialize")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const operation = "initialize schedule store";

  const rawTables = yield* sql<Record<string, unknown>>`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name IN (
        'effect_agent_schedule_store_state',
        'effect_agent_schedules'
      )
    ORDER BY name
  `.pipe(Effect.mapError(() => unavailable(operation)));

  const tables = yield* decodeRows(Schema.Array(ScheduleTableNameRow), rawTables, operation);
  const hasState = tables.some((row) => row.name === "effect_agent_schedule_store_state");
  const hasSchedules = tables.some((row) => row.name === "effect_agent_schedules");

  if (!hasState) {
    if (hasSchedules) return yield* corrupt(operation);
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`
            CREATE TABLE effect_agent_schedule_store_state (
              singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
              storage_version INTEGER NOT NULL,
              alarm_generation INTEGER NOT NULL
            )
          `.withoutTransform;
          yield* sql`
            CREATE TABLE effect_agent_schedules (
              tenant_id TEXT NOT NULL,
              owner_id TEXT NOT NULL,
              schedule_id TEXT NOT NULL,
              deadline_at_millis INTEGER,
              record_json TEXT NOT NULL,
              PRIMARY KEY (tenant_id, owner_id, schedule_id)
            )
          `.withoutTransform;
          yield* sql`
            CREATE INDEX effect_agent_schedules_deadline
              ON effect_agent_schedules (deadline_at_millis, tenant_id, owner_id, schedule_id)
              WHERE deadline_at_millis IS NOT NULL
          `.withoutTransform;
          yield* sql`
            CREATE INDEX effect_agent_schedules_owner_deadline
              ON effect_agent_schedules (tenant_id, owner_id, deadline_at_millis, schedule_id)
              WHERE deadline_at_millis IS NOT NULL
          `.withoutTransform;
          yield* sql`
            INSERT INTO effect_agent_schedule_store_state (
              singleton, storage_version, alarm_generation
            ) VALUES (1, ${CURRENT_SCHEDULE_STORE_VERSION}, 0)
          `.withoutTransform;
        }),
      )
      .pipe(Effect.mapError(() => unavailable(operation)));

    return;
  }

  if (!hasSchedules) return yield* corrupt(operation);

  const rawState = yield* sql<Record<string, unknown>>`
    SELECT storage_version, alarm_generation
    FROM effect_agent_schedule_store_state
    WHERE singleton = 1
  `.pipe(Effect.mapError(() => unavailable(operation)));

  const state = yield* decodeRows(Schema.Array(ScheduleStoreStateRow), rawState, operation);

  if (state.length !== 1 || state[0].storage_version !== CURRENT_SCHEDULE_STORE_VERSION) {
    return yield* corrupt(
      state.length === 1
        ? `${operation}: incompatible storage version ${state[0].storage_version}; expected ${CURRENT_SCHEDULE_STORE_VERSION}`
        : `${operation}: invalid storage version row`,
    );
  }
});

const makeServices = Effect.gen(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const transactions = yield* DoScheduleTransaction;
  const scheduleFailpoint = yield* ScheduleFailpoint;

  yield* initializeScheduleStore();

  const readRows = Effect.fn("DoScheduleStore.readRows")(function* (
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

  const readOne = Effect.fn("DoScheduleStore.readOne")(function* (
    key: ScheduleKey,
    operation: string,
  ): Effect.fn.Return<ScheduleRecord | null, ScheduleStorageError> {
    const rows = yield* readRows(key, operation);

    if (rows.length === 0) return null;
    if (rows.length !== 1) return yield* corrupt(operation);

    return yield* decodeRecord(rows[0]);
  });

  const readNextDeadline = Effect.fn("DoScheduleStore.readNextDeadline")(function* (
    owner: ScheduleOwner | undefined,
    operation: string,
  ): Effect.fn.Return<number | null, ScheduleStorageError> {
    const rows =
      owner === undefined
        ? yield* sql<Record<string, unknown>>`
            SELECT MIN(deadline_at_millis) AS deadline_at_millis
            FROM effect_agent_schedules
            WHERE deadline_at_millis IS NOT NULL
          `.pipe(Effect.mapError(() => unavailable(operation)))
        : yield* sql<Record<string, unknown>>`
            SELECT MIN(deadline_at_millis) AS deadline_at_millis
            FROM effect_agent_schedules
            WHERE tenant_id = ${owner.tenantId}
              AND owner_id = ${owner.ownerId}
              AND deadline_at_millis IS NOT NULL
          `.pipe(Effect.mapError(() => unavailable(operation)));

    const decoded = yield* decodeRows(Schema.Array(ScheduleDeadlineRow), rows, operation);

    if (decoded.length !== 1) return yield* corrupt(operation);

    return decoded[0].deadline_at_millis;
  });

  const replaceAlarm = Effect.fn("DoScheduleStore.replaceAlarm")(function* (
    replace: DoScheduleReplaceAlarm,
    deadlineAtMillis: number | null,
    operation: string,
  ) {
    const rawState = yield* sql<Record<string, unknown>>`
      UPDATE effect_agent_schedule_store_state
      SET alarm_generation = alarm_generation + 1
      WHERE singleton = 1
      RETURNING storage_version, alarm_generation
    `.pipe(Effect.mapError(() => unavailable(operation)));

    const state = yield* decodeRows(Schema.Array(ScheduleStoreStateRow), rawState, operation);

    if (state.length !== 1 || state[0].storage_version !== CURRENT_SCHEDULE_STORE_VERSION) {
      return yield* corrupt(operation);
    }
    yield* scheduleFailpoint.hit("schedule:alarm:before");
    yield* replace({ deadlineAtMillis, generation: state[0].alarm_generation });
    yield* scheduleFailpoint.hit("schedule:alarm:after");
  });

  const insert: ScheduleStore["Service"]["insert"] = Effect.fn("DoScheduleStore.insert")(
    function* (record, ownerLimit) {
      const operation = "insert schedule";
      const canonical = yield* decodeBoundary(ScheduleRecord, record, operation);
      const recordJson = yield* encodeRecord(canonical);

      const result = yield* transactions.run((replace) =>
        Effect.gen(function* () {
          const existing = yield* readOne(canonical, operation);

          if (existing !== null) {
            if (existing.creationFingerprint === canonical.creationFingerprint) {
              return { record: existing, inserted: false } as const;
            }

            return yield* ScheduleConflict.make({ reason: "creation", key: canonical });
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
          const deadline = yield* readNextDeadline(undefined, operation);

          yield* replaceAlarm(replace, deadline, operation);

          return { record: canonical, inserted: true } as const;
        }),
      );

      if (result.inserted) yield* scheduleFailpoint.hit("schedule:insert:after");

      return result.record;
    },
  );

  const get: ScheduleStore["Service"]["get"] = Effect.fn("DoScheduleStore.get")(function* (key) {
    const canonical = yield* decodeBoundary(ScheduleKey, key, "get schedule");

    return yield* readOne(canonical, "get schedule");
  });

  const list: ScheduleStore["Service"]["list"] = Effect.fn("DoScheduleStore.list")(function* (
    requestValue: SchedulePageRequest,
  ): Effect.fn.Return<SchedulePage, ScheduleStorageError> {
    const operation = "list schedules";
    const request = yield* decodeBoundary(SchedulePageRequest, requestValue, operation);

    const rows =
      request.after === undefined
        ? yield* sql<Record<string, unknown>>`
            SELECT tenant_id, owner_id, schedule_id, deadline_at_millis, record_json
            FROM effect_agent_schedules
            WHERE tenant_id = ${request.owner.tenantId}
              AND owner_id = ${request.owner.ownerId}
            ORDER BY schedule_id
            LIMIT ${request.limit + 1}
          `.pipe(Effect.mapError(() => unavailable(operation)))
        : yield* sql<Record<string, unknown>>`
            SELECT tenant_id, owner_id, schedule_id, deadline_at_millis, record_json
            FROM effect_agent_schedules
            WHERE tenant_id = ${request.owner.tenantId}
              AND owner_id = ${request.owner.ownerId}
              AND schedule_id > ${request.after}
            ORDER BY schedule_id
            LIMIT ${request.limit + 1}
          `.pipe(Effect.mapError(() => unavailable(operation)));

    const decoded = yield* decodeRows(Schema.Array(ScheduleRow), rows, operation);
    const records = yield* Effect.forEach(decoded, decodeRecord);
    const hasNext = records.length > request.limit;
    const items = hasNext ? records.slice(0, request.limit) : records;

    return { items, next: hasNext ? (items.at(-1)?.scheduleId ?? null) : null };
  });

  const change: ScheduleStore["Service"]["change"] = Effect.fn("DoScheduleStore.change")(function* (
    key,
    change,
    ownerLimit = defaultSchedulingLimits.maxSchedulesPerOwner,
  ) {
    const operation = "change schedule";
    const canonicalKey = yield* decodeBoundary(ScheduleKey, key, operation);
    const canonicalChange = yield* decodeBoundary(ScheduleChange, change, operation);

    const result = yield* transactions.run((replace) =>
      Effect.gen(function* () {
        const current = yield* readOne(canonicalKey, operation);

        if (current === null) return yield* ScheduleNotFound.make({ key: canonicalKey });
        const transition = applyScheduleChange(current, canonicalChange);

        if (Result.isFailure(transition)) return yield* transition.failure;
        const next = transition.success;

        if (!scheduleUsesCapacity(current) && scheduleUsesCapacity(next)) {
          const rawCounts = yield* sql<Record<string, unknown>>`
              SELECT COUNT(*) AS schedule_count FROM effect_agent_schedules
              WHERE tenant_id = ${key.owner.tenantId} AND owner_id = ${key.owner.ownerId}
                AND (json_extract(record_json, '$.pending') IS NOT NULL OR
                  (json_extract(record_json, '$.state') != 'cancelled' AND json_extract(record_json, '$.nextAtMillis') IS NOT NULL))
            `.pipe(Effect.mapError(() => unavailable(operation)));

          const counts = yield* decodeRows(Schema.Array(ScheduleCountRow), rawCounts, operation);

          if (counts.length !== 1) return yield* corrupt(operation);
          if (counts[0].schedule_count >= ownerLimit)
            return yield* ScheduleCapacityError.make({ limit: ownerLimit });
        }
        if (next === current) return { record: current, changed: false } as const;
        const recordJson = yield* encodeRecord(next);

        yield* scheduleFailpoint.hit(`schedule:${canonicalChange._tag.toLowerCase()}:before`);
        yield* sql`
            UPDATE effect_agent_schedules
            SET deadline_at_millis = ${scheduleDeadline(next)}, record_json = ${recordJson}
            WHERE tenant_id = ${canonicalKey.owner.tenantId}
              AND owner_id = ${canonicalKey.owner.ownerId}
              AND schedule_id = ${canonicalKey.scheduleId}
          `.pipe(Effect.mapError(() => unavailable(operation)));
        const deadline = yield* readNextDeadline(undefined, operation);

        yield* replaceAlarm(replace, deadline, operation);

        return { record: next, changed: true } as const;
      }),
    );

    if (result.changed) {
      yield* scheduleFailpoint.hit(`schedule:${canonicalChange._tag.toLowerCase()}:after`);
    }

    return result.record;
  });

  const due: ScheduleStore["Service"]["due"] = Effect.fn("DoScheduleStore.due")(function* (
    nowMillis,
    limit,
    owner?: ScheduleOwner,
    after?: ScheduleDueCursor,
  ) {
    const operation = "query due schedules";

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
      owner === undefined
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
            WHERE tenant_id = ${owner.tenantId}
              AND owner_id = ${owner.ownerId}
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
    "DoScheduleStore.nextDeadline",
  )(function* (owner?: ScheduleOwner) {
    const operation = "query next schedule deadline";

    return yield* readNextDeadline(owner, operation);
  });

  const prearm = Effect.fn("DoScheduleStore.prearm")(function* (
    deadlineAtMillis: number,
  ): Effect.fn.Return<void, ScheduleStorageError | ScheduleFailpointError> {
    yield* decodeBoundary(ScheduleInstant, deadlineAtMillis, "pre-arm schedule recovery");
    yield* transactions.run((replace) =>
      replaceAlarm(replace, deadlineAtMillis, "pre-arm schedule recovery"),
    );
    yield* scheduleFailpoint.hit("schedule:prearm:after");
  });

  const reconcile = Effect.gen(function* () {
    yield* transactions.run((replace) =>
      Effect.gen(function* () {
        const deadline = yield* readNextDeadline(undefined, "reconcile schedule alarm");

        yield* replaceAlarm(replace, deadline, "reconcile schedule alarm");
      }),
    );
    yield* scheduleFailpoint.hit("schedule:reconcile:after");
  });

  return Context.make(ScheduleStore, {
    insert,
    get,
    list,
    change,
    due,
    nextDeadline,
  }).pipe(Context.add(DoScheduleAlarmControl, { prearm, reconcile }));
});

/**
 * Durable Object SQLite ScheduleStore. Platform code supplies the one transaction owner that
 * combines these SQL mutations with its logical and native alarm lifecycle.
 */
export const scheduleStoreLayer: Layer.Layer<
  ScheduleStore | DoScheduleAlarmControl,
  ScheduleStorageError,
  SqlClientService.SqlClient | DoScheduleTransaction
> = Layer.effectContext(makeServices);
