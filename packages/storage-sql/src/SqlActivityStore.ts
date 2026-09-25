import { Clock, Effect, Schema } from "effect";
import {
  ActivityBusy,
  ActivityClaim,
  ActivityClaimRequest,
  ActivityMutationFailpoint,
  ActivityOwnershipLost,
  ActivityProcessorKey,
  ActivityProcessorStore,
  ActivityProgress,
  ActivityStoreError,
  ActivityWorkConflict,
  PreparedActivity,
} from "effect-agent/activity-store";
import { Digest } from "effect-agent/records";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { makeSqlQuery, SqlInteger, SqlNumber, type SqlWriteTransaction } from "./SqlStorage.ts";

const STORAGE_VERSION = 1 as const;
const StoredJson = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const sameKey = Schema.toEquivalence(ActivityProcessorKey);
const sameWork = Schema.toEquivalence(PreparedActivity);

class ActivityStateRow extends Schema.Class<ActivityStateRow>(
  "@effect-agent/storage-sql/ActivityStateRow",
)({
  processor_id: ActivityProcessorKey.fields.processorId,
  processor_version: ActivityProcessorKey.fields.processorVersion,
  thread_id: ActivityProcessorKey.fields.threadId,
  format_version: SqlInteger,
  through_sequence: SqlInteger.pipe(Schema.decodeTo(ActivityProgress.fields.throughSequence)),
  epoch: SqlInteger.pipe(Schema.decodeTo(ActivityProgress.fields.epoch)),
  owner: ActivityProgress.fields.owner,
  lease_expires_at: SqlNumber.pipe(Schema.decodeTo(ActivityProgress.fields.leaseExpiresAt)),
  progress_json: StoredJson,
}) {}

class ActivityChangeCountRow extends Schema.Class<ActivityChangeCountRow>(
  "@effect-agent/storage-sql/ActivityChangeCountRow",
)({
  changed: SqlInteger,
}) {}

const StoredVersionHeader = Schema.Struct({ version: Schema.Int });

const storeError = (
  operation: string,
  reason: ActivityStoreError["reason"] = "unavailable",
): ActivityStoreError => ActivityStoreError.make({ operation, reason });

const decodeRows = Effect.fn("SqlActivityStore.decodeRows")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  rows: ReadonlyArray<unknown>,
  operation: string,
): Effect.fn.Return<ReadonlyArray<A>, ActivityStoreError> {
  return yield* Schema.decodeUnknownEffect(Schema.Array(schema))(rows).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
  );
});

const decodeInput = Effect.fn("SqlActivityStore.decodeInput")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  value: unknown,
  operation: string,
): Effect.fn.Return<A, ActivityStoreError> {
  return yield* Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(() => storeError(operation, "invalid-input")),
  );
});

const encodeProgress = Effect.fn("SqlActivityStore.encodeProgress")(function* (
  progress: ActivityProgress,
  operation: string,
): Effect.fn.Return<string, ActivityStoreError> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(ActivityProgress))(progress).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
    Effect.flatMap((encoded) =>
      Schema.decodeEffect(StoredJson)(encoded).pipe(
        Effect.mapError(() => storeError(operation, "invalid-input")),
      ),
    ),
  );
});

const decodeProgress = Effect.fn("SqlActivityStore.decodeProgress")(function* (
  value: string,
  operation: string,
): Effect.fn.Return<ActivityProgress, ActivityStoreError> {
  const header = yield* Schema.decodeEffect(Schema.fromJsonString(StoredVersionHeader))(value).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
  );

  if (header.version !== STORAGE_VERSION) {
    return yield* storeError(operation, "incompatible");
  }

  const progress = yield* Schema.decodeEffect(Schema.fromJsonString(ActivityProgress))(value).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
  );

  const canonical = yield* encodeProgress(progress, operation);

  if (canonical !== value) return yield* storeError(operation, "corrupt");

  return progress;
});

const validateProgress = Effect.fn("SqlActivityStore.validateProgress")(function* (
  progress: ActivityProgress,
  operation: string,
): Effect.fn.Return<ActivityProgress, ActivityStoreError> {
  if (
    progress.epoch < 1 ||
    (progress.owner === null && progress.leaseExpiresAt !== 0) ||
    (progress.pending !== null &&
      (!sameKey(progress.pending.key, progress.key) ||
        progress.pending.sequence !== progress.throughSequence + 1))
  ) {
    return yield* storeError(operation, "corrupt");
  }

  return progress;
});

const makeClaim = (progress: ActivityProgress): ActivityClaim | null =>
  progress.owner === null
    ? null
    : ActivityClaim.make({
        key: progress.key,
        owner: progress.owner,
        epoch: progress.epoch,
        throughSequence: progress.throughSequence,
        leaseExpiresAt: progress.leaseExpiresAt,
        pending: progress.pending,
      });

const ownershipLost = (claim: ActivityClaim) =>
  ActivityOwnershipLost.make({ key: claim.key, owner: claim.owner, epoch: claim.epoch });

/**
 * Initialize standalone activity progress and provide its transitions. The adapter supplies
 * its transaction semantics and optional namespace setup, which runs in the initialization transaction.
 */
export const makeSqlActivityStore = Effect.fn("SqlActivityStore.make")(function* (
  withWriteTransaction: SqlWriteTransaction,
  initializeNamespace: Effect.Effect<
    void,
    ActivityStoreError,
    SqlClientService.SqlClient
  > = Effect.void,
  namespace?: string,
) {
  const sql = yield* SqlClientService.SqlClient;
  const { table: relation, execute } = yield* makeSqlQuery(namespace);
  const failpoint = yield* ActivityMutationFailpoint;

  yield* failpoint.hit("activity:initialize:before");
  yield* withWriteTransaction(
    Effect.gen(function* () {
      yield* initializeNamespace;

      const integer = sql.literal(
        sql.onDialectOrElse({ pg: () => "BIGINT", orElse: () => "INTEGER" }),
      );

      const real = sql.literal(
        sql.onDialectOrElse({ pg: () => "DOUBLE PRECISION", orElse: () => "REAL" }),
      );

      yield* sql`
        CREATE TABLE IF NOT EXISTS ${relation("effect_agent_activity_metadata")} (
          component TEXT PRIMARY KEY NOT NULL,
          version ${integer} NOT NULL
        )
      `.pipe(execute);

      const metadataRows = yield* sql<Record<string, unknown>>`
        SELECT version FROM ${relation("effect_agent_activity_metadata")}
        WHERE component = ${"activity"}
      `.pipe(execute);

      const metadata = yield* decodeRows(
        Schema.Struct({ version: SqlInteger }),
        metadataRows,
        "decode activity schema version",
      );

      if (metadata.length > 1) {
        return yield* storeError("decode activity schema version", "corrupt");
      }
      const currentVersion = metadata[0]?.version;

      if (currentVersion !== undefined && currentVersion !== STORAGE_VERSION) {
        return yield* storeError("initialize activity schema", "incompatible");
      }
      if (currentVersion === undefined) {
        const tableRows = yield* sql.onDialectOrElse({
          pg: () =>
            sql<Record<string, unknown>>`
            SELECT c.relname AS name FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
              AND c.relname = 'effect_agent_activity_processor_state_v1'
              AND n.nspname = ${namespace ?? sql`current_schema()`}
          `.pipe(execute),
          orElse: () =>
            sql<Record<string, unknown>>`
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = 'effect_agent_activity_processor_state_v1'
          `.pipe(execute),
        });

        const existing = yield* decodeRows(
          Schema.Struct({ name: Schema.NonEmptyString }),
          tableRows,
          "inspect activity schema",
        );

        if (existing.length > 0) {
          return yield* storeError("initialize activity schema", "incompatible");
        }
        yield* sql`
          CREATE TABLE ${relation("effect_agent_activity_processor_state_v1")} (
            processor_id TEXT NOT NULL,
            processor_version TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            format_version ${integer} NOT NULL,
            through_sequence ${integer} NOT NULL,
            epoch ${integer} NOT NULL,
            owner TEXT,
            lease_expires_at ${real} NOT NULL,
            progress_json TEXT NOT NULL,
            PRIMARY KEY (processor_id, processor_version, thread_id)
          )
        `.pipe(execute);
        yield* sql`
          INSERT INTO ${relation("effect_agent_activity_metadata")} (component, version)
          VALUES (${"activity"}, ${STORAGE_VERSION})
        `.pipe(execute);
      }
      yield* sql`
        SELECT processor_id, processor_version, thread_id, format_version,
          through_sequence, epoch, owner, lease_expires_at, progress_json
        FROM ${relation("effect_agent_activity_processor_state_v1")}
        LIMIT 0
      `.pipe(execute);
    }),
  ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storeError("initialize activity schema"))));
  yield* failpoint.hit("activity:initialize:after");

  const readProgress = Effect.fn("SqlActivityStore.readProgress")(function* (
    key: ActivityProcessorKey,
    operation: string,
  ): Effect.fn.Return<ActivityProgress | null, ActivityStoreError> {
    const rawRows = yield* sql<Record<string, unknown>>`
        SELECT processor_id, processor_version, thread_id, format_version,
          through_sequence, epoch, owner, lease_expires_at, progress_json
        FROM ${relation("effect_agent_activity_processor_state_v1")}
        WHERE processor_id = ${key.processorId}
          AND processor_version = ${key.processorVersion}
          AND thread_id = ${key.threadId}
      `.pipe(
      execute,
      Effect.mapError(() => storeError(operation)),
    );

    const rows = yield* decodeRows(ActivityStateRow, rawRows, operation);

    if (rows.length === 0) return null;
    if (rows.length !== 1) return yield* storeError(operation, "corrupt");
    const row = rows[0];

    if (row.format_version !== STORAGE_VERSION) {
      return yield* storeError(operation, "incompatible");
    }
    const progress = yield* decodeProgress(row.progress_json, operation);

    yield* validateProgress(progress, operation);
    if (
      !sameKey(progress.key, key) ||
      row.processor_id !== key.processorId ||
      row.processor_version !== key.processorVersion ||
      row.thread_id !== key.threadId ||
      row.through_sequence !== progress.throughSequence ||
      row.epoch !== progress.epoch ||
      row.owner !== progress.owner ||
      row.lease_expires_at !== progress.leaseExpiresAt
    ) {
      return yield* storeError(operation, "corrupt");
    }

    return progress;
  });

  const checkChanged = Effect.fn("SqlActivityStore.checkChanged")(function* (
    rawRows: ReadonlyArray<Record<string, unknown>>,
    operation: string,
  ): Effect.fn.Return<void, ActivityStoreError> {
    const rows = yield* decodeRows(ActivityChangeCountRow, rawRows, operation);

    if (rows.length !== 1 || rows[0].changed !== 1) {
      return yield* storeError(operation, "corrupt");
    }
  });

  const insertProgress = Effect.fn("SqlActivityStore.insertProgress")(function* (
    progress: ActivityProgress,
    operation: string,
  ) {
    const progressJson = yield* encodeProgress(progress, operation);

    const changed = yield* sql<Record<string, unknown>>`
      INSERT INTO ${relation("effect_agent_activity_processor_state_v1")} (
        processor_id, processor_version, thread_id, format_version, through_sequence,
        epoch, owner, lease_expires_at, progress_json
      ) VALUES (
        ${progress.key.processorId}, ${progress.key.processorVersion}, ${progress.key.threadId},
        ${STORAGE_VERSION}, ${progress.throughSequence}, ${progress.epoch}, ${progress.owner},
        ${progress.leaseExpiresAt}, ${progressJson}
      )
      RETURNING 1 AS changed
    `.pipe(execute);

    yield* checkChanged(changed, operation);
  });

  const updateProgress = Effect.fn("SqlActivityStore.updateProgress")(function* (
    current: ActivityProgress,
    next: ActivityProgress,
    operation: string,
  ) {
    const progressJson = yield* encodeProgress(next, operation);

    const changed = yield* sql<Record<string, unknown>>`
      UPDATE ${relation("effect_agent_activity_processor_state_v1")}
      SET format_version = ${STORAGE_VERSION},
          through_sequence = ${next.throughSequence},
          epoch = ${next.epoch},
          owner = ${next.owner},
          lease_expires_at = ${next.leaseExpiresAt},
          progress_json = ${progressJson}
      WHERE processor_id = ${current.key.processorId}
        AND processor_version = ${current.key.processorVersion}
        AND thread_id = ${current.key.threadId}
        AND through_sequence = ${current.throughSequence}
        AND epoch = ${current.epoch}
      RETURNING 1 AS changed
    `.pipe(execute);

    yield* checkChanged(changed, operation);
  });

  const requireLive = Effect.fn("SqlActivityStore.requireLive")(function* (
    progress: ActivityProgress | null,
    claim: ActivityClaim,
    requireSequence: boolean,
  ): Effect.fn.Return<ActivityProgress, ActivityOwnershipLost> {
    const now = yield* Clock.currentTimeMillis;

    if (
      progress === null ||
      !sameKey(progress.key, claim.key) ||
      progress.owner !== claim.owner ||
      progress.epoch !== claim.epoch ||
      progress.leaseExpiresAt <= now ||
      (requireSequence && progress.throughSequence !== claim.throughSequence)
    ) {
      return yield* ownershipLost(claim);
    }

    return progress;
  });

  const inspect: ActivityProcessorStore["Service"]["inspect"] = Effect.fn(
    "SqlActivityStore.inspect",
  )(function* (key) {
    const decodedKey = yield* decodeInput(ActivityProcessorKey, key, "inspect activity progress");

    return yield* readProgress(decodedKey, "inspect activity progress");
  });

  const claim: ActivityProcessorStore["Service"]["claim"] = Effect.fn("SqlActivityStore.claim")(
    function* (request) {
      const operation = "claim activity progress";
      const decoded = yield* decodeInput(ActivityClaimRequest, request, operation);

      yield* failpoint.hit("activity:claim:before");

      const claimed = yield* withWriteTransaction(
        Effect.gen(function* () {
          const current = yield* readProgress(decoded.key, operation);
          const now = yield* Clock.currentTimeMillis;

          if (current !== null && current.owner !== null && current.leaseExpiresAt > now) {
            return yield* ActivityBusy.make({
              key: decoded.key,
              leaseExpiresAt: current.leaseExpiresAt,
            });
          }

          const next = yield* Schema.decodeEffect(ActivityProgress)({
            version: STORAGE_VERSION,
            key: decoded.key,
            throughSequence: current?.throughSequence ?? 0,
            epoch: (current?.epoch ?? 0) + 1,
            owner: decoded.owner,
            leaseExpiresAt: now + decoded.leaseMillis,
            pending: current?.pending ?? null,
            advancedAt: current?.advancedAt ?? null,
          }).pipe(Effect.mapError(() => storeError(operation, "corrupt")));

          if (current === null) yield* insertProgress(next, operation);
          else yield* updateProgress(current, next, operation);
          yield* failpoint.hit("activity:claim:after-state");
          const result = makeClaim(next);

          if (result === null) return yield* storeError(operation, "corrupt");

          return result;
        }),
      ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storeError(operation))));

      yield* failpoint.hit("activity:claim:after");

      return claimed;
    },
  );

  const prepare: ActivityProcessorStore["Service"]["prepare"] = Effect.fn(
    "SqlActivityStore.prepare",
  )(function* (request) {
    const operation = "prepare activity output";
    const claim = yield* decodeInput(ActivityClaim, request.claim, operation);
    const work = yield* decodeInput(PreparedActivity, request.work, operation);

    yield* failpoint.hit("activity:prepare:before");

    const result = yield* withWriteTransaction(
      Effect.gen(function* () {
        const current = yield* requireLive(yield* readProgress(claim.key, operation), claim, true);

        if (current.pending !== null) {
          if (sameWork(current.pending, work)) {
            return { work: current.pending, changed: false } as const;
          }

          return yield* ActivityWorkConflict.make({ key: claim.key, workId: work.workId });
        }
        if (!sameKey(work.key, claim.key) || work.sequence !== current.throughSequence + 1) {
          return yield* ActivityWorkConflict.make({ key: claim.key, workId: work.workId });
        }
        const next = ActivityProgress.make({ ...current, pending: work });

        yield* updateProgress(current, next, operation);
        yield* failpoint.hit("activity:prepare:after-state");

        return { work, changed: true } as const;
      }),
    ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storeError(operation))));

    if (result.changed) yield* failpoint.hit("activity:prepare:after");

    return result.work;
  });

  const advance: ActivityProcessorStore["Service"]["advance"] = Effect.fn(
    "SqlActivityStore.advance",
  )(function* (request) {
    const operation = "advance activity progress";
    const claim = yield* decodeInput(ActivityClaim, request.claim, operation);
    const workId = yield* decodeInput(Digest, request.workId, operation);

    yield* failpoint.hit("activity:advance:before");

    const nextClaim = yield* withWriteTransaction(
      Effect.gen(function* () {
        const current = yield* requireLive(yield* readProgress(claim.key, operation), claim, true);

        if (current.pending === null || current.pending.workId !== workId) {
          return yield* ActivityWorkConflict.make({ key: claim.key, workId });
        }

        const next = ActivityProgress.make({
          ...current,
          throughSequence: current.pending.sequence,
          pending: null,
          advancedAt: yield* Clock.currentTimeMillis,
        });

        yield* updateProgress(current, next, operation);
        yield* failpoint.hit("activity:advance:after-state");
        const result = makeClaim(next);

        if (result === null) return yield* storeError(operation, "corrupt");

        return result;
      }),
    ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storeError(operation))));

    yield* failpoint.hit("activity:advance:after");

    return nextClaim;
  });

  const release: ActivityProcessorStore["Service"]["release"] = Effect.fn(
    "SqlActivityStore.release",
  )(function* (claim) {
    const operation = "release activity claim";
    const decoded = yield* decodeInput(ActivityClaim, claim, operation);

    yield* failpoint.hit("activity:release:before");
    yield* withWriteTransaction(
      Effect.gen(function* () {
        const current = yield* readProgress(decoded.key, operation);

        if (
          current === null ||
          current.owner !== decoded.owner ||
          current.epoch !== decoded.epoch
        ) {
          return yield* ownershipLost(decoded);
        }
        const next = ActivityProgress.make({ ...current, owner: null, leaseExpiresAt: 0 });

        yield* updateProgress(current, next, operation);
        yield* failpoint.hit("activity:release:after-state");
      }),
    ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storeError(operation))));
    yield* failpoint.hit("activity:release:after");
  });

  return ActivityProcessorStore.of({ inspect, claim, prepare, advance, release });
});
