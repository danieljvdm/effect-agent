import { Clock, Effect, Layer, Schema } from "effect";
import {
  ActivityBusy,
  ActivityClaim,
  ActivityClaimRequest,
  ActivityMutationFailpoint,
  type ActivityMutationFailure,
  ActivityOwnershipLost,
  ActivityProcessorKey,
  ActivityProcessorStore,
  ActivityProgress,
  ActivityStoreError,
  ActivityWorkConflict,
  PreparedActivity,
} from "effect-agent/activity-store";
import { Digest } from "effect-agent/records";
import { postgresLayer, SqlDialect } from "effect-agent/sql-dialect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { withWriterLockTransaction } from "./internal/postgres-transactions.ts";

const STORAGE_VERSION = 1 as const;
const METADATA_COMPONENT = "activity";
const STATE_TABLE = "effect_agent_activity_processor_state_v1";
const StoredJson = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const sameKey = Schema.toEquivalence(ActivityProcessorKey);
const sameWork = Schema.toEquivalence(PreparedActivity);

class ActivityMetadataRow extends Schema.Class<ActivityMetadataRow>(
  "@effect-agent/storage-postgres/ActivityMetadataRow",
)({
  version: Schema.Int,
}) {}

class ActivityStateRow extends Schema.Class<ActivityStateRow>(
  "@effect-agent/storage-postgres/ActivityStateRow",
)({
  processor_id: ActivityProcessorKey.fields.processorId,
  processor_version: ActivityProcessorKey.fields.processorVersion,
  thread_id: ActivityProcessorKey.fields.threadId,
  format_version: Schema.Int,
  through_sequence: ActivityProgress.fields.throughSequence,
  epoch: ActivityProgress.fields.epoch,
  owner: ActivityProgress.fields.owner,
  lease_expires_at: ActivityProgress.fields.leaseExpiresAt,
  progress_json: StoredJson,
}) {}

class ActivityChangeCountRow extends Schema.Class<ActivityChangeCountRow>(
  "@effect-agent/storage-postgres/ActivityChangeCountRow",
)({
  changed: Schema.Int,
}) {}

const StoredVersionHeader = Schema.Struct({ version: Schema.Int });

export type PostgresActivityInitializationError = ActivityStoreError | ActivityMutationFailure;

const storeError = (
  operation: string,
  reason: ActivityStoreError["reason"] = "unavailable",
): ActivityStoreError => ActivityStoreError.make({ operation, reason });

const query = <A extends object>(
  effect: Effect.Effect<ReadonlyArray<A>, SqlError>,
  operation: string,
) => effect.pipe(Effect.mapError(() => storeError(operation)));

const decodeRows = Effect.fn("PostgresActivityStore.decodeRows")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  rows: ReadonlyArray<unknown>,
  operation: string,
): Effect.fn.Return<ReadonlyArray<A>, ActivityStoreError> {
  return yield* Schema.decodeUnknownEffect(Schema.Array(schema))(rows).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
  );
});

const decodeInput = Effect.fn("PostgresActivityStore.decodeInput")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  value: unknown,
  operation: string,
): Effect.fn.Return<A, ActivityStoreError> {
  return yield* Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(() => storeError(operation, "invalid-input")),
  );
});

const encodeProgress = Effect.fn("PostgresActivityStore.encodeProgress")(function* (
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

const decodeProgress = Effect.fn("PostgresActivityStore.decodeProgress")(function* (
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

const validateProgress = Effect.fn("PostgresActivityStore.validateProgress")(function* (
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

// Every mutation reads progress and then writes it back under the same transaction. The
// write itself carries the compare-and-set predicate (epoch and through-sequence), so a
// racing connection that commits first leaves this transaction's UPDATE matching no row.
const makeActivityStore = Effect.fn("PostgresActivityStore.make")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const dialect = yield* SqlDialect;
  const failpoint = yield* ActivityMutationFailpoint;

  yield* failpoint.hit("activity:initialize:before");
  yield* withWriterLockTransaction(sql)(
    Effect.gen(function* () {
      yield* sql`
          CREATE TABLE IF NOT EXISTS effect_agent_activity_metadata (
            component TEXT PRIMARY KEY NOT NULL,
            version BIGINT NOT NULL
          )
        `;

      const metadataRows = yield* sql<Record<string, unknown>>`
          SELECT version FROM effect_agent_activity_metadata
          WHERE component = ${METADATA_COMPONENT}
        `;

      const metadata = yield* decodeRows(
        ActivityMetadataRow,
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
        const existing = yield* dialect.existingObjects("table", [STATE_TABLE]);

        if (existing.length > 0) {
          return yield* storeError("initialize activity schema", "incompatible");
        }
        yield* sql`
            CREATE TABLE effect_agent_activity_processor_state_v1 (
              processor_id TEXT NOT NULL,
              processor_version TEXT NOT NULL,
              thread_id TEXT NOT NULL,
              format_version BIGINT NOT NULL,
              through_sequence BIGINT NOT NULL,
              epoch BIGINT NOT NULL,
              owner TEXT,
              lease_expires_at DOUBLE PRECISION NOT NULL,
              progress_json TEXT NOT NULL,
              PRIMARY KEY (processor_id, processor_version, thread_id)
            )
          `;
        yield* sql`
            INSERT INTO effect_agent_activity_metadata (component, version)
            VALUES (${METADATA_COMPONENT}, ${STORAGE_VERSION})
          `;
      }
      yield* sql`
          SELECT processor_id, processor_version, thread_id, format_version,
            through_sequence, epoch, owner, lease_expires_at, progress_json
          FROM effect_agent_activity_processor_state_v1
          LIMIT 0
        `;
    }),
  ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storeError("initialize activity schema"))));
  yield* failpoint.hit("activity:initialize:after");

  const readProgress = Effect.fn("PostgresActivityStore.readProgress")(function* (
    key: ActivityProcessorKey,
    operation: string,
  ): Effect.fn.Return<ActivityProgress | null, ActivityStoreError> {
    const rawRows = yield* query(
      sql<Record<string, unknown>>`
        SELECT processor_id, processor_version, thread_id, format_version,
          through_sequence, epoch, owner, lease_expires_at, progress_json
        FROM effect_agent_activity_processor_state_v1
        WHERE processor_id = ${key.processorId}
          AND processor_version = ${key.processorVersion}
          AND thread_id = ${key.threadId}
      `,
      operation,
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

  // Postgres has no statement-scoped `changes()`, so each mutation returns a marker row per
  // affected row and the same one-row expectation is checked on that result.
  const checkChanged = Effect.fn("PostgresActivityStore.checkChanged")(function* (
    rawRows: ReadonlyArray<Record<string, unknown>>,
    operation: string,
  ): Effect.fn.Return<void, ActivityStoreError> {
    const rows = yield* decodeRows(ActivityChangeCountRow, rawRows, operation);

    if (rows.length !== 1 || rows[0].changed !== 1) {
      return yield* storeError(operation, "corrupt");
    }
  });

  const insertProgress = Effect.fn("PostgresActivityStore.insertProgress")(function* (
    progress: ActivityProgress,
    operation: string,
  ) {
    const progressJson = yield* encodeProgress(progress, operation);

    const changed = yield* sql<Record<string, unknown>>`
      INSERT INTO effect_agent_activity_processor_state_v1 (
        processor_id, processor_version, thread_id, format_version, through_sequence,
        epoch, owner, lease_expires_at, progress_json
      ) VALUES (
        ${progress.key.processorId}, ${progress.key.processorVersion}, ${progress.key.threadId},
        ${STORAGE_VERSION}, ${progress.throughSequence}, ${progress.epoch}, ${progress.owner},
        ${progress.leaseExpiresAt}, ${progressJson}
      )
      RETURNING 1 AS changed
    `;

    yield* checkChanged(changed, operation);
  });

  const updateProgress = Effect.fn("PostgresActivityStore.updateProgress")(function* (
    current: ActivityProgress,
    next: ActivityProgress,
    operation: string,
  ) {
    const progressJson = yield* encodeProgress(next, operation);

    const changed = yield* sql<Record<string, unknown>>`
      UPDATE effect_agent_activity_processor_state_v1
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
    `;

    yield* checkChanged(changed, operation);
  });

  const requireLive = Effect.fn("PostgresActivityStore.requireLive")(function* (
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
    "PostgresActivityStore.inspect",
  )(function* (key) {
    const decodedKey = yield* decodeInput(ActivityProcessorKey, key, "inspect activity progress");

    return yield* readProgress(decodedKey, "inspect activity progress");
  });

  const claim: ActivityProcessorStore["Service"]["claim"] = Effect.fn(
    "PostgresActivityStore.claim",
  )(function* (request) {
    const operation = "claim activity progress";
    const decoded = yield* decodeInput(ActivityClaimRequest, request, operation);

    yield* failpoint.hit("activity:claim:before");

    const claimed = yield* withWriterLockTransaction(sql)(
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
  });

  const prepare: ActivityProcessorStore["Service"]["prepare"] = Effect.fn(
    "PostgresActivityStore.prepare",
  )(function* (request) {
    const operation = "prepare activity output";
    const claim = yield* decodeInput(ActivityClaim, request.claim, operation);
    const work = yield* decodeInput(PreparedActivity, request.work, operation);

    yield* failpoint.hit("activity:prepare:before");

    const result = yield* withWriterLockTransaction(sql)(
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
    "PostgresActivityStore.advance",
  )(function* (request) {
    const operation = "advance activity progress";
    const claim = yield* decodeInput(ActivityClaim, request.claim, operation);
    const workId = yield* decodeInput(Digest, request.workId, operation);

    yield* failpoint.hit("activity:advance:before");

    const nextClaim = yield* withWriterLockTransaction(sql)(
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
    "PostgresActivityStore.release",
  )(function* (claim) {
    const operation = "release activity claim";
    const decoded = yield* decodeInput(ActivityClaim, claim, operation);

    yield* failpoint.hit("activity:release:before");
    yield* withWriterLockTransaction(sql)(
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

/** Postgres activity progress with mutation failpoints kept injectable for recovery tests. */
export const activityProcessorStoreLayerWithFailpoints: Layer.Layer<
  ActivityProcessorStore,
  PostgresActivityInitializationError,
  SqlClientService.SqlClient | ActivityMutationFailpoint
> = Layer.effect(ActivityProcessorStore, makeActivityStore()).pipe(Layer.provide(postgresLayer));

/** Postgres activity progress with the production no-op mutation failpoint. */
export const activityProcessorStoreLayer: Layer.Layer<
  ActivityProcessorStore,
  PostgresActivityInitializationError,
  SqlClientService.SqlClient
> = activityProcessorStoreLayerWithFailpoints.pipe(Layer.provide(ActivityMutationFailpoint.layer));
