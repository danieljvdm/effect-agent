import { makeSqlActivityStore } from "@effect-agent/storage-sql/sql-activity-store";
import { Effect, Layer, Schema } from "effect";
import {
  ActivityMutationFailpoint,
  type ActivityMutationFailure,
  ActivityProcessorStore,
  ActivityStoreError,
} from "effect-agent/activity-store";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

const STORAGE_VERSION = 1 as const;
const METADATA_COMPONENT = "activity";
const STATE_TABLE = "effect_agent_activity_processor_state_v1";

class ActivityMetadataRow extends Schema.Class<ActivityMetadataRow>(
  "@effect-agent/storage-sqlite/ActivityMetadataRow",
)({
  version: Schema.Int,
}) {}

class ActivityTableRow extends Schema.Class<ActivityTableRow>(
  "@effect-agent/storage-sqlite/ActivityTableRow",
)({
  name: Schema.NonEmptyString,
}) {}

export type SqliteActivityInitializationError = ActivityStoreError | ActivityMutationFailure;

const storeError = (
  operation: string,
  reason: ActivityStoreError["reason"] = "unavailable",
): ActivityStoreError => ActivityStoreError.make({ operation, reason });

const decodeRows = Effect.fn("SqliteActivityStore.decodeRows")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  rows: ReadonlyArray<unknown>,
  operation: string,
): Effect.fn.Return<ReadonlyArray<A>, ActivityStoreError> {
  return yield* Schema.decodeUnknownEffect(Schema.Array(schema))(rows).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
  );
});

const makeActivityStore = Effect.fn("SqliteActivityStore.make")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const failpoint = yield* ActivityMutationFailpoint;
  const withWriteTransaction = sql.withTransaction;

  yield* failpoint.hit("activity:initialize:before");
  yield* withWriteTransaction(
    Effect.gen(function* () {
      yield* sql`
          CREATE TABLE IF NOT EXISTS effect_agent_activity_metadata (
            component TEXT PRIMARY KEY NOT NULL,
            version INTEGER NOT NULL
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
        const tableRows = yield* sql<Record<string, unknown>>`
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = ${STATE_TABLE}
          `;

        const existing = yield* decodeRows(ActivityTableRow, tableRows, "inspect activity schema");

        if (existing.length > 0) {
          return yield* storeError("initialize activity schema", "incompatible");
        }
        yield* sql`
            CREATE TABLE effect_agent_activity_processor_state_v1 (
              processor_id TEXT NOT NULL,
              processor_version TEXT NOT NULL,
              thread_id TEXT NOT NULL,
              format_version INTEGER NOT NULL,
              through_sequence INTEGER NOT NULL,
              epoch INTEGER NOT NULL,
              owner TEXT,
              lease_expires_at REAL NOT NULL,
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

  return yield* makeSqlActivityStore(withWriteTransaction);
});

/** SQLite activity progress with mutation failpoints kept injectable for recovery tests. */
export const activityProcessorStoreLayerWithFailpoints: Layer.Layer<
  ActivityProcessorStore,
  SqliteActivityInitializationError,
  SqlClientService.SqlClient | ActivityMutationFailpoint
> = Layer.effect(ActivityProcessorStore, makeActivityStore());

/** SQLite activity progress with the production no-op mutation failpoint. */
export const activityProcessorStoreLayer: Layer.Layer<
  ActivityProcessorStore,
  SqliteActivityInitializationError,
  SqlClientService.SqlClient
> = activityProcessorStoreLayerWithFailpoints.pipe(Layer.provide(ActivityMutationFailpoint.layer));
