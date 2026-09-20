import { makeSqlActivityStore } from "@effect-agent/storage-sql/sql-activity-store";
import { Effect, Layer, Schema } from "effect";
import {
  ActivityMutationFailpoint,
  type ActivityMutationFailure,
  ActivityProcessorStore,
  ActivityStoreError,
} from "effect-agent/activity-store";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { ensurePostgresSchema } from "./internal/postgres-schema.ts";
import { withWriterLockTransaction } from "./internal/postgres-transactions.ts";
import { PostgresStorageConfig } from "./PostgresStorageConfig.ts";

const STORAGE_VERSION = 1;
const METADATA_COMPONENT = "activity";
const STATE_TABLE = "effect_agent_activity_processor_state_v1";
const ActivityMetadataRow = Schema.Struct({ version: Schema.Int });
const ActivityTableRow = Schema.Struct({ name: Schema.NonEmptyString });

export type PostgresActivityInitializationError = ActivityStoreError | ActivityMutationFailure;

const storeError = (operation: string, reason: ActivityStoreError["reason"] = "unavailable") =>
  ActivityStoreError.make({ operation, reason });

const decodeRows = <A, I>(schema: Schema.Codec<A, I>, rows: unknown, operation: string) =>
  Schema.decodeUnknownEffect(Schema.Array(schema))(rows).pipe(
    Effect.mapError(() => storeError(operation, "corrupt")),
  );

const makeActivityStore = Effect.fn("PostgresActivityStore.make")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const { lockTimeout, schema } = yield* PostgresStorageConfig;
  const failpoint = yield* ActivityMutationFailpoint;
  const withWriteTransaction = withWriterLockTransaction(sql, lockTimeout);

  yield* failpoint.hit("activity:initialize:before");
  yield* withWriteTransaction(
    Effect.gen(function* () {
      yield* ensurePostgresSchema(sql, schema).pipe(
        Effect.mapError(() => storeError("initialize activity schema")),
      );
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
        const tableRows = yield* sql<Record<string, unknown>>`
          SELECT c.relname AS name
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relkind IN ('r', 'p')
            AND c.relname = ${STATE_TABLE}
            AND n.nspname = ANY (current_schemas(FALSE))
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

  return yield* makeSqlActivityStore(withWriteTransaction);
});

/** Postgres activity progress with mutation failpoints kept injectable for recovery tests. */
export const layerWithFailpoints: Layer.Layer<
  ActivityProcessorStore,
  PostgresActivityInitializationError,
  PostgresStorageConfig | SqlClientService.SqlClient | ActivityMutationFailpoint
> = Layer.effect(ActivityProcessorStore, makeActivityStore());

/** Postgres activity progress with the production no-op mutation failpoint. */
export const layer: Layer.Layer<
  ActivityProcessorStore,
  PostgresActivityInitializationError,
  PostgresStorageConfig | SqlClientService.SqlClient
> = layerWithFailpoints.pipe(Layer.provide(ActivityMutationFailpoint.layer));
