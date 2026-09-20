import { makeSqlJournal } from "@effect-agent/storage-sql/sql-journal";
import {
  makeRowDecoder,
  type StorageErrorFields,
  type CorruptionErrorFields,
} from "@effect-agent/storage-sql/sql-storage";
import { Effect, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { isSqlError } from "effect/unstable/sql/SqlError";

import { PostgresStorageConfig } from "../PostgresStorageConfig.ts";
import {
  PostgresStorageCompatibilityError,
  PostgresStorageCorruptionError,
  PostgresStorageError,
  PostgresWriteContention,
} from "../PostgresStorageError.ts";
import { PostgresStorageFailpoint } from "../PostgresStorageFailpoint.ts";
import { CurrentPostgresStorageVersion, createPostgresStorageSchema } from "./migrations.ts";
import { ensurePostgresSchema } from "./postgres-schema.ts";
import {
  classifyWriteFailure,
  storageError,
  withReadTransaction,
  withWriterLockTransaction,
} from "./postgres-transactions.ts";

const VERSION_TABLE = "effect_agent_storage_version";

const REQUIRED_OBJECTS = [
  "effect_agent_abort_intents",
  "effect_agent_approval_decisions",
  "effect_agent_attempts",
  "effect_agent_canonical_batches",
  "effect_agent_canonical_records",
  "effect_agent_checkpoints",
  "effect_agent_message_deliveries",
  "effect_agent_message_deliveries_pending",
  "effect_agent_recovery_checkpoints",
  "effect_agent_records_call",
  "effect_agent_records_outstanding",
  "effect_agent_records_run_input",
  "effect_agent_records_subtree",
  "effect_agent_records_worker_input",
  "effect_agent_schedules",
  "effect_agent_settlement_reservations",
  "effect_agent_submission_ownership",
  "effect_agent_submissions",
  "effect_agent_submissions_nonterminal",
  "effect_agent_threads",
  "effect_agent_unknown_resolutions",
  "effect_agent_child_reservations",
  "effect_agent_subscription_sequences",
  "effect_agent_subscriptions",
  "effect_agent_subscription_events",
  "effect_agent_subscription_deliveries",
  "effect_agent_worker_stops",
  "effect_agent_worker_execution",
  "effect_agent_worker_starts",
  "effect_agent_worker_pending",
] as const;

const PostgresVersionRow = Schema.Struct({
  version: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});

const PostgresNameRow = Schema.Struct({ name: Schema.NonEmptyString });

export const postgresStorageErrors = {
  storage: (fields: StorageErrorFields) => PostgresStorageError.make(fields),
  corruption: (fields: CorruptionErrorFields) => PostgresStorageCorruptionError.make(fields),
  isCorruption: Schema.is(PostgresStorageCorruptionError),
};

const { decodeRows, decodeSingleRow } = makeRowDecoder(postgresStorageErrors.corruption);

const isTransactionFailure = Schema.is(
  Schema.Union([PostgresStorageError, PostgresWriteContention]),
);

/** PostgreSQL owns schema/version checks; relational state transitions belong to storage-sql. */
export const initializePostgresJournal = Effect.fn("PostgresJournal.initialize")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const failpoint = yield* PostgresStorageFailpoint;
  const { lockTimeout, schema } = yield* PostgresStorageConfig;
  const write = withWriterLockTransaction(sql, lockTimeout);
  const read = withReadTransaction(sql);

  yield* write(
    Effect.gen(function* () {
      yield* ensurePostgresSchema(sql, schema);

      const existingRows = yield* sql<Record<string, unknown>>`
        SELECT c.relname AS name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ${schema}
          AND c.relkind IN ('r', 'p')
          AND starts_with(c.relname, 'effect_agent_')
        ORDER BY c.relname
      `.pipe(Effect.mapError(storageError("inspect storage schema")));

      const existing = yield* decodeRows(
        Schema.Array(PostgresNameRow),
        "pg_class",
        "effect_agent_%",
        existingRows,
      );

      if (existing.every((relation) => relation.name !== VERSION_TABLE)) {
        if (existing.length > 0) {
          return yield* PostgresStorageCompatibilityError.make({
            actualVersion: 0,
            supportedVersion: CurrentPostgresStorageVersion,
            message:
              `Schema ${schema} contains unversioned Effect Agent tables. Refusing to mutate ` +
              "ambiguous stored data; retain it for inspection with its original writer.",
          });
        }

        yield* createPostgresStorageSchema.pipe(
          Effect.mapError(storageError("initialize current storage")),
        );
      } else {
        const versionRows = yield* sql<Record<string, unknown>>`
          SELECT version
          FROM effect_agent_storage_version
          WHERE id
        `.pipe(Effect.mapError(storageError("read storage version")));

        const version = yield* decodeSingleRow(
          Schema.Array(PostgresVersionRow),
          VERSION_TABLE,
          "singleton",
          versionRows,
        );

        // One adapter, one format: there is no predecessor layout to upgrade from.
        if (version.version !== CurrentPostgresStorageVersion) {
          return yield* PostgresStorageCompatibilityError.make({
            actualVersion: version.version,
            supportedVersion: CurrentPostgresStorageVersion,
            message:
              `Schema ${schema} uses storage version ${version.version}; this build supports ` +
              `exactly version ${CurrentPostgresStorageVersion}. Keep the original database and ` +
              "use a compatible library version.",
          });
        }
      }

      const requiredRows = yield* sql<Record<string, unknown>>`
    SELECT c.relname AS name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ${schema}
      AND c.relkind IN ('r', 'p', 'i')
      AND c.relname IN ${sql.in(REQUIRED_OBJECTS)}
    ORDER BY c.relname
  `.pipe(Effect.mapError(storageError("verify storage tables")));

      const required = yield* decodeRows(
        Schema.Array(PostgresNameRow),
        "pg_class",
        "required_tables",
        requiredRows,
      );

      if (required.length !== REQUIRED_OBJECTS.length) {
        return yield* PostgresStorageCompatibilityError.make({
          actualVersion: CurrentPostgresStorageVersion,
          supportedVersion: CurrentPostgresStorageVersion,
          message:
            `Schema ${schema} claims the current format but is missing required tables or ` +
            "indexes. Retain the original database for inspection.",
        });
      }
    }),
  ).pipe(
    Effect.catchTag("SqlError", (error) =>
      Effect.fail(classifyWriteFailure("initialize storage")(error)),
    ),
  );

  return yield* makeSqlJournal({
    errors: postgresStorageErrors,
    hitFailpoint: failpoint.hit,
    transactions: {
      withWriteTransaction:
        (operation) =>
        <A, E, R>(body: Effect.Effect<A, E, R>) =>
          write(body).pipe(
            Effect.mapError((error) =>
              isSqlError(error) ? classifyWriteFailure(operation)(error) : error,
            ),
          ),
      withReadTransaction:
        (operation) =>
        <A, E, R>(body: Effect.Effect<A, E, R>) =>
          read(body).pipe(
            Effect.mapError((error) =>
              isSqlError(error) ? storageError(operation)(error) : error,
            ),
          ),
      isTransactionFailure,
    },
  });
});
