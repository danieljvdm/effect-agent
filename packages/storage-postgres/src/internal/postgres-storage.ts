import { makeSqlJournal } from "@effect-agent/storage-sql/sql-journal";
import {
  makeRowDecoder,
  makeSqlTransaction,
  type StorageErrorFields,
  type CorruptionErrorFields,
} from "@effect-agent/storage-sql/sql-storage";
import { createStorageSchema } from "@effect-agent/storage-sql/sql-storage-schema";
import { Effect, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { isSqlError, type SqlError } from "effect/unstable/sql/SqlError";

import {
  PostgresStorageCompatibilityError,
  PostgresStorageCorruptionError,
  PostgresStorageError,
  PostgresWriteContention,
  type PostgresStorageFailpointLocation,
  type PostgresStorageFailpointError,
} from "../PostgresStorageError.ts";

/**
 * This exact FNV-1a hash, including its tag and UTF-8 encoding, is a persistent advisory-lock
 * wire format and must never change: a different key would let an old and a new deployment write
 * concurrently. The shape follows `SqlRunnerStorage`'s lock namespace in Effect's cluster module.
 */
const advisoryLockKey = (tag: string): number => {
  const bytes = new TextEncoder().encode(`effect-agent:${tag}`);
  let hash = 0x811c9dc5;

  for (const byte of bytes) hash = Math.imul(hash ^ byte, 0x01000193);

  return hash | 0;
};

/** One key serialises every writer, which is the scope SQLite's write lock had. */
export const WRITER_LOCK_KEY = advisoryLockKey("storage/writer");

const storageError = (operation: string) => (cause: SqlError) =>
  PostgresStorageError.make({ operation, cause, message: cause.message });

export const classifyWriteFailure =
  (operation: string) =>
  (cause: SqlError): PostgresStorageError | PostgresWriteContention =>
    cause.reason._tag === "SerializationError" ||
    cause.reason._tag === "DeadlockError" ||
    cause.reason._tag === "LockTimeoutError"
      ? PostgresWriteContention.make({
          operation,
          cause,
          message: `Another producer won the Postgres write race; ${operation} is safe to retry.`,
        })
      : storageError(operation)(cause);

/** The lock wait is interruptible; the shared transaction rolls back before releasing its connection. */
export const withWriterLockTransaction = (sql: SqlClient.SqlClient, lockTimeout: number) =>
  makeSqlTransaction(sql, {
    begin: "BEGIN",
    prelude: Effect.gen(function* () {
      yield* sql`SELECT set_config('lock_timeout', ${`${lockTimeout}ms`}, true)`;
      yield* sql`SELECT pg_advisory_xact_lock(${WRITER_LOCK_KEY})`;
    }),
  });

/** Every page of a multi-query export observes the same snapshot without taking the writer lock. */
const withReadTransaction = (sql: SqlClient.SqlClient) =>
  makeSqlTransaction(sql, { begin: "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" });

/** Run while holding the writer transaction, including when this schema does not yet exist. */
export const ensurePostgresSchema = Effect.fnUntraced(function* (schema: string) {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE SCHEMA IF NOT EXISTS ${sql(schema)}`.withoutTransform;
  const rows = yield* sql`SELECT current_schema() AS name`;

  const decoded = yield* Schema.decodeUnknownEffect(
    Schema.Array(Schema.Struct({ name: Schema.NullOr(Schema.String) })),
  )(rows).pipe(
    Effect.mapError((cause) =>
      PostgresStorageError.make({
        operation: "verify storage schema",
        cause,
        message: cause.message,
      }),
    ),
  );

  if (decoded.length !== 1 || decoded[0]?.name !== schema) {
    return yield* PostgresStorageError.make({
      operation: "verify storage schema",
      message: `The client must select schema ${schema} for every pooled connection. Use PostgresStorageClient.layer(client, schema).`,
    });
  }
});

export const CurrentPostgresStorageVersion = 1;

/** Initialize empty storage with the complete current schema. */
const createPostgresStorageSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // The boolean primary key is what keeps the version marker single-row: no second value can
  // satisfy the constraint. SQLite records its format in `PRAGMA user_version` instead.
  yield* sql`
    CREATE TABLE effect_agent_storage_version (
      id BOOLEAN PRIMARY KEY NOT NULL,
      version BIGINT NOT NULL,
      CONSTRAINT effect_agent_storage_version_single_row CHECK (id)
    )
  `;
  yield* createStorageSchema;
  yield* sql`
    INSERT INTO effect_agent_storage_version (id, version)
    VALUES (TRUE, ${CurrentPostgresStorageVersion})
  `;
});

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
export const initializePostgresStorage = Effect.fn("PostgresStorage.initialize")(function* ({
  lockTimeout,
  schema,
}: {
  readonly lockTimeout: number;
  readonly schema: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const write = withWriterLockTransaction(sql, lockTimeout);

  yield* write(
    Effect.gen(function* () {
      yield* ensurePostgresSchema(schema);

      const existingRows = yield* sql<Record<string, unknown>>`
        SELECT c.relname AS name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ${schema}
          AND c.relkind IN ('r', 'p')
          AND starts_with(c.relname, 'effect_agent_')
          AND c.relname NOT IN ('effect_agent_activity_metadata', 'effect_agent_activity_processor_state_v1')
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
});

/** Bind shared journal operations without repeating format initialization. */
export const makePostgresJournal = Effect.fnUntraced(function* (
  lockTimeout: number,
  hitFailpoint: (
    location: PostgresStorageFailpointLocation,
  ) => Effect.Effect<void, PostgresStorageFailpointError>,
) {
  const sql = yield* SqlClient.SqlClient;
  const write = withWriterLockTransaction(sql, lockTimeout);
  const read = withReadTransaction(sql);

  return yield* makeSqlJournal({
    errors: postgresStorageErrors,
    hitFailpoint,
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
