import { Effect, Exit, Schema } from "effect";
import { EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import { CanonicalRecord, CanonicalSequence, ProducerEpoch } from "effect-agent/records";
import { indexCanonicalRecord } from "effect-agent/sql-thread-native-reads";
import {
  MAX_THREAD_EXPORT_RECORDS,
  CheckpointRejected,
  FenceRejected,
  ThreadNotMaterialized,
  type SaveRecoveryCheckpointRequest,
} from "effect-agent/thread-store";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { PostgresStorageConfig } from "../PostgresStorageConfig.ts";
import type { PostgresStorageFailpointError } from "../PostgresStorageError.ts";
import {
  PostgresAppendConflict,
  PostgresCheckpointConflict,
  PostgresFenceRejected,
  PostgresStorageCompatibilityError,
  PostgresStorageCorruptionError,
  PostgresStorageError,
  PostgresWriteContention,
} from "../PostgresStorageError.ts";
import { PostgresStorageFailpoint } from "../PostgresStorageFailpoint.ts";
import { CurrentPostgresStorageVersion, createPostgresStorageSchema } from "./migrations.ts";
import { WRITER_LOCK_KEY } from "./postgres-transactions.ts";

const BoundedStoredText = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const MAX_RECORDS_PER_THREAD = MAX_THREAD_EXPORT_RECORDS;
const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
const MAX_STORED_TEXT_BYTES = 16 * 1024 * 1024;
const MAX_IDENTIFIER_LENGTH = 1_024;
/**
 * Enough concurrent statements to grow a cold pool past its first connection, which is what
 * makes a `search_path` that only one connection carries observable at startup.
 */
const SEARCH_PATH_PROBES = 16;

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
] as const;

const storedTextBytes = (value: string): number => new TextEncoder().encode(value).byteLength;

class PostgresVersionRow extends Schema.Class<PostgresVersionRow>("PostgresVersionRow")({
  version: NonNegativeInt,
}) {}

class PostgresNameRow extends Schema.Class<PostgresNameRow>("PostgresNameRow")({
  name: BoundedIdentifier,
}) {}

class ThreadRow extends Schema.Class<ThreadRow>("ThreadRow")({
  thread_id: BoundedIdentifier,
  created_at: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  producer_epoch: ProducerEpoch,
  tail_digest: BoundedStoredText,
  tail_sequence: CanonicalSequence,
}) {}

class BatchRow extends Schema.Class<BatchRow>("BatchRow")({
  batch_digest: BoundedStoredText,
  batch_id: BoundedIdentifier,
  batch_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  first_sequence: CanonicalSequence,
  last_sequence: CanonicalSequence,
  tail_digest: BoundedStoredText,
}) {}

class RecordRow extends Schema.Class<RecordRow>("RecordRow")({
  batch_id: BoundedIdentifier,
  thread_id: BoundedIdentifier,
  record_id: BoundedIdentifier,
  record_json: BoundedStoredText,
  sequence: CanonicalSequence,
}) {}

class CheckpointRow extends Schema.Class<CheckpointRow>("CheckpointRow")({
  checkpoint_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  tail_digest: BoundedStoredText,
  through_sequence: CanonicalSequence,
}) {}

export class RawRecord extends Schema.Class<RawRecord>("@effect-agent/storage-postgres/RawRecord")({
  recordId: BoundedIdentifier,
  recordJson: BoundedStoredText,
}) {}

export class RawAppendRequest extends Schema.Class<RawAppendRequest>(
  "@effect-agent/storage-postgres/RawAppendRequest",
)({
  batchDigest: BoundedStoredText,
  batchId: BoundedIdentifier,
  batchJson: BoundedStoredText,
  threadId: BoundedIdentifier,
  expectedTailDigest: BoundedStoredText,
  expectedTailSequence: CanonicalSequence,
  producerEpoch: ProducerEpoch,
  records: Schema.NonEmptyArray(RawRecord).check(Schema.isMaxLength(256)),
  tailDigest: BoundedStoredText,
}) {}

export class RawAppendResult extends Schema.Class<RawAppendResult>(
  "@effect-agent/storage-postgres/RawAppendResult",
)({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  replayed: Schema.Boolean,
  tailDigest: BoundedStoredText,
}) {}

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-postgres/RawReadRequest",
)({
  threadId: BoundedIdentifier,
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class RawCheckpoint extends Schema.Class<RawCheckpoint>(
  "@effect-agent/storage-postgres/RawCheckpoint",
)({
  checkpointJson: BoundedStoredText,
  threadId: BoundedIdentifier,
  tailDigest: BoundedStoredText,
  throughSequence: CanonicalSequence,
}) {}

export class RawThreadExport extends Schema.Class<RawThreadExport>(
  "@effect-agent/storage-postgres/RawThreadExport",
)({
  thread: ThreadRow,
  records: Schema.Array(RecordRow),
}) {}

type AppendError =
  | PostgresAppendConflict
  | PostgresFenceRejected
  | PostgresStorageCorruptionError
  | PostgresStorageError
  | PostgresStorageFailpointError
  | PostgresWriteContention;

type CheckpointError =
  | PostgresCheckpointConflict
  | PostgresStorageCorruptionError
  | PostgresStorageError
  | PostgresWriteContention;

const storageError =
  (operation: string) =>
  (error: SqlError): PostgresStorageError =>
    PostgresStorageError.make({
      cause: error,
      operation,
      message: error.message,
    });

/** Decode raw Postgres rows against a Schema, reporting failures as typed corruption. */
export const decodeRows = Effect.fn("PostgresJournal.decodeRows")(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<ReadonlyArray<A>, PostgresStorageCorruptionError> =>
    Schema.decodeUnknownEffect(schema)(rows).pipe(
      Effect.mapError((error) =>
        PostgresStorageCorruptionError.make({
          table,
          rowKey,
          message: String(error),
        }),
      ),
    ),
);

/** Decode exactly one raw Postgres row against a Schema, reporting failures as typed corruption. */
export const decodeSingleRow = Effect.fn("PostgresJournal.decodeSingleRow")(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<A, PostgresStorageCorruptionError> =>
    decodeRows(schema, table, rowKey, rows).pipe(
      Effect.flatMap((decoded) =>
        decoded.length === 1
          ? Effect.succeed(decoded[0])
          : Effect.fail(
              PostgresStorageCorruptionError.make({
                table,
                rowKey,
                message: `Expected exactly one row but found ${decoded.length}.`,
              }),
            ),
      ),
    ),
);

/**
 * The adapter cannot select its own schema: `SET search_path` binds to one connection while the
 * client is a pool, and `@effect/sql-pg` exposes no startup parameter or per-connection hook (its
 * config carries no `options` field, and a URL `options=-c search_path=...` is not forwarded).
 *
 * So the configured schema must already be the connection's. Probing several pooled connections
 * turns a mismatch into a startup failure naming the fix, rather than statements silently
 * resolving in `public`.
 */
const verifySearchPath = Effect.fn("PostgresJournal.verifySearchPath")(function* (schema: string) {
  const sql = yield* SqlClient.SqlClient;

  const probes = yield* Effect.all(
    Array.from(
      { length: SEARCH_PATH_PROBES },
      () => sql<Record<string, unknown>>`SELECT current_schema() AS name`,
    ),
    { concurrency: SEARCH_PATH_PROBES },
  ).pipe(Effect.mapError(storageError("verify storage schema")));

  for (const rows of probes) {
    const resolved = yield* decodeSingleRow(
      Schema.Array(PostgresNameRow),
      "current_schema",
      "singleton",
      rows,
    );

    if (resolved.name !== schema) {
      return yield* PostgresStorageError.make({
        operation: "verify storage schema",
        message:
          `A pooled connection resolved schema ${resolved.name} instead of ${schema}. ` +
          "`search_path` is per connection and this driver cannot set it for the pool: make " +
          `${schema} the connection default (ALTER ROLE ... SET search_path TO ${schema}), or ` +
          "leave the schema option at the connection's own default.",
      });
    }
  }
});

export const initializePostgresJournal = Effect.fn("PostgresJournal.initialize")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const { hit: failpoint } = yield* PostgresStorageFailpoint;
  const { lockTimeout, schema } = yield* PostgresStorageConfig;

  // `CREATE SCHEMA` and `SET` accept no bound parameters, so the schema name is interpolated.
  // `PostgresStorageConfigValue` validates it against ^[a-z_][a-z0-9_]*$, which admits no quote,
  // separator or statement syntax; the value can only ever name one schema.
  yield* sql
    .unsafe(`CREATE SCHEMA IF NOT EXISTS ${schema}`)
    .pipe(Effect.mapError(storageError("create storage schema")));
  yield* verifySearchPath(schema);
  // Statements outside a journal transaction run on whichever connection the pool hands out, so
  // this session bound is best effort; each write transaction re-applies it with `SET LOCAL`.
  yield* sql
    .unsafe(`SET lock_timeout = ${lockTimeout}`)
    .pipe(Effect.mapError(storageError("configure lock timeout")));

  /**
   * `@effect/sql-pg` maps the SQLSTATE onto a structured reason, so the codes this adapter treats
   * as retryable are read from the reason tag rather than the message: 40001 serialization_failure,
   * 40P01 deadlock_detected, and 55P03 lock_not_available — which is also what the configured
   * `lock_timeout` raises. Each of those rolls the transaction back whole, so no canonical state
   * was mutated.
   */
  const classifyWriteFailure =
    (operation: string) =>
    (error: SqlError): PostgresStorageError | PostgresWriteContention =>
      error.reason._tag === "SerializationError" ||
      error.reason._tag === "DeadlockError" ||
      error.reason._tag === "LockTimeoutError"
        ? PostgresWriteContention.make({
            cause: error,
            operation,
            message: `Another producer won the Postgres write race; ${operation} is safe to retry.`,
          })
        : storageError(operation)(error);

  // Two ports over one pooled client can initialize concurrently. Holding the writer lock makes
  // the second initializer observe the first one's committed schema and take the
  // current-version branch instead of reaching `CREATE TABLE`.
  yield* sql
    .withTransaction(
      Effect.gen(function* () {
        yield* sql.unsafe(`SET LOCAL lock_timeout = '${lockTimeout}ms'`);
        yield* sql`SELECT pg_advisory_xact_lock(${WRITER_LOCK_KEY})`;

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
      }),
    )
    .pipe(
      Effect.catchTag("SqlError", (error) =>
        Effect.fail(classifyWriteFailure("initialize storage")(error)),
      ),
    );

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

  /**
   * Runs one journal write transaction holding the adapter's writer lock, which is what makes
   * the read-then-write invariants — tail comparison, batch idempotency, record identity, ledger
   * admission — sound without a stricter isolation level. A failed `BEGIN` leaves no transaction
   * to roll back; anything after it does.
   *
   * Journal write transactions are always top level. Nesting one inside another would
   * deadlock against its own reserved connection, so new journal operations must not wrap
   * this helper inside another transaction.
   */
  const withWriteTransaction =
    (operation: string) =>
    <A, E>(
      effect: Effect.Effect<A, E>,
    ): Effect.Effect<A, E | PostgresStorageError | PostgresWriteContention> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.scoped(
          Effect.gen(function* () {
            const connection = yield* sql.reserve.pipe(
              Effect.mapError(classifyWriteFailure(operation)),
            );

            yield* connection
              .executeUnprepared("BEGIN", [], undefined)
              .pipe(Effect.mapError(classifyWriteFailure(operation)));

            // The timeout must be in place before the lock wait it bounds. SERIALIZABLE would
            // also be safe, but it converts a lost race into a 40001 abort at COMMIT, so
            // operations that resolve as a typed conflict or an idempotent replay would instead
            // fail after doing their work.
            const prelude = connection
              .executeUnprepared(`SET LOCAL lock_timeout = '${lockTimeout}ms'`, [], undefined)
              .pipe(
                Effect.andThen(
                  connection.executeUnprepared(
                    `SELECT pg_advisory_xact_lock(${WRITER_LOCK_KEY})`,
                    [],
                    undefined,
                  ),
                ),
                Effect.mapError(classifyWriteFailure(operation)),
              );

            // A lock wait that times out has already aborted the open transaction; rolling it
            // back here is what keeps the reserved connection reusable by the pool.
            const exit = yield* prelude.pipe(
              Effect.andThen(
                restore(
                  Effect.provideService(effect, sql.transactionService, [connection, 0] as const),
                ),
              ),
              Effect.exit,
            );

            if (Exit.isSuccess(exit)) {
              yield* connection
                .executeUnprepared("COMMIT", [], undefined)
                .pipe(Effect.mapError(classifyWriteFailure(operation)));

              return exit.value;
            }
            yield* Effect.orDie(connection.executeUnprepared("ROLLBACK", [], undefined));

            return yield* exit;
          }),
        ).pipe(
          Effect.withSpan("PostgresJournal.withWriteTransaction", { attributes: { operation } }),
        ),
      );

  /**
   * Runs a read-only snapshot under `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`. Every
   * statement in the transaction then sees the one snapshot taken at its first read, which is
   * the adapter's snapshot-with-concurrent-writer contract: a paged export stays internally
   * consistent while an append commits alongside it. `READ ONLY` also keeps the reader out of
   * serializable conflict detection, so a scan can never abort a writer. As with the write
   * helper, a failed `BEGIN` is reported directly because there is no transaction to roll back.
   */
  const withReadTransaction =
    (operation: string) =>
    <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E | PostgresStorageError> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.scoped(
          Effect.gen(function* () {
            const connection = yield* sql.reserve.pipe(Effect.mapError(storageError(operation)));

            yield* connection
              .executeUnprepared("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", [], undefined)
              .pipe(Effect.mapError(storageError(operation)));

            const exit = yield* restore(
              Effect.provideService(effect, sql.transactionService, [connection, 0] as const),
            ).pipe(Effect.exit);

            if (Exit.isSuccess(exit)) {
              yield* connection
                .executeUnprepared("COMMIT", [], undefined)
                .pipe(Effect.mapError(storageError(operation)));

              return exit.value;
            }
            yield* Effect.orDie(connection.executeUnprepared("ROLLBACK", [], undefined));

            return yield* exit;
          }),
        ).pipe(
          Effect.withSpan("PostgresJournal.withReadTransaction", { attributes: { operation } }),
        ),
      );

  const materialize = Effect.fn("PostgresJournal.materialize")(function* (
    threadId: string,
    createdAt: string,
    emptyTailDigest: string,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<
    void,
    | PostgresFenceRejected
    | PostgresStorageCorruptionError
    | PostgresStorageError
    | PostgresWriteContention
  > {
    if (
      threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(emptyTailDigest) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* PostgresStorageError.make({
        operation: "materialize thread",
        message: "Thread identity or initial digest exceeds the Postgres storage bounds.",
      });
    }
    yield* withWriteTransaction("materialize transaction")(
      Effect.gen(function* () {
        const existingRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            tail_digest,
            producer_epoch
          FROM effect_agent_threads
          WHERE thread_id = ${threadId}
        `.pipe(Effect.mapError(storageError("read materialized thread")));

        const existingThreads = yield* decodeRows(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          threadId,
          existingRows,
        );

        if (existingThreads.length > 1) {
          return yield* PostgresStorageCorruptionError.make({
            table: "effect_agent_threads",
            rowKey: threadId,
            message: "A thread primary key returned more than one row.",
          });
        }
        if (existingThreads.length === 0) {
          yield* sql`
            INSERT INTO effect_agent_threads (
              thread_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            ) VALUES (
              ${threadId},
              ${createdAt},
              0,
              ${emptyTailDigest},
              ${producerEpoch}
            )
          `.pipe(Effect.mapError(storageError("materialize thread")));

          return;
        }
        if (producerEpoch < existingThreads[0].producer_epoch) {
          return yield* PostgresFenceRejected.make({
            producerEpoch,
            actualEpoch: existingThreads[0].producer_epoch,
            message: `Producer epoch ${producerEpoch} is stale; current epoch is ${existingThreads[0].producer_epoch}.`,
          });
        }
        if (producerEpoch > existingThreads[0].producer_epoch) {
          yield* sql`
            UPDATE effect_agent_threads
            SET producer_epoch = ${producerEpoch}
            WHERE thread_id = ${threadId}
          `.pipe(Effect.mapError(storageError("advance materialization epoch")));
        }
      }),
    );
  });

  const getThread = Effect.fn("PostgresJournal.getThread")(function* (threadId: string) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        created_at,
        tail_sequence,
        tail_digest,
        producer_epoch
      FROM effect_agent_threads
      WHERE thread_id = ${threadId}
    `.pipe(Effect.mapError(storageError("read thread")));

    return yield* decodeRows(Schema.Array(ThreadRow), "effect_agent_threads", threadId, rows);
  });

  const append = Effect.fn("PostgresJournal.append")(function* (
    request: RawAppendRequest,
  ): Effect.fn.Return<RawAppendResult, AppendError> {
    if (
      request.threadId.length > MAX_IDENTIFIER_LENGTH ||
      request.batchId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(request.batchJson) > MAX_STORED_TEXT_BYTES ||
      storedTextBytes(request.batchDigest) > MAX_STORED_TEXT_BYTES ||
      storedTextBytes(request.tailDigest) > MAX_STORED_TEXT_BYTES ||
      request.records.some(
        (record) =>
          record.recordId.length > MAX_IDENTIFIER_LENGTH ||
          storedTextBytes(record.recordJson) > MAX_STORED_TEXT_BYTES,
      )
    ) {
      return yield* PostgresStorageError.make({
        operation: "append canonical batch",
        message: "Canonical identifiers or encoded JSON exceed the Postgres storage bounds.",
      });
    }

    return yield* withWriteTransaction("append transaction")(
      Effect.gen(function* () {
        const recordIds = request.records.map((record) => record.recordId);

        if (new Set(recordIds).size !== recordIds.length) {
          return yield* PostgresAppendConflict.make({
            message: `Batch ${request.batchId} contains duplicate canonical record IDs.`,
            reason: "record-identity",
          });
        }

        const threadRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            tail_digest,
            producer_epoch
          FROM effect_agent_threads
          WHERE thread_id = ${request.threadId}
        `.pipe(Effect.mapError(storageError("read append tail")));

        const thread = yield* decodeSingleRow(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          request.threadId,
          threadRows,
        );

        if (request.producerEpoch !== thread.producer_epoch) {
          return yield* PostgresFenceRejected.make({
            producerEpoch: request.producerEpoch,
            actualEpoch: thread.producer_epoch,
            message: `Producer epoch ${request.producerEpoch} is not the current epoch ${thread.producer_epoch}.`,
          });
        }

        const batchRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            batch_id,
            first_sequence,
            last_sequence,
            batch_digest,
            tail_digest,
            batch_json
          FROM effect_agent_canonical_batches
          WHERE thread_id = ${request.threadId}
            AND batch_id = ${request.batchId}
        `.pipe(Effect.mapError(storageError("read idempotent batch")));

        const batches = yield* decodeRows(
          Schema.Array(BatchRow),
          "effect_agent_canonical_batches",
          `${request.threadId}/${request.batchId}`,
          batchRows,
        );

        if (batches.length > 1) {
          return yield* PostgresStorageCorruptionError.make({
            table: "effect_agent_canonical_batches",
            rowKey: `${request.threadId}/${request.batchId}`,
            message: "A canonical batch primary key returned more than one row.",
          });
        }
        if (batches.length === 1) {
          const existingBatch = batches[0];

          if (existingBatch.batch_digest !== request.batchDigest) {
            return yield* PostgresAppendConflict.make({
              message: `Batch ${request.batchId} already exists with different canonical content.`,
              reason: "batch-digest",
            });
          }

          return RawAppendResult.make({
            firstSequence: existingBatch.first_sequence,
            lastSequence: existingBatch.last_sequence,
            replayed: true,
            tailDigest: existingBatch.tail_digest,
          });
        }

        if (
          request.expectedTailSequence !== thread.tail_sequence ||
          request.expectedTailDigest !== thread.tail_digest
        ) {
          return yield* PostgresAppendConflict.make({
            message:
              `Expected tail ${request.expectedTailSequence}/${request.expectedTailDigest} ` +
              `but found ${thread.tail_sequence}/${thread.tail_digest}.`,
            reason: "tail",
            actualTailSequence: thread.tail_sequence,
            actualTailDigest: thread.tail_digest,
          });
        }
        if (thread.tail_sequence + request.records.length > MAX_RECORDS_PER_THREAD) {
          return yield* PostgresStorageError.make({
            operation: "append canonical batch",
            message: `Thread record limit ${MAX_RECORDS_PER_THREAD} would be exceeded.`,
          });
        }

        const existingRecordRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            sequence,
            record_id,
            batch_id,
            record_json
          FROM effect_agent_canonical_records
          WHERE thread_id = ${request.threadId}
            AND record_id IN ${sql.in(recordIds)}
          ORDER BY sequence
        `.pipe(Effect.mapError(storageError("check canonical record identities")));

        const existingRecords = yield* decodeRows(
          Schema.Array(RecordRow),
          "effect_agent_canonical_records",
          `${request.threadId}/record_ids`,
          existingRecordRows,
        );

        if (existingRecords.length > 0) {
          return yield* PostgresAppendConflict.make({
            message: `Canonical record ID ${existingRecords[0].record_id} already exists.`,
            reason: "record-identity",
          });
        }

        const firstSequence = yield* Schema.decodeEffect(CanonicalSequence)(
          thread.tail_sequence + 1,
        ).pipe(
          Effect.mapError((error) =>
            PostgresStorageError.make({
              cause: error,
              operation: "append canonical batch",
              message: error.message,
            }),
          ),
        );

        const lastSequence = yield* Schema.decodeEffect(CanonicalSequence)(
          firstSequence + request.records.length - 1,
        ).pipe(
          Effect.mapError((error) =>
            PostgresStorageError.make({
              cause: error,
              operation: "append canonical batch",
              message: error.message,
            }),
          ),
        );

        yield* sql`
          INSERT INTO effect_agent_canonical_batches (
            thread_id,
            batch_id,
            first_sequence,
            last_sequence,
            batch_digest,
            tail_digest,
            batch_json
          ) VALUES (
            ${request.threadId},
            ${request.batchId},
            ${firstSequence},
            ${lastSequence},
            ${request.batchDigest},
            ${request.tailDigest},
            ${request.batchJson}
          )
        `.pipe(Effect.mapError(storageError("insert canonical batch")));
        yield* failpoint("append:after-batch-insert");

        yield* Effect.forEach(
          request.records,
          (record, index) =>
            Effect.gen(function* () {
              yield* sql`
                  INSERT INTO effect_agent_canonical_records (
                    thread_id,
                    sequence,
                    record_id,
                    batch_id,
                    record_json
                  ) VALUES (
                    ${request.threadId},
                    ${firstSequence + index},
                    ${record.recordId},
                    ${request.batchId},
                    ${record.recordJson}
                  )
                `.pipe(Effect.mapError(storageError("insert canonical record")));

              const canonical = yield* Schema.decodeEffect(Schema.fromJsonString(CanonicalRecord))(
                record.recordJson,
              ).pipe(
                Effect.mapError((error) =>
                  PostgresStorageCorruptionError.make({
                    table: "effect_agent_canonical_records",
                    rowKey: record.recordId,
                    message: error.message,
                  }),
                ),
              );

              yield* indexCanonicalRecord(request.threadId, canonical).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.mapError(storageError("index canonical record")),
              );
              yield* failpoint("append:after-record-insert");
            }),
          { discard: true },
        );

        yield* sql`
          UPDATE effect_agent_threads
          SET
            tail_sequence = ${lastSequence},
            tail_digest = ${request.tailDigest},
            producer_epoch = ${request.producerEpoch}
          WHERE thread_id = ${request.threadId}
        `.pipe(Effect.mapError(storageError("advance thread tail")));
        yield* failpoint("append:after-tail-update");

        return RawAppendResult.make({
          firstSequence,
          lastSequence,
          replayed: false,
          tailDigest: request.tailDigest,
        });
      }),
    );
  });

  const read = Effect.fn("PostgresJournal.read")(function* (request: RawReadRequest) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        sequence,
        record_id,
        batch_id,
        record_json
      FROM effect_agent_canonical_records
      WHERE thread_id = ${request.threadId}
        AND sequence > ${request.fromSequenceExclusive}
      ORDER BY sequence
      LIMIT ${request.limit}
    `.pipe(Effect.mapError(storageError("read canonical records")));

    return yield* decodeRows(
      Schema.Array(RecordRow),
      "effect_agent_canonical_records",
      `${request.threadId}>${request.fromSequenceExclusive}`,
      rows,
    );
  });

  const exportThread = Effect.fn("PostgresJournal.exportThread")(function* (threadId: string) {
    return yield* withReadTransaction("export transaction")(
      Effect.gen(function* () {
        const threadRows = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            FROM effect_agent_threads
            WHERE thread_id = ${threadId}
          `.pipe(Effect.mapError(storageError("export thread")));

        const thread = yield* decodeSingleRow(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          threadId,
          threadRows,
        );

        yield* failpoint("export:after-thread-read");

        if (thread.tail_sequence > MAX_RECORDS_PER_THREAD)
          return yield* PostgresStorageError.make({
            operation: "export thread",
            message: "The thread exceeds the current export record limit.",
          });
        const records: Array<RecordRow> = [];
        let afterSequence = ZERO_SEQUENCE;

        while (afterSequence < thread.tail_sequence) {
          const limit = Math.min(1_024, thread.tail_sequence - afterSequence);

          const request = RawReadRequest.make({
            threadId,
            fromSequenceExclusive: afterSequence,
            limit,
          });

          const page = yield* read(request);

          if (
            page.length !== limit ||
            page.some((record, index) => record.sequence !== afterSequence + index + 1)
          ) {
            return yield* PostgresStorageCorruptionError.make({
              table: "effect_agent_canonical_records",
              rowKey: threadId,
              message: "The exported canonical prefix is not contiguous through its captured tail.",
            });
          }
          records.push(...page);
          afterSequence = page[page.length - 1].sequence;
        }

        const beyondTail =
          yield* sql`SELECT sequence FROM effect_agent_canonical_records WHERE thread_id=${threadId} AND sequence > ${thread.tail_sequence} LIMIT 1`.pipe(
            Effect.mapError(storageError("verify export tail")),
          );

        if (beyondTail.length !== 0)
          return yield* PostgresStorageCorruptionError.make({
            table: "effect_agent_canonical_records",
            rowKey: threadId,
            message: "Canonical records exist beyond the captured thread tail.",
          });

        return RawThreadExport.make({ thread, records });
      }),
    );
  });

  const saveCheckpoint = Effect.fn("PostgresJournal.saveCheckpoint")(function* (
    checkpoint: RawCheckpoint,
  ): Effect.fn.Return<void, CheckpointError> {
    if (
      checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(checkpoint.checkpointJson) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* PostgresStorageError.make({
        operation: "save checkpoint",
        message: "Checkpoint identity or encoded JSON exceeds the Postgres storage bounds.",
      });
    }
    yield* withWriteTransaction("checkpoint transaction")(
      Effect.gen(function* () {
        const threadRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            tail_digest,
            producer_epoch
          FROM effect_agent_threads
          WHERE thread_id = ${checkpoint.threadId}
        `.pipe(Effect.mapError(storageError("read checkpoint tail")));

        const thread = yield* decodeSingleRow(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          checkpoint.threadId,
          threadRows,
        );

        if (checkpoint.throughSequence > thread.tail_sequence) {
          return yield* PostgresCheckpointConflict.make({
            message:
              `Checkpoint sequence ${checkpoint.throughSequence} is after canonical tail ` +
              `${thread.tail_sequence}.`,
          });
        }

        const checkpointRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            through_sequence,
            tail_digest,
            checkpoint_json
          FROM effect_agent_checkpoints
          WHERE thread_id = ${checkpoint.threadId}
            AND through_sequence = ${checkpoint.throughSequence}
        `.pipe(Effect.mapError(storageError("read idempotent checkpoint")));

        const existingCheckpoints = yield* decodeRows(
          Schema.Array(CheckpointRow),
          "effect_agent_checkpoints",
          `${checkpoint.threadId}/${checkpoint.throughSequence}`,
          checkpointRows,
        );

        if (existingCheckpoints.length > 1) {
          return yield* PostgresStorageCorruptionError.make({
            table: "effect_agent_checkpoints",
            rowKey: `${checkpoint.threadId}/${checkpoint.throughSequence}`,
            message: "A checkpoint primary key returned more than one row.",
          });
        }
        if (existingCheckpoints.length === 1) {
          if (
            existingCheckpoints[0].tail_digest !== checkpoint.tailDigest ||
            existingCheckpoints[0].checkpoint_json !== checkpoint.checkpointJson
          ) {
            return yield* PostgresCheckpointConflict.make({
              message: "A different checkpoint already exists at this canonical sequence.",
            });
          }

          return;
        }

        yield* sql`
          INSERT INTO effect_agent_checkpoints (
            thread_id,
            through_sequence,
            tail_digest,
            checkpoint_json
          ) VALUES (
            ${checkpoint.threadId},
            ${checkpoint.throughSequence},
            ${checkpoint.tailDigest},
            ${checkpoint.checkpointJson}
          )
        `.pipe(Effect.mapError(storageError("insert checkpoint")));
      }),
    );
  });

  const saveRecoveryCheckpoint = Effect.fn("PostgresJournal.saveRecoveryCheckpoint")(function* (
    request: SaveRecoveryCheckpointRequest,
    checkpointJson: string,
  ) {
    const { checkpoint } = request;

    if (
      checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(checkpointJson) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* PostgresStorageError.make({
        operation: "save recovery checkpoint",
        message: "Checkpoint identity or encoded JSON exceeds the Postgres storage bounds.",
      });
    }
    yield* withWriteTransaction("recovery checkpoint transaction")(
      Effect.gen(function* () {
        const threads = yield* getThread(checkpoint.threadId);
        const thread = threads[0];

        if (thread === undefined)
          return yield* ThreadNotMaterialized.make({ threadId: checkpoint.threadId });
        if (request.producerEpoch !== thread.producer_epoch)
          return yield* FenceRejected.make({
            threadId: checkpoint.threadId,
            actualEpoch: thread.producer_epoch,
            attemptedEpoch: request.producerEpoch,
          });
        if (checkpoint.throughSequence > thread.tail_sequence)
          return yield* CheckpointRejected.make({
            threadId: checkpoint.threadId,
            reason: "ahead-of-tail",
          });

        const digests =
          checkpoint.throughSequence === 0
            ? [EMPTY_TAIL_DIGEST]
            : yield* getTailDigestAt(checkpoint.threadId, checkpoint.throughSequence);

        if (digests.length !== 1 || digests[0] !== checkpoint.tailDigest)
          return yield* CheckpointRejected.make({
            threadId: checkpoint.threadId,
            reason: "digest-mismatch",
          });

        yield* failpoint("save-recovery-checkpoint:before");
        yield* sql`
          INSERT INTO effect_agent_recovery_checkpoints (thread_id, through_sequence, tail_digest, checkpoint_json)
          VALUES (${checkpoint.threadId}, ${checkpoint.throughSequence}, ${checkpoint.tailDigest}, ${checkpointJson})
          ON CONFLICT (thread_id) DO UPDATE SET
            through_sequence = excluded.through_sequence,
            tail_digest = excluded.tail_digest,
            checkpoint_json = excluded.checkpoint_json
          WHERE excluded.through_sequence >= effect_agent_recovery_checkpoints.through_sequence
        `.pipe(Effect.mapError(storageError("save recovery checkpoint")));
      }),
    );
    yield* failpoint("save-recovery-checkpoint:after");
  });

  const loadRecoveryCheckpoint = Effect.fn("PostgresJournal.loadRecoveryCheckpoint")(function* (
    threadId: string,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT thread_id, through_sequence, tail_digest, checkpoint_json
      FROM effect_agent_recovery_checkpoints
      WHERE thread_id = ${threadId}
    `.pipe(Effect.mapError(storageError("load recovery checkpoint")));

    return yield* decodeRows(
      Schema.Array(CheckpointRow),
      "effect_agent_recovery_checkpoints",
      threadId,
      rows,
    );
  });

  const loadCheckpoint = Effect.fn("PostgresJournal.loadCheckpoint")(function* (
    threadId: string,
    atOrBeforeSequence: CanonicalSequence,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        through_sequence,
        tail_digest,
        checkpoint_json
      FROM effect_agent_checkpoints
      WHERE thread_id = ${threadId}
        AND through_sequence <= ${atOrBeforeSequence}
      ORDER BY through_sequence DESC
      LIMIT 1
    `.pipe(Effect.mapError(storageError("load checkpoint")));

    return yield* decodeRows(
      Schema.Array(CheckpointRow),
      "effect_agent_checkpoints",
      `${threadId}<=${atOrBeforeSequence}`,
      rows,
    );
  });

  const getTailDigestAt = Effect.fn("PostgresJournal.getTailDigestAt")(function* (
    threadId: string,
    sequence: CanonicalSequence,
  ) {
    if (sequence === 0) {
      const threads = yield* getThread(threadId);

      return threads.length === 0
        ? []
        : [threads[0].tail_sequence === 0 ? threads[0].tail_digest : undefined].filter(
            (value): value is string => value !== undefined,
          );
    }

    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        batch_id,
        first_sequence,
        last_sequence,
        batch_digest,
        tail_digest,
        batch_json
      FROM effect_agent_canonical_batches
      WHERE thread_id = ${threadId}
        AND last_sequence = ${sequence}
    `.pipe(Effect.mapError(storageError("read canonical digest at sequence")));

    const batches = yield* decodeRows(
      Schema.Array(BatchRow),
      "effect_agent_canonical_batches",
      `${threadId}/${sequence}`,
      rows,
    );

    return batches.map((batch) => batch.tail_digest);
  });

  const scanStoredPayloads = Effect.fn("PostgresJournal.scanStoredPayloads")(function* () {
    return yield* withReadTransaction("startup scan transaction")(
      Effect.gen(function* () {
        const threads = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            FROM effect_agent_threads
            ORDER BY thread_id
          `.pipe(Effect.mapError(storageError("scan threads")));

        const batches = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              batch_id,
              first_sequence,
              last_sequence,
              batch_digest,
              tail_digest,
              batch_json
            FROM effect_agent_canonical_batches
            ORDER BY thread_id, first_sequence
          `.pipe(Effect.mapError(storageError("scan canonical batches")));

        const records = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              sequence,
              record_id,
              batch_id,
              record_json
            FROM effect_agent_canonical_records
            ORDER BY thread_id, sequence
          `.pipe(Effect.mapError(storageError("scan canonical records")));

        const checkpoints = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              through_sequence,
              tail_digest,
              checkpoint_json
            FROM effect_agent_checkpoints
            ORDER BY thread_id, through_sequence
          `.pipe(Effect.mapError(storageError("scan checkpoints")));

        return {
          threads: yield* decodeRows(
            Schema.Array(ThreadRow),
            "effect_agent_threads",
            "startup_scan",
            threads,
          ),
          batches: yield* decodeRows(
            Schema.Array(BatchRow),
            "effect_agent_canonical_batches",
            "startup_scan",
            batches,
          ),
          records: yield* decodeRows(
            Schema.Array(RecordRow),
            "effect_agent_canonical_records",
            "startup_scan",
            records,
          ),
          checkpoints: yield* decodeRows(
            Schema.Array(CheckpointRow),
            "effect_agent_checkpoints",
            "startup_scan",
            checkpoints,
          ),
        };
      }),
    );
  });

  return {
    append,
    exportThread,
    getThread,
    getTailDigestAt,
    loadCheckpoint,
    loadRecoveryCheckpoint,
    saveRecoveryCheckpoint,
    materialize,
    read,
    saveCheckpoint,
    scanStoredPayloads,
    withWriteTransaction,
    withReadTransaction,
  } as const;
});

export type PostgresJournal = Effect.Success<ReturnType<typeof initializePostgresJournal>>;
