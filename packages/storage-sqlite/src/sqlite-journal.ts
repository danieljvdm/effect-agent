import { CanonicalSequence, ProducerEpoch } from "@effect-agent/thread";
import { SqliteMigrator } from "@effect/sql-sqlite-node";
import { Effect, Exit, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import type { SqliteStorageFailpointError } from "./errors.ts";
import {
  SqliteAppendConflict,
  SqliteCheckpointConflict,
  SqliteFenceRejected,
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
  SqliteWriteContention,
} from "./errors.ts";
import { CurrentSqliteStorageVersion, sqliteMigrations } from "./migrations.ts";
import { SqliteStorageConfig } from "./sqlite-storage-config.ts";
import { SqliteStorageFailpoint } from "./sqlite-storage-failpoint.ts";

const BoundedStoredText = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const MAX_RECORDS_PER_THREAD = 65_536;
const MAX_STORED_TEXT_BYTES = 16 * 1024 * 1024;
const MAX_IDENTIFIER_LENGTH = 1_024;

const storedTextBytes = (value: string): number => new TextEncoder().encode(value).byteLength;

class SqliteVersionRow extends Schema.Class<SqliteVersionRow>("SqliteVersionRow")({
  user_version: NonNegativeInt,
}) {}

class SqliteJournalModeRow extends Schema.Class<SqliteJournalModeRow>("SqliteJournalModeRow")({
  journal_mode: Schema.NonEmptyString.check(Schema.isMaxLength(32)),
}) {}

class SqliteNameRow extends Schema.Class<SqliteNameRow>("SqliteNameRow")({
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

export class RawRecord extends Schema.Class<RawRecord>("@effect-agent/storage-sqlite/RawRecord")({
  recordId: BoundedIdentifier,
  recordJson: BoundedStoredText,
}) {}

export class RawAppendRequest extends Schema.Class<RawAppendRequest>(
  "@effect-agent/storage-sqlite/RawAppendRequest",
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
  "@effect-agent/storage-sqlite/RawAppendResult",
)({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  replayed: Schema.Boolean,
  tailDigest: BoundedStoredText,
}) {}

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-sqlite/RawReadRequest",
)({
  threadId: BoundedIdentifier,
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class RawCheckpoint extends Schema.Class<RawCheckpoint>(
  "@effect-agent/storage-sqlite/RawCheckpoint",
)({
  checkpointJson: BoundedStoredText,
  threadId: BoundedIdentifier,
  tailDigest: BoundedStoredText,
  throughSequence: CanonicalSequence,
}) {}

export class RawThreadExport extends Schema.Class<RawThreadExport>(
  "@effect-agent/storage-sqlite/RawThreadExport",
)({
  batches: Schema.Array(BatchRow),
  checkpoints: Schema.Array(CheckpointRow),
  thread: ThreadRow,
  records: Schema.Array(RecordRow),
}) {}

type AppendError =
  | SqliteAppendConflict
  | SqliteFenceRejected
  | SqliteStorageCorruptionError
  | SqliteStorageError
  | SqliteStorageFailpointError
  | SqliteWriteContention;

type CheckpointError =
  | SqliteCheckpointConflict
  | SqliteStorageCorruptionError
  | SqliteStorageError
  | SqliteWriteContention;
const storageError =
  (operation: string) =>
  (error: SqlError): SqliteStorageError =>
    SqliteStorageError.make({
      cause: error,
      operation,
      message: error.message,
    });

/** Decode raw SQLite rows against a Schema, reporting failures as typed corruption. */
export const decodeRows = Effect.fn("SqliteJournal.decodeRows")(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<ReadonlyArray<A>, SqliteStorageCorruptionError> =>
    Schema.decodeUnknownEffect(schema)(rows).pipe(
      Effect.mapError((error) =>
        SqliteStorageCorruptionError.make({
          table,
          rowKey,
          message: String(error),
        }),
      ),
    ),
);

/** Decode exactly one raw SQLite row against a Schema, reporting failures as typed corruption. */
export const decodeSingleRow = Effect.fn("SqliteJournal.decodeSingleRow")(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<A, SqliteStorageCorruptionError> =>
    decodeRows(schema, table, rowKey, rows).pipe(
      Effect.flatMap((decoded) =>
        decoded.length === 1
          ? Effect.succeed(decoded[0])
          : Effect.fail(
              SqliteStorageCorruptionError.make({
                table,
                rowKey,
                message: `Expected exactly one row but found ${decoded.length}.`,
              }),
            ),
      ),
    ),
);

export const initializeSqliteJournal = Effect.fn("SqliteJournal.initialize")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const { hit: failpoint } = yield* SqliteStorageFailpoint;
  const { busyTimeout } = yield* SqliteStorageConfig;
  yield* sql`PRAGMA foreign_keys = ON`.pipe(Effect.mapError(storageError("enable foreign keys")));
  // PRAGMA statements do not accept bound parameters; the value is a schema-validated
  // non-negative integer, never caller-controlled text.
  yield* sql
    .unsafe(`PRAGMA busy_timeout = ${busyTimeout}`)
    .pipe(Effect.mapError(storageError("configure busy timeout")));
  const journalModeRows = yield* sql<Record<string, unknown>>`PRAGMA journal_mode`.pipe(
    Effect.mapError(storageError("read journal mode")),
  );
  const journalMode = yield* decodeSingleRow(
    Schema.Array(SqliteJournalModeRow),
    "pragma_journal_mode",
    "singleton",
    journalModeRows,
  );
  if (journalMode.journal_mode.toLowerCase() !== "wal") {
    return yield* SqliteStorageCompatibilityError.make({
      actualVersion: 0,
      supportedVersion: CurrentSqliteStorageVersion,
      message: `SQLite WAL mode is required; the database reported ${journalMode.journal_mode}.`,
    });
  }

  const versionRows = yield* sql<Record<string, unknown>>`PRAGMA user_version`.pipe(
    Effect.mapError(storageError("read storage version")),
  );
  const version = yield* decodeSingleRow(
    Schema.Array(SqliteVersionRow),
    "pragma_user_version",
    "singleton",
    versionRows,
  );

  // D7: the storage version must match EXACTLY (or be 0 for a fresh file). Older
  // private-development versions fail closed with reset guidance rather than being
  // migrated, and newer versions fail closed rather than being decoded incorrectly.
  if (version.user_version !== 0 && version.user_version !== CurrentSqliteStorageVersion) {
    return yield* SqliteStorageCompatibilityError.make({
      actualVersion: version.user_version,
      supportedVersion: CurrentSqliteStorageVersion,
      message:
        `The SQLite file uses private-development storage version ${version.user_version}; ` +
        `this build supports exactly version ${CurrentSqliteStorageVersion}. ` +
        "Reset the database file explicitly; automatic stored-data migrations are not provided during private development.",
    });
  }

  if (version.user_version === 0) {
    const existingRows = yield* sql<Record<string, unknown>>`
      SELECT name
      FROM sqlite_master
      WHERE type = 'table'
        AND name LIKE 'effect_agent_%'
      ORDER BY name
    `.pipe(Effect.mapError(storageError("inspect unversioned storage")));
    const existing = yield* decodeRows(
      Schema.Array(SqliteNameRow),
      "sqlite_master",
      "effect_agent_%",
      existingRows,
    );

    if (existing.length > 0) {
      return yield* SqliteStorageCompatibilityError.make({
        actualVersion: 0,
        supportedVersion: CurrentSqliteStorageVersion,
        message:
          "The SQLite file contains unversioned Effect Agent tables. Reset it explicitly; refusing to mutate ambiguous stored data.",
      });
    }

    yield* SqliteMigrator.run({ loader: sqliteMigrations }).pipe(
      Effect.mapError((error) =>
        SqliteStorageError.make({
          cause: error,
          operation: "initialize current storage",
          message: error.message,
        }),
      ),
    );
  }

  const requiredRows = yield* sql<Record<string, unknown>>`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name IN (
        'effect_agent_threads',
        'effect_agent_canonical_batches',
        'effect_agent_canonical_records',
        'effect_agent_checkpoints',
        'effect_agent_submissions',
        'effect_agent_submission_ownership',
        'effect_agent_attempts',
        'effect_agent_settlement_reservations',
        'effect_agent_abort_intents',
        'effect_agent_approval_decisions',
        'effect_agent_unknown_resolutions',
        'effect_agent_schedules'
      )
    ORDER BY name
  `.pipe(Effect.mapError(storageError("verify storage tables")));
  const required = yield* decodeRows(
    Schema.Array(SqliteNameRow),
    "sqlite_master",
    "required_tables",
    requiredRows,
  );
  if (required.length !== 12) {
    return yield* SqliteStorageCompatibilityError.make({
      actualVersion: CurrentSqliteStorageVersion,
      supportedVersion: CurrentSqliteStorageVersion,
      message:
        "The SQLite file claims the current format but is missing required tables. Reset the corrupt private-development data.",
    });
  }

  const classifyWriteFailure =
    (operation: string) =>
    (error: SqlError): SqliteStorageError | SqliteWriteContention =>
      error.reason._tag === "LockTimeoutError"
        ? SqliteWriteContention.make({
            cause: error,
            operation,
            message: `Another producer holds the SQLite write lock; ${operation} is safe to retry.`,
          })
        : storageError(operation)(error);

  /**
   * Runs one journal write transaction under `BEGIN IMMEDIATE`. SQLite's deferred `BEGIN`
   * would let a read-then-write transaction start as a reader and fail with
   * SQLITE_BUSY_SNAPSHOT on upgrade, which `busy_timeout` never retries. Taking the write
   * lock up front keeps cross-owner contention inside the bounded busy retry; a lock
   * timeout is classified as the retryable SqliteWriteContention. A failed `BEGIN` leaves
   * no transaction, so no rollback is attempted for it.
   *
   * Journal write transactions are always top level. Nesting one inside another would
   * deadlock the single-connection client, so new journal operations must not wrap this
   * helper inside another transaction.
   */
  const withWriteTransaction =
    (operation: string) =>
    <A, E>(
      effect: Effect.Effect<A, E>,
    ): Effect.Effect<A, E | SqliteStorageError | SqliteWriteContention> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.scoped(
          Effect.gen(function* () {
            const connection = yield* sql.reserve.pipe(
              Effect.mapError(classifyWriteFailure(operation)),
            );
            yield* connection
              .executeUnprepared("BEGIN IMMEDIATE", [], undefined)
              .pipe(Effect.mapError(classifyWriteFailure(operation)));
            const exit = yield* restore(
              Effect.provideService(effect, sql.transactionService, [connection, 0] as const),
            ).pipe(Effect.exit);
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
          Effect.withSpan("SqliteJournal.withWriteTransaction", { attributes: { operation } }),
        ),
      );

  /**
   * Runs a read-only snapshot under a deferred transaction. Effect's SQLite client now
   * starts every writable-client `withTransaction` with `BEGIN IMMEDIATE`, which is the
   * right default for mutations but would make exports take the write lock and block a
   * concurrent append. Reserving the connection and beginning explicitly preserves the
   * adapter's snapshot-with-concurrent-writer contract. As with the write helper, a failed
   * `BEGIN` is reported directly because there is no transaction to roll back.
   */
  const withReadTransaction =
    (operation: string) =>
    <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E | SqliteStorageError> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.scoped(
          Effect.gen(function* () {
            const connection = yield* sql.reserve.pipe(Effect.mapError(storageError(operation)));
            yield* connection
              .executeUnprepared("BEGIN", [], undefined)
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
        ).pipe(Effect.withSpan("SqliteJournal.withReadTransaction", { attributes: { operation } })),
      );

  const materialize = Effect.fn("SqliteJournal.materialize")(function* (
    threadId: string,
    createdAt: string,
    emptyTailDigest: string,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<
    void,
    SqliteFenceRejected | SqliteStorageCorruptionError | SqliteStorageError | SqliteWriteContention
  > {
    if (
      threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(emptyTailDigest) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* SqliteStorageError.make({
        operation: "materialize thread",
        message: "Thread identity or initial digest exceeds the SQLite storage bounds.",
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
        const existing = yield* decodeRows(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          threadId,
          existingRows,
        );
        if (existing.length > 1) {
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_threads",
            rowKey: threadId,
            message: "A thread primary key returned more than one row.",
          });
        }
        if (existing.length === 0) {
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
        if (producerEpoch < existing[0].producer_epoch) {
          return yield* SqliteFenceRejected.make({
            producerEpoch,
            actualEpoch: existing[0].producer_epoch,
            message: `Producer epoch ${producerEpoch} is stale; current epoch is ${existing[0].producer_epoch}.`,
          });
        }
        if (producerEpoch > existing[0].producer_epoch) {
          yield* sql`
            UPDATE effect_agent_threads
            SET producer_epoch = ${producerEpoch}
            WHERE thread_id = ${threadId}
          `.pipe(Effect.mapError(storageError("advance materialization epoch")));
        }
      }),
    );
  });

  const getThread = Effect.fn("SqliteJournal.getThread")(function* (threadId: string) {
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

  const append = Effect.fn("SqliteJournal.append")(function* (
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
      return yield* SqliteStorageError.make({
        operation: "append canonical batch",
        message: "Canonical identifiers or encoded JSON exceed the SQLite storage bounds.",
      });
    }
    return yield* withWriteTransaction("append transaction")(
      Effect.gen(function* () {
        const recordIds = request.records.map((record) => record.recordId);
        if (new Set(recordIds).size !== recordIds.length) {
          return yield* SqliteAppendConflict.make({
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
          return yield* SqliteFenceRejected.make({
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
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_canonical_batches",
            rowKey: `${request.threadId}/${request.batchId}`,
            message: "A canonical batch primary key returned more than one row.",
          });
        }
        if (batches.length === 1) {
          const existing = batches[0];
          if (existing.batch_digest !== request.batchDigest) {
            return yield* SqliteAppendConflict.make({
              message: `Batch ${request.batchId} already exists with different canonical content.`,
              reason: "batch-digest",
            });
          }
          return RawAppendResult.make({
            firstSequence: existing.first_sequence,
            lastSequence: existing.last_sequence,
            replayed: true,
            tailDigest: existing.tail_digest,
          });
        }

        if (
          request.expectedTailSequence !== thread.tail_sequence ||
          request.expectedTailDigest !== thread.tail_digest
        ) {
          return yield* SqliteAppendConflict.make({
            message:
              `Expected tail ${request.expectedTailSequence}/${request.expectedTailDigest} ` +
              `but found ${thread.tail_sequence}/${thread.tail_digest}.`,
            reason: "tail",
            actualTailSequence: thread.tail_sequence,
            actualTailDigest: thread.tail_digest,
          });
        }
        if (thread.tail_sequence + request.records.length > MAX_RECORDS_PER_THREAD) {
          return yield* SqliteStorageError.make({
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
          return yield* SqliteAppendConflict.make({
            message: `Canonical record ID ${existingRecords[0].record_id} already exists.`,
            reason: "record-identity",
          });
        }

        const firstSequence = yield* Schema.decodeUnknownEffect(CanonicalSequence)(
          thread.tail_sequence + 1,
        ).pipe(
          Effect.mapError((error) =>
            SqliteStorageError.make({
              cause: error,
              operation: "append canonical batch",
              message: error.message,
            }),
          ),
        );
        const lastSequence = yield* Schema.decodeUnknownEffect(CanonicalSequence)(
          firstSequence + request.records.length - 1,
        ).pipe(
          Effect.mapError((error) =>
            SqliteStorageError.make({
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

  const read = Effect.fn("SqliteJournal.read")(function* (request: RawReadRequest) {
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

  const exportThread = Effect.fn("SqliteJournal.exportThread")(function* (threadId: string) {
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
            WHERE thread_id = ${threadId}
            ORDER BY first_sequence
          `.pipe(Effect.mapError(storageError("export canonical batches")));
        const recordRows = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              sequence,
              record_id,
              batch_id,
              record_json
            FROM effect_agent_canonical_records
            WHERE thread_id = ${threadId}
            ORDER BY sequence
          `.pipe(Effect.mapError(storageError("export canonical records")));
        const checkpointRows = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              through_sequence,
              tail_digest,
              checkpoint_json
            FROM effect_agent_checkpoints
            WHERE thread_id = ${threadId}
            ORDER BY through_sequence
          `.pipe(Effect.mapError(storageError("export checkpoints")));

        return RawThreadExport.make({
          thread,
          batches: yield* decodeRows(
            Schema.Array(BatchRow),
            "effect_agent_canonical_batches",
            threadId,
            batchRows,
          ),
          records: yield* decodeRows(
            Schema.Array(RecordRow),
            "effect_agent_canonical_records",
            threadId,
            recordRows,
          ),
          checkpoints: yield* decodeRows(
            Schema.Array(CheckpointRow),
            "effect_agent_checkpoints",
            threadId,
            checkpointRows,
          ),
        });
      }),
    );
  });

  const saveCheckpoint = Effect.fn("SqliteJournal.saveCheckpoint")(function* (
    checkpoint: RawCheckpoint,
  ): Effect.fn.Return<void, CheckpointError> {
    if (
      checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(checkpoint.checkpointJson) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* SqliteStorageError.make({
        operation: "save checkpoint",
        message: "Checkpoint identity or encoded JSON exceeds the SQLite storage bounds.",
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
          return yield* SqliteCheckpointConflict.make({
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
        const existing = yield* decodeRows(
          Schema.Array(CheckpointRow),
          "effect_agent_checkpoints",
          `${checkpoint.threadId}/${checkpoint.throughSequence}`,
          checkpointRows,
        );
        if (existing.length > 1) {
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_checkpoints",
            rowKey: `${checkpoint.threadId}/${checkpoint.throughSequence}`,
            message: "A checkpoint primary key returned more than one row.",
          });
        }
        if (existing.length === 1) {
          if (
            existing[0].tail_digest !== checkpoint.tailDigest ||
            existing[0].checkpoint_json !== checkpoint.checkpointJson
          ) {
            return yield* SqliteCheckpointConflict.make({
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

  const loadCheckpoint = Effect.fn("SqliteJournal.loadCheckpoint")(function* (
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

  const getTailDigestAt = Effect.fn("SqliteJournal.getTailDigestAt")(function* (
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

  const scanStoredPayloads = Effect.fn("SqliteJournal.scanStoredPayloads")(function* () {
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
    materialize,
    read,
    saveCheckpoint,
    scanStoredPayloads,
    withWriteTransaction,
  } as const;
});

export type SqliteJournal = Effect.Success<ReturnType<typeof initializeSqliteJournal>>;
