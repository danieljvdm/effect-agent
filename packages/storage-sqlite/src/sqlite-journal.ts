import { SqliteMigrator } from "@effect/sql-sqlite-node";
import { CanonicalSequence, ProducerEpoch } from "@effect-agent/session";
import { Effect, Exit, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import {
  SqliteAppendConflict,
  SqliteCheckpointConflict,
  SqliteFenceRejected,
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
  SqliteStorageFailpointError,
  SqliteWriteContention,
  type SqliteStorageFailpointLocation,
} from "./errors.ts";
import { CurrentSqliteStorageVersion, sqliteMigrations } from "./migrations.ts";

const BoundedStoredText = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const MAX_RECORDS_PER_CONVERSATION = 65_536;
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

class ConversationRow extends Schema.Class<ConversationRow>("ConversationRow")({
  conversation_id: BoundedIdentifier,
  created_at: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  producer_epoch: ProducerEpoch,
  tail_digest: BoundedStoredText,
  tail_sequence: CanonicalSequence,
}) {}

class BatchRow extends Schema.Class<BatchRow>("BatchRow")({
  batch_digest: BoundedStoredText,
  batch_id: BoundedIdentifier,
  batch_json: BoundedStoredText,
  conversation_id: BoundedIdentifier,
  first_sequence: CanonicalSequence,
  last_sequence: CanonicalSequence,
  tail_digest: BoundedStoredText,
}) {}

class RecordRow extends Schema.Class<RecordRow>("RecordRow")({
  batch_id: BoundedIdentifier,
  conversation_id: BoundedIdentifier,
  record_id: BoundedIdentifier,
  record_json: BoundedStoredText,
  sequence: CanonicalSequence,
}) {}

class CheckpointRow extends Schema.Class<CheckpointRow>("CheckpointRow")({
  checkpoint_json: BoundedStoredText,
  conversation_id: BoundedIdentifier,
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
  conversationId: BoundedIdentifier,
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
  conversationId: BoundedIdentifier,
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class RawCheckpoint extends Schema.Class<RawCheckpoint>(
  "@effect-agent/storage-sqlite/RawCheckpoint",
)({
  checkpointJson: BoundedStoredText,
  conversationId: BoundedIdentifier,
  tailDigest: BoundedStoredText,
  throughSequence: CanonicalSequence,
}) {}

export class RawConversationExport extends Schema.Class<RawConversationExport>(
  "@effect-agent/storage-sqlite/RawConversationExport",
)({
  batches: Schema.Array(BatchRow),
  checkpoints: Schema.Array(CheckpointRow),
  conversation: ConversationRow,
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
type SqliteJournalFailpoint = (
  location: SqliteStorageFailpointLocation,
) => Effect.Effect<void, SqliteStorageFailpointError>;

const noFailpoint: SqliteJournalFailpoint = () => Effect.void;

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

const ensureCurrentStorage = Effect.fn("SqliteJournal.ensureCurrentStorage")(function* (
  sql: SqlClient.SqlClient,
  failpoint: SqliteJournalFailpoint = noFailpoint,
  busyTimeoutMillis = 5_000,
) {
  yield* sql`PRAGMA foreign_keys = ON`.pipe(Effect.mapError(storageError("enable foreign keys")));
  // PRAGMA statements do not accept bound parameters; the value is a schema-validated
  // non-negative integer, never caller-controlled text.
  yield* sql
    .unsafe(`PRAGMA busy_timeout = ${busyTimeoutMillis}`)
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
      // SqliteMigrator depends on the generic client supplied by this adapter.
      // The concrete Node client is kept at the outer Layer boundary.
      Effect.provideService(SqlClient.SqlClient, sql),
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
        'effect_agent_conversations',
        'effect_agent_canonical_batches',
        'effect_agent_canonical_records',
        'effect_agent_checkpoints',
        'effect_agent_submissions',
        'effect_agent_submission_ownership',
        'effect_agent_attempts',
        'effect_agent_settlement_reservations',
        'effect_agent_abort_intents',
        'effect_agent_approval_decisions',
        'effect_agent_unknown_resolutions'
      )
    ORDER BY name
  `.pipe(Effect.mapError(storageError("verify storage tables")));
  const required = yield* decodeRows(
    Schema.Array(SqliteNameRow),
    "sqlite_master",
    "required_tables",
    requiredRows,
  );
  if (required.length !== 11) {
    return yield* SqliteStorageCompatibilityError.make({
      actualVersion: CurrentSqliteStorageVersion,
      supportedVersion: CurrentSqliteStorageVersion,
      message:
        "The SQLite file claims the current format but is missing required tables. Reset the corrupt private-development data.",
    });
  }

  return makeJournal(sql, failpoint);
});

const makeJournal = (sql: SqlClient.SqlClient, failpoint: SqliteJournalFailpoint) => {
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

  const materialize = Effect.fn("SqliteJournal.materialize")(function* (
    conversationId: string,
    createdAt: string,
    emptyTailDigest: string,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<
    void,
    SqliteFenceRejected | SqliteStorageCorruptionError | SqliteStorageError | SqliteWriteContention
  > {
    if (
      conversationId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(emptyTailDigest) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* SqliteStorageError.make({
        operation: "materialize conversation",
        message: "Conversation identity or initial digest exceeds the SQLite storage bounds.",
      });
    }
    yield* withWriteTransaction("materialize transaction")(
      Effect.gen(function* () {
        const existingRows = yield* sql<Record<string, unknown>>`
          SELECT
            conversation_id,
            created_at,
            tail_sequence,
            tail_digest,
            producer_epoch
          FROM effect_agent_conversations
          WHERE conversation_id = ${conversationId}
        `.pipe(Effect.mapError(storageError("read materialized conversation")));
        const existing = yield* decodeRows(
          Schema.Array(ConversationRow),
          "effect_agent_conversations",
          conversationId,
          existingRows,
        );
        if (existing.length > 1) {
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_conversations",
            rowKey: conversationId,
            message: "A conversation primary key returned more than one row.",
          });
        }
        if (existing.length === 0) {
          yield* sql`
            INSERT INTO effect_agent_conversations (
              conversation_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            ) VALUES (
              ${conversationId},
              ${createdAt},
              0,
              ${emptyTailDigest},
              ${producerEpoch}
            )
          `.pipe(Effect.mapError(storageError("materialize conversation")));
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
            UPDATE effect_agent_conversations
            SET producer_epoch = ${producerEpoch}
            WHERE conversation_id = ${conversationId}
          `.pipe(Effect.mapError(storageError("advance materialization epoch")));
        }
      }),
    );
  });

  const getConversation = Effect.fn("SqliteJournal.getConversation")(function* (
    conversationId: string,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        conversation_id,
        created_at,
        tail_sequence,
        tail_digest,
        producer_epoch
      FROM effect_agent_conversations
      WHERE conversation_id = ${conversationId}
    `.pipe(Effect.mapError(storageError("read conversation")));
    return yield* decodeRows(
      Schema.Array(ConversationRow),
      "effect_agent_conversations",
      conversationId,
      rows,
    );
  });

  const append = Effect.fn("SqliteJournal.append")(function* (
    request: RawAppendRequest,
  ): Effect.fn.Return<RawAppendResult, AppendError> {
    if (
      request.conversationId.length > MAX_IDENTIFIER_LENGTH ||
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

        const conversationRows = yield* sql<Record<string, unknown>>`
          SELECT
            conversation_id,
            created_at,
            tail_sequence,
            tail_digest,
            producer_epoch
          FROM effect_agent_conversations
          WHERE conversation_id = ${request.conversationId}
        `.pipe(Effect.mapError(storageError("read append tail")));
        const conversation = yield* decodeSingleRow(
          Schema.Array(ConversationRow),
          "effect_agent_conversations",
          request.conversationId,
          conversationRows,
        );

        if (request.producerEpoch !== conversation.producer_epoch) {
          return yield* SqliteFenceRejected.make({
            producerEpoch: request.producerEpoch,
            actualEpoch: conversation.producer_epoch,
            message: `Producer epoch ${request.producerEpoch} is not the current epoch ${conversation.producer_epoch}.`,
          });
        }

        const batchRows = yield* sql<Record<string, unknown>>`
          SELECT
            conversation_id,
            batch_id,
            first_sequence,
            last_sequence,
            batch_digest,
            tail_digest,
            batch_json
          FROM effect_agent_canonical_batches
          WHERE conversation_id = ${request.conversationId}
            AND batch_id = ${request.batchId}
        `.pipe(Effect.mapError(storageError("read idempotent batch")));
        const batches = yield* decodeRows(
          Schema.Array(BatchRow),
          "effect_agent_canonical_batches",
          `${request.conversationId}/${request.batchId}`,
          batchRows,
        );

        if (batches.length > 1) {
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_canonical_batches",
            rowKey: `${request.conversationId}/${request.batchId}`,
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
          request.expectedTailSequence !== conversation.tail_sequence ||
          request.expectedTailDigest !== conversation.tail_digest
        ) {
          return yield* SqliteAppendConflict.make({
            message:
              `Expected tail ${request.expectedTailSequence}/${request.expectedTailDigest} ` +
              `but found ${conversation.tail_sequence}/${conversation.tail_digest}.`,
            reason: "tail",
            actualTailSequence: conversation.tail_sequence,
            actualTailDigest: conversation.tail_digest,
          });
        }
        if (conversation.tail_sequence + request.records.length > MAX_RECORDS_PER_CONVERSATION) {
          return yield* SqliteStorageError.make({
            operation: "append canonical batch",
            message: `Conversation record limit ${MAX_RECORDS_PER_CONVERSATION} would be exceeded.`,
          });
        }

        const existingRecordRows = yield* sql<Record<string, unknown>>`
          SELECT
            conversation_id,
            sequence,
            record_id,
            batch_id,
            record_json
          FROM effect_agent_canonical_records
          WHERE conversation_id = ${request.conversationId}
            AND record_id IN ${sql.in(recordIds)}
          ORDER BY sequence
        `.pipe(Effect.mapError(storageError("check canonical record identities")));
        const existingRecords = yield* decodeRows(
          Schema.Array(RecordRow),
          "effect_agent_canonical_records",
          `${request.conversationId}/record_ids`,
          existingRecordRows,
        );
        if (existingRecords.length > 0) {
          return yield* SqliteAppendConflict.make({
            message: `Canonical record ID ${existingRecords[0].record_id} already exists.`,
            reason: "record-identity",
          });
        }

        const firstSequence = yield* Schema.decodeUnknownEffect(CanonicalSequence)(
          conversation.tail_sequence + 1,
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
            conversation_id,
            batch_id,
            first_sequence,
            last_sequence,
            batch_digest,
            tail_digest,
            batch_json
          ) VALUES (
            ${request.conversationId},
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
                    conversation_id,
                    sequence,
                    record_id,
                    batch_id,
                    record_json
                  ) VALUES (
                    ${request.conversationId},
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
          UPDATE effect_agent_conversations
          SET
            tail_sequence = ${lastSequence},
            tail_digest = ${request.tailDigest},
            producer_epoch = ${request.producerEpoch}
          WHERE conversation_id = ${request.conversationId}
        `.pipe(Effect.mapError(storageError("advance conversation tail")));
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
        conversation_id,
        sequence,
        record_id,
        batch_id,
        record_json
      FROM effect_agent_canonical_records
      WHERE conversation_id = ${request.conversationId}
        AND sequence > ${request.fromSequenceExclusive}
      ORDER BY sequence
      LIMIT ${request.limit}
    `.pipe(Effect.mapError(storageError("read canonical records")));
    return yield* decodeRows(
      Schema.Array(RecordRow),
      "effect_agent_canonical_records",
      `${request.conversationId}>${request.fromSequenceExclusive}`,
      rows,
    );
  });

  const exportConversation = Effect.fn("SqliteJournal.exportConversation")(function* (
    conversationId: string,
  ) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const conversationRows = yield* sql<Record<string, unknown>>`
            SELECT
              conversation_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            FROM effect_agent_conversations
            WHERE conversation_id = ${conversationId}
          `.pipe(Effect.mapError(storageError("export conversation")));
          const conversation = yield* decodeSingleRow(
            Schema.Array(ConversationRow),
            "effect_agent_conversations",
            conversationId,
            conversationRows,
          );
          yield* failpoint("export:after-conversation-read");
          const batchRows = yield* sql<Record<string, unknown>>`
            SELECT
              conversation_id,
              batch_id,
              first_sequence,
              last_sequence,
              batch_digest,
              tail_digest,
              batch_json
            FROM effect_agent_canonical_batches
            WHERE conversation_id = ${conversationId}
            ORDER BY first_sequence
          `.pipe(Effect.mapError(storageError("export canonical batches")));
          const recordRows = yield* sql<Record<string, unknown>>`
            SELECT
              conversation_id,
              sequence,
              record_id,
              batch_id,
              record_json
            FROM effect_agent_canonical_records
            WHERE conversation_id = ${conversationId}
            ORDER BY sequence
          `.pipe(Effect.mapError(storageError("export canonical records")));
          const checkpointRows = yield* sql<Record<string, unknown>>`
            SELECT
              conversation_id,
              through_sequence,
              tail_digest,
              checkpoint_json
            FROM effect_agent_checkpoints
            WHERE conversation_id = ${conversationId}
            ORDER BY through_sequence
          `.pipe(Effect.mapError(storageError("export checkpoints")));

          return RawConversationExport.make({
            conversation,
            batches: yield* decodeRows(
              Schema.Array(BatchRow),
              "effect_agent_canonical_batches",
              conversationId,
              batchRows,
            ),
            records: yield* decodeRows(
              Schema.Array(RecordRow),
              "effect_agent_canonical_records",
              conversationId,
              recordRows,
            ),
            checkpoints: yield* decodeRows(
              Schema.Array(CheckpointRow),
              "effect_agent_checkpoints",
              conversationId,
              checkpointRows,
            ),
          });
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", (error) =>
          Effect.fail(storageError("export transaction")(error)),
        ),
      );
  });

  const saveCheckpoint = Effect.fn("SqliteJournal.saveCheckpoint")(function* (
    checkpoint: RawCheckpoint,
  ): Effect.fn.Return<void, CheckpointError> {
    if (
      checkpoint.conversationId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(checkpoint.checkpointJson) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* SqliteStorageError.make({
        operation: "save checkpoint",
        message: "Checkpoint identity or encoded JSON exceeds the SQLite storage bounds.",
      });
    }
    yield* withWriteTransaction("checkpoint transaction")(
      Effect.gen(function* () {
        const conversationRows = yield* sql<Record<string, unknown>>`
          SELECT
            conversation_id,
            created_at,
            tail_sequence,
            tail_digest,
            producer_epoch
          FROM effect_agent_conversations
          WHERE conversation_id = ${checkpoint.conversationId}
        `.pipe(Effect.mapError(storageError("read checkpoint tail")));
        const conversation = yield* decodeSingleRow(
          Schema.Array(ConversationRow),
          "effect_agent_conversations",
          checkpoint.conversationId,
          conversationRows,
        );
        if (checkpoint.throughSequence > conversation.tail_sequence) {
          return yield* SqliteCheckpointConflict.make({
            message:
              `Checkpoint sequence ${checkpoint.throughSequence} is after canonical tail ` +
              `${conversation.tail_sequence}.`,
          });
        }

        const checkpointRows = yield* sql<Record<string, unknown>>`
          SELECT
            conversation_id,
            through_sequence,
            tail_digest,
            checkpoint_json
          FROM effect_agent_checkpoints
          WHERE conversation_id = ${checkpoint.conversationId}
            AND through_sequence = ${checkpoint.throughSequence}
        `.pipe(Effect.mapError(storageError("read idempotent checkpoint")));
        const existing = yield* decodeRows(
          Schema.Array(CheckpointRow),
          "effect_agent_checkpoints",
          `${checkpoint.conversationId}/${checkpoint.throughSequence}`,
          checkpointRows,
        );
        if (existing.length > 1) {
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_checkpoints",
            rowKey: `${checkpoint.conversationId}/${checkpoint.throughSequence}`,
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
            conversation_id,
            through_sequence,
            tail_digest,
            checkpoint_json
          ) VALUES (
            ${checkpoint.conversationId},
            ${checkpoint.throughSequence},
            ${checkpoint.tailDigest},
            ${checkpoint.checkpointJson}
          )
        `.pipe(Effect.mapError(storageError("insert checkpoint")));
      }),
    );
  });

  const loadCheckpoint = Effect.fn("SqliteJournal.loadCheckpoint")(function* (
    conversationId: string,
    atOrBeforeSequence: CanonicalSequence,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        conversation_id,
        through_sequence,
        tail_digest,
        checkpoint_json
      FROM effect_agent_checkpoints
      WHERE conversation_id = ${conversationId}
        AND through_sequence <= ${atOrBeforeSequence}
      ORDER BY through_sequence DESC
      LIMIT 1
    `.pipe(Effect.mapError(storageError("load checkpoint")));
    return yield* decodeRows(
      Schema.Array(CheckpointRow),
      "effect_agent_checkpoints",
      `${conversationId}<=${atOrBeforeSequence}`,
      rows,
    );
  });

  const getTailDigestAt = Effect.fn("SqliteJournal.getTailDigestAt")(function* (
    conversationId: string,
    sequence: CanonicalSequence,
  ) {
    if (sequence === 0) {
      const conversations = yield* getConversation(conversationId);
      return conversations.length === 0
        ? []
        : [conversations[0].tail_sequence === 0 ? conversations[0].tail_digest : undefined].filter(
            (value): value is string => value !== undefined,
          );
    }
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        conversation_id,
        batch_id,
        first_sequence,
        last_sequence,
        batch_digest,
        tail_digest,
        batch_json
      FROM effect_agent_canonical_batches
      WHERE conversation_id = ${conversationId}
        AND last_sequence = ${sequence}
    `.pipe(Effect.mapError(storageError("read canonical digest at sequence")));
    const batches = yield* decodeRows(
      Schema.Array(BatchRow),
      "effect_agent_canonical_batches",
      `${conversationId}/${sequence}`,
      rows,
    );
    return batches.map((batch) => batch.tail_digest);
  });

  const scanStoredPayloads = Effect.fn("SqliteJournal.scanStoredPayloads")(function* () {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const conversations = yield* sql<Record<string, unknown>>`
            SELECT
              conversation_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            FROM effect_agent_conversations
            ORDER BY conversation_id
          `.pipe(Effect.mapError(storageError("scan conversations")));
          const batches = yield* sql<Record<string, unknown>>`
            SELECT
              conversation_id,
              batch_id,
              first_sequence,
              last_sequence,
              batch_digest,
              tail_digest,
              batch_json
            FROM effect_agent_canonical_batches
            ORDER BY conversation_id, first_sequence
          `.pipe(Effect.mapError(storageError("scan canonical batches")));
          const records = yield* sql<Record<string, unknown>>`
            SELECT
              conversation_id,
              sequence,
              record_id,
              batch_id,
              record_json
            FROM effect_agent_canonical_records
            ORDER BY conversation_id, sequence
          `.pipe(Effect.mapError(storageError("scan canonical records")));
          const checkpoints = yield* sql<Record<string, unknown>>`
            SELECT
              conversation_id,
              through_sequence,
              tail_digest,
              checkpoint_json
            FROM effect_agent_checkpoints
            ORDER BY conversation_id, through_sequence
          `.pipe(Effect.mapError(storageError("scan checkpoints")));
          return {
            conversations: yield* decodeRows(
              Schema.Array(ConversationRow),
              "effect_agent_conversations",
              "startup_scan",
              conversations,
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
      )
      .pipe(
        Effect.catchTag("SqlError", (error) =>
          Effect.fail(storageError("startup scan transaction")(error)),
        ),
      );
  });

  return {
    append,
    exportConversation,
    getConversation,
    getTailDigestAt,
    loadCheckpoint,
    materialize,
    read,
    saveCheckpoint,
    scanStoredPayloads,
    withWriteTransaction,
  } as const;
};

export type SqliteJournal = ReturnType<typeof makeJournal>;

export const initializeSqliteJournal = ensureCurrentStorage;
