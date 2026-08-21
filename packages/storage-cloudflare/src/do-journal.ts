import { CanonicalSequence, ProducerEpoch } from "@effect-agent/session";
import { SqliteMigrator } from "@effect/sql-sqlite-do";
import { Effect, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlError } from "effect/unstable/sql/SqlError";

import type { DoStorageFailpointError } from "./errors.ts";
import {
  DoAppendConflict,
  DoCheckpointConflict,
  DoFenceRejected,
  DoStorageCompatibilityError,
  DoStorageCorruptionError,
  DoStorageError,
  DoValueBoundExceeded,
  type DoStorageFailpointLocation,
} from "./errors.ts";
import { CurrentDoStorageVersion, doMigrations } from "./migrations.ts";

/**
 * Static schema ceiling for stored text columns. Writes are bounded in BYTES by the
 * configured `maxStoredValueBytes` (always ≤ 2,000,000); UTF-8 byte length is never smaller
 * than UTF-16 string length, so any value that passed the byte bound also passes this
 * decode-side character ceiling.
 */
const BoundedStoredText = Schema.String.check(Schema.isMaxLength(2_000_000));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const MAX_RECORDS_PER_CONVERSATION = 65_536;
const MAX_IDENTIFIER_LENGTH = 1_024;
/** Durable Object SQL storage allows at most 100 bound parameters per statement. */
const MAX_BOUND_PARAMETERS = 100;
const isSqlError = Schema.is(SqlError);

const storedTextBytes = (value: string): number => new TextEncoder().encode(value).byteLength;

const chunked = <A>(values: ReadonlyArray<A>, size: number): Array<ReadonlyArray<A>> => {
  const chunks: Array<ReadonlyArray<A>> = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
};

class DoMetaRow extends Schema.Class<DoMetaRow>("DoMetaRow")({
  value: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
}) {}

class DoNameRow extends Schema.Class<DoNameRow>("DoNameRow")({
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

export class RawRecord extends Schema.Class<RawRecord>(
  "@effect-agent/storage-cloudflare/RawRecord",
)({
  recordId: BoundedIdentifier,
  recordJson: BoundedStoredText,
}) {}

export class RawAppendRequest extends Schema.Class<RawAppendRequest>(
  "@effect-agent/storage-cloudflare/RawAppendRequest",
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
  "@effect-agent/storage-cloudflare/RawAppendResult",
)({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  replayed: Schema.Boolean,
  tailDigest: BoundedStoredText,
}) {}

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-cloudflare/RawReadRequest",
)({
  conversationId: BoundedIdentifier,
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class RawCheckpoint extends Schema.Class<RawCheckpoint>(
  "@effect-agent/storage-cloudflare/RawCheckpoint",
)({
  checkpointJson: BoundedStoredText,
  conversationId: BoundedIdentifier,
  tailDigest: BoundedStoredText,
  throughSequence: CanonicalSequence,
}) {}

export class RawConversationExport extends Schema.Class<RawConversationExport>(
  "@effect-agent/storage-cloudflare/RawConversationExport",
)({
  batches: Schema.Array(BatchRow),
  checkpoints: Schema.Array(CheckpointRow),
  conversation: ConversationRow,
  records: Schema.Array(RecordRow),
}) {}

type AppendError =
  | DoAppendConflict
  | DoFenceRejected
  | DoStorageCorruptionError
  | DoStorageError
  | DoStorageFailpointError
  | DoValueBoundExceeded;

type CheckpointError =
  | DoCheckpointConflict
  | DoStorageCorruptionError
  | DoStorageError
  | DoValueBoundExceeded;

type DoJournalFailpoint = (
  location: DoStorageFailpointLocation,
) => Effect.Effect<void, DoStorageFailpointError>;

const noFailpoint: DoJournalFailpoint = () => Effect.void;

const storageError =
  (operation: string) =>
  (error: SqlError): DoStorageError =>
    DoStorageError.make({
      cause: error,
      operation,
      message: error.message,
    });

/** Decode raw Durable Object SQLite rows against a Schema, reporting failures as typed corruption. */
export const decodeRows = Effect.fn("DoJournal.decodeRows")(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<ReadonlyArray<A>, DoStorageCorruptionError> =>
    Schema.decodeUnknownEffect(schema)(rows).pipe(
      Effect.mapError((error) =>
        DoStorageCorruptionError.make({
          table,
          rowKey,
          message: String(error),
        }),
      ),
    ),
);

/** Decode exactly one raw row against a Schema, reporting failures as typed corruption. */
export const decodeSingleRow = Effect.fn("DoJournal.decodeSingleRow")(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<A, DoStorageCorruptionError> =>
    decodeRows(schema, table, rowKey, rows).pipe(
      Effect.flatMap((decoded) =>
        decoded.length === 1
          ? Effect.succeed(decoded[0])
          : Effect.fail(
              DoStorageCorruptionError.make({
                table,
                rowKey,
                message: `Expected exactly one row but found ${decoded.length}.`,
              }),
            ),
      ),
    ),
);

const REQUIRED_TABLES = [
  "effect_agent_abort_intents",
  "effect_agent_approval_decisions",
  "effect_agent_attempts",
  "effect_agent_canonical_batches",
  "effect_agent_canonical_records",
  "effect_agent_checkpoints",
  "effect_agent_child_reservations",
  "effect_agent_child_settlements",
  "effect_agent_conversations",
  "effect_agent_meta",
  "effect_agent_settlement_reservations",
  "effect_agent_submission_ownership",
  "effect_agent_submissions",
  "effect_agent_unknown_resolutions",
] as const;

/**
 * Exact-or-fresh storage gate (DEPLOY-008) over `effect_agent_meta` instead of
 * `PRAGMA user_version` (unverified on Durable Object SQL storage; a meta table is portable
 * regardless). No WAL check (Durable Object storage owns durability and confirms writes
 * through output gates) and no busy timeout (a Durable Object has exactly one writer): the
 * Node machinery those served has no DC analogue and is deliberately absent.
 */
const ensureCurrentStorage = Effect.fn("DoJournal.ensureCurrentStorage")(function* (
  sql: SqlClient.SqlClient,
  failpoint: DoJournalFailpoint = noFailpoint,
  maxStoredValueBytes: number,
) {
  const metaTableRows = yield* sql<Record<string, unknown>>`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name = 'effect_agent_meta'
  `.pipe(Effect.mapError(storageError("read storage version table")));
  const metaTables = yield* decodeRows(
    Schema.Array(DoNameRow),
    "sqlite_master",
    "effect_agent_meta",
    metaTableRows,
  );

  if (metaTables.length === 0) {
    const existingRows = yield* sql<Record<string, unknown>>`
      SELECT name
      FROM sqlite_master
      WHERE type = 'table'
        AND name LIKE 'effect_agent_%'
      ORDER BY name
    `.pipe(Effect.mapError(storageError("inspect unversioned storage")));
    const existing = yield* decodeRows(
      Schema.Array(DoNameRow),
      "sqlite_master",
      "effect_agent_%",
      existingRows,
    );

    if (existing.length > 0) {
      return yield* DoStorageCompatibilityError.make({
        actualVersion: 0,
        supportedVersion: CurrentDoStorageVersion,
        message:
          "The Durable Object contains unversioned Effect Agent tables. Reset the development namespace explicitly; refusing to mutate ambiguous stored data.",
      });
    }

    yield* SqliteMigrator.run({ loader: doMigrations }).pipe(
      // SqliteMigrator depends on the generic client supplied by this adapter. The concrete
      // Durable Object client is kept at the outer Layer boundary.
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.mapError((error) =>
        DoStorageError.make({
          cause: error,
          operation: "initialize current storage",
          message: error.message,
        }),
      ),
    );
  } else {
    const versionRows = yield* sql<Record<string, unknown>>`
      SELECT value
      FROM effect_agent_meta
      WHERE key = 'storage_version'
    `.pipe(Effect.mapError(storageError("read storage version")));
    const version = yield* decodeSingleRow(
      Schema.Array(DoMetaRow),
      "effect_agent_meta",
      "storage_version",
      versionRows,
    );

    // The storage version must match EXACTLY. Older private-development versions fail
    // closed with reset guidance rather than being migrated, and newer versions fail closed
    // rather than being decoded incorrectly (DEPLOY-008).
    if (version.value !== String(CurrentDoStorageVersion)) {
      const actualVersion = Number.parseInt(version.value, 10);
      return yield* DoStorageCompatibilityError.make({
        actualVersion: Number.isSafeInteger(actualVersion) ? actualVersion : -1,
        supportedVersion: CurrentDoStorageVersion,
        message:
          `The Durable Object uses private-development storage version ${version.value}; ` +
          `this build supports exactly version ${CurrentDoStorageVersion}. ` +
          "Replace the development namespace explicitly; automatic stored-data migrations are not provided during private development.",
      });
    }
  }

  const requiredRows = yield* sql<Record<string, unknown>>`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name IN ${sql.in([...REQUIRED_TABLES])}
    ORDER BY name
  `.pipe(Effect.mapError(storageError("verify storage tables")));
  const required = yield* decodeRows(
    Schema.Array(DoNameRow),
    "sqlite_master",
    "required_tables",
    requiredRows,
  );
  if (required.length !== REQUIRED_TABLES.length) {
    return yield* DoStorageCompatibilityError.make({
      actualVersion: CurrentDoStorageVersion,
      supportedVersion: CurrentDoStorageVersion,
      message:
        "The Durable Object claims the current format but is missing required tables. Reset the corrupt private-development data.",
    });
  }

  return makeJournal(sql, failpoint, maxStoredValueBytes);
});

const makeJournal = (
  sql: SqlClient.SqlClient,
  failpoint: DoJournalFailpoint,
  maxStoredValueBytes: number,
) => {
  /** Typed pre-write refusal for any single value over the configured byte bound. */
  const checkValueBound = (
    operation: string,
    value: string,
  ): Effect.Effect<void, DoValueBoundExceeded> => {
    const actualBytes = storedTextBytes(value);
    return actualBytes > maxStoredValueBytes
      ? Effect.fail(
          DoValueBoundExceeded.make({
            actualBytes,
            maxBytes: maxStoredValueBytes,
            operation,
          }),
        )
      : Effect.void;
  };

  /**
   * Runs one journal write transaction on the Durable Object storage-backed
   * `withTransaction` (`ctx.storage.transaction()` under the hood). Within one Durable
   * Object there is exactly ONE writer, so the Node `BEGIN IMMEDIATE` + busy-retry +
   * `SqliteWriteContention` machinery has no analogue here and is deliberately absent.
   * Ownership-token and epoch checks still run INSIDE the transaction, so fencing atomicity
   * (DUR-006) is preserved identically.
   *
   * Journal write transactions are always top level: the Durable Object client rejects
   * nested transactions, so new journal operations must not wrap this helper inside another
   * transaction.
   */
  const withWriteTransaction =
    (operation: string) =>
    <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E | DoStorageError> =>
      sql.withTransaction(effect).pipe(
        Effect.mapError((error) => (isSqlError(error) ? storageError(operation)(error) : error)),
        Effect.withSpan("DoJournal.withWriteTransaction", { attributes: { operation } }),
      );

  const materialize = Effect.fn("DoJournal.materialize")(function* (
    conversationId: string,
    createdAt: string,
    emptyTailDigest: string,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<
    void,
    DoFenceRejected | DoStorageCorruptionError | DoStorageError | DoValueBoundExceeded
  > {
    if (conversationId.length > MAX_IDENTIFIER_LENGTH) {
      return yield* DoStorageError.make({
        operation: "materialize conversation",
        message: "Conversation identity exceeds the Durable Object storage bounds.",
      });
    }
    yield* checkValueBound("materialize conversation", emptyTailDigest);
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
          return yield* DoStorageCorruptionError.make({
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
          return yield* DoFenceRejected.make({
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

  const getConversation = Effect.fn("DoJournal.getConversation")(function* (
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

  const append = Effect.fn("DoJournal.append")(function* (
    request: RawAppendRequest,
  ): Effect.fn.Return<RawAppendResult, AppendError> {
    if (
      request.conversationId.length > MAX_IDENTIFIER_LENGTH ||
      request.batchId.length > MAX_IDENTIFIER_LENGTH ||
      request.records.some((record) => record.recordId.length > MAX_IDENTIFIER_LENGTH)
    ) {
      return yield* DoStorageError.make({
        operation: "append canonical batch",
        message: "Canonical identifiers exceed the Durable Object storage bounds.",
      });
    }
    // The platform's ~2 MB per-value limit, enforced typed BEFORE any write (plan §1.2).
    yield* checkValueBound("append canonical batch", request.batchJson);
    yield* checkValueBound("append canonical batch", request.batchDigest);
    yield* checkValueBound("append canonical batch", request.tailDigest);
    yield* Effect.forEach(
      request.records,
      (record) => checkValueBound("append canonical record", record.recordJson),
      { discard: true },
    );
    return yield* withWriteTransaction("append transaction")(
      Effect.gen(function* () {
        const recordIds = request.records.map((record) => record.recordId);
        if (new Set(recordIds).size !== recordIds.length) {
          return yield* DoAppendConflict.make({
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
          return yield* DoFenceRejected.make({
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
          return yield* DoStorageCorruptionError.make({
            table: "effect_agent_canonical_batches",
            rowKey: `${request.conversationId}/${request.batchId}`,
            message: "A canonical batch primary key returned more than one row.",
          });
        }
        if (batches.length === 1) {
          const existing = batches[0];
          if (existing.batch_digest !== request.batchDigest) {
            return yield* DoAppendConflict.make({
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
          return yield* DoAppendConflict.make({
            message:
              `Expected tail ${request.expectedTailSequence}/${request.expectedTailDigest} ` +
              `but found ${conversation.tail_sequence}/${conversation.tail_digest}.`,
            reason: "tail",
            actualTailSequence: conversation.tail_sequence,
            actualTailDigest: conversation.tail_digest,
          });
        }
        if (conversation.tail_sequence + request.records.length > MAX_RECORDS_PER_CONVERSATION) {
          return yield* DoStorageError.make({
            operation: "append canonical batch",
            message: `Conversation record limit ${MAX_RECORDS_PER_CONVERSATION} would be exceeded.`,
          });
        }

        // Chunked to respect the Durable Object platform's 100-bound-parameter statement
        // limit: a batch may carry up to 256 records.
        const existingRecords: Array<RecordRow> = [];
        for (const chunk of chunked(recordIds, MAX_BOUND_PARAMETERS - 10)) {
          const existingRecordRows = yield* sql<Record<string, unknown>>`
            SELECT
              conversation_id,
              sequence,
              record_id,
              batch_id,
              record_json
            FROM effect_agent_canonical_records
            WHERE conversation_id = ${request.conversationId}
              AND record_id IN ${sql.in([...chunk])}
            ORDER BY sequence
          `.pipe(Effect.mapError(storageError("check canonical record identities")));
          existingRecords.push(
            ...(yield* decodeRows(
              Schema.Array(RecordRow),
              "effect_agent_canonical_records",
              `${request.conversationId}/record_ids`,
              existingRecordRows,
            )),
          );
        }
        if (existingRecords.length > 0) {
          return yield* DoAppendConflict.make({
            message: `Canonical record ID ${existingRecords[0].record_id} already exists.`,
            reason: "record-identity",
          });
        }

        const firstSequence = yield* Schema.decodeUnknownEffect(CanonicalSequence)(
          conversation.tail_sequence + 1,
        ).pipe(
          Effect.mapError((error) =>
            DoStorageError.make({
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
            DoStorageError.make({
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

  const read = Effect.fn("DoJournal.read")(function* (request: RawReadRequest) {
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

  const exportConversation = Effect.fn("DoJournal.exportConversation")(function* (
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

  const saveCheckpoint = Effect.fn("DoJournal.saveCheckpoint")(function* (
    checkpoint: RawCheckpoint,
  ): Effect.fn.Return<void, CheckpointError> {
    if (checkpoint.conversationId.length > MAX_IDENTIFIER_LENGTH) {
      return yield* DoStorageError.make({
        operation: "save checkpoint",
        message: "Checkpoint identity exceeds the Durable Object storage bounds.",
      });
    }
    yield* checkValueBound("save checkpoint", checkpoint.checkpointJson);
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
          return yield* DoCheckpointConflict.make({
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
          return yield* DoStorageCorruptionError.make({
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
            return yield* DoCheckpointConflict.make({
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

  const loadCheckpoint = Effect.fn("DoJournal.loadCheckpoint")(function* (
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

  const getTailDigestAt = Effect.fn("DoJournal.getTailDigestAt")(function* (
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

  const scanStoredPayloads = Effect.fn("DoJournal.scanStoredPayloads")(function* () {
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
    checkValueBound,
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

export type DoJournal = ReturnType<typeof makeJournal>;

export const initializeDoJournal = ensureCurrentStorage;
