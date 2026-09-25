import { Effect, Schema } from "effect";
import { EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import { ThreadId } from "effect-agent/identifiers";
import {
  BatchId,
  CanonicalRecord,
  CanonicalSequence,
  Digest,
  ProducerEpoch,
} from "effect-agent/records";
import {
  MAX_THREAD_EXPORT_RECORDS,
  AppendConflict,
  CheckpointRejected,
  FenceRejected,
  ThreadNotMaterialized,
  type SaveRecoveryCheckpointRequest,
} from "effect-agent/thread-store";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import {
  makeRowDecoder,
  makeSqlQuery,
  SqlInteger,
  type Diagnostic,
  type SqlStorageErrors,
  type SqlStorageFailpoint,
  type SqlTransactions,
} from "./SqlStorage.ts";
import { canonicalRecordMetadata, indexCanonicalRecord } from "./SqlThreadNativeReads.ts";

export interface SqlJournalOptions<
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
> {
  readonly namespace?: string;
  readonly errors: SqlStorageErrors<S, C>;
  readonly transactions: SqlTransactions<S, W>;
  readonly hitFailpoint: SqlStorageFailpoint<F>;
}

const BoundedStoredText = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const MAX_RECORDS_PER_THREAD = MAX_THREAD_EXPORT_RECORDS;
const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
const MAX_STORED_TEXT_BYTES = 16 * 1024 * 1024;
const MAX_IDENTIFIER_LENGTH = 1_024;

const storedTextBytes = (value: string): number => new TextEncoder().encode(value).byteLength;

class ThreadRow extends Schema.Class<ThreadRow>("ThreadRow")({
  thread_id: BoundedIdentifier,
  created_at: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  producer_epoch: SqlInteger.pipe(Schema.decodeTo(ProducerEpoch)),
  tail_digest: BoundedStoredText,
  tail_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

class BatchRow extends Schema.Class<BatchRow>("BatchRow")({
  batch_digest: BoundedStoredText,
  batch_id: BoundedIdentifier,
  batch_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  first_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  last_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  tail_digest: BoundedStoredText,
}) {}

class RecordRow extends Schema.Class<RecordRow>("RecordRow")({
  batch_id: BoundedIdentifier,
  thread_id: BoundedIdentifier,
  record_id: BoundedIdentifier,
  record_json: BoundedStoredText,
  sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

class CheckpointRow extends Schema.Class<CheckpointRow>("CheckpointRow")({
  checkpoint_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  tail_digest: BoundedStoredText,
  through_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
}) {}

export class RawRecord extends Schema.Class<RawRecord>("@effect-agent/storage-sql/RawRecord")({
  recordId: BoundedIdentifier,
  recordJson: BoundedStoredText,
}) {}

export class RawAppendRequest extends Schema.Class<RawAppendRequest>(
  "@effect-agent/storage-sql/RawAppendRequest",
)({
  batchDigest: BoundedStoredText,
  batchId: BatchId.check(Schema.isMaxLength(MAX_IDENTIFIER_LENGTH)),
  batchJson: BoundedStoredText,
  threadId: ThreadId.check(Schema.isMaxLength(MAX_IDENTIFIER_LENGTH)),
  expectedTailDigest: BoundedStoredText,
  expectedTailSequence: CanonicalSequence,
  producerEpoch: ProducerEpoch,
  records: Schema.NonEmptyArray(RawRecord).check(Schema.isMaxLength(256)),
  tailDigest: BoundedStoredText,
}) {}

export class RawAppendResult extends Schema.Class<RawAppendResult>(
  "@effect-agent/storage-sql/RawAppendResult",
)({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  replayed: Schema.Boolean,
  tailDigest: BoundedStoredText,
}) {}

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-sql/RawReadRequest",
)({
  threadId: ThreadId.check(Schema.isMaxLength(MAX_IDENTIFIER_LENGTH)),
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class RawCheckpoint extends Schema.Class<RawCheckpoint>(
  "@effect-agent/storage-sql/RawCheckpoint",
)({
  checkpointJson: BoundedStoredText,
  threadId: ThreadId.check(Schema.isMaxLength(MAX_IDENTIFIER_LENGTH)),
  tailDigest: BoundedStoredText,
  throughSequence: CanonicalSequence,
}) {}

export class RawThreadExport extends Schema.Class<RawThreadExport>(
  "@effect-agent/storage-sql/RawThreadExport",
)({
  thread: ThreadRow,
  records: Schema.Array(RecordRow),
}) {}

/** Construct journal operations over an already initialized database. */
export const makeSqlJournal = Effect.fn("SqlJournal.make")(function* <
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
>(options: SqlJournalOptions<S, C, W, F>) {
  const sql = yield* SqlClient.SqlClient;
  const { table: relation, execute } = yield* makeSqlQuery(options.namespace);
  const failpoint = options.hitFailpoint;
  const { withReadTransaction, withWriteTransaction } = options.transactions;
  const { decodeRows, decodeSingleRow } = makeRowDecoder(options.errors.corruption);

  const storageError =
    (operation: string) =>
    (error: SqlError): S =>
      options.errors.storage({ operation, message: error.message, cause: error });

  const materialize = Effect.fn("SqlJournal.materialize")(function* (
    threadId: ThreadId,
    createdAt: string,
    emptyTailDigest: string,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<void, FenceRejected | C | S | W> {
    if (
      threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(emptyTailDigest) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* options.errors.storage({
        operation: "materialize thread",
        message: "Thread identity or initial digest exceeds the SQL storage bounds.",
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
          FROM ${relation("effect_agent_threads")}
          WHERE thread_id = ${threadId}
        `.pipe(execute, Effect.mapError(storageError("read materialized thread")));

        const existing = yield* decodeRows(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          threadId,
          existingRows,
        );

        if (existing.length > 1) {
          return yield* options.errors.corruption({
            table: "effect_agent_threads",
            rowKey: threadId,
            message: "A thread primary key returned more than one row.",
          });
        }
        if (existing.length === 0) {
          yield* sql`
            INSERT INTO ${relation("effect_agent_threads")} (
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
          `.pipe(execute, Effect.mapError(storageError("materialize thread")));

          return;
        }
        if (producerEpoch < existing[0].producer_epoch) {
          return yield* FenceRejected.make({
            threadId,
            attemptedEpoch: producerEpoch,
            actualEpoch: existing[0].producer_epoch,
          });
        }
        if (producerEpoch > existing[0].producer_epoch) {
          yield* sql`
            UPDATE ${relation("effect_agent_threads")}
            SET producer_epoch = ${producerEpoch}
            WHERE thread_id = ${threadId}
          `.pipe(execute, Effect.mapError(storageError("advance materialization epoch")));
        }
      }),
    );
  });

  const getThread = Effect.fn("SqlJournal.getThread")(function* (threadId: string) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        created_at,
        tail_sequence,
        tail_digest,
        producer_epoch
      FROM ${relation("effect_agent_threads")}
      WHERE thread_id = ${threadId}
    `.pipe(execute, Effect.mapError(storageError("read thread")));

    return yield* decodeRows(Schema.Array(ThreadRow), "effect_agent_threads", threadId, rows);
  });

  const append = Effect.fn("SqlJournal.append")(function* (
    request: RawAppendRequest,
  ): Effect.fn.Return<RawAppendResult, AppendConflict | FenceRejected | C | S | F | W> {
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
      return yield* options.errors.storage({
        operation: "append canonical batch",
        message: "Canonical identifiers or encoded JSON exceed the SQL storage bounds.",
      });
    }

    return yield* withWriteTransaction("append transaction")(
      Effect.gen(function* () {
        const recordIds = request.records.map((record) => record.recordId);

        if (new Set(recordIds).size !== recordIds.length) {
          return yield* AppendConflict.make({
            threadId: request.threadId,
            batchId: request.batchId,
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
          FROM ${relation("effect_agent_threads")}
          WHERE thread_id = ${request.threadId}
        `.pipe(execute, Effect.mapError(storageError("read append tail")));

        const thread = yield* decodeSingleRow(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          request.threadId,
          threadRows,
        );

        if (request.producerEpoch !== thread.producer_epoch) {
          return yield* FenceRejected.make({
            threadId: request.threadId,
            attemptedEpoch: request.producerEpoch,
            actualEpoch: thread.producer_epoch,
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
          FROM ${relation("effect_agent_canonical_batches")}
          WHERE thread_id = ${request.threadId}
            AND batch_id = ${request.batchId}
        `.pipe(execute, Effect.mapError(storageError("read idempotent batch")));

        const batches = yield* decodeRows(
          Schema.Array(BatchRow),
          "effect_agent_canonical_batches",
          `${request.threadId}/${request.batchId}`,
          batchRows,
        );

        if (batches.length > 1) {
          return yield* options.errors.corruption({
            table: "effect_agent_canonical_batches",
            rowKey: `${request.threadId}/${request.batchId}`,
            message: "A canonical batch primary key returned more than one row.",
          });
        }
        if (batches.length === 1) {
          const existing = batches[0];

          if (existing.batch_digest !== request.batchDigest) {
            return yield* AppendConflict.make({
              threadId: request.threadId,
              batchId: request.batchId,
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
          return yield* AppendConflict.make({
            threadId: request.threadId,
            batchId: request.batchId,
            reason: "tail",
            ...(Schema.is(Digest)(thread.tail_digest)
              ? { actualTailSequence: thread.tail_sequence, actualTailDigest: thread.tail_digest }
              : {}),
          });
        }
        if (thread.tail_sequence + request.records.length > MAX_RECORDS_PER_THREAD) {
          return yield* options.errors.storage({
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
          FROM ${relation("effect_agent_canonical_records")}
          WHERE thread_id = ${request.threadId}
            AND record_id IN ${sql.in(recordIds)}
          ORDER BY sequence
        `.pipe(execute, Effect.mapError(storageError("check canonical record identities")));

        const existingRecords = yield* decodeRows(
          Schema.Array(RecordRow),
          "effect_agent_canonical_records",
          `${request.threadId}/record_ids`,
          existingRecordRows,
        );

        if (existingRecords.length > 0) {
          return yield* AppendConflict.make({
            threadId: request.threadId,
            batchId: request.batchId,
            reason: "record-identity",
          });
        }

        const firstSequence = yield* Schema.decodeEffect(CanonicalSequence)(
          thread.tail_sequence + 1,
        ).pipe(
          Effect.mapError((error) =>
            options.errors.storage({
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
            options.errors.storage({
              cause: error,
              operation: "append canonical batch",
              message: error.message,
            }),
          ),
        );

        yield* sql`
          INSERT INTO ${relation("effect_agent_canonical_batches")} (
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
        `.pipe(execute, Effect.mapError(storageError("insert canonical batch")));
        yield* failpoint("append:after-batch-insert");

        yield* Effect.forEach(
          request.records,
          (record, index) =>
            Effect.gen(function* () {
              const canonical = yield* Schema.decodeEffect(Schema.fromJsonString(CanonicalRecord))(
                record.recordJson,
              ).pipe(
                Effect.mapError((error) =>
                  options.errors.corruption({
                    table: "effect_agent_canonical_records",
                    rowKey: record.recordId,
                    message: error.message,
                  }),
                ),
              );

              yield* sql`
                  INSERT INTO ${relation("effect_agent_canonical_records")} (
                    thread_id,
                    sequence,
                    record_id,
                    batch_id,
                    record_json${sql.onDialectOrElse({ pg: () => sql`, read_metadata`, orElse: () => sql`` })}
                  ) VALUES (
                    ${request.threadId},
                    ${firstSequence + index},
                    ${record.recordId},
                    ${request.batchId},
                    ${record.recordJson}${sql.onDialectOrElse({ pg: () => sql`, ${canonicalRecordMetadata(canonical)}::jsonb`, orElse: () => sql`` })}
                  )
                `.pipe(execute, Effect.mapError(storageError("insert canonical record")));

              yield* indexCanonicalRecord(request.threadId, canonical, options.namespace).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.mapError(storageError("index canonical record")),
              );
              yield* failpoint("append:after-record-insert");
            }),
          { discard: true },
        );

        yield* sql`
          UPDATE ${relation("effect_agent_threads")}
          SET
            tail_sequence = ${lastSequence},
            tail_digest = ${request.tailDigest},
            producer_epoch = ${request.producerEpoch}
          WHERE thread_id = ${request.threadId}
        `.pipe(execute, Effect.mapError(storageError("advance thread tail")));
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

  const read = Effect.fn("SqlJournal.read")(function* (request: RawReadRequest) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        sequence,
        record_id,
        batch_id,
        record_json
      FROM ${relation("effect_agent_canonical_records")}
      WHERE thread_id = ${request.threadId}
        AND sequence > ${request.fromSequenceExclusive}
      ORDER BY sequence
      LIMIT ${request.limit}
    `.pipe(execute, Effect.mapError(storageError("read canonical records")));

    return yield* decodeRows(
      Schema.Array(RecordRow),
      "effect_agent_canonical_records",
      `${request.threadId}>${request.fromSequenceExclusive}`,
      rows,
    );
  });

  const exportThread = Effect.fn("SqlJournal.exportThread")(function* (threadId: ThreadId) {
    return yield* withReadTransaction("export transaction")(
      Effect.gen(function* () {
        const threadRows = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            FROM ${relation("effect_agent_threads")}
            WHERE thread_id = ${threadId}
          `.pipe(execute, Effect.mapError(storageError("export thread")));

        const thread = yield* decodeSingleRow(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          threadId,
          threadRows,
        );

        yield* failpoint("export:after-thread-read");

        if (thread.tail_sequence > MAX_RECORDS_PER_THREAD)
          return yield* options.errors.storage({
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
            return yield* options.errors.corruption({
              table: "effect_agent_canonical_records",
              rowKey: threadId,
              message: "The exported canonical prefix is not contiguous through its captured tail.",
            });
          }
          records.push(...page);
          afterSequence = page[page.length - 1].sequence;
        }

        const beyondTail =
          yield* sql`SELECT sequence FROM ${relation("effect_agent_canonical_records")} WHERE thread_id=${threadId} AND sequence > ${thread.tail_sequence} LIMIT 1`.pipe(
            execute,
            Effect.mapError(storageError("verify export tail")),
          );

        if (beyondTail.length !== 0)
          return yield* options.errors.corruption({
            table: "effect_agent_canonical_records",
            rowKey: threadId,
            message: "Canonical records exist beyond the captured thread tail.",
          });

        return RawThreadExport.make({ thread, records });
      }),
    );
  });

  const saveCheckpoint = Effect.fn("SqlJournal.saveCheckpoint")(function* (
    checkpoint: RawCheckpoint,
  ): Effect.fn.Return<void, CheckpointRejected | C | S | W> {
    if (
      checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(checkpoint.checkpointJson) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* options.errors.storage({
        operation: "save checkpoint",
        message: "Checkpoint identity or encoded JSON exceeds the SQL storage bounds.",
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
          FROM ${relation("effect_agent_threads")}
          WHERE thread_id = ${checkpoint.threadId}
        `.pipe(execute, Effect.mapError(storageError("read checkpoint tail")));

        const thread = yield* decodeSingleRow(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          checkpoint.threadId,
          threadRows,
        );

        if (checkpoint.throughSequence > thread.tail_sequence) {
          return yield* CheckpointRejected.make({
            threadId: checkpoint.threadId,
            reason: "digest-mismatch",
          });
        }

        const checkpointRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            through_sequence,
            tail_digest,
            checkpoint_json
          FROM ${relation("effect_agent_checkpoints")}
          WHERE thread_id = ${checkpoint.threadId}
            AND through_sequence = ${checkpoint.throughSequence}
        `.pipe(execute, Effect.mapError(storageError("read idempotent checkpoint")));

        const existing = yield* decodeRows(
          Schema.Array(CheckpointRow),
          "effect_agent_checkpoints",
          `${checkpoint.threadId}/${checkpoint.throughSequence}`,
          checkpointRows,
        );

        if (existing.length > 1) {
          return yield* options.errors.corruption({
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
            return yield* CheckpointRejected.make({
              threadId: checkpoint.threadId,
              reason: "digest-mismatch",
            });
          }

          return;
        }

        yield* sql`
          INSERT INTO ${relation("effect_agent_checkpoints")} (
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
        `.pipe(execute, Effect.mapError(storageError("insert checkpoint")));
      }),
    );
  });

  const saveRecoveryCheckpoint = Effect.fn("SqlJournal.saveRecoveryCheckpoint")(function* (
    request: SaveRecoveryCheckpointRequest,
    checkpointJson: string,
  ) {
    const { checkpoint } = request;

    if (
      checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(checkpointJson) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* options.errors.storage({
        operation: "save recovery checkpoint",
        message: "Checkpoint identity or encoded JSON exceeds the SQL storage bounds.",
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
          INSERT INTO ${relation("effect_agent_recovery_checkpoints")} (thread_id, through_sequence, tail_digest, checkpoint_json)
          VALUES (${checkpoint.threadId}, ${checkpoint.throughSequence}, ${checkpoint.tailDigest}, ${checkpointJson})
          ON CONFLICT (thread_id) DO UPDATE SET
            through_sequence = excluded.through_sequence,
            tail_digest = excluded.tail_digest,
            checkpoint_json = excluded.checkpoint_json
          WHERE excluded.through_sequence >= ${relation("effect_agent_recovery_checkpoints")}.through_sequence
        `.pipe(execute, Effect.mapError(storageError("save recovery checkpoint")));
      }),
    );
    yield* failpoint("save-recovery-checkpoint:after");
  });

  const loadRecoveryCheckpoint = Effect.fn("SqlJournal.loadRecoveryCheckpoint")(function* (
    threadId: string,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT thread_id, through_sequence, tail_digest, checkpoint_json
      FROM ${relation("effect_agent_recovery_checkpoints")}
      WHERE thread_id = ${threadId}
    `.pipe(execute, Effect.mapError(storageError("load recovery checkpoint")));

    return yield* decodeRows(
      Schema.Array(CheckpointRow),
      "effect_agent_recovery_checkpoints",
      threadId,
      rows,
    );
  });

  const loadCheckpoint = Effect.fn("SqlJournal.loadCheckpoint")(function* (
    threadId: string,
    atOrBeforeSequence: CanonicalSequence,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        through_sequence,
        tail_digest,
        checkpoint_json
      FROM ${relation("effect_agent_checkpoints")}
      WHERE thread_id = ${threadId}
        AND through_sequence <= ${atOrBeforeSequence}
      ORDER BY through_sequence DESC
      LIMIT 1
    `.pipe(execute, Effect.mapError(storageError("load checkpoint")));

    return yield* decodeRows(
      Schema.Array(CheckpointRow),
      "effect_agent_checkpoints",
      `${threadId}<=${atOrBeforeSequence}`,
      rows,
    );
  });

  const getTailDigestAt = Effect.fn("SqlJournal.getTailDigestAt")(function* (
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
      FROM ${relation("effect_agent_canonical_batches")}
      WHERE thread_id = ${threadId}
        AND last_sequence = ${sequence}
    `.pipe(execute, Effect.mapError(storageError("read canonical digest at sequence")));

    const batches = yield* decodeRows(
      Schema.Array(BatchRow),
      "effect_agent_canonical_batches",
      `${threadId}/${sequence}`,
      rows,
    );

    return batches.map((batch) => batch.tail_digest);
  });

  const scanStoredPayloads = Effect.fn("SqlJournal.scanStoredPayloads")(function* () {
    return yield* withReadTransaction("startup scan transaction")(
      Effect.gen(function* () {
        const threads = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            FROM ${relation("effect_agent_threads")}
            ORDER BY thread_id
          `.pipe(execute, Effect.mapError(storageError("scan threads")));

        const batches = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              batch_id,
              first_sequence,
              last_sequence,
              batch_digest,
              tail_digest,
              batch_json
            FROM ${relation("effect_agent_canonical_batches")}
            ORDER BY thread_id, first_sequence
          `.pipe(execute, Effect.mapError(storageError("scan canonical batches")));

        const records = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              sequence,
              record_id,
              batch_id,
              record_json
            FROM ${relation("effect_agent_canonical_records")}
            ORDER BY thread_id, sequence
          `.pipe(execute, Effect.mapError(storageError("scan canonical records")));

        const checkpoints = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              through_sequence,
              tail_digest,
              checkpoint_json
            FROM ${relation("effect_agent_checkpoints")}
            ORDER BY thread_id, through_sequence
          `.pipe(execute, Effect.mapError(storageError("scan checkpoints")));

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
    isTransactionFailure: options.transactions.isTransactionFailure,
  } as const;
});

export type SqlJournal<
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
> = Effect.Success<ReturnType<typeof makeSqlJournal<S, C, W, F>>>;
