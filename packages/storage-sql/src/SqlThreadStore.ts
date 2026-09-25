import { Clock, Context, Crypto, Effect, Option, Ref, Schema, Stream } from "effect";
import { digestCanonicalBatch, EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import {
  CanonicalBatch,
  CanonicalRecord,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ObservationOffset,
} from "effect-agent/records";
import {
  AppendConflict,
  AppendResult,
  CheckpointRejected,
  ThreadCheckpoint,
  ThreadExport,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadObservation,
  ThreadReadRequest,
  ThreadStore,
  type ThreadCheckpoints,
  ThreadStoreError,
  ThreadTail,
  ThreadTailRequest,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  SaveRecoveryCheckpointRequest,
  MAX_THREAD_EXPORT_RECORDS,
  type ThreadRecoveryCheckpoints,
} from "effect-agent/thread-store";

import { RawAppendRequest, RawCheckpoint, RawReadRequest, type SqlJournal } from "./SqlJournal.ts";
import type { Diagnostic, SqlStorageErrors, SqlStorageFailpoint } from "./SqlStorage.ts";
import type { SqlStorageFailpointLocation } from "./SqlStorageFailpoint.ts";
import { makeSelectedReads } from "./SqlThreadNativeReads.ts";

export interface SqlThreadStoreOptions<
  S extends Diagnostic,
  C extends Diagnostic,
  F extends Diagnostic,
> {
  readonly errors: SqlStorageErrors<S, C>;
  readonly hitFailpoint: SqlStorageFailpoint<F>;
  readonly observationPollInterval: number;
  readonly verifyOnOpen: boolean;
  readonly offsetPrefix: string;
}

/** Canonical ThreadStore behavior over an adapter-initialized SQL journal. */
export const makeSqlThreadStore = Effect.fn("SqlThreadStore.make")(function* <
  S extends Diagnostic,
  C extends Diagnostic,
  W extends Diagnostic,
  F extends Diagnostic,
>(journal: SqlJournal<S, C, W, F>, options: SqlThreadStoreOptions<S, C, F>) {
  const OffsetText = Schema.String.check(Schema.isMaxLength(4 * 1024));
  const OFFSET_PREFIX = options.offsetPrefix;
  const ZERO_CANONICAL_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
  const isFenceRejected = Schema.is(FenceRejected);
  const isAppendConflict = Schema.is(AppendConflict);
  const isCheckpointRejected = Schema.is(CheckpointRejected);

  const storeError = (operation: string, error: { readonly message: string }) =>
    ThreadStoreError.make({
      cause: error,
      operation,
      message: error.message,
    });

  const schemaStoreError = (operation: string, error: { readonly message: string }) =>
    ThreadStoreError.make({
      cause: error,
      operation,
      message: error.message,
    });

  const makeOffset = Effect.fn("SqlThreadStore.makeOffset")(function* (
    threadId: ThreadMaterialization["threadId"],
    sequence: number,
  ): Effect.fn.Return<ObservationOffset, ThreadStoreError> {
    return yield* Schema.decodeEffect(CanonicalSequence)(sequence).pipe(
      Effect.flatMap((validatedSequence) =>
        Schema.decodeEffect(ObservationOffset)(
          `${OFFSET_PREFIX}${encodeURIComponent(threadId)}:${validatedSequence}`,
        ),
      ),
      Effect.mapError((error) => schemaStoreError("encode observation offset", error)),
    );
  });

  const parseOffset = Effect.fn("SqlThreadStore.parseOffset")(function* (
    threadId: ThreadMaterialization["threadId"],
    offset: ObservationOffset | undefined,
  ): Effect.fn.Return<CanonicalSequence, ThreadStoreError> {
    if (offset === undefined) return ZERO_CANONICAL_SEQUENCE;

    const text = yield* Schema.decodeEffect(OffsetText)(offset).pipe(
      Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
    );

    const threadPrefix = `${OFFSET_PREFIX}${encodeURIComponent(threadId)}:`;

    if (!text.startsWith(threadPrefix)) {
      return yield* ThreadStoreError.make({
        operation: "decode observation offset",
        message:
          "The observation offset belongs to a different adapter, storage version, or Thread.",
      });
    }
    const sequenceText = text.slice(threadPrefix.length);

    if (!/^(0|[1-9][0-9]*)$/.test(sequenceText)) {
      return yield* ThreadStoreError.make({
        operation: "decode observation offset",
        message: "The observation offset is malformed.",
      });
    }

    return yield* Schema.decodeEffect(CanonicalSequence)(Number(sequenceText)).pipe(
      Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
    );
  });

  const encodeCanonicalRecord = Effect.fn("SqlThreadStore.encodeCanonicalRecord")(function* (
    record: CanonicalRecord,
  ): Effect.fn.Return<string, ThreadStoreError> {
    return yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalRecord))(record).pipe(
      Effect.mapError((error) => schemaStoreError("encode canonical record", error)),
    );
  });

  const encodeCanonicalBatch = Effect.fn("SqlThreadStore.encodeCanonicalBatch")(function* (
    batch: CanonicalBatch,
  ): Effect.fn.Return<string, ThreadStoreError> {
    return yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalBatch))(batch).pipe(
      Effect.mapError((error) => schemaStoreError("encode canonical batch", error)),
    );
  });

  const encodeCheckpoint = Effect.fn("SqlThreadStore.encodeCheckpoint")(function* (
    checkpoint: ThreadCheckpoint,
  ): Effect.fn.Return<string, ThreadStoreError> {
    return yield* Schema.encodeEffect(Schema.fromJsonString(ThreadCheckpoint))(checkpoint).pipe(
      Effect.mapError((error) => schemaStoreError("encode checkpoint", error)),
    );
  });

  const decodeEnvelope = Effect.fn("SqlThreadStore.decodeEnvelope")(function* (row: {
    readonly batch_id: string;
    readonly thread_id: string;
    readonly record_json: string;
    readonly sequence: CanonicalSequence;
  }) {
    const record = yield* Schema.decodeEffect(Schema.fromJsonString(CanonicalRecord))(
      row.record_json,
    ).pipe(
      Effect.mapError((error) =>
        ThreadStoreError.make({
          operation: "decode canonical record",
          message: error.message,
        }),
      ),
    );

    const threadId = yield* Schema.decodeEffect(CanonicalRecordEnvelope.fields.threadId)(
      row.thread_id,
    ).pipe(Effect.mapError((error) => schemaStoreError("decode thread identity", error)));

    const offset = yield* makeOffset(threadId, row.sequence);

    const batchId = yield* Schema.decodeEffect(CanonicalRecordEnvelope.fields.batchId)(
      row.batch_id,
    ).pipe(Effect.mapError((error) => schemaStoreError("decode batch identity", error)));

    return CanonicalRecordEnvelope.make({
      threadId,
      batchId,
      sequence: row.sequence,
      offset,
      record,
    });
  });

  const decodeCheckpoint = Effect.fn("SqlThreadStore.decodeCheckpoint")(function* (
    checkpointJson: string,
  ): Effect.fn.Return<ThreadCheckpoint, ThreadStoreError> {
    return yield* Schema.decodeEffect(Schema.fromJsonString(ThreadCheckpoint))(checkpointJson).pipe(
      Effect.mapError((error) => schemaStoreError("decode checkpoint", error)),
    );
  });

  const requireThread = Effect.fn("SqlThreadStore.requireThread")(function* (
    journal: SqlJournal<S, C, W, F>,
    threadId: ThreadMaterialization["threadId"],
  ) {
    const rows = yield* journal
      .getThread(threadId)
      .pipe(Effect.mapError((error) => storeError("read thread", error)));

    if (rows.length === 0) {
      return yield* ThreadNotMaterialized.make({ threadId });
    }

    return rows[0];
  });

  const tailDigestAt = Effect.fn("SqlThreadStore.tailDigestAt")(function* (
    journal: SqlJournal<S, C, W, F>,
    threadId: ThreadMaterialization["threadId"],
    sequence: CanonicalSequence,
  ) {
    if (sequence === 0) return EMPTY_TAIL_DIGEST;

    const digests = yield* journal
      .getTailDigestAt(threadId, sequence)
      .pipe(Effect.mapError((error) => storeError("read checkpoint digest", error)));

    if (digests.length !== 1) {
      return yield* CheckpointRejected.make({
        threadId,
        reason: "digest-mismatch",
      });
    }

    return yield* Schema.decodeEffect(Digest)(digests[0]).pipe(
      Effect.mapError((error) => schemaStoreError("decode checkpoint digest", error)),
    );
  });

  const groupByKey = <A>(
    rows: ReadonlyArray<A>,
    key: (row: A) => string,
  ): ReadonlyMap<string, ReadonlyArray<A>> => {
    const grouped = new Map<string, Array<A>>();

    for (const row of rows) {
      const existing = grouped.get(key(row));

      if (existing === undefined) {
        grouped.set(key(row), [row]);
      } else {
        existing.push(row);
      }
    }

    return grouped;
  };

  /**
   * Opt-in integrity audit (`verifyOnOpen`) of canonical payloads, their digest chains and generic
   * projection checkpoints. Disposable recovery checkpoints are validated when loaded. Routine opens
   * skip this scan: per-operation Schema decoding fails clearly on corrupt canonical rows.
   */
  const decodeStartupPayloads = Effect.fn("SqlThreadStore.decodeStartupPayloads")(function* (
    journal: SqlJournal<S, C, W, F>,
    crypto: Crypto.Crypto,
  ) {
    const stored = yield* journal.scanStoredPayloads();

    const batches = yield* Effect.forEach(stored.batches, (batch) =>
      Schema.decodeEffect(Schema.fromJsonString(CanonicalBatch))(batch.batch_json).pipe(
        Effect.map((decoded) => ({ decoded, row: batch })),
        Effect.mapError((error) =>
          options.errors.corruption({
            table: "effect_agent_canonical_batches",
            rowKey: `${batch.thread_id}/${batch.batch_id}`,
            message: error.message,
          }),
        ),
      ),
    );

    const records = yield* Effect.forEach(stored.records, (record) =>
      Schema.decodeEffect(Schema.fromJsonString(CanonicalRecord))(record.record_json).pipe(
        Effect.map((decoded) => ({ decoded, row: record })),
        Effect.mapError((error) =>
          options.errors.corruption({
            table: "effect_agent_canonical_records",
            rowKey: `${record.thread_id}/${record.sequence}`,
            message: error.message,
          }),
        ),
      ),
    );

    const checkpoints = yield* Effect.forEach(stored.checkpoints, (checkpoint) =>
      Schema.decodeEffect(Schema.fromJsonString(ThreadCheckpoint))(checkpoint.checkpoint_json).pipe(
        Effect.map((decoded) => ({ decoded, row: checkpoint })),
        Effect.mapError((error) =>
          options.errors.corruption({
            table: "effect_agent_checkpoints",
            rowKey: `${checkpoint.thread_id}/${checkpoint.through_sequence}`,
            message: error.message,
          }),
        ),
      ),
    );

    const batchesByThread = groupByKey(batches, ({ row }) => row.thread_id);
    const recordsByThread = groupByKey(records, ({ row }) => row.thread_id);
    const checkpointsByThread = groupByKey(checkpoints, ({ row }) => row.thread_id);
    const materializedIds = new Set(stored.threads.map((thread) => thread.thread_id));

    for (const thread of stored.threads) {
      const threadBatches = batchesByThread.get(thread.thread_id) ?? [];
      const threadRecords = recordsByThread.get(thread.thread_id) ?? [];
      const threadCheckpoints = checkpointsByThread.get(thread.thread_id) ?? [];
      const recordsByBatch = groupByKey(threadRecords, ({ row }) => row.batch_id);
      let previousDigest = EMPTY_TAIL_DIGEST;
      let expectedSequence = 1;
      const tailDigests = new Map<number, string>([[0, EMPTY_TAIL_DIGEST]]);

      for (const { decoded: canonicalBatch, row: batchRow } of threadBatches) {
        const key = `${batchRow.thread_id}/${batchRow.batch_id}`;

        if (
          canonicalBatch.batchId !== batchRow.batch_id ||
          batchRow.first_sequence !== expectedSequence ||
          batchRow.last_sequence !== batchRow.first_sequence + canonicalBatch.records.length - 1
        ) {
          return yield* options.errors.corruption({
            table: "effect_agent_canonical_batches",
            rowKey: key,
            message: "Canonical batch identity, sequence, or record count is inconsistent.",
          });
        }

        const digest = yield* digestCanonicalBatch(previousDigest, canonicalBatch).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((error) =>
            options.errors.corruption({
              table: "effect_agent_canonical_batches",
              rowKey: key,
              message: error.message,
            }),
          ),
        );

        if (batchRow.batch_digest !== digest || batchRow.tail_digest !== digest) {
          return yield* options.errors.corruption({
            table: "effect_agent_canonical_batches",
            rowKey: key,
            message: "Canonical batch digest does not match its decoded content and prior tail.",
          });
        }

        const batchRecords = recordsByBatch.get(batchRow.batch_id) ?? [];

        if (batchRecords.length !== canonicalBatch.records.length) {
          return yield* options.errors.corruption({
            table: "effect_agent_canonical_records",
            rowKey: key,
            message: "Canonical batch and record-table counts differ.",
          });
        }
        for (let index = 0; index < canonicalBatch.records.length; index++) {
          const expectedRecord = canonicalBatch.records[index];
          const storedRecord = batchRecords[index];

          const expectedJson = yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalRecord))(
            expectedRecord,
          ).pipe(
            Effect.mapError((error) =>
              options.errors.corruption({
                table: "effect_agent_canonical_batches",
                rowKey: key,
                message: error.message,
              }),
            ),
          );

          const storedJson = yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalRecord))(
            storedRecord.decoded,
          ).pipe(
            Effect.mapError((error) =>
              options.errors.corruption({
                table: "effect_agent_canonical_records",
                rowKey: `${key}/${storedRecord.row.sequence}`,
                message: error.message,
              }),
            ),
          );

          if (
            storedRecord.row.sequence !== batchRow.first_sequence + index ||
            storedRecord.row.record_id !== expectedRecord.recordId ||
            expectedJson !== storedJson
          ) {
            return yield* options.errors.corruption({
              table: "effect_agent_canonical_records",
              rowKey: `${key}/${storedRecord.row.sequence}`,
              message: "Canonical record identity, sequence, or payload differs from its batch.",
            });
          }
        }

        previousDigest = digest;
        expectedSequence = batchRow.last_sequence + 1;
        tailDigests.set(batchRow.last_sequence, digest);
      }

      if (
        threadRecords.length !== thread.tail_sequence ||
        thread.tail_sequence !== expectedSequence - 1 ||
        thread.tail_digest !== previousDigest
      ) {
        return yield* options.errors.corruption({
          table: "effect_agent_threads",
          rowKey: thread.thread_id,
          message: "Thread tail does not match its canonical batch chain.",
        });
      }

      for (const checkpoint of threadCheckpoints) {
        if (
          checkpoint.decoded.threadId !== thread.thread_id ||
          checkpoint.decoded.throughSequence !== checkpoint.row.through_sequence ||
          checkpoint.decoded.tailDigest !== checkpoint.row.tail_digest ||
          tailDigests.get(checkpoint.row.through_sequence) !== checkpoint.row.tail_digest
        ) {
          return yield* options.errors.corruption({
            table: "effect_agent_checkpoints",
            rowKey: `${thread.thread_id}/${checkpoint.row.through_sequence}`,
            message: "Checkpoint identity or digest is not bound to a canonical batch tail.",
          });
        }
      }
    }

    if (
      batches.some(({ row }) => !materializedIds.has(row.thread_id)) ||
      records.some(({ row }) => !materializedIds.has(row.thread_id)) ||
      checkpoints.some(({ row }) => !materializedIds.has(row.thread_id))
    ) {
      return yield* options.errors.corruption({
        table: "effect_agent_threads",
        rowKey: "startup_scan",
        message: "Canonical rows exist without a materialized Thread.",
      });
    }
  });

  const config = options;
  const failpoint = { hit: options.hitFailpoint };
  const crypto = yield* Crypto.Crypto;

  if (config.verifyOnOpen) {
    yield* decodeStartupPayloads(journal, crypto);
  }

  const provideCrypto = <A, E>(effect: Effect.Effect<A, E, Crypto.Crypto>) =>
    Effect.provideService(effect, Crypto.Crypto, crypto);

  const hitFailpoint = Effect.fn("SqlThreadStore.hitFailpoint")(
    (location: SqlStorageFailpointLocation): Effect.Effect<void, ThreadStoreError> =>
      failpoint
        .hit(location)
        .pipe(Effect.mapError((error) => storeError(`storage failpoint ${location}`, error))),
  );

  const materialize: ThreadStore["Service"]["materialize"] = Effect.fn(
    "SqlThreadStore.materialize",
  )(function* (request: ThreadMaterialization) {
    const validated = yield* Schema.decodeEffect(Schema.toType(ThreadMaterialization))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate materialization", error)));

    const now = yield* Clock.currentTimeMillis;

    yield* hitFailpoint("materialize:before");
    yield* journal
      .materialize(
        validated.threadId,
        new Date(now).toISOString(),
        EMPTY_TAIL_DIGEST,
        validated.producerEpoch,
      )
      .pipe(
        Effect.mapError((error) =>
          isFenceRejected(error) ? error : storeError("materialize thread", error),
        ),
      );
    yield* hitFailpoint("materialize:after");
  });

  const append: ThreadStore["Service"]["append"] = Effect.fn("SqlThreadStore.append")(function* (
    request: FencedAppendRequest,
  ) {
    const validated = yield* Schema.decodeEffect(Schema.toType(FencedAppendRequest))(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate canonical append", error)),
    );

    yield* requireThread(journal, validated.threadId);

    const tailDigest = yield* provideCrypto(
      digestCanonicalBatch(validated.expectedTailDigest, validated.batch),
    ).pipe(Effect.mapError((error) => storeError("digest canonical append", error)));

    const batchJson = yield* encodeCanonicalBatch(validated.batch);

    const rawRecords = yield* Effect.forEach(validated.batch.records, (record) =>
      encodeCanonicalRecord(record).pipe(
        Effect.map((recordJson) => ({
          recordId: record.recordId,
          recordJson,
        })),
      ),
    );

    const rawRequest = yield* Schema.decodeEffect(RawAppendRequest)({
      threadId: validated.threadId,
      batchId: validated.batch.batchId,
      batchDigest: tailDigest,
      batchJson,
      expectedTailSequence: validated.expectedTailSequence,
      expectedTailDigest: validated.expectedTailDigest,
      producerEpoch: validated.producerEpoch,
      records: rawRecords,
      tailDigest,
    }).pipe(Effect.mapError((error) => schemaStoreError("encode canonical append", error)));

    yield* hitFailpoint("append:before");

    const result = yield* journal.append(rawRequest).pipe(
      Effect.mapError((error) => {
        if (isFenceRejected(error) || isAppendConflict(error)) return error;

        return storeError("append canonical batch", error);
      }),
      Effect.flatMap((result) =>
        Schema.decodeEffect(AppendResult)(result).pipe(
          Effect.mapError((error) => schemaStoreError("decode append result", error)),
        ),
      ),
    );

    yield* hitFailpoint("append:after");

    return result;
  });

  const loadRecords = Effect.fn("SqlThreadStore.loadRecords")(function* (request: RawReadRequest) {
    const rows = yield* journal
      .read(request)
      .pipe(Effect.mapError((error) => storeError("read canonical records", error)));

    return yield* Effect.forEach(rows, decodeEnvelope);
  });

  const readEffect = Effect.fn("SqlThreadStore.read")(function* (request: ThreadReadRequest) {
    const validated = yield* Schema.decodeEffect(Schema.toType(ThreadReadRequest))(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate thread read", error)),
    );

    if ("selection" in validated) return Stream.fromIterable(yield* selectedReads.read(validated));
    yield* requireThread(journal, validated.threadId);

    const records = yield* loadRecords(
      RawReadRequest.make({
        threadId: validated.threadId,
        fromSequenceExclusive: validated.afterSequence ?? ZERO_CANONICAL_SEQUENCE,
        limit: validated.limit,
      }),
    );

    return Stream.fromIterable(records);
  });

  const read: ThreadStore["Service"]["read"] = (request) => Stream.unwrap(readEffect(request));

  const observeEffect = Effect.fn("SqlThreadStore.observe")(function* (request: ThreadObservation) {
    const validated = yield* Schema.decodeEffect(Schema.toType(ThreadObservation))(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate thread observation", error)),
    );

    yield* requireThread(journal, validated.threadId);
    const initialSequence = yield* parseOffset(validated.threadId, validated.afterOffset);
    const cursor = yield* Ref.make(initialSequence);

    const poll = Effect.fn("SqlThreadStore.observePoll")(function* () {
      const fromSequenceExclusive = yield* Ref.get(cursor);

      const records = yield* loadRecords(
        RawReadRequest.make({
          threadId: validated.threadId,
          fromSequenceExclusive,
          limit: 1_024,
        }),
      );

      if (records.length === 0) {
        yield* Effect.sleep(config.observationPollInterval);

        return [];
      }
      yield* Ref.set(cursor, records[records.length - 1].sequence);

      return records;
    });

    return Stream.fromIterableEffectRepeat(poll());
  });

  const observe: ThreadStore["Service"]["observe"] = (request) =>
    Stream.unwrap(observeEffect(request));

  const exportThread: ThreadStore["Service"]["export"] = Effect.fn("SqlThreadStore.export")(
    function* (request: ThreadExportRequest) {
      const validated = yield* Schema.decodeEffect(Schema.toType(ThreadExportRequest))(
        request,
      ).pipe(Effect.mapError((error) => schemaStoreError("validate thread export", error)));

      yield* requireThread(journal, validated.threadId);

      const exported = yield* journal
        .exportThread(validated.threadId)
        .pipe(Effect.mapError((error) => storeError("export thread", error)));

      const records = yield* Effect.forEach(exported.records, decodeEnvelope);

      if (records.length > MAX_THREAD_EXPORT_RECORDS) {
        return yield* ThreadStoreError.make({
          operation: "decode thread export",
          message: "The thread exceeds the current export record limit.",
        });
      }

      const tailDigest = yield* Schema.decodeEffect(Digest)(exported.thread.tail_digest).pipe(
        Effect.mapError((error) => schemaStoreError("decode export tail digest", error)),
      );

      return ThreadExport.make({
        format: "effect-agent/thread@1",
        threadId: validated.threadId,
        tailSequence: exported.thread.tail_sequence,
        tailDigest,
        records,
      });
    },
  );

  const inspectTail: ThreadStore["Service"]["inspectTail"] = Effect.fn(
    "SqlThreadStore.inspectTail",
  )(function* (request: ThreadTailRequest) {
    const validated = yield* Schema.decodeEffect(Schema.toType(ThreadTailRequest))(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate tail inspection", error)),
    );

    const thread = yield* requireThread(journal, validated.threadId);

    const tailDigest = yield* Schema.decodeEffect(Digest)(thread.tail_digest).pipe(
      Effect.mapError((error) => schemaStoreError("decode tail digest", error)),
    );

    return ThreadTail.make({
      threadId: validated.threadId,
      tailSequence: thread.tail_sequence,
      tailDigest,
      producerEpoch: thread.producer_epoch,
    });
  });

  const saveCheckpoint: ThreadCheckpoints["save"] = Effect.fn("SqlThreadStore.saveCheckpoint")(
    function* (request: SaveCheckpointRequest) {
      const validated = yield* Schema.decodeEffect(Schema.toType(SaveCheckpointRequest))(
        request,
      ).pipe(Effect.mapError((error) => schemaStoreError("validate checkpoint", error)));

      const thread = yield* requireThread(journal, validated.checkpoint.threadId);

      if (validated.checkpoint.throughSequence > thread.tail_sequence) {
        return yield* CheckpointRejected.make({
          threadId: validated.checkpoint.threadId,
          reason: "ahead-of-tail",
        });
      }

      const canonicalDigest = yield* tailDigestAt(
        journal,
        validated.checkpoint.threadId,
        validated.checkpoint.throughSequence,
      );

      if (canonicalDigest !== validated.checkpoint.tailDigest) {
        return yield* CheckpointRejected.make({
          threadId: validated.checkpoint.threadId,
          reason: "digest-mismatch",
        });
      }
      const checkpointJson = yield* encodeCheckpoint(validated.checkpoint);

      const raw = RawCheckpoint.make({
        threadId: validated.checkpoint.threadId,
        throughSequence: validated.checkpoint.throughSequence,
        tailDigest: validated.checkpoint.tailDigest,
        checkpointJson,
      });

      yield* hitFailpoint("save-checkpoint:before");
      yield* journal
        .saveCheckpoint(raw)
        .pipe(
          Effect.mapError((error) =>
            isCheckpointRejected(error) ? error : storeError("save checkpoint", error),
          ),
        );
      yield* hitFailpoint("save-checkpoint:after");
    },
  );

  const loadCheckpoint: ThreadCheckpoints["load"] = Effect.fn("SqlThreadStore.loadCheckpoint")(
    function* (request: LoadCheckpointRequest) {
      const validated = yield* Schema.decodeEffect(Schema.toType(LoadCheckpointRequest))(
        request,
      ).pipe(Effect.mapError((error) => schemaStoreError("validate checkpoint lookup", error)));

      const thread = yield* requireThread(journal, validated.threadId);

      const rows = yield* journal
        .loadCheckpoint(validated.threadId, validated.atOrBeforeSequence ?? thread.tail_sequence)
        .pipe(Effect.mapError((error) => storeError("load checkpoint", error)));

      if (rows.length === 0) return Option.none();
      if (rows.length !== 1) {
        return yield* ThreadStoreError.make({
          operation: "load checkpoint",
          message: `Expected at most one checkpoint row but found ${rows.length}.`,
        });
      }
      const row = rows[0];
      const checkpoint = yield* decodeCheckpoint(row.checkpoint_json);

      if (
        row.thread_id !== validated.threadId ||
        checkpoint.threadId !== row.thread_id ||
        checkpoint.throughSequence !== row.through_sequence ||
        checkpoint.tailDigest !== row.tail_digest
      ) {
        return yield* ThreadStoreError.make({
          operation: "load checkpoint",
          message: "Stored checkpoint metadata does not match its canonical row.",
        });
      }

      const canonicalDigest = yield* tailDigestAt(
        journal,
        checkpoint.threadId,
        checkpoint.throughSequence,
      );

      if (canonicalDigest !== checkpoint.tailDigest) {
        return yield* CheckpointRejected.make({
          threadId: checkpoint.threadId,
          reason: "digest-mismatch",
        });
      }

      return Option.some(checkpoint);
    },
  );

  const saveRecoveryCheckpoint: ThreadRecoveryCheckpoints["save"] = Effect.fn(
    "SqlThreadStore.saveRecoveryCheckpoint",
  )(function* (request) {
    const validated = yield* Schema.decodeEffect(Schema.toType(SaveRecoveryCheckpointRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate recovery checkpoint", error)));

    const checkpointJson = yield* encodeCheckpoint(validated.checkpoint);

    yield* journal
      .saveRecoveryCheckpoint(validated, checkpointJson)
      .pipe(
        Effect.mapError((error) =>
          Schema.is(CheckpointRejected)(error) ||
          Schema.is(FenceRejected)(error) ||
          Schema.is(ThreadNotMaterialized)(error)
            ? error
            : storeError("save recovery checkpoint", error),
        ),
      );
  });

  const loadRecoveryCheckpoint: ThreadRecoveryCheckpoints["load"] = Effect.fn(
    "SqlThreadStore.loadRecoveryCheckpoint",
  )(function* (request) {
    const validated = yield* Schema.decodeEffect(Schema.toType(LoadCheckpointRequest))(
      request,
    ).pipe(
      Effect.mapError((error) => schemaStoreError("validate recovery checkpoint lookup", error)),
    );

    const thread = yield* requireThread(journal, validated.threadId);

    const corrupt = () =>
      CheckpointRejected.make({ threadId: validated.threadId, reason: "corrupt" });

    const rows = yield* journal
      .loadRecoveryCheckpoint(validated.threadId)
      .pipe(
        Effect.mapError((error) =>
          options.errors.isCorruption(error)
            ? corrupt()
            : storeError("load recovery checkpoint", error),
        ),
      );

    if (rows.length === 0) return Option.none();
    if (rows.length !== 1) return yield* corrupt();
    const row = rows[0];

    const checkpoint = yield* Schema.decodeEffect(Schema.fromJsonString(ThreadCheckpoint))(
      row.checkpoint_json,
    ).pipe(Effect.mapError(corrupt));

    if (
      row.thread_id !== validated.threadId ||
      checkpoint.threadId !== row.thread_id ||
      checkpoint.throughSequence !== row.through_sequence ||
      checkpoint.tailDigest !== row.tail_digest
    )
      return yield* corrupt();
    if (checkpoint.throughSequence > thread.tail_sequence)
      return yield* CheckpointRejected.make({
        threadId: validated.threadId,
        reason: "ahead-of-tail",
      });
    if (checkpoint.throughSequence > (validated.atOrBeforeSequence ?? thread.tail_sequence))
      return Option.none();

    const canonicalDigest = yield* tailDigestAt(
      journal,
      checkpoint.threadId,
      checkpoint.throughSequence,
    );

    if (canonicalDigest !== checkpoint.tailDigest)
      return yield* CheckpointRejected.make({
        threadId: validated.threadId,
        reason: "digest-mismatch",
      });

    return Option.some(checkpoint);
  });

  const selectedReads = yield* makeSelectedReads(decodeEnvelope);

  const threadStore = ThreadStore.of({
    countPeerMessages: selectedReads.countPeerMessages,
    readIdentity: selectedReads.readIdentity,
    append,
    export: exportThread,
    inspectTail,
    materialize,
    observe,
    read,
    checkpoints: { save: saveCheckpoint, load: loadCheckpoint },
    recoveryCheckpoints: { save: saveRecoveryCheckpoint, load: loadRecoveryCheckpoint },
  });

  return Context.make(ThreadStore, threadStore);
});
