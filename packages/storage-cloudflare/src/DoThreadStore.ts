import { digestCanonicalBatch, EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import {
  CanonicalBatch,
  CanonicalRecord,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ObservationOffset,
} from "@effect-agent/thread/Records";
import { DEFAULT_OWNERSHIP_LEASE_DURATION } from "@effect-agent/thread/SubmissionLedger";
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
  ThreadRead,
  ThreadStore,
  type ThreadCheckpoints,
  ThreadStoreError,
  ThreadTail,
  ThreadTailRequest,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
} from "@effect-agent/thread/ThreadStore";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import {
  Clock,
  Context,
  Crypto,
  Duration,
  Effect,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import {
  DEFAULT_MAX_STORED_VALUE_BYTES,
  DoStorageConfig,
  DoStorageConfigValue,
} from "./DoStorageConfig.ts";
import {
  type DoStorageCompatibilityError,
  DoAppendConflict,
  DoCheckpointConflict,
  DoFenceRejected,
  type DoStorageFailpointLocation,
  DoStorageCorruptionError,
  DoStorageError,
} from "./DoStorageError.ts";
import { DoStorageFailpoint, type DoStorageFailpointHandler } from "./DoStorageFailpoint.ts";
import {
  initializeDoJournal,
  RawAppendRequest,
  RawCheckpoint,
  RawReadRequest,
  type DoJournal,
} from "./internal/do-journal.ts";

/**
 * Convenience-layer construction options. `storage` is the Durable Object's own
 * `ctx.storage` handle, injected as a value (DEPLOY-010: platform bindings enter only
 * through Layers; this package never imports `cloudflare:workers`).
 */
export interface DoStorageOptions {
  readonly storage: DurableObjectStorage;
  readonly observationPollInterval?: number | undefined;
  /**
   * Submission ownership lease duration in milliseconds (D5). Defaults to
   * `DEFAULT_OWNERSHIP_LEASE_DURATION` from `@effect-agent/thread`.
   */
  readonly ownershipLeaseDuration?: number | undefined;
  /**
   * Maximum bytes for any single stored value; must stay under the platform's 2 MB
   * per-value limit. Defaults to `DEFAULT_MAX_STORED_VALUE_BYTES`.
   */
  readonly maxStoredValueBytes?: number | undefined;
  /**
   * Re-verify every stored payload and digest chain while opening the store. Defaults to
   * off: per-operation Schema decoding and the digest chain already fail clearly on corrupt
   * rows without scanning the whole database on every open.
   */
  readonly verifyOnOpen?: boolean | undefined;
  readonly failpoint?: DoStorageFailpointHandler | undefined;
}

export type DoStorageInitializationError =
  | DoStorageCompatibilityError
  | DoStorageCorruptionError
  | DoStorageError;

const OffsetText = Schema.String.check(Schema.isMaxLength(4 * 1024));
const DO_OFFSET_PREFIX = "effect-agent-do@1:";
const ZERO_CANONICAL_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
const isDigest = Schema.is(Digest);
const isDoFenceRejected = Schema.is(DoFenceRejected);
const isDoAppendConflict = Schema.is(DoAppendConflict);
const isDoCheckpointConflict = Schema.is(DoCheckpointConflict);

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

const makeOffset = Effect.fn(function* (
  threadId: ThreadMaterialization["threadId"],
  sequence: number,
): Effect.fn.Return<ObservationOffset, ThreadStoreError> {
  return yield* Schema.decodeUnknownEffect(CanonicalSequence)(sequence).pipe(
    Effect.flatMap((validatedSequence) =>
      Schema.decodeUnknownEffect(ObservationOffset)(
        `${DO_OFFSET_PREFIX}${encodeURIComponent(threadId)}:${validatedSequence}`,
      ),
    ),
    Effect.mapError((error) => schemaStoreError("encode observation offset", error)),
  );
});

const parseOffset = Effect.fn(function* (
  threadId: ThreadMaterialization["threadId"],
  offset: ObservationOffset | undefined,
): Effect.fn.Return<CanonicalSequence, ThreadStoreError> {
  if (offset === undefined) return ZERO_CANONICAL_SEQUENCE;

  const text = yield* Schema.decodeUnknownEffect(OffsetText)(offset).pipe(
    Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
  );

  const threadPrefix = `${DO_OFFSET_PREFIX}${encodeURIComponent(threadId)}:`;

  if (!text.startsWith(threadPrefix)) {
    return yield* ThreadStoreError.make({
      operation: "decode observation offset",
      message: "The observation offset belongs to a different adapter, storage version, or Thread.",
    });
  }
  const sequenceText = text.slice(threadPrefix.length);

  if (!/^(0|[1-9][0-9]*)$/.test(sequenceText)) {
    return yield* ThreadStoreError.make({
      operation: "decode observation offset",
      message: "The observation offset is malformed.",
    });
  }

  return yield* Schema.decodeUnknownEffect(CanonicalSequence)(Number(sequenceText)).pipe(
    Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
  );
});

const mapFence = (threadId: ThreadMaterialization["threadId"], error: DoFenceRejected) =>
  FenceRejected.make({
    threadId,
    actualEpoch: error.actualEpoch,
    attemptedEpoch: error.producerEpoch,
  });

const encodeCanonicalRecord = Effect.fn(function* (
  record: CanonicalRecord,
): Effect.fn.Return<string, ThreadStoreError> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalRecord))(record).pipe(
    Effect.mapError((error) => schemaStoreError("encode canonical record", error)),
  );
});

const encodeCanonicalBatch = Effect.fn(function* (
  batch: CanonicalBatch,
): Effect.fn.Return<string, ThreadStoreError> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalBatch))(batch).pipe(
    Effect.mapError((error) => schemaStoreError("encode canonical batch", error)),
  );
});

const encodeCheckpoint = Effect.fn(function* (
  checkpoint: ThreadCheckpoint,
): Effect.fn.Return<string, ThreadStoreError> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(ThreadCheckpoint))(checkpoint).pipe(
    Effect.mapError((error) => schemaStoreError("encode checkpoint", error)),
  );
});

const decodeEnvelope = Effect.fn(function* (row: {
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

  const threadId = yield* Schema.decodeUnknownEffect(CanonicalRecordEnvelope.fields.threadId)(
    row.thread_id,
  ).pipe(Effect.mapError((error) => schemaStoreError("decode thread identity", error)));

  const offset = yield* makeOffset(threadId, row.sequence);

  const batchId = yield* Schema.decodeUnknownEffect(CanonicalRecordEnvelope.fields.batchId)(
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

const decodeCheckpoint = Effect.fn(function* (
  checkpointJson: string,
): Effect.fn.Return<ThreadCheckpoint, ThreadStoreError> {
  return yield* Schema.decodeEffect(Schema.fromJsonString(ThreadCheckpoint))(checkpointJson).pipe(
    Effect.mapError((error) => schemaStoreError("decode checkpoint", error)),
  );
});

const requireThread = Effect.fn("DoThreadStore.requireThread")(function* (
  journal: DoJournal,
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

const tailDigestAt = Effect.fn("DoThreadStore.tailDigestAt")(function* (
  journal: DoJournal,
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

  return yield* Schema.decodeUnknownEffect(Digest)(digests[0]).pipe(
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
 * Opt-in full integrity audit (`verifyOnOpen`). Every stored payload is decoded, re-encoded,
 * and re-digested against the canonical chain. Routine opens skip this scan: per-operation
 * Schema decoding plus the digest chain already fail clearly on corrupt rows.
 */
const decodeStartupPayloads = Effect.fn("DoThreadStore.decodeStartupPayloads")(function* (
  journal: DoJournal,
  crypto: Crypto.Crypto,
) {
  const stored = yield* journal.scanStoredPayloads();

  const batches = yield* Effect.forEach(stored.batches, (batch) =>
    Schema.decodeEffect(Schema.fromJsonString(CanonicalBatch))(batch.batch_json).pipe(
      Effect.map((decoded) => ({ decoded, row: batch })),
      Effect.mapError((error) =>
        DoStorageCorruptionError.make({
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
        DoStorageCorruptionError.make({
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
        DoStorageCorruptionError.make({
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
        return yield* DoStorageCorruptionError.make({
          table: "effect_agent_canonical_batches",
          rowKey: key,
          message: "Canonical batch identity, sequence, or record count is inconsistent.",
        });
      }

      const digest = yield* digestCanonicalBatch(previousDigest, canonicalBatch).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError((error) =>
          DoStorageCorruptionError.make({
            table: "effect_agent_canonical_batches",
            rowKey: key,
            message: error.message,
          }),
        ),
      );

      if (batchRow.batch_digest !== digest || batchRow.tail_digest !== digest) {
        return yield* DoStorageCorruptionError.make({
          table: "effect_agent_canonical_batches",
          rowKey: key,
          message: "Canonical batch digest does not match its decoded content and prior tail.",
        });
      }

      const batchRecords = recordsByBatch.get(batchRow.batch_id) ?? [];

      if (batchRecords.length !== canonicalBatch.records.length) {
        return yield* DoStorageCorruptionError.make({
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
            DoStorageCorruptionError.make({
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
            DoStorageCorruptionError.make({
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
          return yield* DoStorageCorruptionError.make({
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
      return yield* DoStorageCorruptionError.make({
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
        return yield* DoStorageCorruptionError.make({
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
    return yield* DoStorageCorruptionError.make({
      table: "effect_agent_threads",
      rowKey: "startup_scan",
      message: "Canonical rows exist without a materialized Thread.",
    });
  }
});

const makeServices = Effect.fn("DoThreadStore.makeServices")(function* () {
  const config = yield* DoStorageConfig;
  const failpoint = yield* DoStorageFailpoint;
  const sql = yield* SqlClientService.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const journal = yield* initializeDoJournal(sql, failpoint.hit, config.maxStoredValueBytes);

  if (config.verifyOnOpen) {
    yield* decodeStartupPayloads(journal, crypto);
  }

  const provideCrypto = <A, E>(effect: Effect.Effect<A, E, Crypto.Crypto>) =>
    Effect.provideService(effect, Crypto.Crypto, crypto);

  const hitFailpoint = Effect.fn(
    (location: DoStorageFailpointLocation): Effect.Effect<void, ThreadStoreError> =>
      failpoint
        .hit(location)
        .pipe(Effect.mapError((error) => storeError(`storage failpoint ${location}`, error))),
  );

  const materialize: ThreadStore["Service"]["materialize"] = Effect.fn("DoThreadStore.materialize")(
    function* (request: ThreadMaterialization) {
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ThreadMaterialization))(
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
            error._tag === "DoFenceRejected"
              ? mapFence(validated.threadId, error)
              : storeError("materialize thread", error),
          ),
        );
      yield* hitFailpoint("materialize:after");
    },
  );

  const append: ThreadStore["Service"]["append"] = Effect.fn("DoThreadStore.append")(function* (
    request: FencedAppendRequest,
  ) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(FencedAppendRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate canonical append", error)));

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

    const rawRequest = yield* Schema.decodeUnknownEffect(RawAppendRequest)({
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
        if (isDoFenceRejected(error)) {
          return mapFence(validated.threadId, error);
        }
        if (isDoAppendConflict(error)) {
          return error.actualTailSequence !== undefined && isDigest(error.actualTailDigest)
            ? AppendConflict.make({
                threadId: validated.threadId,
                batchId: validated.batch.batchId,
                reason: error.reason,
                actualTailSequence: error.actualTailSequence,
                actualTailDigest: error.actualTailDigest,
              })
            : AppendConflict.make({
                threadId: validated.threadId,
                batchId: validated.batch.batchId,
                reason: error.reason,
              });
        }

        return storeError("append canonical batch", error);
      }),
      Effect.flatMap((result) =>
        Schema.decodeUnknownEffect(AppendResult)(result).pipe(
          Effect.mapError((error) => schemaStoreError("decode append result", error)),
        ),
      ),
    );

    yield* hitFailpoint("append:after");

    return result;
  });

  const loadRecords = Effect.fn("DoThreadStore.loadRecords")(function* (request: RawReadRequest) {
    const result = yield* journal
      .read(request)
      .pipe(Effect.mapError((error) => storeError("read canonical records", error)));

    return {
      count: result.count,
      records: result.records.pipe(
        Stream.mapError((error) => storeError("read canonical records", error)),
        Stream.mapEffect(decodeEnvelope),
      ),
    };
  });

  const readEffect = Effect.fn("DoThreadStore.read")(function* (request: ThreadRead) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ThreadRead))(request).pipe(
      Effect.mapError((error) => schemaStoreError("validate thread read", error)),
    );

    yield* requireThread(journal, validated.threadId);

    const result = yield* loadRecords(
      RawReadRequest.make({
        threadId: validated.threadId,
        fromSequenceExclusive: validated.afterSequence ?? ZERO_CANONICAL_SEQUENCE,
        limit: validated.limit,
      }),
    );

    return result.records;
  });

  const read: ThreadStore["Service"]["read"] = (request) => Stream.unwrap(readEffect(request));

  const observeEffect = Effect.fn("DoThreadStore.observe")(function* (request: ThreadObservation) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ThreadObservation))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate thread observation", error)));

    yield* requireThread(journal, validated.threadId);
    const initialSequence = yield* parseOffset(validated.threadId, validated.afterOffset);
    const cursor = yield* Ref.make(initialSequence);

    const poll = Effect.fn("DoThreadStore.observePoll")(function* () {
      const fromSequenceExclusive = yield* Ref.get(cursor);

      const result = yield* loadRecords(
        RawReadRequest.make({
          threadId: validated.threadId,
          fromSequenceExclusive,
          limit: 1_024,
        }),
      );

      if (result.count === 0) {
        yield* Effect.sleep(config.observationPollInterval);

        return Stream.empty;
      }

      return result.records.pipe(Stream.tap((record) => Ref.set(cursor, record.sequence)));
    });

    return Stream.fromEffectRepeat(poll()).pipe(Stream.flatten);
  });

  const observe: ThreadStore["Service"]["observe"] = (request) =>
    Stream.unwrap(observeEffect(request));

  const exportThread: ThreadStore["Service"]["export"] = Effect.fn("DoThreadStore.export")(
    function* (request: ThreadExportRequest) {
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ThreadExportRequest))(
        request,
      ).pipe(Effect.mapError((error) => schemaStoreError("validate thread export", error)));

      yield* requireThread(journal, validated.threadId);

      const exported = yield* journal
        .exportThread(validated.threadId)
        .pipe(Effect.mapError((error) => storeError("export thread", error)));

      const records = yield* Effect.forEach(exported.records, decodeEnvelope);

      if (records.length > 65_536) {
        return yield* ThreadStoreError.make({
          operation: "decode thread export",
          message: "The thread exceeds the current export record limit.",
        });
      }

      const tailDigest = yield* Schema.decodeUnknownEffect(Digest)(
        exported.thread.tail_digest,
      ).pipe(Effect.mapError((error) => schemaStoreError("decode export tail digest", error)));

      return ThreadExport.make({
        format: "effect-agent/thread@1",
        threadId: validated.threadId,
        tailSequence: exported.thread.tail_sequence,
        tailDigest,
        records,
      });
    },
  );

  const inspectTail: ThreadStore["Service"]["inspectTail"] = Effect.fn("DoThreadStore.inspectTail")(
    function* (request: ThreadTailRequest) {
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ThreadTailRequest))(
        request,
      ).pipe(Effect.mapError((error) => schemaStoreError("validate tail inspection", error)));

      const thread = yield* requireThread(journal, validated.threadId);

      const tailDigest = yield* Schema.decodeUnknownEffect(Digest)(thread.tail_digest).pipe(
        Effect.mapError((error) => schemaStoreError("decode tail digest", error)),
      );

      return ThreadTail.make({
        threadId: validated.threadId,
        tailSequence: thread.tail_sequence,
        tailDigest,
        producerEpoch: thread.producer_epoch,
      });
    },
  );

  const saveCheckpoint: ThreadCheckpoints["save"] = Effect.fn("DoThreadStore.saveCheckpoint")(
    function* (request: SaveCheckpointRequest) {
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SaveCheckpointRequest))(
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
      yield* journal.saveCheckpoint(raw).pipe(
        Effect.mapError((error) =>
          isDoCheckpointConflict(error)
            ? CheckpointRejected.make({
                threadId: validated.checkpoint.threadId,
                reason: "digest-mismatch",
              })
            : storeError("save checkpoint", error),
        ),
      );
      yield* hitFailpoint("save-checkpoint:after");
    },
  );

  const loadCheckpoint: ThreadCheckpoints["load"] = Effect.fn("DoThreadStore.loadCheckpoint")(
    function* (request: LoadCheckpointRequest) {
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(LoadCheckpointRequest))(
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
      const checkpoint = yield* decodeCheckpoint(rows[0].checkpoint_json);

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

  const threadStore = ThreadStore.of({
    append,
    export: exportThread,
    inspectTail,
    materialize,
    observe,
    read,
    checkpoints: { save: saveCheckpoint, load: loadCheckpoint },
  });

  return Context.make(ThreadStore, threadStore);
});

/**
 * Durable Object Thread Store implementation with configuration, failpoint, SQL, and
 * Crypto authority kept visible in its input channel.
 */
export const threadStoreLayer: Layer.Layer<
  ThreadStore,
  DoStorageInitializationError,
  DoStorageConfig | DoStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(makeServices());

/**
 * Validated Durable Object storage configuration Layer with the documented defaults applied.
 * Shared by the ThreadStore and SubmissionLedger convenience layers so their defaults
 * cannot drift.
 */
export const storageConfigLayer = (
  options: DoStorageOptions,
): Layer.Layer<DoStorageConfig, DoStorageError> =>
  Layer.effect(DoStorageConfig)(
    Schema.decodeUnknownEffect(DoStorageConfigValue)({
      observationPollInterval: options.observationPollInterval ?? 25,
      ownershipLeaseDuration:
        options.ownershipLeaseDuration ?? Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION),
      maxStoredValueBytes: options.maxStoredValueBytes ?? DEFAULT_MAX_STORED_VALUE_BYTES,
      verifyOnOpen: options.verifyOnOpen ?? false,
    }).pipe(
      Effect.mapError((error) =>
        DoStorageError.make({
          cause: error,
          operation: "configure Durable Object storage",
          message: error.message,
        }),
      ),
    ),
  );

/** The failpoint Layer selected by convenience options: explicit handler or the no-op default. */
export const storageFailpointLayer = (
  options: DoStorageOptions,
): Layer.Layer<DoStorageFailpoint> =>
  options.failpoint === undefined
    ? DoStorageFailpoint.layer
    : Layer.succeed(DoStorageFailpoint)({ hit: options.failpoint });

/**
 * A composition-root convenience Layer for canonical Threads inside one Durable Object,
 * built over `ctx.storage`. Durable accepted work is served by the separate SubmissionLedger
 * port; point both at the SAME `ctx.storage` so claims fence the same producer epochs
 * (ADR-0011 D7's "same file" rule, transposed to one object's private database).
 */
export const layer = (
  options: DoStorageOptions,
): Layer.Layer<ThreadStore, DoStorageInitializationError> =>
  Layer.unwrap(
    Effect.map(DoStorageConfig, (config) =>
      threadStoreLayer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(DoStorageConfig)(config),
            storageFailpointLayer(options),
            SqliteClient.layer({ storage: options.storage }),
            BrowserCrypto.layer,
          ),
        ),
      ),
    ),
  ).pipe(Layer.provide(storageConfigLayer(options)));
