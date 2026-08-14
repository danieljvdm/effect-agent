import {
  AppendConflict,
  AppendResult,
  CanonicalBatch,
  CanonicalRecord,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  CheckpointRejected,
  ConversationCheckpoint,
  ConversationExport,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationNotMaterialized,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  ConversationStoreError,
  ConversationTail,
  ConversationTailRequest,
  DEFAULT_OWNERSHIP_LEASE_DURATION,
  digestCanonicalBatch,
  Digest,
  EMPTY_TAIL_DIGEST,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  ObservationOffset,
  SaveCheckpointRequest,
} from "@effect-agent/session";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
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
  SqliteAppendConflict,
  SqliteCheckpointConflict,
  SqliteFenceRejected,
  type SqliteStorageFailpointLocation,
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
} from "./errors.ts";
import {
  initializeSqliteJournal,
  RawAppendRequest,
  RawCheckpoint,
  RawReadRequest,
  type SqliteJournal,
} from "./sqlite-journal.ts";
import { SqliteStorageConfig, SqliteStorageConfigValue } from "./sqlite-storage-config.ts";
import {
  SqliteStorageFailpoint,
  type SqliteStorageFailpointHandler,
} from "./sqlite-storage-failpoint.ts";

export interface SqliteStorageOptions {
  readonly filename: string;
  readonly observationPollInterval?: number | undefined;
  /** Bounded SQLITE_BUSY retry window for write-lock acquisition, in milliseconds. */
  readonly busyTimeout?: number | undefined;
  /**
   * Submission ownership lease duration in milliseconds (D5). Defaults to
   * `DEFAULT_OWNERSHIP_LEASE_DURATION` from `@effect-agent/session`.
   */
  readonly ownershipLeaseDuration?: number | undefined;
  /**
   * Re-verify every stored payload and digest chain while opening the store. Defaults to
   * off: per-operation Schema decoding and the digest chain already fail clearly on corrupt
   * rows without scanning the whole database on every open.
   */
  readonly verifyOnOpen?: boolean | undefined;
  readonly failpoint?: SqliteStorageFailpointHandler | undefined;
}

export type SqliteStorageInitializationError =
  | SqliteStorageCompatibilityError
  | SqliteStorageCorruptionError
  | SqliteStorageError;

const OffsetText = Schema.String.check(Schema.isMaxLength(4 * 1024));
const SQLITE_OFFSET_PREFIX = "effect-agent-sqlite@1:";
const ZERO_CANONICAL_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
const isDigest = Schema.is(Digest);

const storeError = (operation: string, error: { readonly message: string }) =>
  ConversationStoreError.make({
    cause: error,
    operation,
    message: error.message,
  });

const schemaStoreError = (operation: string, error: { readonly message: string }) =>
  ConversationStoreError.make({
    cause: error,
    operation,
    message: error.message,
  });

const makeOffset = Effect.fn("SqliteConversationStore.makeOffset")(function* (
  conversationId: ConversationMaterialization["conversationId"],
  sequence: number,
): Effect.fn.Return<ObservationOffset, ConversationStoreError> {
  return yield* Schema.decodeUnknownEffect(CanonicalSequence)(sequence).pipe(
    Effect.flatMap((validatedSequence) =>
      Schema.decodeUnknownEffect(ObservationOffset)(
        `${SQLITE_OFFSET_PREFIX}${encodeURIComponent(conversationId)}:${validatedSequence}`,
      ),
    ),
    Effect.mapError((error) => schemaStoreError("encode observation offset", error)),
  );
});

const parseOffset = Effect.fn("SqliteConversationStore.parseOffset")(function* (
  conversationId: ConversationMaterialization["conversationId"],
  offset: ObservationOffset | undefined,
): Effect.fn.Return<CanonicalSequence, ConversationStoreError> {
  if (offset === undefined) return ZERO_CANONICAL_SEQUENCE;
  const text = yield* Schema.decodeUnknownEffect(OffsetText)(offset).pipe(
    Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
  );
  const conversationPrefix = `${SQLITE_OFFSET_PREFIX}${encodeURIComponent(conversationId)}:`;
  if (!text.startsWith(conversationPrefix)) {
    return yield* ConversationStoreError.make({
      operation: "decode observation offset",
      message:
        "The observation offset belongs to a different adapter, storage version, or Conversation.",
    });
  }
  const sequenceText = text.slice(conversationPrefix.length);
  if (!/^(0|[1-9][0-9]*)$/.test(sequenceText)) {
    return yield* ConversationStoreError.make({
      operation: "decode observation offset",
      message: "The observation offset is malformed.",
    });
  }
  return yield* Schema.decodeUnknownEffect(CanonicalSequence)(Number(sequenceText)).pipe(
    Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
  );
});

const mapFence = (
  conversationId: ConversationMaterialization["conversationId"],
  error: SqliteFenceRejected,
) =>
  FenceRejected.make({
    conversationId,
    actualEpoch: error.actualEpoch,
    attemptedEpoch: error.producerEpoch,
  });

const encodeCanonicalRecord = Effect.fn("SqliteConversationStore.encodeCanonicalRecord")(function* (
  record: CanonicalRecord,
): Effect.fn.Return<string, ConversationStoreError> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalRecord))(record).pipe(
    Effect.mapError((error) => schemaStoreError("encode canonical record", error)),
  );
});

const encodeCanonicalBatch = Effect.fn("SqliteConversationStore.encodeCanonicalBatch")(function* (
  batch: CanonicalBatch,
): Effect.fn.Return<string, ConversationStoreError> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalBatch))(batch).pipe(
    Effect.mapError((error) => schemaStoreError("encode canonical batch", error)),
  );
});

const encodeCheckpoint = Effect.fn("SqliteConversationStore.encodeCheckpoint")(function* (
  checkpoint: ConversationCheckpoint,
): Effect.fn.Return<string, ConversationStoreError> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(ConversationCheckpoint))(checkpoint).pipe(
    Effect.mapError((error) => schemaStoreError("encode checkpoint", error)),
  );
});

const decodeEnvelope = Effect.fn("SqliteConversationStore.decodeEnvelope")(function* (row: {
  readonly batch_id: string;
  readonly conversation_id: string;
  readonly record_json: string;
  readonly sequence: CanonicalSequence;
}) {
  const record = yield* Schema.decodeEffect(Schema.fromJsonString(CanonicalRecord))(
    row.record_json,
  ).pipe(
    Effect.mapError((error) =>
      ConversationStoreError.make({
        operation: "decode canonical record",
        message: error.message,
      }),
    ),
  );
  const conversationId = yield* Schema.decodeUnknownEffect(
    CanonicalRecordEnvelope.fields.conversationId,
  )(row.conversation_id).pipe(
    Effect.mapError((error) => schemaStoreError("decode conversation identity", error)),
  );
  const offset = yield* makeOffset(conversationId, row.sequence);
  const batchId = yield* Schema.decodeUnknownEffect(CanonicalRecordEnvelope.fields.batchId)(
    row.batch_id,
  ).pipe(Effect.mapError((error) => schemaStoreError("decode batch identity", error)));
  return CanonicalRecordEnvelope.make({
    conversationId,
    batchId,
    sequence: row.sequence,
    offset,
    record,
  });
});

const decodeCheckpoint = Effect.fn("SqliteConversationStore.decodeCheckpoint")(function* (
  checkpointJson: string,
): Effect.fn.Return<ConversationCheckpoint, ConversationStoreError> {
  return yield* Schema.decodeEffect(Schema.fromJsonString(ConversationCheckpoint))(
    checkpointJson,
  ).pipe(Effect.mapError((error) => schemaStoreError("decode checkpoint", error)));
});

const requireConversation = Effect.fn("SqliteConversationStore.requireConversation")(function* (
  journal: SqliteJournal,
  conversationId: ConversationMaterialization["conversationId"],
) {
  const rows = yield* journal
    .getConversation(conversationId)
    .pipe(Effect.mapError((error) => storeError("read conversation", error)));
  if (rows.length === 0) {
    return yield* ConversationNotMaterialized.make({ conversationId });
  }
  return rows[0];
});

const tailDigestAt = Effect.fn("SqliteConversationStore.tailDigestAt")(function* (
  journal: SqliteJournal,
  conversationId: ConversationMaterialization["conversationId"],
  sequence: CanonicalSequence,
) {
  if (sequence === 0) return EMPTY_TAIL_DIGEST;
  const digests = yield* journal
    .getTailDigestAt(conversationId, sequence)
    .pipe(Effect.mapError((error) => storeError("read checkpoint digest", error)));
  if (digests.length !== 1) {
    return yield* CheckpointRejected.make({
      conversationId,
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
const decodeStartupPayloads = Effect.fn("SqliteConversationStore.decodeStartupPayloads")(function* (
  journal: SqliteJournal,
  crypto: Crypto.Crypto,
) {
  const stored = yield* journal.scanStoredPayloads();
  const batches = yield* Effect.forEach(stored.batches, (batch) =>
    Schema.decodeEffect(Schema.fromJsonString(CanonicalBatch))(batch.batch_json).pipe(
      Effect.map((decoded) => ({ decoded, row: batch })),
      Effect.mapError((error) =>
        SqliteStorageCorruptionError.make({
          table: "effect_agent_canonical_batches",
          rowKey: `${batch.conversation_id}/${batch.batch_id}`,
          message: error.message,
        }),
      ),
    ),
  );
  const records = yield* Effect.forEach(stored.records, (record) =>
    Schema.decodeEffect(Schema.fromJsonString(CanonicalRecord))(record.record_json).pipe(
      Effect.map((decoded) => ({ decoded, row: record })),
      Effect.mapError((error) =>
        SqliteStorageCorruptionError.make({
          table: "effect_agent_canonical_records",
          rowKey: `${record.conversation_id}/${record.sequence}`,
          message: error.message,
        }),
      ),
    ),
  );
  const checkpoints = yield* Effect.forEach(stored.checkpoints, (checkpoint) =>
    Schema.decodeEffect(Schema.fromJsonString(ConversationCheckpoint))(
      checkpoint.checkpoint_json,
    ).pipe(
      Effect.map((decoded) => ({ decoded, row: checkpoint })),
      Effect.mapError((error) =>
        SqliteStorageCorruptionError.make({
          table: "effect_agent_checkpoints",
          rowKey: `${checkpoint.conversation_id}/${checkpoint.through_sequence}`,
          message: error.message,
        }),
      ),
    ),
  );

  const batchesByConversation = groupByKey(batches, ({ row }) => row.conversation_id);
  const recordsByConversation = groupByKey(records, ({ row }) => row.conversation_id);
  const checkpointsByConversation = groupByKey(checkpoints, ({ row }) => row.conversation_id);
  const materializedIds = new Set(
    stored.conversations.map((conversation) => conversation.conversation_id),
  );

  for (const conversation of stored.conversations) {
    const conversationBatches = batchesByConversation.get(conversation.conversation_id) ?? [];
    const conversationRecords = recordsByConversation.get(conversation.conversation_id) ?? [];
    const conversationCheckpoints =
      checkpointsByConversation.get(conversation.conversation_id) ?? [];
    const recordsByBatch = groupByKey(conversationRecords, ({ row }) => row.batch_id);
    let previousDigest = EMPTY_TAIL_DIGEST;
    let expectedSequence = 1;
    const tailDigests = new Map<number, string>([[0, EMPTY_TAIL_DIGEST]]);

    for (const { decoded: canonicalBatch, row: batchRow } of conversationBatches) {
      const key = `${batchRow.conversation_id}/${batchRow.batch_id}`;
      if (
        canonicalBatch.batchId !== batchRow.batch_id ||
        batchRow.first_sequence !== expectedSequence ||
        batchRow.last_sequence !== batchRow.first_sequence + canonicalBatch.records.length - 1
      ) {
        return yield* SqliteStorageCorruptionError.make({
          table: "effect_agent_canonical_batches",
          rowKey: key,
          message: "Canonical batch identity, sequence, or record count is inconsistent.",
        });
      }

      const digest = yield* digestCanonicalBatch(previousDigest, canonicalBatch).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError((error) =>
          SqliteStorageCorruptionError.make({
            table: "effect_agent_canonical_batches",
            rowKey: key,
            message: error.message,
          }),
        ),
      );
      if (batchRow.batch_digest !== digest || batchRow.tail_digest !== digest) {
        return yield* SqliteStorageCorruptionError.make({
          table: "effect_agent_canonical_batches",
          rowKey: key,
          message: "Canonical batch digest does not match its decoded content and prior tail.",
        });
      }

      const batchRecords = recordsByBatch.get(batchRow.batch_id) ?? [];
      if (batchRecords.length !== canonicalBatch.records.length) {
        return yield* SqliteStorageCorruptionError.make({
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
            SqliteStorageCorruptionError.make({
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
            SqliteStorageCorruptionError.make({
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
          return yield* SqliteStorageCorruptionError.make({
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
      conversationRecords.length !== conversation.tail_sequence ||
      conversation.tail_sequence !== expectedSequence - 1 ||
      conversation.tail_digest !== previousDigest
    ) {
      return yield* SqliteStorageCorruptionError.make({
        table: "effect_agent_conversations",
        rowKey: conversation.conversation_id,
        message: "Conversation tail does not match its canonical batch chain.",
      });
    }

    for (const checkpoint of conversationCheckpoints) {
      if (
        checkpoint.decoded.conversationId !== conversation.conversation_id ||
        checkpoint.decoded.throughSequence !== checkpoint.row.through_sequence ||
        checkpoint.decoded.tailDigest !== checkpoint.row.tail_digest ||
        tailDigests.get(checkpoint.row.through_sequence) !== checkpoint.row.tail_digest
      ) {
        return yield* SqliteStorageCorruptionError.make({
          table: "effect_agent_checkpoints",
          rowKey: `${conversation.conversation_id}/${checkpoint.row.through_sequence}`,
          message: "Checkpoint identity or digest is not bound to a canonical batch tail.",
        });
      }
    }
  }

  if (
    batches.some(({ row }) => !materializedIds.has(row.conversation_id)) ||
    records.some(({ row }) => !materializedIds.has(row.conversation_id)) ||
    checkpoints.some(({ row }) => !materializedIds.has(row.conversation_id))
  ) {
    return yield* SqliteStorageCorruptionError.make({
      table: "effect_agent_conversations",
      rowKey: "startup_scan",
      message: "Canonical rows exist without a materialized Conversation.",
    });
  }
});

const makeServices = Effect.fn("SqliteConversationStore.makeServices")(function* () {
  const config = yield* SqliteStorageConfig;
  const failpoint = yield* SqliteStorageFailpoint;
  const sql = yield* SqlClientService.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const journal = yield* initializeSqliteJournal(sql, failpoint.hit, config.busyTimeout);
  if (config.verifyOnOpen) {
    yield* decodeStartupPayloads(journal, crypto);
  }

  const provideCrypto = <A, E>(effect: Effect.Effect<A, E, Crypto.Crypto>) =>
    Effect.provideService(effect, Crypto.Crypto, crypto);
  const hitFailpoint = Effect.fn("SqliteConversationStore.hitFailpoint")(
    (location: SqliteStorageFailpointLocation): Effect.Effect<void, ConversationStoreError> =>
      failpoint
        .hit(location)
        .pipe(Effect.mapError((error) => storeError(`storage failpoint ${location}`, error))),
  );

  const materialize: ConversationStore["Service"]["materialize"] = Effect.fn(
    "SqliteConversationStore.materialize",
  )(function* (request: ConversationMaterialization) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ConversationMaterialization))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate materialization", error)));
    const now = yield* Clock.currentTimeMillis;
    yield* hitFailpoint("materialize:before");
    yield* journal
      .materialize(
        validated.conversationId,
        new Date(now).toISOString(),
        EMPTY_TAIL_DIGEST,
        validated.producerEpoch,
      )
      .pipe(
        Effect.mapError((error) =>
          error._tag === "SqliteFenceRejected"
            ? mapFence(validated.conversationId, error)
            : storeError("materialize conversation", error),
        ),
      );
    yield* hitFailpoint("materialize:after");
  });

  const append: ConversationStore["Service"]["append"] = Effect.fn(
    "SqliteConversationStore.append",
  )(function* (request: FencedAppendRequest) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(FencedAppendRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate canonical append", error)));
    yield* requireConversation(journal, validated.conversationId);
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
      conversationId: validated.conversationId,
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
        if (error instanceof SqliteFenceRejected) {
          return mapFence(validated.conversationId, error);
        }
        if (error instanceof SqliteAppendConflict) {
          return error.actualTailSequence !== undefined && isDigest(error.actualTailDigest)
            ? AppendConflict.make({
                conversationId: validated.conversationId,
                batchId: validated.batch.batchId,
                reason: error.reason,
                actualTailSequence: error.actualTailSequence,
                actualTailDigest: error.actualTailDigest,
              })
            : AppendConflict.make({
                conversationId: validated.conversationId,
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

  const loadRecords = Effect.fn("SqliteConversationStore.loadRecords")(function* (
    request: RawReadRequest,
  ) {
    const rows = yield* journal
      .read(request)
      .pipe(Effect.mapError((error) => storeError("read canonical records", error)));
    return yield* Effect.forEach(rows, decodeEnvelope);
  });

  const readEffect = Effect.fn("SqliteConversationStore.read")(function* (
    request: ConversationRead,
  ) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ConversationRead))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate conversation read", error)));
    yield* requireConversation(journal, validated.conversationId);
    const records = yield* loadRecords(
      RawReadRequest.make({
        conversationId: validated.conversationId,
        fromSequenceExclusive: validated.afterSequence ?? ZERO_CANONICAL_SEQUENCE,
        limit: validated.limit,
      }),
    );
    return Stream.fromIterable(records);
  });
  const read: ConversationStore["Service"]["read"] = (request) =>
    Stream.unwrap(readEffect(request));

  const observeEffect = Effect.fn("SqliteConversationStore.observe")(function* (
    request: ConversationObservation,
  ) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ConversationObservation))(
      request,
    ).pipe(
      Effect.mapError((error) => schemaStoreError("validate conversation observation", error)),
    );
    yield* requireConversation(journal, validated.conversationId);
    const initialSequence = yield* parseOffset(validated.conversationId, validated.afterOffset);
    const cursor = yield* Ref.make(initialSequence);
    const poll = Effect.fn("SqliteConversationStore.observePoll")(function* () {
      const fromSequenceExclusive = yield* Ref.get(cursor);
      const records = yield* loadRecords(
        RawReadRequest.make({
          conversationId: validated.conversationId,
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
  const observe: ConversationStore["Service"]["observe"] = (request) =>
    Stream.unwrap(observeEffect(request));

  const exportConversation: ConversationStore["Service"]["export"] = Effect.fn(
    "SqliteConversationStore.export",
  )(function* (request: ConversationExportRequest) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ConversationExportRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate conversation export", error)));
    yield* requireConversation(journal, validated.conversationId);
    const exported = yield* journal
      .exportConversation(validated.conversationId)
      .pipe(Effect.mapError((error) => storeError("export conversation", error)));
    const records = yield* Effect.forEach(exported.records, decodeEnvelope);
    if (records.length > 65_536) {
      return yield* ConversationStoreError.make({
        operation: "decode conversation export",
        message: "The conversation exceeds the current export record limit.",
      });
    }
    const tailDigest = yield* Schema.decodeUnknownEffect(Digest)(
      exported.conversation.tail_digest,
    ).pipe(Effect.mapError((error) => schemaStoreError("decode export tail digest", error)));
    return ConversationExport.make({
      format: "effect-agent/conversation@1",
      conversationId: validated.conversationId,
      tailSequence: exported.conversation.tail_sequence,
      tailDigest,
      records,
    });
  });

  const inspectTail: ConversationStore["Service"]["inspectTail"] = Effect.fn(
    "SqliteConversationStore.inspectTail",
  )(function* (request: ConversationTailRequest) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ConversationTailRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate tail inspection", error)));
    const conversation = yield* requireConversation(journal, validated.conversationId);
    const tailDigest = yield* Schema.decodeUnknownEffect(Digest)(conversation.tail_digest).pipe(
      Effect.mapError((error) => schemaStoreError("decode tail digest", error)),
    );
    return ConversationTail.make({
      conversationId: validated.conversationId,
      tailSequence: conversation.tail_sequence,
      tailDigest,
      producerEpoch: conversation.producer_epoch,
    });
  });

  const saveCheckpoint: ConversationStore["Service"]["saveCheckpoint"] = Effect.fn(
    "SqliteConversationStore.saveCheckpoint",
  )(function* (request: SaveCheckpointRequest) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SaveCheckpointRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate checkpoint", error)));
    const conversation = yield* requireConversation(journal, validated.checkpoint.conversationId);
    if (validated.checkpoint.throughSequence > conversation.tail_sequence) {
      return yield* CheckpointRejected.make({
        conversationId: validated.checkpoint.conversationId,
        reason: "ahead-of-tail",
      });
    }
    const canonicalDigest = yield* tailDigestAt(
      journal,
      validated.checkpoint.conversationId,
      validated.checkpoint.throughSequence,
    );
    if (canonicalDigest !== validated.checkpoint.tailDigest) {
      return yield* CheckpointRejected.make({
        conversationId: validated.checkpoint.conversationId,
        reason: "digest-mismatch",
      });
    }
    const checkpointJson = yield* encodeCheckpoint(validated.checkpoint);
    const raw = RawCheckpoint.make({
      conversationId: validated.checkpoint.conversationId,
      throughSequence: validated.checkpoint.throughSequence,
      tailDigest: validated.checkpoint.tailDigest,
      checkpointJson,
    });
    yield* hitFailpoint("save-checkpoint:before");
    yield* journal.saveCheckpoint(raw).pipe(
      Effect.mapError((error) =>
        error instanceof SqliteCheckpointConflict
          ? CheckpointRejected.make({
              conversationId: validated.checkpoint.conversationId,
              reason: "digest-mismatch",
            })
          : storeError("save checkpoint", error),
      ),
    );
    yield* hitFailpoint("save-checkpoint:after");
  });

  const loadCheckpoint: ConversationStore["Service"]["loadCheckpoint"] = Effect.fn(
    "SqliteConversationStore.loadCheckpoint",
  )(function* (request: LoadCheckpointRequest) {
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(LoadCheckpointRequest))(
      request,
    ).pipe(Effect.mapError((error) => schemaStoreError("validate checkpoint lookup", error)));
    const conversation = yield* requireConversation(journal, validated.conversationId);
    const rows = yield* journal
      .loadCheckpoint(
        validated.conversationId,
        validated.atOrBeforeSequence ?? conversation.tail_sequence,
      )
      .pipe(Effect.mapError((error) => storeError("load checkpoint", error)));
    if (rows.length === 0) return Option.none();
    if (rows.length !== 1) {
      return yield* ConversationStoreError.make({
        operation: "load checkpoint",
        message: `Expected at most one checkpoint row but found ${rows.length}.`,
      });
    }
    const checkpoint = yield* decodeCheckpoint(rows[0].checkpoint_json);
    const canonicalDigest = yield* tailDigestAt(
      journal,
      checkpoint.conversationId,
      checkpoint.throughSequence,
    );
    if (canonicalDigest !== checkpoint.tailDigest) {
      return yield* CheckpointRejected.make({
        conversationId: checkpoint.conversationId,
        reason: "digest-mismatch",
      });
    }
    return Option.some(checkpoint);
  });

  const conversationStore = ConversationStore.of({
    append,
    export: exportConversation,
    inspectTail,
    loadCheckpoint,
    materialize,
    observe,
    read,
    saveCheckpoint,
  });

  return Context.make(ConversationStore, conversationStore);
});

/**
 * SQLite Conversation Store implementation with configuration, failpoint, SQL, and Crypto
 * authority kept visible in its input channel.
 */
export const conversationStoreLayer: Layer.Layer<
  ConversationStore,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(makeServices());

/**
 * Validated SQLite storage configuration Layer with the documented defaults applied. Shared
 * by the ConversationStore and SubmissionLedger convenience layers so their defaults cannot
 * drift.
 */
export const storageConfigLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SqliteStorageConfig, SqliteStorageError> =>
  Layer.effect(SqliteStorageConfig)(
    Schema.decodeUnknownEffect(SqliteStorageConfigValue)({
      observationPollInterval: options.observationPollInterval ?? 25,
      busyTimeout: options.busyTimeout ?? 5_000,
      ownershipLeaseDuration:
        options.ownershipLeaseDuration ?? Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION),
      verifyOnOpen: options.verifyOnOpen ?? false,
    }).pipe(
      Effect.mapError((error) =>
        SqliteStorageError.make({
          cause: error,
          operation: "configure SQLite storage",
          message: error.message,
        }),
      ),
    ),
  );

/** The failpoint Layer selected by convenience options: explicit handler or the no-op default. */
export const storageFailpointLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SqliteStorageFailpoint> =>
  options.failpoint === undefined
    ? SqliteStorageFailpoint.layer
    : Layer.succeed(SqliteStorageFailpoint)({ hit: options.failpoint });

/**
 * A composition-root convenience Layer for canonical Conversations. Durable accepted work is
 * served by the separate SubmissionLedger port.
 */
export const layer = (
  options: SqliteStorageOptions,
): Layer.Layer<ConversationStore, SqliteStorageInitializationError> => {
  const sqlLayer = SqliteClient.layer({ filename: options.filename });
  return conversationStoreLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        storageConfigLayer(options),
        storageFailpointLayer(options),
        sqlLayer,
        NodeCrypto.layer,
      ),
    ),
  );
};

/** Create an adapter-owned resumable observation offset for a known canonical sequence. */
export const observationOffsetAt = makeOffset;
