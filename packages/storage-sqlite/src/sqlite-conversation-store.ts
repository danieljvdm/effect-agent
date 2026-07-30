import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import {
  AppendConflict,
  AppendResult,
  CanonicalBatch,
  CanonicalRecord,
  CanonicalRecordEnvelope,
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
  digestCanonicalBatch,
  Digest,
  EMPTY_TAIL_DIGEST,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  ObservationOffset,
  SaveCheckpointRequest,
  SubmissionStore,
  SubmissionStoreCapabilities,
} from "@effect-agent/session";
import { Clock, Context, Crypto, Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import {
  SqliteAppendConflict,
  SqliteCheckpointConflict,
  SqliteFenceRejected,
  SqliteStorageFailpointError,
  type SqliteStorageFailpointLocation,
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
} from "./errors.ts";
import {
  initializeSqliteJournal,
  type RawCheckpoint,
  type RawReadRequest,
  type SqliteJournal,
} from "./sqlite-journal.ts";

export interface SqliteStorageOptions {
  readonly filename: string;
  readonly observationPollInterval?: number | undefined;
  readonly failpoint?: (
    location: SqliteStorageFailpointLocation,
  ) => Effect.Effect<void, SqliteStorageFailpointError>;
}

export type SqliteStorageInitializationError =
  | SqliteStorageCompatibilityError
  | SqliteStorageCorruptionError
  | SqliteStorageError;

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const OffsetText = Schema.String.check(Schema.isMaxLength(4 * 1024));
const SQLITE_OFFSET_PREFIX = "effect-agent-sqlite@1:";

const storeError = (operation: string, error: { readonly message: string }) =>
  new ConversationStoreError({
    operation,
    message: error.message,
  });

const schemaStoreError = (operation: string, error: { readonly message: string }) =>
  new ConversationStoreError({
    operation,
    message: error.message,
  });

const makeOffset = (
  conversationId: ConversationMaterialization["conversationId"],
  sequence: number,
): Effect.Effect<ObservationOffset, ConversationStoreError> =>
  Schema.decodeUnknownEffect(ObservationOffset)(
    `${SQLITE_OFFSET_PREFIX}${encodeURIComponent(conversationId)}:${sequence}`,
  ).pipe(Effect.mapError((error) => schemaStoreError("encode observation offset", error)));

const parseOffset = (
  conversationId: ConversationMaterialization["conversationId"],
  offset: ObservationOffset | undefined,
): Effect.Effect<number, ConversationStoreError> => {
  if (offset === undefined) return Effect.succeed(0);
  return Effect.gen(function* () {
    const text = yield* Schema.decodeUnknownEffect(OffsetText)(offset).pipe(
      Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
    );
    const conversationPrefix = `${SQLITE_OFFSET_PREFIX}${encodeURIComponent(conversationId)}:`;
    if (!text.startsWith(conversationPrefix)) {
      return yield* new ConversationStoreError({
        operation: "decode observation offset",
        message:
          "The observation offset belongs to a different adapter, storage version, or Conversation.",
      });
    }
    const sequenceText = text.slice(conversationPrefix.length);
    if (!/^(0|[1-9][0-9]*)$/.test(sequenceText)) {
      return yield* new ConversationStoreError({
        operation: "decode observation offset",
        message: "The observation offset is malformed.",
      });
    }
    return yield* Schema.decodeUnknownEffect(NonNegativeInt)(Number(sequenceText)).pipe(
      Effect.mapError((error) => schemaStoreError("decode observation offset", error)),
    );
  });
};

const mapFence = (
  conversationId: ConversationMaterialization["conversationId"],
  error: SqliteFenceRejected,
) =>
  new FenceRejected({
    conversationId,
    actualEpoch: error.actualEpoch,
    attemptedEpoch: error.producerEpoch,
  });

const encodeCanonicalRecord = (
  record: CanonicalRecord,
): Effect.Effect<string, ConversationStoreError> =>
  Schema.encodeEffect(Schema.fromJsonString(CanonicalRecord))(record).pipe(
    Effect.mapError((error) => schemaStoreError("encode canonical record", error)),
  );

const encodeCanonicalBatch = (
  batch: CanonicalBatch,
): Effect.Effect<string, ConversationStoreError> =>
  Schema.encodeEffect(Schema.fromJsonString(CanonicalBatch))(batch).pipe(
    Effect.mapError((error) => schemaStoreError("encode canonical batch", error)),
  );

const encodeCheckpoint = (
  checkpoint: ConversationCheckpoint,
): Effect.Effect<string, ConversationStoreError> =>
  Schema.encodeEffect(Schema.fromJsonString(ConversationCheckpoint))(checkpoint).pipe(
    Effect.mapError((error) => schemaStoreError("encode checkpoint", error)),
  );

const decodeEnvelope = Effect.fn("SqliteConversationStore.decodeEnvelope")(function* (row: {
  readonly batch_id: string;
  readonly conversation_id: string;
  readonly record_json: string;
  readonly sequence: number;
}) {
  const record = yield* Schema.decodeEffect(Schema.fromJsonString(CanonicalRecord))(
    row.record_json,
  ).pipe(
    Effect.mapError(
      (error) =>
        new ConversationStoreError({
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

const decodeCheckpoint = (
  checkpointJson: string,
): Effect.Effect<ConversationCheckpoint, ConversationStoreError> =>
  Schema.decodeEffect(Schema.fromJsonString(ConversationCheckpoint))(checkpointJson).pipe(
    Effect.mapError((error) => schemaStoreError("decode checkpoint", error)),
  );

const requireConversation = Effect.fn("SqliteConversationStore.requireConversation")(function* (
  journal: SqliteJournal,
  conversationId: ConversationMaterialization["conversationId"],
) {
  const rows = yield* journal
    .getConversation(conversationId)
    .pipe(Effect.mapError((error) => storeError("read conversation", error)));
  if (rows.length === 0) {
    return yield* new ConversationNotMaterialized({ conversationId });
  }
  return rows[0];
});

const tailDigestAt = Effect.fn("SqliteConversationStore.tailDigestAt")(function* (
  journal: SqliteJournal,
  conversationId: ConversationMaterialization["conversationId"],
  sequence: number,
) {
  if (sequence === 0) return EMPTY_TAIL_DIGEST;
  const digests = yield* journal
    .getTailDigestAt(conversationId, sequence)
    .pipe(Effect.mapError((error) => storeError("read checkpoint digest", error)));
  if (digests.length !== 1) {
    return yield* new CheckpointRejected({
      conversationId,
      reason: "digest-mismatch",
    });
  }
  return yield* Schema.decodeUnknownEffect(Digest)(digests[0]).pipe(
    Effect.mapError((error) => schemaStoreError("decode checkpoint digest", error)),
  );
});

const decodeStartupPayloads = Effect.fn("SqliteConversationStore.decodeStartupPayloads")(function* (
  journal: SqliteJournal,
  crypto: Crypto.Crypto,
) {
  const stored = yield* journal.scanStoredPayloads();
  const batches = yield* Effect.forEach(stored.batches, (batch) =>
    Schema.decodeEffect(Schema.fromJsonString(CanonicalBatch))(batch.batch_json).pipe(
      Effect.map((decoded) => ({ decoded, row: batch })),
      Effect.mapError(
        (error) =>
          new SqliteStorageCorruptionError({
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
      Effect.mapError(
        (error) =>
          new SqliteStorageCorruptionError({
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
      Effect.mapError(
        (error) =>
          new SqliteStorageCorruptionError({
            table: "effect_agent_checkpoints",
            rowKey: `${checkpoint.conversation_id}/${checkpoint.through_sequence}`,
            message: error.message,
          }),
      ),
    ),
  );

  for (const conversation of stored.conversations) {
    const conversationBatches = batches.filter(
      ({ row }) => row.conversation_id === conversation.conversation_id,
    );
    const conversationRecords = records.filter(
      ({ row }) => row.conversation_id === conversation.conversation_id,
    );
    const conversationCheckpoints = checkpoints.filter(
      ({ row }) => row.conversation_id === conversation.conversation_id,
    );
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
        return yield* new SqliteStorageCorruptionError({
          table: "effect_agent_canonical_batches",
          rowKey: key,
          message: "Canonical batch identity, sequence, or record count is inconsistent.",
        });
      }

      const digest = yield* digestCanonicalBatch(previousDigest, canonicalBatch).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError(
          (error) =>
            new SqliteStorageCorruptionError({
              table: "effect_agent_canonical_batches",
              rowKey: key,
              message: error.message,
            }),
        ),
      );
      if (batchRow.batch_digest !== digest || batchRow.tail_digest !== digest) {
        return yield* new SqliteStorageCorruptionError({
          table: "effect_agent_canonical_batches",
          rowKey: key,
          message: "Canonical batch digest does not match its decoded content and prior tail.",
        });
      }

      const batchRecords = conversationRecords.filter(
        ({ row }) => row.batch_id === batchRow.batch_id,
      );
      if (batchRecords.length !== canonicalBatch.records.length) {
        return yield* new SqliteStorageCorruptionError({
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
          Effect.mapError(
            (error) =>
              new SqliteStorageCorruptionError({
                table: "effect_agent_canonical_batches",
                rowKey: key,
                message: error.message,
              }),
          ),
        );
        const storedJson = yield* Schema.encodeEffect(Schema.fromJsonString(CanonicalRecord))(
          storedRecord.decoded,
        ).pipe(
          Effect.mapError(
            (error) =>
              new SqliteStorageCorruptionError({
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
          return yield* new SqliteStorageCorruptionError({
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
      return yield* new SqliteStorageCorruptionError({
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
        return yield* new SqliteStorageCorruptionError({
          table: "effect_agent_checkpoints",
          rowKey: `${conversation.conversation_id}/${checkpoint.row.through_sequence}`,
          message: "Checkpoint identity or digest is not bound to a canonical batch tail.",
        });
      }
    }
  }

  if (
    batches.some(
      ({ row }) =>
        !stored.conversations.some(
          (conversation) => conversation.conversation_id === row.conversation_id,
        ),
    ) ||
    records.some(
      ({ row }) =>
        !stored.conversations.some(
          (conversation) => conversation.conversation_id === row.conversation_id,
        ),
    ) ||
    checkpoints.some(
      ({ row }) =>
        !stored.conversations.some(
          (conversation) => conversation.conversation_id === row.conversation_id,
        ),
    )
  ) {
    return yield* new SqliteStorageCorruptionError({
      table: "effect_agent_conversations",
      rowKey: "startup_scan",
      message: "Canonical rows exist without a materialized Conversation.",
    });
  }
});

const makeServices = Effect.fn("SqliteConversationStore.makeServices")(function* (
  options: SqliteStorageOptions,
) {
  const sql = yield* SqlClientService.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const journal = yield* initializeSqliteJournal(sql);
  yield* decodeStartupPayloads(journal, crypto);

  const provideCrypto = <A, E>(effect: Effect.Effect<A, E, Crypto.Crypto>) =>
    Effect.provideService(effect, Crypto.Crypto, crypto);
  const hitFailpoint = (location: SqliteStorageFailpointLocation) =>
    options.failpoint === undefined
      ? Effect.void
      : options
          .failpoint(location)
          .pipe(Effect.mapError((error) => storeError(`storage failpoint ${location}`, error)));

  const materialize: ConversationStore["Service"]["materialize"] = (request) =>
    Effect.gen(function* () {
      const validated = yield* Schema.decodeUnknownEffect(
        Schema.toType(ConversationMaterialization),
      )(request).pipe(
        Effect.mapError((error) => schemaStoreError("validate materialization", error)),
      );
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

  const append: ConversationStore["Service"]["append"] = (request) =>
    Effect.gen(function* () {
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(FencedAppendRequest))(
        request,
      ).pipe(Effect.mapError((error) => schemaStoreError("validate canonical append", error)));
      yield* requireConversation(journal, validated.conversationId);
      const tailDigest = yield* provideCrypto(
        digestCanonicalBatch(validated.expectedTailDigest, validated.batch),
      ).pipe(Effect.mapError((error) => storeError("digest canonical append", error)));
      const batchJson = yield* encodeCanonicalBatch(validated.batch);
      const firstRecordJson = yield* encodeCanonicalRecord(validated.batch.records[0]);
      const remainingRecordJson = yield* Effect.forEach(
        validated.batch.records.slice(1),
        encodeCanonicalRecord,
      );
      const rawRecords: [
        { readonly recordId: string; readonly recordJson: string },
        ...Array<{ readonly recordId: string; readonly recordJson: string }>,
      ] = [
        {
          recordId: validated.batch.records[0].recordId,
          recordJson: firstRecordJson,
        },
        ...validated.batch.records.slice(1).map((record, index) => ({
          recordId: record.recordId,
          recordJson: remainingRecordJson[index],
        })),
      ];
      yield* hitFailpoint("append:before");
      const result = yield* journal
        .append({
          conversationId: validated.conversationId,
          batchId: validated.batch.batchId,
          batchDigest: tailDigest,
          batchJson,
          expectedTailSequence: validated.expectedTailSequence,
          expectedTailDigest: validated.expectedTailDigest,
          producerEpoch: validated.producerEpoch,
          records: rawRecords,
          tailDigest,
        })
        .pipe(
          Effect.mapError((error) => {
            if (error instanceof SqliteFenceRejected) {
              return mapFence(validated.conversationId, error);
            }
            if (error instanceof SqliteAppendConflict) {
              return new AppendConflict({
                conversationId: validated.conversationId,
                batchId: validated.batch.batchId,
                reason: error.message.includes("tail") ? "tail" : "batch-digest",
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

  const read: ConversationStore["Service"]["read"] = (request) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ConversationRead))(
          request,
        ).pipe(Effect.mapError((error) => schemaStoreError("validate conversation read", error)));
        yield* requireConversation(journal, validated.conversationId);
        const records = yield* loadRecords({
          conversationId: validated.conversationId,
          fromSequenceExclusive: validated.afterSequence ?? 0,
          limit: validated.limit,
        });
        return Stream.fromIterable(records);
      }),
    );

  const observe: ConversationStore["Service"]["observe"] = (request) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ConversationObservation))(
          request,
        ).pipe(
          Effect.mapError((error) => schemaStoreError("validate conversation observation", error)),
        );
        yield* requireConversation(journal, validated.conversationId);
        const initialSequence = yield* parseOffset(validated.conversationId, validated.afterOffset);
        const cursor = yield* Ref.make(initialSequence);
        return Stream.fromIterableEffectRepeat(
          Effect.gen(function* () {
            const fromSequenceExclusive = yield* Ref.get(cursor);
            const records = yield* loadRecords({
              conversationId: validated.conversationId,
              fromSequenceExclusive,
              limit: 1_024,
            });
            if (records.length === 0) {
              yield* Effect.sleep(options.observationPollInterval ?? 25);
              return [];
            }
            yield* Ref.set(cursor, records[records.length - 1].sequence);
            return records;
          }),
        );
      }),
    );

  const exportConversation: ConversationStore["Service"]["export"] = (request) =>
    Effect.gen(function* () {
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ConversationExportRequest))(
        request,
      ).pipe(Effect.mapError((error) => schemaStoreError("validate conversation export", error)));
      yield* requireConversation(journal, validated.conversationId);
      const exported = yield* journal
        .exportConversation(validated.conversationId)
        .pipe(Effect.mapError((error) => storeError("export conversation", error)));
      const records = yield* Effect.forEach(exported.records, decodeEnvelope);
      if (records.length > 65_536) {
        return yield* new ConversationStoreError({
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

  const saveCheckpoint: ConversationStore["Service"]["saveCheckpoint"] = (request) =>
    Effect.gen(function* () {
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SaveCheckpointRequest))(
        request,
      ).pipe(Effect.mapError((error) => schemaStoreError("validate checkpoint", error)));
      const conversation = yield* requireConversation(journal, validated.checkpoint.conversationId);
      if (validated.checkpoint.throughSequence > conversation.tail_sequence) {
        return yield* new CheckpointRejected({
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
        return yield* new CheckpointRejected({
          conversationId: validated.checkpoint.conversationId,
          reason: "digest-mismatch",
        });
      }
      const checkpointJson = yield* encodeCheckpoint(validated.checkpoint);
      const raw: RawCheckpoint = {
        conversationId: validated.checkpoint.conversationId,
        throughSequence: validated.checkpoint.throughSequence,
        tailDigest: validated.checkpoint.tailDigest,
        checkpointJson,
      };
      yield* hitFailpoint("save-checkpoint:before");
      yield* journal.saveCheckpoint(raw).pipe(
        Effect.mapError((error) =>
          error instanceof SqliteCheckpointConflict
            ? new CheckpointRejected({
                conversationId: validated.checkpoint.conversationId,
                reason: "digest-mismatch",
              })
            : storeError("save checkpoint", error),
        ),
      );
      yield* hitFailpoint("save-checkpoint:after");
    });

  const loadCheckpoint: ConversationStore["Service"]["loadCheckpoint"] = (request) =>
    Effect.gen(function* () {
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
        return yield* new ConversationStoreError({
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
        return yield* new CheckpointRejected({
          conversationId: checkpoint.conversationId,
          reason: "digest-mismatch",
        });
      }
      return Option.some(checkpoint);
    });

  const conversationStore = ConversationStore.of({
    append,
    export: exportConversation,
    loadCheckpoint,
    materialize,
    observe,
    read,
    saveCheckpoint,
  });

  const submissionStore = SubmissionStore.of({
    capabilities: Effect.succeed(
      SubmissionStoreCapabilities.make({
        durability: "non-durable",
        acceptsDurableWork: false,
      }),
    ),
    inspect: () => Effect.succeed(Option.none()),
  });

  return Context.make(ConversationStore, conversationStore).pipe(
    Context.add(SubmissionStore, submissionStore),
  );
});

/**
 * A Node SQLite persistence Layer for canonical Conversations.
 *
 * It explicitly exposes a non-durable SubmissionStore: this Phase 3 adapter persists
 * conversation history but does not accept or settle durable work.
 */
export const layer = (
  options: SqliteStorageOptions,
): Layer.Layer<ConversationStore | SubmissionStore, SqliteStorageInitializationError> => {
  const sqlLayer = SqliteClient.layer({ filename: options.filename });
  return Layer.effectContext(makeServices(options)).pipe(
    Layer.provide(Layer.merge(sqlLayer, NodeCrypto.layer)),
  );
};

/** Create an adapter-owned resumable observation offset for a known canonical sequence. */
export const observationOffsetAt = makeOffset;
