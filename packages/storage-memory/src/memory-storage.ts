import { Crypto, Effect, Encoding, Layer, Option, PubSub, Ref, Schema, Stream } from "effect";

import {
  AppendConflict,
  AppendResult,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  CheckpointRejected,
  ConversationExportRequest,
  ConversationCheckpoint,
  ConversationExport,
  ConversationMaterialization,
  ConversationNotMaterialized,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  ConversationStoreError,
  ConversationTail,
  ConversationTailRequest,
  digestCanonicalBatch,
  EMPTY_TAIL_DIGEST,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  ObservationOffset,
  ProducerEpoch,
  RecordId,
  SaveCheckpointRequest,
  type BatchId,
  type Digest,
} from "@effect-agent/session";
import { ConversationId } from "@effect-agent/core";

const MAX_CONVERSATIONS = 256;
const MAX_RECORDS_PER_CONVERSATION = 65_536;
const MAX_CHECKPOINTS_PER_CONVERSATION = 1_024;

interface StoredBatch {
  readonly digest: Digest;
  readonly result: AppendResult;
}

interface StoredConversation {
  readonly producerEpoch: ProducerEpoch;
  readonly tailSequence: CanonicalSequence;
  readonly tailDigest: Digest;
  readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
  readonly recordIds: ReadonlySet<RecordId>;
  readonly batches: ReadonlyMap<BatchId, StoredBatch>;
  readonly tailDigests: ReadonlyMap<CanonicalSequence, Digest>;
  readonly checkpoints: ReadonlyMap<CanonicalSequence, ConversationCheckpoint>;
}

interface MemoryState {
  readonly conversations: ReadonlyMap<ConversationId, StoredConversation>;
}

type AppendDecision =
  | {
      readonly _tag: "failure";
      readonly error:
        | ConversationStoreError
        | ConversationNotMaterialized
        | AppendConflict
        | FenceRejected;
    }
  | {
      readonly _tag: "success";
      readonly result: AppendResult;
      readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
    };

type MaterializeDecision =
  | { readonly _tag: "failure"; readonly error: ConversationStoreError | FenceRejected }
  | { readonly _tag: "success" };

type CheckpointDecision =
  | {
      readonly _tag: "failure";
      readonly error: ConversationNotMaterialized | ConversationStoreError | CheckpointRejected;
    }
  | { readonly _tag: "success" };

const storeError = (operation: string, message: string, cause?: unknown): ConversationStoreError =>
  cause === undefined
    ? ConversationStoreError.make({ operation, message })
    : ConversationStoreError.make({ operation, message, cause });

const validate = Effect.fn("MemoryConversationStore.validate")(
  <A, I>(
    schema: Schema.Codec<A, I>,
    operation: string,
    value: unknown,
  ): Effect.Effect<A, ConversationStoreError> =>
    Schema.encodeUnknownEffect(schema)(value).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
      Effect.mapError((error) => storeError(operation, `Invalid ${operation} request`, error)),
    ),
);

const decodeCanonicalSequence = Schema.decodeSync(CanonicalSequence);
const ZERO_CANONICAL_SEQUENCE = decodeCanonicalSequence(0);

const offsetSequence = Effect.fn("MemoryConversationStore.offsetSequence")((
  conversationId: ConversationId,
  offset: ObservationOffset | undefined,
): Effect.Effect<CanonicalSequence, ConversationStoreError> => {
  if (offset === undefined) return Effect.succeed(ZERO_CANONICAL_SEQUENCE);
  const prefix = `memory:v1:${Encoding.encodeBase64(conversationId)}:`;
  const encodedSequence = offset.startsWith(prefix) ? offset.slice(prefix.length) : "";
  if (!/^\d+$/.test(encodedSequence)) {
    return Effect.fail(storeError("observe", "Malformed observation offset"));
  }
  const sequence = Number(encodedSequence);
  return Number.isSafeInteger(sequence)
    ? Schema.decodeUnknownEffect(CanonicalSequence)(sequence).pipe(
        Effect.mapError(() => storeError("observe", "Malformed observation offset")),
      )
    : Effect.fail(storeError("observe", "Malformed observation offset"));
});

const observationOffset = (
  conversationId: ConversationId,
  sequence: CanonicalSequence,
): ObservationOffset =>
  Schema.decodeSync(ObservationOffset)(
    `memory:v1:${Encoding.encodeBase64(conversationId)}:${sequence}`,
  );

const findConversation = Effect.fn("MemoryConversationStore.findConversation")((
  state: MemoryState,
  conversationId: ConversationId,
): Effect.Effect<StoredConversation, ConversationNotMaterialized> => {
  const conversation = state.conversations.get(conversationId);
  return conversation === undefined
    ? Effect.fail(ConversationNotMaterialized.make({ conversationId }))
    : Effect.succeed(conversation);
});

const CheckpointVersionEnvelope = Schema.Struct({
  checkpoint: Schema.Struct({
    conversationId: ConversationId,
    schemaVersion: Schema.Natural,
  }),
});

const validateCheckpointVersion = Effect.fn("MemoryConversationStore.validateCheckpointVersion")(
  function* (value: unknown): Effect.fn.Return<void, ConversationStoreError | CheckpointRejected> {
    const envelope = yield* Schema.decodeUnknownEffect(CheckpointVersionEnvelope)(value).pipe(
      Effect.mapError(() => storeError("saveCheckpoint", "Invalid saveCheckpoint request")),
    );
    if (envelope.checkpoint.schemaVersion !== 1) {
      return yield* CheckpointRejected.make({
        conversationId: envelope.checkpoint.conversationId,
        reason: "unsupported-version",
      });
    }
  },
);

const makeConversationStore = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const state = yield* Ref.make<MemoryState>({ conversations: new Map() });
  const updates = yield* PubSub.sliding<void>(1);
  yield* Effect.addFinalizer(() => PubSub.shutdown(updates));

  const materialize: ConversationStore["Service"]["materialize"] = Effect.fn(
    "MemoryConversationStore.materialize",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(ConversationMaterialization, "materialize", unvalidated);
      const decision = yield* Ref.modify(
        state,
        (current): readonly [MaterializeDecision, MemoryState] => {
          const existing = current.conversations.get(request.conversationId);
          if (existing !== undefined) {
            if (request.producerEpoch < existing.producerEpoch) {
              return [
                {
                  _tag: "failure",
                  error: FenceRejected.make({
                    conversationId: request.conversationId,
                    actualEpoch: existing.producerEpoch,
                    attemptedEpoch: request.producerEpoch,
                  }),
                },
                current,
              ];
            }
            if (request.producerEpoch === existing.producerEpoch) {
              return [{ _tag: "success" }, current];
            }
            const conversations = new Map(current.conversations);
            conversations.set(request.conversationId, {
              ...existing,
              producerEpoch: request.producerEpoch,
            });
            return [{ _tag: "success" }, { conversations }];
          }
          if (current.conversations.size >= MAX_CONVERSATIONS) {
            return [
              {
                _tag: "failure",
                error: storeError(
                  "materialize",
                  `In-memory conversation limit ${MAX_CONVERSATIONS} exceeded`,
                ),
              },
              current,
            ];
          }
          const conversations = new Map(current.conversations);
          conversations.set(request.conversationId, {
            producerEpoch: request.producerEpoch,
            tailSequence: ZERO_CANONICAL_SEQUENCE,
            tailDigest: EMPTY_TAIL_DIGEST,
            records: [],
            recordIds: new Set(),
            batches: new Map(),
            tailDigests: new Map([[ZERO_CANONICAL_SEQUENCE, EMPTY_TAIL_DIGEST]]),
            checkpoints: new Map(),
          });
          return [{ _tag: "success" }, { conversations }];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const append: ConversationStore["Service"]["append"] = Effect.fn(
    "MemoryConversationStore.append",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(FencedAppendRequest, "append", unvalidated);
      const digest = yield* digestCanonicalBatch(request.expectedTailDigest, request.batch).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError((error) => storeError("append", error.message, error)),
      );

      const decision = yield* Effect.uninterruptible(
        Ref.modify(state, (current): readonly [AppendDecision, MemoryState] => {
          const conversation = current.conversations.get(request.conversationId);
          if (conversation === undefined) {
            return [
              {
                _tag: "failure",
                error: ConversationNotMaterialized.make({
                  conversationId: request.conversationId,
                }),
              },
              current,
            ];
          }
          if (request.producerEpoch !== conversation.producerEpoch) {
            return [
              {
                _tag: "failure",
                error: FenceRejected.make({
                  conversationId: request.conversationId,
                  actualEpoch: conversation.producerEpoch,
                  attemptedEpoch: request.producerEpoch,
                }),
              },
              current,
            ];
          }
          const previous = conversation.batches.get(request.batch.batchId);
          if (previous !== undefined) {
            if (previous.digest !== digest) {
              return [
                {
                  _tag: "failure",
                  error: AppendConflict.make({
                    conversationId: request.conversationId,
                    batchId: request.batch.batchId,
                    reason: "batch-digest",
                  }),
                },
                current,
              ];
            }
            return [
              {
                _tag: "success",
                result: AppendResult.make({
                  firstSequence: previous.result.firstSequence,
                  lastSequence: previous.result.lastSequence,
                  tailDigest: previous.result.tailDigest,
                  replayed: true,
                }),
                records: [],
              },
              current,
            ];
          }
          if (
            request.expectedTailSequence !== conversation.tailSequence ||
            request.expectedTailDigest !== conversation.tailDigest
          ) {
            return [
              {
                _tag: "failure",
                error: AppendConflict.make({
                  conversationId: request.conversationId,
                  batchId: request.batch.batchId,
                  reason: "tail",
                  actualTailSequence: conversation.tailSequence,
                  actualTailDigest: conversation.tailDigest,
                }),
              },
              current,
            ];
          }
          if (
            conversation.records.length + request.batch.records.length >
            MAX_RECORDS_PER_CONVERSATION
          ) {
            return [
              {
                _tag: "failure",
                error: storeError(
                  "append",
                  `In-memory record limit ${MAX_RECORDS_PER_CONVERSATION} exceeded`,
                ),
              },
              current,
            ];
          }

          const batchRecordIds = new Set<RecordId>();
          for (const record of request.batch.records) {
            if (
              conversation.recordIds.has(record.recordId) ||
              batchRecordIds.has(record.recordId)
            ) {
              return [
                {
                  _tag: "failure",
                  error: AppendConflict.make({
                    conversationId: request.conversationId,
                    batchId: request.batch.batchId,
                    reason: "record-identity",
                  }),
                },
                current,
              ];
            }
            batchRecordIds.add(record.recordId);
          }

          const records = request.batch.records.map((record, index) => {
            const sequence = decodeCanonicalSequence(conversation.tailSequence + index + 1);
            return CanonicalRecordEnvelope.make({
              conversationId: request.conversationId,
              batchId: request.batch.batchId,
              sequence,
              offset: observationOffset(request.conversationId, sequence),
              record,
            });
          });
          const lastSequence = decodeCanonicalSequence(conversation.tailSequence + records.length);
          const result = AppendResult.make({
            firstSequence: decodeCanonicalSequence(conversation.tailSequence + 1),
            lastSequence,
            tailDigest: digest,
            replayed: false,
          });
          const batches = new Map(conversation.batches);
          batches.set(request.batch.batchId, { digest, result });
          const recordIds = new Set(conversation.recordIds);
          for (const recordId of batchRecordIds) recordIds.add(recordId);
          const tailDigests = new Map(conversation.tailDigests);
          tailDigests.set(lastSequence, digest);
          const conversations = new Map(current.conversations);
          conversations.set(request.conversationId, {
            ...conversation,
            tailSequence: lastSequence,
            tailDigest: digest,
            records: [...conversation.records, ...records],
            recordIds,
            batches,
            tailDigests,
          });
          return [{ _tag: "success", result, records }, { conversations }];
        }).pipe(
          Effect.tap((decision) =>
            decision._tag === "success" && decision.records.length > 0
              ? PubSub.publish(updates, undefined)
              : Effect.void,
          ),
        ),
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.result;
    }),
  );

  const readSnapshot = Effect.fn("MemoryConversationStore.readSnapshot")(
    (conversationId: ConversationId, afterSequence: CanonicalSequence | undefined, limit: number) =>
      Ref.get(state).pipe(
        Effect.flatMap((current) => findConversation(current, conversationId)),
        Effect.map((conversation) =>
          conversation.records
            .filter((record) => record.sequence > (afterSequence ?? ZERO_CANONICAL_SEQUENCE))
            .slice(0, limit),
        ),
      ),
  );

  const read: ConversationStore["Service"]["read"] = (unvalidated) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ConversationRead, "read", unvalidated);
        const records = yield* readSnapshot(
          request.conversationId,
          request.afterSequence,
          request.limit,
        );
        return Stream.fromIterable(records);
      }),
    );

  const observe: ConversationStore["Service"]["observe"] = (unvalidated) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ConversationObservation, "observe", unvalidated);
        const afterSequence = yield* offsetSequence(request.conversationId, request.afterOffset);
        return Stream.unwrap(
          Effect.gen(function* () {
            const subscription = yield* PubSub.subscribe(updates);
            const initial = yield* readSnapshot(
              request.conversationId,
              afterSequence,
              MAX_RECORDS_PER_CONVERSATION,
            );
            const highWater =
              initial.length === 0 ? afterSequence : (initial.at(-1)?.sequence ?? afterSequence);
            const live = Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(
              Stream.mapAccumEffect(
                () => highWater,
                (lastSequence) =>
                  readSnapshot(
                    request.conversationId,
                    lastSequence,
                    MAX_RECORDS_PER_CONVERSATION,
                  ).pipe(
                    Effect.map(
                      (records) => [records.at(-1)?.sequence ?? lastSequence, records] as const,
                    ),
                  ),
              ),
            );
            return Stream.fromIterable(initial).pipe(Stream.concat(live));
          }),
        );
      }),
    );

  const exportConversation: ConversationStore["Service"]["export"] = Effect.fn(
    "MemoryConversationStore.export",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(ConversationExportRequest, "export", unvalidated);
      const conversation = yield* Ref.get(state).pipe(
        Effect.flatMap((current) => findConversation(current, request.conversationId)),
      );
      return ConversationExport.make({
        format: "effect-agent/conversation@1",
        conversationId: request.conversationId,
        tailSequence: conversation.tailSequence,
        tailDigest: conversation.tailDigest,
        records: conversation.records,
      });
    }),
  );

  const inspectTail: ConversationStore["Service"]["inspectTail"] = Effect.fn(
    "MemoryConversationStore.inspectTail",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(ConversationTailRequest, "inspectTail", unvalidated);
      const conversation = yield* Ref.get(state).pipe(
        Effect.flatMap((current) => findConversation(current, request.conversationId)),
      );
      return ConversationTail.make({
        conversationId: request.conversationId,
        tailSequence: conversation.tailSequence,
        tailDigest: conversation.tailDigest,
        producerEpoch: conversation.producerEpoch,
      });
    }),
  );

  const saveCheckpoint: ConversationStore["Service"]["saveCheckpoint"] = Effect.fn(
    "MemoryConversationStore.saveCheckpoint",
  )((unvalidated) =>
    Effect.gen(function* () {
      yield* validateCheckpointVersion(unvalidated);
      const request = yield* validate(SaveCheckpointRequest, "saveCheckpoint", unvalidated);
      const decision = yield* Ref.modify(
        state,
        (current): readonly [CheckpointDecision, MemoryState] => {
          const checkpoint = request.checkpoint;
          const conversation = current.conversations.get(checkpoint.conversationId);
          if (conversation === undefined) {
            return [
              {
                _tag: "failure",
                error: ConversationNotMaterialized.make({
                  conversationId: checkpoint.conversationId,
                }),
              },
              current,
            ];
          }
          if (checkpoint.throughSequence > conversation.tailSequence) {
            return [
              {
                _tag: "failure",
                error: CheckpointRejected.make({
                  conversationId: checkpoint.conversationId,
                  reason: "ahead-of-tail",
                }),
              },
              current,
            ];
          }
          if (conversation.tailDigests.get(checkpoint.throughSequence) !== checkpoint.tailDigest) {
            return [
              {
                _tag: "failure",
                error: CheckpointRejected.make({
                  conversationId: checkpoint.conversationId,
                  reason: "digest-mismatch",
                }),
              },
              current,
            ];
          }
          if (
            !conversation.checkpoints.has(checkpoint.throughSequence) &&
            conversation.checkpoints.size >= MAX_CHECKPOINTS_PER_CONVERSATION
          ) {
            return [
              {
                _tag: "failure",
                error: storeError(
                  "saveCheckpoint",
                  `In-memory checkpoint limit ${MAX_CHECKPOINTS_PER_CONVERSATION} exceeded`,
                ),
              },
              current,
            ];
          }
          const checkpoints = new Map(conversation.checkpoints);
          checkpoints.set(checkpoint.throughSequence, checkpoint);
          const conversations = new Map(current.conversations);
          conversations.set(checkpoint.conversationId, { ...conversation, checkpoints });
          return [{ _tag: "success" }, { conversations }];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const loadCheckpoint: ConversationStore["Service"]["loadCheckpoint"] = Effect.fn(
    "MemoryConversationStore.loadCheckpoint",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(LoadCheckpointRequest, "loadCheckpoint", unvalidated);
      const conversation = yield* Ref.get(state).pipe(
        Effect.flatMap((current) => findConversation(current, request.conversationId)),
      );
      const maximum = request.atOrBeforeSequence ?? conversation.tailSequence;
      let selected: ConversationCheckpoint | undefined;
      for (const [sequence, checkpoint] of conversation.checkpoints) {
        if (
          sequence <= maximum &&
          (selected === undefined || sequence > selected.throughSequence)
        ) {
          selected = checkpoint;
        }
      }
      if (
        selected !== undefined &&
        conversation.tailDigests.get(selected.throughSequence) !== selected.tailDigest
      ) {
        return yield* CheckpointRejected.make({
          conversationId: request.conversationId,
          reason: "digest-mismatch",
        });
      }
      return Option.fromNullishOr(selected);
    }),
  );

  return ConversationStore.of({
    materialize,
    append,
    read,
    observe,
    export: exportConversation,
    inspectTail,
    saveCheckpoint,
    loadCheckpoint,
  });
});

export const MemoryConversationStoreLive = Layer.effect(ConversationStore, makeConversationStore);

/**
 * In-memory canonical Conversation persistence. Durable accepted work is served by the separate
 * SubmissionLedger port; this Layer deliberately provides only the ConversationStore.
 */
export const MemoryStorageLive = MemoryConversationStoreLive;
