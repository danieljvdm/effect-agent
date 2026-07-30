import { Crypto, Effect, Encoding, Layer, Option, PubSub, Ref, Schema, Stream } from "effect";

import {
  AppendConflict,
  AppendResult,
  CanonicalRecordEnvelope,
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
  digestCanonicalBatch,
  EMPTY_TAIL_DIGEST,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  type ObservationOffset,
  SaveCheckpointRequest,
  SubmissionStore,
  SubmissionStoreCapabilities,
  type BatchId,
  type Digest,
} from "@effect-agent/session";
import type { ConversationId } from "@effect-agent/core";

const MAX_CONVERSATIONS = 256;
const MAX_RECORDS_PER_CONVERSATION = 65_536;
const MAX_CHECKPOINTS_PER_CONVERSATION = 1_024;

interface StoredBatch {
  readonly digest: Digest;
  readonly result: AppendResult;
}

interface StoredConversation {
  readonly producerEpoch: number;
  readonly tailSequence: number;
  readonly tailDigest: Digest;
  readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
  readonly recordIds: ReadonlySet<string>;
  readonly batches: ReadonlyMap<BatchId, StoredBatch>;
  readonly tailDigests: ReadonlyMap<number, Digest>;
  readonly checkpoints: ReadonlyMap<number, ConversationCheckpoint>;
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

const storeError = (operation: string, message: string): ConversationStoreError =>
  ConversationStoreError.make({ operation, message });

const validate = <A, I>(
  schema: Schema.Codec<A, I>,
  operation: string,
  value: unknown,
): Effect.Effect<A, ConversationStoreError> =>
  Schema.encodeUnknownEffect(schema)(value).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(schema)),
    Effect.mapError((error) => storeError(operation, error.message)),
  );

const offsetSequence = (
  conversationId: ConversationId,
  offset: ObservationOffset | undefined,
): Effect.Effect<number, ConversationStoreError> => {
  if (offset === undefined) return Effect.succeed(0);
  const prefix = `memory:v1:${Encoding.encodeBase64(conversationId)}:`;
  const encodedSequence = offset.startsWith(prefix) ? offset.slice(prefix.length) : "";
  if (!/^\d+$/.test(encodedSequence)) {
    return Effect.fail(storeError("observe", "Malformed observation offset"));
  }
  const sequence = Number(encodedSequence);
  return Number.isSafeInteger(sequence)
    ? Effect.succeed(sequence)
    : Effect.fail(storeError("observe", "Malformed observation offset"));
};

const observationOffset = (conversationId: ConversationId, sequence: number): ObservationOffset =>
  Schema.decodeSync(
    Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/ObservationOffset")),
  )(`memory:v1:${Encoding.encodeBase64(conversationId)}:${sequence}`);

const findConversation = (
  state: MemoryState,
  conversationId: ConversationId,
): Effect.Effect<StoredConversation, ConversationNotMaterialized> => {
  const conversation = state.conversations.get(conversationId);
  return conversation === undefined
    ? Effect.fail(ConversationNotMaterialized.make({ conversationId }))
    : Effect.succeed(conversation);
};

const makeConversationStore = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const state = yield* Ref.make<MemoryState>({ conversations: new Map() });
  const updates = yield* PubSub.unbounded<CanonicalRecordEnvelope>();
  yield* Effect.addFinalizer(() => PubSub.shutdown(updates));

  const materialize: ConversationStore["Service"]["materialize"] = (unvalidated) =>
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
            tailSequence: 0,
            tailDigest: EMPTY_TAIL_DIGEST,
            records: [],
            recordIds: new Set(),
            batches: new Map(),
            tailDigests: new Map([[0, EMPTY_TAIL_DIGEST]]),
            checkpoints: new Map(),
          });
          return [{ _tag: "success" }, { conversations }];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
    });

  const append: ConversationStore["Service"]["append"] = (unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(FencedAppendRequest, "append", unvalidated);
      const digest = yield* digestCanonicalBatch(request.expectedTailDigest, request.batch).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError((error) => storeError("append", error.message)),
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

          const batchRecordIds = new Set<string>();
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
                    reason: "batch-digest",
                  }),
                },
                current,
              ];
            }
            batchRecordIds.add(record.recordId);
          }

          const records = request.batch.records.map((record, index) => {
            const sequence = conversation.tailSequence + index + 1;
            return CanonicalRecordEnvelope.make({
              conversationId: request.conversationId,
              batchId: request.batch.batchId,
              sequence,
              offset: observationOffset(request.conversationId, sequence),
              record,
            });
          });
          const lastSequence = conversation.tailSequence + records.length;
          const result = AppendResult.make({
            firstSequence: conversation.tailSequence + 1,
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
              ? PubSub.publishAll(updates, decision.records)
              : Effect.void,
          ),
        ),
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.result;
    });

  const readSnapshot = (
    conversationId: ConversationId,
    afterSequence: number | undefined,
    limit: number,
  ) =>
    Ref.get(state).pipe(
      Effect.flatMap((current) => findConversation(current, conversationId)),
      Effect.map((conversation) =>
        conversation.records
          .filter((record) => record.sequence > (afterSequence ?? 0))
          .slice(0, limit),
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
              initial.length === 0 ? afterSequence : initial[initial.length - 1]!.sequence;
            const live = Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(
              Stream.filter(
                (record) =>
                  record.conversationId === request.conversationId && record.sequence > highWater,
              ),
            );
            return Stream.fromIterable(initial).pipe(Stream.concat(live));
          }),
        );
      }),
    );

  const exportConversation: ConversationStore["Service"]["export"] = (unvalidated) =>
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
    });

  const saveCheckpoint: ConversationStore["Service"]["saveCheckpoint"] = (unvalidated) =>
    Effect.gen(function* () {
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
    });

  const loadCheckpoint: ConversationStore["Service"]["loadCheckpoint"] = (unvalidated) =>
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
    });

  return ConversationStore.of({
    materialize,
    append,
    read,
    observe,
    export: exportConversation,
    saveCheckpoint,
    loadCheckpoint,
  });
});

export const MemoryConversationStoreLive = Layer.effect(ConversationStore, makeConversationStore);

export const MemorySubmissionStoreLive = Layer.succeed(
  SubmissionStore,
  SubmissionStore.of({
    capabilities: Effect.succeed(
      SubmissionStoreCapabilities.make({
        durability: "non-durable",
        acceptsDurableWork: false,
      }),
    ),
    inspect: () => Effect.succeed(Option.none()),
  }),
);

export const MemoryStorageLive = Layer.merge(
  MemoryConversationStoreLive,
  MemorySubmissionStoreLive,
);
