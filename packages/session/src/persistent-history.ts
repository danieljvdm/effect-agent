import {
  type ConversationId,
  type RunCompleted as RunCompletedEvent,
  type RunId,
} from "@effect-agent/core";
import {
  ConversationHistory,
  ConversationHistoryError,
  type ConversationHistoryRun,
} from "@effect-agent/engine";
import { DateTime, Effect, Layer, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";

import {
  BatchId,
  CanonicalBatch,
  DeploymentId,
  ModelCompleted,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RunCompleted,
  UserInputRecorded,
} from "./records.ts";
import { promptFromCanonicalRecords } from "./run-journal.ts";
import {
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationStore,
  FencedAppendRequest,
  MAX_CONVERSATION_EXPORT_RECORDS,
  type ConversationStoreFailure,
} from "./store.ts";

const HISTORY_EPOCH = Schema.decodeSync(ProducerEpoch)(0);
const HISTORY_DEPLOYMENT = Schema.decodeSync(DeploymentId)("persistent-history");
const batchId = Schema.decodeSync(BatchId);
const producer = Schema.decodeSync(ProducerId);
const recordId = Schema.decodeSync(RecordId);

const historyError = (
  conversationId: ConversationId,
  reason: ConversationHistoryError["reason"],
  message: string,
  cause?: unknown,
) =>
  ConversationHistoryError.make({
    conversationId,
    reason,
    message,
    ...(cause === undefined ? {} : { cause }),
  });

const storageError = (conversationId: ConversationId, cause: ConversationStoreFailure) =>
  historyError(
    conversationId,
    cause._tag === "AppendConflict"
      ? "conflict"
      : cause._tag === "FenceRejected"
        ? "fenced"
        : cause._tag === "ConversationNotMaterialized"
          ? "not-found"
          : "storage",
    cause._tag === "ConversationStoreError" ? cause.message : cause._tag,
    cause,
  );

/**
 * Provide retained history to the normal AgentRuntime entry points. Each successful Run appends
 * one three-record batch. Staging is private to that Run; interruption discards it. Epoch zero
 * and the loaded tail fence stale writers without replaying external execution. A storage failure
 * after append may leave the whole Run recorded. Adapter failpoints cover both durable mutations.
 */
const layer = Layer.effect(
  ConversationHistory,
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    const load = Effect.fn("PersistentHistory.load")(function* (conversationId: ConversationId) {
      const exported = yield* store
        .export(ConversationExportRequest.make({ conversationId }))
        .pipe(Effect.mapError((cause) => storageError(conversationId, cause)));
      return yield* promptFromCanonicalRecords(exported.records).pipe(
        Effect.mapError((cause) => historyError(conversationId, "encoding", cause.message, cause)),
      );
    });
    const open = Effect.fn("PersistentHistory.open")(function* ({
      conversationId,
      runId,
    }: {
      readonly conversationId: ConversationId;
      readonly runId: RunId;
    }): Effect.fn.Return<ConversationHistoryRun, ConversationHistoryError> {
      const error = (
        reason: ConversationHistoryError["reason"],
        message: string,
        cause?: unknown,
      ) => historyError(conversationId, reason, message, cause);
      const persisted = (value: unknown) =>
        Schema.decodeUnknownEffect(PersistedJson)(value).pipe(
          Effect.mapError((cause) =>
            error("limit", "Run data exceeds canonical persistence bounds", cause),
          ),
        );
      yield* store
        .materialize(
          ConversationMaterialization.make({ conversationId, producerEpoch: HISTORY_EPOCH }),
        )
        .pipe(Effect.mapError((cause) => storageError(conversationId, cause)));
      const base = yield* store
        .export(ConversationExportRequest.make({ conversationId }))
        .pipe(Effect.mapError((cause) => storageError(conversationId, cause)));
      if (base.records.length + 3 > MAX_CONVERSATION_EXPORT_RECORDS) {
        return yield* error(
          "limit",
          `This Run would exceed the history limit of ${MAX_CONVERSATION_EXPORT_RECORDS} records; use a new Conversation`,
        );
      }
      if (
        base.records.some(
          ({ record }) =>
            (record.payload._tag === "UserInputRecorded" &&
              record.payload.submissionId !== undefined) ||
            record.payload._tag === "SubmissionSettled" ||
            record.payload._tag === "AbortRequested",
        )
      ) {
        return yield* error(
          "incompatible",
          "This Conversation belongs to durable accepted work; use a separate history Conversation",
        );
      }
      const prompt = yield* promptFromCanonicalRecords(base.records).pipe(
        Effect.mapError((cause) => error("encoding", cause.message, cause)),
      );
      const createdAt = yield* DateTime.now;
      const record = (id: string, payload: RecordEnvelope["payload"], timestamp = createdAt) =>
        RecordEnvelope.make({
          recordId: recordId(id),
          family: "conversation",
          schemaVersion: 1,
          deploymentId: HISTORY_DEPLOYMENT,
          createdAt: timestamp,
          payload,
        });
      let input: RecordEnvelope | undefined;
      let messages: PersistedJson | undefined;
      return {
        prompt,
        stageInput: Effect.fn("PersistentHistory.stageInput")(function* (encodedInput: unknown) {
          input = record(
            `history-input:${runId}`,
            UserInputRecorded.make({
              kind: "user",
              runId,
              input: yield* persisted(encodedInput),
            }),
          );
        }),
        stageHistory: Effect.fn("PersistentHistory.stageHistory")(function* (next: Prompt.Prompt) {
          messages = yield* Schema.encodeEffect(Prompt.Prompt)(
            Prompt.fromMessages(next.content.slice(prompt.content.length)),
          ).pipe(
            Effect.mapError((cause) =>
              error("encoding", "Run history could not be encoded", cause),
            ),
            Effect.flatMap(persisted),
          );
        }),
        commit: Effect.fn("PersistentHistory.commit")(function* (completion: RunCompletedEvent) {
          if (input === undefined || messages === undefined) {
            return yield* error("encoding", "Run completed without its encoded input and history");
          }
          const output = yield* persisted(completion.output);
          const runDisposition =
            completion.runDisposition === undefined
              ? undefined
              : yield* persisted(completion.runDisposition);
          const completedAt = yield* DateTime.now;
          const modelCompleted = yield* ModelCompleted.makeEffect({ runId, output, messages }).pipe(
            Effect.mapError((cause) =>
              error("encoding", "Run history is not a canonical Prompt", cause),
            ),
          );
          yield* store
            .append(
              FencedAppendRequest.make({
                conversationId,
                producerEpoch: HISTORY_EPOCH,
                expectedTailSequence: base.tailSequence,
                expectedTailDigest: base.tailDigest,
                batch: CanonicalBatch.make({
                  batchId: batchId(`history:${runId}`),
                  producerId: producer(`history:${runId}`),
                  records: [
                    input,
                    record(`history-output:${runId}`, modelCompleted, completedAt),
                    record(
                      `run-completed:${runId}`,
                      RunCompleted.make({
                        runId,
                        output,
                        ...(runDisposition === undefined ? {} : { runDisposition }),
                        ...(completion.finishReason === "budget-exhausted"
                          ? {
                              finishReason: completion.finishReason,
                              exhausted: completion.exhausted,
                            }
                          : {}),
                      }),
                      completedAt,
                    ),
                  ],
                }),
              }),
            )
            .pipe(Effect.mapError((cause) => storageError(conversationId, cause)));
        }),
      };
    });
    return ConversationHistory.of({ load, open });
  }),
);

export const PersistentHistory = { layer };
