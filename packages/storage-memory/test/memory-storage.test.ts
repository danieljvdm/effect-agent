import { NodeCrypto } from "@effect/platform-node";
import { expect, describe, it } from "@effect/vitest";
import { DateTime, Effect, Exit, Fiber, Layer, Option, Schema, Stream } from "effect";

import {
  AppendResult,
  CanonicalBatch,
  CanonicalRecord,
  ConversationCheckpoint,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationObservation,
  ConversationProjection,
  ConversationRead,
  ConversationStore,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  LoadCheckpointRequest,
  ObservationOffset,
  replayConversation,
  replayConversationFromCheckpoint,
  RunCompleted,
  SaveCheckpointRequest,
  SubmissionStore,
  UserInputRecorded,
  type CanonicalRecordPayload,
} from "@effect-agent/session";
import { ConversationId, RunId, SubmissionId } from "@effect-agent/core";

import { MemoryStorageLive } from "../src/index.ts";
import { inspectConversationStoreConformance } from "../src/testing.ts";

const testLayer = MemoryStorageLive.pipe(Layer.provide(NodeCrypto.layer));

const conversationId = Schema.decodeSync(ConversationId)("conversation-memory-1");
const runId = Schema.decodeSync(RunId)("run-memory-1");
const submissionId = Schema.decodeSync(SubmissionId)("submission-memory-1");

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const canonicalRecord = (recordId: string, payload: CanonicalRecordPayload): CanonicalRecord =>
  CanonicalRecord.make({
    recordId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/RecordId")),
      recordId,
    ),
    family: "conversation",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/DeploymentId")),
      "deployment-memory",
    ),
    payload,
  });

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/BatchId")), batchId),
    producerId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/ProducerId")),
      "producer-memory",
    ),
    records,
  });

const inputRecord = (recordId: string, input: string): CanonicalRecord =>
  canonicalRecord(
    recordId,
    UserInputRecorded.make({
      submissionId,
      kind: "user",
      runId,
      input,
    }),
  );

const append = (
  store: ConversationStore["Service"],
  canonicalBatch: CanonicalBatch,
  tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = {
    lastSequence: 0,
    tailDigest: EMPTY_TAIL_DIGEST,
  },
  producerEpoch = 1,
) =>
  store.append(
    FencedAppendRequest.make({
      conversationId,
      batch: canonicalBatch,
      expectedTailSequence: tail.lastSequence,
      expectedTailDigest: tail.tailDigest,
      producerEpoch,
    }),
  );

describe("MemoryConversationStore", () => {
  it.layer(testLayer)((it) => {
    it.effect(
      "atomically appends, replays identical batches, conflicts, and fences stale epochs",
      () =>
        Effect.gen(function* () {
          const store = yield* ConversationStore;
          yield* store.materialize(
            ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
          );
          const firstBatch = batch("batch-1", [inputRecord("record-1", "Lisbon")]);
          const first = yield* append(store, firstBatch);
          const replayed = yield* append(store, firstBatch);

          expect(first.replayed).toBe(false);
          expect(replayed).toEqual(
            AppendResult.make({
              firstSequence: first.firstSequence,
              lastSequence: first.lastSequence,
              tailDigest: first.tailDigest,
              replayed: true,
            }),
          );

          const conflictingBatch = batch("batch-1", [inputRecord("record-2", "Porto")]);
          const batchConflict = yield* append(store, conflictingBatch).pipe(Effect.exit);
          expect(Exit.isFailure(batchConflict)).toBe(true);

          const staleTail = yield* append(
            store,
            batch("batch-2", [inputRecord("record-3", "Coimbra")]),
          ).pipe(Effect.exit);
          expect(Exit.isFailure(staleTail)).toBe(true);

          yield* store.materialize(
            ConversationMaterialization.make({ conversationId, producerEpoch: 2 }),
          );
          const staleProducer = yield* append(
            store,
            batch("batch-3", [inputRecord("record-4", "Braga")]),
            first,
            1,
          ).pipe(Effect.exit);
          expect(Exit.isFailure(staleProducer)).toBe(true);

          const records = yield* store
            .read(ConversationRead.make({ conversationId, limit: 1_024 }))
            .pipe(Stream.runCollect);
          expect(records).toHaveLength(1);
        }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("commits no batch prefix when duplicate record identity conflicts", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
        );
        const duplicate = inputRecord("duplicate-record", "first");
        const exit = yield* append(store, batch("duplicate-batch", [duplicate, duplicate])).pipe(
          Effect.exit,
        );
        expect(Exit.isFailure(exit)).toBe(true);

        const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
        expect(exported.records).toEqual([]);
        expect(exported.tailDigest).toBe(EMPTY_TAIL_DIGEST);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("rejects unsupported record versions before mutating canonical state", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
        );
        const invalid = {
          conversationId,
          expectedTailSequence: 0,
          expectedTailDigest: EMPTY_TAIL_DIGEST,
          producerEpoch: 1,
          batch: {
            batchId: "unsupported-batch",
            producerId: "producer-memory",
            records: [
              {
                recordId: "unsupported-record",
                family: "conversation",
                schemaVersion: 2,
                createdAt: "1970-01-01T00:00:00.001Z",
                deploymentId: "deployment-memory",
                payload: {
                  _tag: "UserInputRecorded",
                  submissionId: "submission-memory-1",
                  kind: "user",
                  runId: "run-memory-1",
                  input: "invalid",
                },
              },
            ],
          },
        };
        const appendBoundary: unknown = store.append;
        if (typeof appendBoundary !== "function") {
          return yield* Effect.die(new Error("Expected an append function"));
        }
        const unvalidatedResult: unknown = appendBoundary(invalid);
        if (!Effect.isEffect(unvalidatedResult)) {
          return yield* Effect.die(new Error("Expected append to return an Effect"));
        }
        const exit = yield* unvalidatedResult.pipe(Effect.exit);
        expect(Exit.isFailure(exit)).toBe(true);

        const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
        expect(exported.records).toEqual([]);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("resumes observation from an opaque cursor and emits later appends", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
        );
        const first = yield* append(
          store,
          batch("observe-1", [inputRecord("observe-record-1", "first")]),
        );
        const second = yield* append(
          store,
          batch("observe-2", [inputRecord("observe-record-2", "second")]),
          first,
        );
        const existing = yield* store
          .read(ConversationRead.make({ conversationId, limit: 1_024 }))
          .pipe(Stream.runCollect);
        const malformedOffset = id(ObservationOffset, "foreign-adapter:1");
        const malformed = yield* store
          .observe(
            ConversationObservation.make({
              conversationId,
              afterOffset: malformedOffset,
            }),
          )
          .pipe(Stream.take(1), Stream.runCollect, Effect.exit);
        expect(Exit.isFailure(malformed)).toBe(true);

        const resumed = yield* store
          .observe(
            ConversationObservation.make({
              conversationId,
              afterOffset: existing[0]!.offset,
            }),
          )
          .pipe(Stream.take(1), Stream.runCollect);
        expect(resumed.map((record) => record.record.recordId)).toEqual([
          existing[1]!.record.recordId,
        ]);

        const liveFiber = yield* store
          .observe(
            ConversationObservation.make({
              conversationId,
              afterOffset: existing[1]!.offset,
            }),
          )
          .pipe(Stream.take(1), Stream.runCollect, Effect.forkChild);
        yield* Effect.yieldNow;
        yield* append(
          store,
          batch("observe-3", [inputRecord("observe-record-3", "third")]),
          second,
        );
        const live = yield* Fiber.join(liveFiber);
        expect(live[0]?.record.recordId).toBe(
          id(
            Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/RecordId")),
            "observe-record-3",
          ),
        );
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("makes valid checkpoint replay equivalent to full export replay", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
        );
        const first = yield* append(
          store,
          batch("checkpoint-1", [inputRecord("checkpoint-record-1", "Kyoto")]),
        );
        const firstRecords = yield* store
          .read(ConversationRead.make({ conversationId, limit: 1_024 }))
          .pipe(Stream.runCollect);
        const atCheckpoint = replayConversation(conversationId, firstRecords, first.tailDigest);
        const checkpointState = yield* Schema.encodeEffect(ConversationProjection)(atCheckpoint);
        const rejectedCheckpoint = yield* store
          .saveCheckpoint(
            SaveCheckpointRequest.make({
              checkpoint: ConversationCheckpoint.make({
                schemaVersion: 1,
                conversationId,
                throughSequence: first.lastSequence,
                tailDigest: EMPTY_TAIL_DIGEST,
                engineVersion: "phase-3",
                agentDefinitionDigest: first.tailDigest,
                modelDigest: first.tailDigest,
                toolDigest: first.tailDigest,
                state: checkpointState,
                createdAt: at(2),
              }),
            }),
          )
          .pipe(Effect.exit);
        expect(Exit.isFailure(rejectedCheckpoint)).toBe(true);
        expect(
          Option.isNone(
            yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId })),
          ),
        ).toBe(true);

        yield* store.saveCheckpoint(
          SaveCheckpointRequest.make({
            checkpoint: ConversationCheckpoint.make({
              schemaVersion: 1,
              conversationId,
              throughSequence: first.lastSequence,
              tailDigest: first.tailDigest,
              engineVersion: "phase-3",
              agentDefinitionDigest: first.tailDigest,
              modelDigest: first.tailDigest,
              toolDigest: first.tailDigest,
              state: checkpointState,
              createdAt: at(2),
            }),
          }),
        );

        const completed = canonicalRecord(
          "checkpoint-record-2",
          RunCompleted.make({ runId, output: { itinerary: "Kyoto" } }),
        );
        const second = yield* append(store, batch("checkpoint-2", [completed]), first);
        const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
        const loaded = yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId }));
        expect(Option.isSome(loaded)).toBe(true);
        if (Option.isNone(loaded)) return;
        const decodedCheckpoint = yield* Schema.decodeUnknownEffect(ConversationProjection)(
          loaded.value.state,
        );
        const tail = exported.records.filter(
          (record) => record.sequence > loaded.value.throughSequence,
        );
        const checkpointReplay = replayConversationFromCheckpoint(
          decodedCheckpoint,
          tail,
          second.tailDigest,
        );
        const fullReplay = replayConversation(
          conversationId,
          exported.records,
          exported.tailDigest,
        );

        expect(checkpointReplay).toEqual(fullReplay);
        expect(yield* inspectConversationStoreConformance(conversationId)).toEqual({
          readCount: 2,
          observedCount: 2,
          exportCount: 2,
          hasCheckpoint: true,
        });
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("states explicitly that the SubmissionStore accepts no durable work", () =>
      Effect.gen(function* () {
        const submissions = yield* SubmissionStore;
        expect(yield* submissions.capabilities).toEqual({
          durability: "non-durable",
          acceptsDurableWork: false,
        });
        expect(Option.isNone(yield* submissions.inspect(submissionId))).toBe(true);
      }),
    );
  });
});
