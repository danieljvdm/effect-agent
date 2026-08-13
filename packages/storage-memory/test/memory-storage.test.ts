import { NodeCrypto } from "@effect/platform-node";
import { expect, describe, it } from "@effect/vitest";
import {
  Cause,
  Context,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schema,
  Scope,
  Stream,
} from "effect";

import {
  AppendResult,
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  CheckpointRejected,
  ConversationCheckpoint,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationObservation,
  ConversationProjection,
  ConversationRead,
  ConversationStore,
  ConversationStoreError,
  conversationStoreConformanceCases,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  LoadCheckpointRequest,
  MAX_PERSISTED_JSON_BYTES,
  ObservationOffset,
  ProducerEpoch,
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

const testLayer = MemoryStorageLive.pipe(Layer.provide(NodeCrypto.layer));

const conversationId = Schema.decodeSync(ConversationId)("conversation-memory-1");
const runId = Schema.decodeSync(RunId)("run-memory-1");
const submissionId = Schema.decodeSync(SubmissionId)("submission-memory-1");
const canonicalSequence = Schema.decodeSync(CanonicalSequence);
const producerEpoch = Schema.decodeSync(ProducerEpoch);
const ZERO_CANONICAL_SEQUENCE = canonicalSequence(0);
const FIRST_PRODUCER_EPOCH = producerEpoch(1);

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
    lastSequence: ZERO_CANONICAL_SEQUENCE,
    tailDigest: EMPTY_TAIL_DIGEST,
  },
  producerEpochValue: ProducerEpoch = FIRST_PRODUCER_EPOCH,
) =>
  store.append(
    FencedAppendRequest.make({
      conversationId,
      batch: canonicalBatch,
      expectedTailSequence: tail.lastSequence,
      expectedTailDigest: tail.tailDigest,
      producerEpoch: producerEpochValue,
    }),
  );

describe("MemoryConversationStore", () => {
  describe("shared ConversationStore conformance", () => {
    for (const conformanceCase of conversationStoreConformanceCases) {
      it.effect(conformanceCase.name, () => conformanceCase.run.pipe(Effect.provide(testLayer)));
    }
  });

  it.layer(testLayer)((it) => {
    it.effect("rejects unsupported record versions before mutating canonical state", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
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
        const failure = yield* unvalidatedResult.pipe(Effect.flip);
        if (!(failure instanceof ConversationStoreError)) {
          return yield* Effect.die(new Error("Expected a ConversationStoreError"));
        }
        expect(failure).toMatchObject({
          _tag: "ConversationStoreError",
          operation: "append",
        });
        expect(failure.cause).toBeDefined();

        const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
        expect(exported.records).toEqual([]);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("rejects oversized persisted JSON before mutating canonical state", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
        );
        const invalid = {
          conversationId,
          expectedTailSequence: 0,
          expectedTailDigest: EMPTY_TAIL_DIGEST,
          producerEpoch: 1,
          batch: {
            batchId: "oversized-batch",
            producerId: "producer-memory",
            records: [
              {
                recordId: "oversized-record",
                family: "conversation",
                schemaVersion: 1,
                createdAt: "1970-01-01T00:00:00.001Z",
                deploymentId: "deployment-memory",
                payload: {
                  _tag: "UserInputRecorded",
                  submissionId: "submission-memory-1",
                  kind: "user",
                  runId: "run-memory-1",
                  input: "x".repeat(MAX_PERSISTED_JSON_BYTES + 1),
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
        const failure = yield* unvalidatedResult.pipe(Effect.flip);
        if (!(failure instanceof ConversationStoreError)) {
          return yield* Effect.die(new Error("Expected a ConversationStoreError"));
        }
        expect(failure).toMatchObject({
          _tag: "ConversationStoreError",
          operation: "append",
        });
        expect(failure.cause).toBeDefined();

        const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
        expect(exported.records).toEqual([]);
        expect(exported.tailDigest).toBe(EMPTY_TAIL_DIGEST);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("classifies an unsupported checkpoint version before mutation", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
        );
        const invalid = {
          checkpoint: {
            schemaVersion: 2,
            conversationId,
            throughSequence: 0,
            tailDigest: EMPTY_TAIL_DIGEST,
            engineVersion: "phase-3",
            agentDefinitionDigest: EMPTY_TAIL_DIGEST,
            modelDigest: EMPTY_TAIL_DIGEST,
            toolDigest: EMPTY_TAIL_DIGEST,
            state: {},
            createdAt: "1970-01-01T00:00:00.001Z",
          },
        };
        const saveBoundary: unknown = store.saveCheckpoint;
        if (typeof saveBoundary !== "function") {
          return yield* Effect.die(new Error("Expected a saveCheckpoint function"));
        }
        const unvalidatedResult: unknown = saveBoundary(invalid);
        if (!Effect.isEffect(unvalidatedResult)) {
          return yield* Effect.die(new Error("Expected saveCheckpoint to return an Effect"));
        }
        const failure = yield* unvalidatedResult.pipe(Effect.flip);
        const checkpointFailure = yield* Schema.decodeUnknownEffect(CheckpointRejected)(failure);
        expect(checkpointFailure).toMatchObject({
          _tag: "CheckpointRejected",
          conversationId,
          reason: "unsupported-version",
        });
        expect(
          Option.isNone(
            yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId })),
          ),
        ).toBe(true);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("resumes observation from an opaque cursor and emits later appends", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
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
        const firstExisting = existing.at(0);
        const secondExisting = existing.at(1);
        if (firstExisting === undefined || secondExisting === undefined) {
          return yield* Effect.die(new Error("Expected two existing observation records"));
        }
        const malformedOffset = id(ObservationOffset, "foreign-adapter:1");
        const malformed = yield* store
          .observe(
            ConversationObservation.make({
              conversationId,
              afterOffset: malformedOffset,
            }),
          )
          .pipe(Stream.take(1), Stream.runCollect, Effect.flip);
        expect(malformed).toMatchObject({
          _tag: "ConversationStoreError",
          operation: "observe",
          message: "Malformed observation offset",
        });

        const resumed = yield* store
          .observe(
            ConversationObservation.make({
              conversationId,
              afterOffset: firstExisting.offset,
            }),
          )
          .pipe(Stream.take(1), Stream.runCollect);
        expect(resumed.map((record) => record.record.recordId)).toEqual([
          secondExisting.record.recordId,
        ]);

        const liveFiber = yield* store
          .observe(
            ConversationObservation.make({
              conversationId,
              afterOffset: secondExisting.offset,
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
    it.effect("coalesces wakeups without dropping records for a slow observer", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
        );
        const releaseObserver = yield* Deferred.make<void>();
        const recordCount = 32;
        const observerFiber = yield* store
          .observe(ConversationObservation.make({ conversationId }))
          .pipe(
            Stream.tap(() => Deferred.await(releaseObserver)),
            Stream.take(recordCount),
            Stream.runCollect,
            Effect.forkChild,
          );
        yield* Effect.yieldNow;

        let tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = {
          lastSequence: ZERO_CANONICAL_SEQUENCE,
          tailDigest: EMPTY_TAIL_DIGEST,
        };
        for (let index = 0; index < recordCount; index++) {
          tail = yield* append(
            store,
            batch(`slow-batch-${index}`, [inputRecord(`slow-record-${index}`, `input-${index}`)]),
            tail,
          );
        }

        yield* Deferred.succeed(releaseObserver, undefined);
        const observed = yield* Fiber.join(observerFiber);
        expect(observed.map((record) => record.record.recordId)).toEqual(
          Array.from({ length: recordCount }, (_, index) => `slow-record-${index}`),
        );
      }),
    );
  });

  it.effect("shuts down active observers when the storage Layer scope closes", () =>
    Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(testLayer, layerScope);
      const store = Context.get(context, ConversationStore);
      yield* store.materialize(
        ConversationMaterialization.make({
          conversationId,
          producerEpoch: FIRST_PRODUCER_EPOCH,
        }),
      );
      const observerFiber = yield* store
        .observe(ConversationObservation.make({ conversationId }))
        .pipe(Stream.runDrain, Effect.forkChild);
      yield* Effect.yieldNow;

      yield* Scope.close(layerScope, Exit.void);
      const observerExit = yield* Fiber.await(observerFiber);
      expect(Exit.isFailure(observerExit)).toBe(true);
      if (Exit.isFailure(observerExit)) {
        expect(Cause.hasInterrupts(observerExit.cause)).toBe(true);
      }
    }),
  );

  it.layer(testLayer)((it) => {
    it.effect("makes valid checkpoint replay equivalent to full export replay", () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
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
          .pipe(Effect.flip);
        expect(rejectedCheckpoint).toMatchObject({
          _tag: "CheckpointRejected",
          conversationId,
          reason: "digest-mismatch",
        });
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
        expect(exported.records).toHaveLength(2);
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
