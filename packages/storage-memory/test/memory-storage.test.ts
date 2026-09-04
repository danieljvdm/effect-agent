import { ThreadId, RunId, SubmissionId } from "@effect-agent/core/Identifiers";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import {
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  MAX_PERSISTED_JSON_BYTES,
  ObservationOffset,
  ProducerEpoch,
  RunCompleted,
  UserInputRecorded,
  type CanonicalRecordPayload,
} from "@effect-agent/thread/Records";
import {
  threadStoreConformanceCases,
  threadCheckpointConformanceCases,
} from "@effect-agent/thread/testing/ThreadStoreConformance";
import {
  ThreadProjection,
  replayThread,
  replayThreadFromCheckpoint,
} from "@effect-agent/thread/ThreadProjection";
import {
  type AppendResult,
  CheckpointRejected,
  ThreadCheckpoint,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadObservation,
  ThreadRead,
  ThreadStore,
  ThreadStoreError,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
} from "@effect-agent/thread/ThreadStore";
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

const testLayer = MemoryThreadStoreLive.pipe(Layer.provide(NodeCrypto.layer));

const threadId = Schema.decodeSync(ThreadId)("thread-memory-1");
const runId = Schema.decodeSync(RunId)("run-memory-1");
const submissionId = Schema.decodeSync(SubmissionId)("submission-memory-1");
const canonicalSequence = Schema.decodeSync(CanonicalSequence);
const producerEpoch = Schema.decodeSync(ProducerEpoch);
const ZERO_CANONICAL_SEQUENCE = canonicalSequence(0);
const FIRST_PRODUCER_EPOCH = producerEpoch(1);
const isThreadStoreError = Schema.is(ThreadStoreError);

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const canonicalRecord = (recordId: string, payload: CanonicalRecordPayload): CanonicalRecord =>
  CanonicalRecord.make({
    recordId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/RecordId")),
      recordId,
    ),
    family: "thread",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/DeploymentId")),
      "deployment-memory",
    ),
    payload,
  });

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/BatchId")), batchId),
    producerId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/ProducerId")),
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
  store: ThreadStore["Service"],
  canonicalBatch: CanonicalBatch,
  tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = {
    lastSequence: ZERO_CANONICAL_SEQUENCE,
    tailDigest: EMPTY_TAIL_DIGEST,
  },
  producerEpochValue: ProducerEpoch = FIRST_PRODUCER_EPOCH,
) =>
  store.append(
    FencedAppendRequest.make({
      threadId,
      batch: canonicalBatch,
      expectedTailSequence: tail.lastSequence,
      expectedTailDigest: tail.tailDigest,
      producerEpoch: producerEpochValue,
    }),
  );

describe("MemoryThreadStore", () => {
  it.effect("reads bounded pages from one snapshot while another batch is appended", () =>
    Effect.gen(function* () {
      const store = yield* ThreadStore;

      yield* store.materialize(
        ThreadMaterialization.make({ threadId, producerEpoch: FIRST_PRODUCER_EPOCH }),
      );
      expect(
        yield* store.read(ThreadRead.make({ threadId, limit: 2 })).pipe(Stream.runCollect),
      ).toEqual([]);

      const first = yield* append(
        store,
        batch("page-first", [inputRecord("page-1", "one"), inputRecord("page-2", "two")]),
      );

      const tail = yield* append(
        store,
        batch("page-second", [inputRecord("page-3", "three"), inputRecord("page-4", "four")]),
        first,
      );

      const snapshot = yield* store.export(ThreadExportRequest.make({ threadId }));
      const readStarted = yield* Deferred.make<void>();
      const resumeRead = yield* Deferred.make<void>();

      const reader = yield* store.read(ThreadRead.make({ threadId, limit: 10 })).pipe(
        Stream.tap(() =>
          Deferred.succeed(readStarted, undefined).pipe(Effect.andThen(Deferred.await(resumeRead))),
        ),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* Deferred.await(readStarted);
      yield* append(store, batch("page-third", [inputRecord("page-5", "five")]), tail);
      yield* Deferred.succeed(resumeRead, undefined);
      expect(yield* Fiber.join(reader)).toEqual(snapshot.records);

      const exported = yield* store.export(ThreadExportRequest.make({ threadId }));

      const cases = [
        { afterSequence: undefined, expected: [0, 1] },
        { afterSequence: 0, expected: [0, 1] },
        { afterSequence: 1, expected: [1, 2] },
        { afterSequence: 3, expected: [3, 4] },
        { afterSequence: 4, expected: [4] },
        { afterSequence: 5, expected: [] },
        { afterSequence: 6, expected: [] },
        { afterSequence: Number.MAX_SAFE_INTEGER, expected: [] },
      ];

      for (const { afterSequence, expected } of cases) {
        const page = yield* store
          .read(
            ThreadRead.make({
              threadId,
              ...(afterSequence === undefined
                ? {}
                : { afterSequence: canonicalSequence(afterSequence) }),
              limit: 2,
            }),
          )
          .pipe(Stream.runCollect);

        expect(page).toEqual(expected.map((index) => exported.records[index]));
      }
    }).pipe(Effect.provide(testLayer)),
  );

  describe("shared ThreadStore conformance", () => {
    for (const conformanceCase of threadStoreConformanceCases) {
      it.effect(conformanceCase.name, () =>
        conformanceCase.run.pipe(
          Effect.updateService(ThreadStore, (store) => ({
            materialize: store.materialize,
            append: store.append,
            read: store.read,
            observe: store.observe,
            export: store.export,
            inspectTail: store.inspectTail,
          })),
          Effect.provide(testLayer),
        ),
      );
    }
    for (const conformanceCase of threadCheckpointConformanceCases) {
      it.effect(conformanceCase.name, () => conformanceCase.run.pipe(Effect.provide(testLayer)));
    }
  });

  it.layer(testLayer)((it) => {
    it.effect("rejects unsupported record versions before mutating canonical state", () =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
        );

        const invalid = {
          threadId,
          expectedTailSequence: 0,
          expectedTailDigest: EMPTY_TAIL_DIGEST,
          producerEpoch: 1,
          batch: {
            batchId: "unsupported-batch",
            producerId: "producer-memory",
            records: [
              {
                recordId: "unsupported-record",
                family: "thread",
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

        if (!isThreadStoreError(failure)) {
          return yield* Effect.die(new Error("Expected a ThreadStoreError"));
        }
        expect(failure).toMatchObject({
          _tag: "ThreadStoreError",
          operation: "append",
        });
        expect(failure.cause).toBeDefined();

        const exported = yield* store.export(ThreadExportRequest.make({ threadId }));

        expect(exported.records).toEqual([]);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("rejects oversized persisted JSON before mutating canonical state", () =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
        );

        const invalid = {
          threadId,
          expectedTailSequence: 0,
          expectedTailDigest: EMPTY_TAIL_DIGEST,
          producerEpoch: 1,
          batch: {
            batchId: "oversized-batch",
            producerId: "producer-memory",
            records: [
              {
                recordId: "oversized-record",
                family: "thread",
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

        if (!isThreadStoreError(failure)) {
          return yield* Effect.die(new Error("Expected a ThreadStoreError"));
        }
        expect(failure).toMatchObject({
          _tag: "ThreadStoreError",
          operation: "append",
        });
        expect(failure.cause).toBeDefined();

        const exported = yield* store.export(ThreadExportRequest.make({ threadId }));

        expect(exported.records).toEqual([]);
        expect(exported.tailDigest).toBe(EMPTY_TAIL_DIGEST);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("classifies an unsupported checkpoint version before mutation", () =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
        );

        const invalid = {
          checkpoint: {
            schemaVersion: 2,
            threadId,
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

        const saveBoundary: unknown = store.checkpoints!.save;

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
          threadId,
          reason: "unsupported-version",
        });
        expect(
          Option.isNone(yield* store.checkpoints!.load(LoadCheckpointRequest.make({ threadId }))),
        ).toBe(true);
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("resumes observation from an opaque cursor and emits later appends", () =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId,
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
          .read(ThreadRead.make({ threadId, limit: 1_024 }))
          .pipe(Stream.runCollect);

        const firstExisting = existing.at(0);
        const secondExisting = existing.at(1);

        if (firstExisting === undefined || secondExisting === undefined) {
          return yield* Effect.die(new Error("Expected two existing observation records"));
        }
        const malformedOffset = id(ObservationOffset, "foreign-adapter:1");

        const malformed = yield* store
          .observe(
            ThreadObservation.make({
              threadId,
              afterOffset: malformedOffset,
            }),
          )
          .pipe(Stream.take(1), Stream.runCollect, Effect.flip);

        expect(malformed).toMatchObject({
          _tag: "ThreadStoreError",
          operation: "observe",
          message: "Malformed observation offset",
        });

        const resumed = yield* store
          .observe(
            ThreadObservation.make({
              threadId,
              afterOffset: firstExisting.offset,
            }),
          )
          .pipe(Stream.take(1), Stream.runCollect);

        expect(resumed.map((record) => record.record.recordId)).toEqual([
          secondExisting.record.recordId,
        ]);

        const liveFiber = yield* store
          .observe(
            ThreadObservation.make({
              threadId,
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
            Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/RecordId")),
            "observe-record-3",
          ),
        );
      }),
    );
  });

  it.layer(testLayer)((it) => {
    it.effect("coalesces wakeups without dropping records for a slow observer", () =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
        );
        const releaseObserver = yield* Deferred.make<void>();
        const recordCount = 32;

        const observerFiber = yield* store.observe(ThreadObservation.make({ threadId })).pipe(
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
      const store = Context.get(context, ThreadStore);

      yield* store.materialize(
        ThreadMaterialization.make({
          threadId,
          producerEpoch: FIRST_PRODUCER_EPOCH,
        }),
      );

      const observerFiber = yield* store
        .observe(ThreadObservation.make({ threadId }))
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
        const store = yield* ThreadStore;

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId,
            producerEpoch: FIRST_PRODUCER_EPOCH,
          }),
        );

        const first = yield* append(
          store,
          batch("checkpoint-1", [inputRecord("checkpoint-record-1", "Kyoto")]),
        );

        const firstRecords = yield* store
          .read(ThreadRead.make({ threadId, limit: 1_024 }))
          .pipe(Stream.runCollect);

        const atCheckpoint = replayThread(threadId, firstRecords, first.tailDigest);
        const checkpointState = yield* Schema.encodeEffect(ThreadProjection)(atCheckpoint);

        const rejectedCheckpoint = yield* store
          .checkpoints!.save(
            SaveCheckpointRequest.make({
              checkpoint: ThreadCheckpoint.make({
                schemaVersion: 1,
                threadId,
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
          threadId,
          reason: "digest-mismatch",
        });
        expect(
          Option.isNone(yield* store.checkpoints!.load(LoadCheckpointRequest.make({ threadId }))),
        ).toBe(true);

        yield* store.checkpoints!.save(
          SaveCheckpointRequest.make({
            checkpoint: ThreadCheckpoint.make({
              schemaVersion: 1,
              threadId,
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
        const exported = yield* store.export(ThreadExportRequest.make({ threadId }));
        const loaded = yield* store.checkpoints!.load(LoadCheckpointRequest.make({ threadId }));

        expect(Option.isSome(loaded)).toBe(true);
        if (Option.isNone(loaded)) return;

        const decodedCheckpoint = yield* Schema.decodeUnknownEffect(ThreadProjection)(
          loaded.value.state,
        );

        const tail = exported.records.filter(
          (record) => record.sequence > loaded.value.throughSequence,
        );

        const checkpointReplay = replayThreadFromCheckpoint(
          decodedCheckpoint,
          tail,
          second.tailDigest,
        );

        const fullReplay = replayThread(threadId, exported.records, exported.tailDigest);

        expect(checkpointReplay).toEqual(fullReplay);
        expect(exported.records).toHaveLength(2);
      }),
    );
  });
});
