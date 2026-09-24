import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
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
  Schema,
  Scope,
  Stream,
} from "effect";
import { EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import { ThreadId, RunId, SubmissionId } from "effect-agent/identifiers";
import {
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  ProducerEpoch,
  UserInputRecorded,
  type CanonicalRecordPayload,
} from "effect-agent/records";
import {
  threadStoreConformanceCases,
  threadCheckpointConformanceCases,
} from "effect-agent/testing/thread-store-conformance";
import {
  type AppendResult,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadObservation,
  ThreadRead,
  ThreadStore,
  FencedAppendRequest,
} from "effect-agent/thread-store";

const testLayer = MemoryThreadStoreLive.pipe(Layer.provide(NodeCrypto.layer));

const threadId = Schema.decodeSync(ThreadId)("thread-memory-1");
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
    }).pipe(Effect.provide(testLayer)),
  );

  describe("shared ThreadStore conformance", () => {
    for (const conformanceCase of threadStoreConformanceCases) {
      it.effect(conformanceCase.name, () =>
        conformanceCase.run.pipe(
          Effect.updateService(ThreadStore, (store) => ({
            readIdentity: store.readIdentity,
            countPeerMessages: store.countPeerMessages,
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
});
