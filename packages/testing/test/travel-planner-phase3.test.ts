import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { layer as sqliteStorageLayer } from "@effect-agent/storage-sqlite/SqliteThreadStore";
import {
  expectedTravelPlan,
  makePhase3TravelPlannerCheckpoint,
  phase3TravelPlannerBatches,
  phase3TravelPlannerThreadId,
  phase3TravelPlannerEncodedFixture,
  travelPlanFromProjection,
} from "@effect-agent/testing/TravelPlanner";
import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import { CanonicalBatch, CanonicalSequence, ProducerEpoch } from "@effect-agent/thread/Records";
import {
  ThreadProjection,
  replayThread,
  replayThreadFromCheckpoint,
} from "@effect-agent/thread/ThreadProjection";
import {
  ThreadExport,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadObservation,
  ThreadRead,
  ThreadStore,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
} from "@effect-agent/thread/ThreadStore";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer, Option, Schema, Stream } from "effect";

const producerEpoch = Schema.decodeSync(ProducerEpoch)(1);
const initialSequence = Schema.decodeSync(CanonicalSequence)(0);

const writePersistentTravelPlanner = Effect.gen(function* () {
  const store = yield* ThreadStore;

  yield* store.materialize(
    ThreadMaterialization.make({
      threadId: phase3TravelPlannerThreadId,
      producerEpoch,
    }),
  );

  const initial = yield* store.append(
    FencedAppendRequest.make({
      threadId: phase3TravelPlannerThreadId,
      batch: phase3TravelPlannerBatches[0],
      expectedTailSequence: initialSequence,
      expectedTailDigest: EMPTY_TAIL_DIGEST,
      producerEpoch,
    }),
  );

  const prefix = yield* store
    .read(
      ThreadRead.make({
        threadId: phase3TravelPlannerThreadId,
        limit: 1_024,
      }),
    )
    .pipe(Stream.runCollect);

  const prefixProjection = replayThread(phase3TravelPlannerThreadId, prefix, initial.tailDigest);

  yield* store.checkpoints!.save(
    SaveCheckpointRequest.make({
      checkpoint: makePhase3TravelPlannerCheckpoint(prefixProjection),
    }),
  );

  yield* store.append(
    FencedAppendRequest.make({
      threadId: phase3TravelPlannerThreadId,
      batch: phase3TravelPlannerBatches[1],
      expectedTailSequence: initial.lastSequence,
      expectedTailDigest: initial.tailDigest,
      producerEpoch,
    }),
  );
});

const inspectPersistentTravelPlanner = Effect.gen(function* () {
  const store = yield* ThreadStore;

  const exported = yield* store.export(
    ThreadExportRequest.make({
      threadId: phase3TravelPlannerThreadId,
    }),
  );

  const checkpoint = yield* store.checkpoints!.load(
    LoadCheckpointRequest.make({
      threadId: phase3TravelPlannerThreadId,
    }),
  );

  if (Option.isNone(checkpoint)) {
    return yield* Effect.die(new Error("Expected the Travel Planner checkpoint to exist"));
  }

  const checkpointProjection = yield* Schema.decodeUnknownEffect(ThreadProjection)(
    checkpoint.value.state,
  );

  const suffix = exported.records.filter(
    (record) => record.sequence > checkpoint.value.throughSequence,
  );

  const fullReplay = replayThread(
    phase3TravelPlannerThreadId,
    exported.records,
    exported.tailDigest,
  );

  const checkpointReplay = replayThreadFromCheckpoint(
    checkpointProjection,
    suffix,
    exported.tailDigest,
  );

  const plan = yield* travelPlanFromProjection(fullReplay);

  const checkpointRecord = exported.records.find(
    (record) => record.sequence === checkpoint.value.throughSequence,
  );

  if (checkpointRecord === undefined) {
    return yield* Effect.die(new Error("Expected the checkpoint's canonical record"));
  }

  const observedSuffix = yield* store
    .observe(
      ThreadObservation.make({
        threadId: phase3TravelPlannerThreadId,
        afterOffset: checkpointRecord.offset,
      }),
    )
    .pipe(Stream.take(suffix.length), Stream.runCollect);

  const portableExport = yield* Schema.encodeEffect(ThreadExport)(exported).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(ThreadExport)),
  );

  return {
    checkpointReplay,
    fullReplay,
    observedSuffix: Array.from(observedSuffix),
    plan,
    portableExport,
  };
});

const memoryLayer = MemoryThreadStoreLive.pipe(Layer.provide(NodeCrypto.layer));

const withTemporaryDatabase = <A, E>(use: (filename: string) => Effect.Effect<A, E>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-travel-planner-p3-",
      });

      return yield* use(`${directory}/travel-planner.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("TEST-014 P3 persistent Travel Planner profile (P)", () => {
  it("keeps a redacted, current-version canonical fixture", () => {
    const decoded = Schema.decodeUnknownSync(Schema.Array(CanonicalBatch))(
      phase3TravelPlannerEncodedFixture,
    );

    expect(decoded).toEqual(phase3TravelPlannerBatches);
    const fixtureText = JSON.stringify(phase3TravelPlannerEncodedFixture);

    expect(fixtureText).not.toContain("credential");
    expect(fixtureText).not.toContain("passenger");
  });

  it.effect("replays, reattaches, and exports through the memory Layer", () =>
    Effect.gen(function* () {
      yield* writePersistentTravelPlanner;
      const inspected = yield* inspectPersistentTravelPlanner;

      expect(inspected.fullReplay).toEqual(inspected.checkpointReplay);
      expect(inspected.plan).toEqual(expectedTravelPlan);
      expect(inspected.observedSuffix).toHaveLength(2);
      expect(inspected.portableExport.records).toHaveLength(4);
    }).pipe(Effect.provide(memoryLayer)),
  );

  it.effect("reconstructs the same projection after reopening the SQLite Layer", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* writePersistentTravelPlanner.pipe(
          Effect.provide(sqliteStorageLayer({ filename, observationPollInterval: 1 })),
        );

        const inspected = yield* inspectPersistentTravelPlanner.pipe(
          Effect.provide(sqliteStorageLayer({ filename, observationPollInterval: 1 })),
        );

        expect(inspected.fullReplay).toEqual(inspected.checkpointReplay);
        expect(inspected.plan).toEqual(expectedTravelPlan);
        expect(inspected.observedSuffix).toHaveLength(2);
        expect(inspected.portableExport.records).toHaveLength(4);
      }),
    ),
  );
});
