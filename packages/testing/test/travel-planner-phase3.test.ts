import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer, Option, Schema, Stream } from "effect";

import {
  CanonicalBatch,
  CanonicalSequence,
  ConversationExport,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationObservation,
  ConversationProjection,
  ConversationRead,
  ConversationStore,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  LoadCheckpointRequest,
  ProducerEpoch,
  replayConversation,
  replayConversationFromCheckpoint,
  SaveCheckpointRequest,
  SubmissionStore,
} from "@effect-agent/session";
import { MemoryStorageLive } from "@effect-agent/storage-memory";
import { layer as sqliteStorageLayer } from "@effect-agent/storage-sqlite";

import {
  expectedTravelPlan,
  makePhase3TravelPlannerCheckpoint,
  phase3TravelPlannerBatches,
  phase3TravelPlannerConversationId,
  phase3TravelPlannerEncodedFixture,
  phase3TravelPlannerProfile,
  travelPlanFromProjection,
} from "../src/index.ts";

const producerEpoch = Schema.decodeSync(ProducerEpoch)(1);
const initialSequence = Schema.decodeSync(CanonicalSequence)(0);

const writePersistentTravelPlanner = Effect.gen(function* () {
  const store = yield* ConversationStore;
  yield* store.materialize(
    ConversationMaterialization.make({
      conversationId: phase3TravelPlannerConversationId,
      producerEpoch,
    }),
  );

  const initial = yield* store.append(
    FencedAppendRequest.make({
      conversationId: phase3TravelPlannerConversationId,
      batch: phase3TravelPlannerBatches[0],
      expectedTailSequence: initialSequence,
      expectedTailDigest: EMPTY_TAIL_DIGEST,
      producerEpoch,
    }),
  );
  const prefix = yield* store
    .read(
      ConversationRead.make({
        conversationId: phase3TravelPlannerConversationId,
        limit: 1_024,
      }),
    )
    .pipe(Stream.runCollect);
  const prefixProjection = replayConversation(
    phase3TravelPlannerConversationId,
    prefix,
    initial.tailDigest,
  );
  yield* store.saveCheckpoint(
    SaveCheckpointRequest.make({
      checkpoint: makePhase3TravelPlannerCheckpoint(prefixProjection),
    }),
  );

  yield* store.append(
    FencedAppendRequest.make({
      conversationId: phase3TravelPlannerConversationId,
      batch: phase3TravelPlannerBatches[1],
      expectedTailSequence: initial.lastSequence,
      expectedTailDigest: initial.tailDigest,
      producerEpoch,
    }),
  );
});

const inspectPersistentTravelPlanner = Effect.gen(function* () {
  const store = yield* ConversationStore;
  const submissions = yield* SubmissionStore;
  const exported = yield* store.export(
    ConversationExportRequest.make({
      conversationId: phase3TravelPlannerConversationId,
    }),
  );
  const checkpoint = yield* store.loadCheckpoint(
    LoadCheckpointRequest.make({
      conversationId: phase3TravelPlannerConversationId,
    }),
  );
  if (Option.isNone(checkpoint)) {
    return yield* Effect.die(new Error("Expected the Travel Planner checkpoint to exist"));
  }

  const checkpointProjection = yield* Schema.decodeUnknownEffect(ConversationProjection)(
    checkpoint.value.state,
  );
  const suffix = exported.records.filter(
    (record) => record.sequence > checkpoint.value.throughSequence,
  );
  const fullReplay = replayConversation(
    phase3TravelPlannerConversationId,
    exported.records,
    exported.tailDigest,
  );
  const checkpointReplay = replayConversationFromCheckpoint(
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
      ConversationObservation.make({
        conversationId: phase3TravelPlannerConversationId,
        afterOffset: checkpointRecord.offset,
      }),
    )
    .pipe(Stream.take(suffix.length), Stream.runCollect);

  const capabilities = yield* submissions.capabilities;
  const portableExport = yield* Schema.encodeEffect(ConversationExport)(exported).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(ConversationExport)),
  );

  return {
    capabilities,
    checkpointReplay,
    fullReplay,
    observedSuffix: Array.from(observedSuffix),
    plan,
    portableExport,
  };
});

const memoryLayer = MemoryStorageLive.pipe(Layer.provide(NodeCrypto.layer));

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
    expect(phase3TravelPlannerProfile).toEqual({
      deploymentClass: "P",
      durableAcceptedWork: false,
      canonicalSchemaVersion: 1,
    });
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
      expect(inspected.capabilities).toEqual({
        durability: "non-durable",
        acceptsDurableWork: false,
      });
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
        expect(inspected.capabilities).toEqual({
          durability: "non-durable",
          acceptsDurableWork: false,
        });
      }),
    ),
  );
});
