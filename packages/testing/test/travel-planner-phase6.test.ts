import { ThreadId } from "@effect-agent/core";
import {
  NodeDurableAgentRuntime,
  type NodeDurableAgentRuntimeOptions,
} from "@effect-agent/platform-node";
import {
  TravelPlannerCloudflareProfile,
  expectedTravelPlan,
  makePhase4TravelPlannerAgent,
  normalizeCrossPlatformTravelPlannerEvidence,
  phase1Trip,
  phase4TravelPlannerDeploymentId,
  phase4TravelPlannerProducerId,
  phase4TravelPlannerSubmitOptions,
  phase4TravelPlannerWorkerLayer,
  phase6TravelPlannerGoldenEvidence,
  phase6TravelPlannerProfile,
  travelPlanFromDurableSettlement,
} from "@effect-agent/testing/fixtures/travel-planner";
import { ThreadRead, ThreadStore, DurableAgentRuntime, IdempotencyKey } from "@effect-agent/thread";
import { NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import { Effect, FileSystem, Layer, Schema, Stream } from "effect";

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

const withTemporaryDirectory = <A, E>(
  use: (directory: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-travel-planner-p6-",
      });

      return yield* use(directory);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const runtimeOptions = (filename: string): NodeDurableAgentRuntimeOptions => ({
  filename,
  deploymentId: phase4TravelPlannerDeploymentId,
  producerId: phase4TravelPlannerProducerId,
  observationPollInterval: 1,
});

/** One DN "host process": the full Node/SQLite runtime stack plus the deterministic services. */
const dnLayer = (options: NodeDurableAgentRuntimeOptions) =>
  Layer.mergeAll(phase4TravelPlannerWorkerLayer, NodeDurableAgentRuntime.layer(options));

/**
 * TEST-014, the DN HALF of the P6 DN/DC equivalence gate (plan §6, D-P6-6): the SAME
 * uninterrupted Travel Planner planning Submission the DC suite runs inside a Durable Object
 * is run here on the Node/SQLite runtime, and its CROSS-PLATFORM normalized canonical
 * evidence must equal the one committed golden. `travel-planner-dc.test.ts` (platform-
 * cloudflare, in-workerd) asserts its DC run against the same golden, so equivalent canonical
 * outcomes under DN and DC follow transitively without assembling both stacks in one process.
 */
describe("TEST-014 P6 Travel Planner DN/DC equivalence — the DN half", () => {
  it("pins the DC profile: equivalence is claimed, exactly-once external effects are not", () => {
    const decoded = Schema.decodeUnknownSync(TravelPlannerCloudflareProfile)(
      Schema.encodeSync(TravelPlannerCloudflareProfile)(phase6TravelPlannerProfile),
    );

    expect(decoded).toEqual(phase6TravelPlannerProfile);
    expect(phase6TravelPlannerProfile).toEqual({
      deploymentClass: "DC",
      durableAcceptedWork: true,
      canonicalSchemaVersion: 1,
      supplierBookingUncertaintyProtocol: true,
      durableAttachedSubagents: true,
      cloudflareEquivalence: true,
      exactlyOnceExternalEffects: false,
    });
  });

  it.effect(
    "the DN run's cross-platform normalized canonical evidence equals the committed golden",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const store = yield* ThreadStore;
          const threadId = decodeThreadId("travel-planner-p6-golden-dn");
          const agent = makePhase4TravelPlannerAgent();

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            phase4TravelPlannerSubmitOptions(threadId, decodeIdempotencyKey("p6-golden-dn-1")),
          );

          const settlements = yield* runtime.processThread(agent, threadId);

          expect(settlements).toHaveLength(1);
          expect(settlements[0]?.outcome).toBe("completed");

          const records = yield* Stream.runCollect(
            store.read(ThreadRead.make({ threadId, limit: 1_024 })),
          );

          expect(yield* travelPlanFromDurableSettlement(records)).toEqual(expectedTravelPlan);

          const normalized = yield* normalizeCrossPlatformTravelPlannerEvidence(records, receipt, {
            threadId,
            deploymentId: phase4TravelPlannerDeploymentId,
            producerId: phase4TravelPlannerProducerId,
          });

          expect(normalized).toEqual(phase6TravelPlannerGoldenEvidence);
        }).pipe(Effect.provide(dnLayer(runtimeOptions(`${directory}/p6-golden.sqlite`)))),
      ),
  );
});
