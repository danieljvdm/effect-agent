import { ConversationId } from "@effect-agent/core";
import { NodeDurableRuntime, type NodeDurableRuntimeOptions } from "@effect-agent/platform-node";
import {
  ConversationRead,
  ConversationStore,
  DurableAgentRuntime,
  IdempotencyKey,
} from "@effect-agent/session";
import { NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Fiber, FileSystem, Layer, Schema, Stream, type PlatformError } from "effect";

import { TrustedLocalDurableAuthorizationLayer } from "../src/durable-test-authorization.ts";
import {
  TravelPlannerCloudflareProfile,
  expectedTravelPlan,
  makePhase6TravelPlannerHarness,
  makePhase4TravelPlannerAgent,
  normalizeCrossPlatformTravelPlannerEvidence,
  phase1Trip,
  phase4TravelPlannerDefinitionDigests,
  phase4TravelPlannerDeploymentId,
  phase4TravelPlannerProducerId,
  phase4TravelPlannerSubmitOptions,
  phase4TravelPlannerWorkerLayer,
  phase6TravelPlannerGoldenEvidence,
  phase6TravelPlannerProfile,
  travelPlanFromDurableSettlement,
} from "../src/index.ts";

const decodeConversationId = Schema.decodeSync(ConversationId);
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

const runtimeOptions = (filename: string): NodeDurableRuntimeOptions => ({
  filename,
  deploymentId: phase4TravelPlannerDeploymentId,
  producerId: phase4TravelPlannerProducerId,
  observationPollInterval: 1,
});

/** One DN "host process": the full Node/SQLite runtime stack plus the deterministic services. */
const dnLayer = (options: NodeDurableRuntimeOptions) =>
  Layer.mergeAll(
    phase4TravelPlannerWorkerLayer,
    NodeDurableRuntime.layer(options).pipe(Layer.provide(TrustedLocalDurableAuthorizationLayer)),
  );

const assertResetAdvancesGateGeneration = Effect.fn(
  "TravelPlannerPhase6Test.assertResetAdvancesGateGeneration",
)(function* (
  awaitGate: Effect.Effect<void>,
  resetGate: Effect.Effect<void>,
  releaseGate: Effect.Effect<void>,
) {
  const previousWaiter = yield* Effect.forkChild(awaitGate);
  yield* Effect.yieldNow;
  expect(previousWaiter.pollUnsafe()).toBeUndefined();

  yield* resetGate;
  yield* Fiber.join(previousWaiter);

  const nextWaiter = yield* Effect.forkChild(awaitGate);
  yield* Effect.yieldNow;
  expect(nextWaiter.pollUnsafe()).toBeUndefined();

  yield* releaseGate;
  yield* Fiber.join(nextWaiter);
});

describe("Phase 6 fixture gate generations", () => {
  it.effect("planner reset releases existing keyed waiters and blocks the next generation", () =>
    Effect.gen(function* () {
      const harness = yield* makePhase6TravelPlannerHarness();
      const marker = "planner-gate-generation";
      yield* assertResetAdvancesGateGeneration(
        harness.awaitPlannerGate(marker),
        harness.resetPlannerGate(marker),
        harness.releasePlannerGate(marker),
      );
    }),
  );

  it.effect("researcher reset releases existing waiters and blocks the next generation", () =>
    Effect.gen(function* () {
      const harness = yield* makePhase6TravelPlannerHarness();
      yield* assertResetAdvancesGateGeneration(
        harness.awaitResearcherGate,
        harness.resetResearcherGate,
        harness.releaseResearcherGate,
      );
    }),
  );
});

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
          const store = yield* ConversationStore;
          const conversationId = decodeConversationId("travel-planner-p6-golden-dn");
          const agent = makePhase4TravelPlannerAgent();

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            phase4TravelPlannerSubmitOptions(
              conversationId,
              decodeIdempotencyKey("p6-golden-dn-1"),
            ),
          );
          const settlements = yield* runtime.processConversation(
            agent,
            conversationId,
            phase4TravelPlannerDefinitionDigests,
          );
          expect(settlements).toHaveLength(1);
          expect(settlements[0]?.outcome).toBe("completed");

          const records = yield* Stream.runCollect(
            store.read(ConversationRead.make({ conversationId, limit: 1_024 })),
          );
          expect(yield* travelPlanFromDurableSettlement(records)).toEqual(expectedTravelPlan);

          const normalized = yield* normalizeCrossPlatformTravelPlannerEvidence(records, receipt, {
            conversationId,
            deploymentId: phase4TravelPlannerDeploymentId,
            producerId: phase4TravelPlannerProducerId,
          });
          expect(normalized).toEqual(phase6TravelPlannerGoldenEvidence);
        }).pipe(Effect.provide(dnLayer(runtimeOptions(`${directory}/p6-golden.sqlite`)))),
      ),
  );
});
