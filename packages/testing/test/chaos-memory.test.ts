import {
  DeploymentId,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  ProducerId,
  ToolReconciler,
  WakeScheduler,
} from "@effect-agent/session";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Duration, Effect, Exit, Layer, Schema } from "effect";

import {
  ChaosPlan,
  DEFAULT_CHAOS_SEED,
  chaosSeedFromEnv,
  generateChaosPlans,
  runChaosPlan,
} from "../src/index.ts";

/**
 * P7 WP4 memory chaos lane (plan §5): ~200 seeded plans over the in-memory adapter pair,
 * deterministic under the test services (TestClock — no real waiting anywhere in the runner).
 * Every plan ends in the shared `verifyConversationInvariants` convergence claims, a zero-entry
 * `scanObligations`, and the desk non-fabrication sweep. Failure output prints the root seed
 * (replay with `CHAOS_SEED=<seed> bun run test`) and the failing plan's own seed.
 */

const ROOT_SEED = chaosSeedFromEnv(process.env);
const PLAN_COUNT = 200;

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-chaos-memory"),
  producerId: Schema.decodeSync(ProducerId)("producer-chaos-memory"),
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

/** One FRESH adapter pair + coordinator per plan: no state bleeds between plans. */
const freshLayer = () =>
  DurableAgentRuntime.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        MemorySubmissionLedgerLive,
        MemoryConversationStoreLive,
        WakeScheduler.layerNoop,
        DurableRuntimeFailpoint.layerTest,
        ToolReconciler.uncertain,
        configLayer,
      ).pipe(Layer.provideMerge(NodeCrypto.layer)),
    ),
  );

const replayHint = (planIndex: number, plan: ChaosPlan): string =>
  `replay: CHAOS_SEED=${ROOT_SEED} (plan #${planIndex}, plan seed ${plan.seed}, lanes ${plan.lanes}, ` +
  `submissions [${plan.submissions.map((spec) => `${spec.lane}:${spec.kind}`).join(", ")}], ` +
  `arms [${[...plan.failpointArms, ...plan.adapterArms].join(", ")}])`;

describe("DUR-002/DUR-004/DUR-017 P7 chaos (memory adapters)", () => {
  it("accepts only schema-valid safe-integer CHAOS_SEED values", () => {
    expect(chaosSeedFromEnv({ CHAOS_SEED: "-42" })).toBe(-42);
    expect(chaosSeedFromEnv({ CHAOS_SEED: "12x" })).toBe(DEFAULT_CHAOS_SEED);
    expect(chaosSeedFromEnv({ CHAOS_SEED: "9007199254740992" })).toBe(DEFAULT_CHAOS_SEED);
    expect(chaosSeedFromEnv({ CHAOS_SEED: "" })).toBe(DEFAULT_CHAOS_SEED);
  });

  it.effect(
    `CHAOS: ${PLAN_COUNT} seeded failpoint/abort interleavings over memory adapters converge to verified invariants`,
    () =>
      Effect.gen(function* () {
        const plans = generateChaosPlans({ seed: ROOT_SEED, count: PLAN_COUNT });
        // Determinism of the generator itself: the same root seed always derives the same plans.
        const regenerated = generateChaosPlans({ seed: ROOT_SEED, count: PLAN_COUNT });
        expect(JSON.stringify(plans)).toBe(JSON.stringify(regenerated));

        let verifiedLanes = 0;
        for (const [planIndex, plan] of plans.entries()) {
          const exit = yield* Effect.exit(runChaosPlan(plan).pipe(Effect.provide(freshLayer())));
          if (Exit.isFailure(exit)) {
            throw new Error(
              `chaos plan failed — ${replayHint(planIndex, plan)}\n${Cause.pretty(exit.cause)}`,
            );
          }
          expect(exit.value.openObligations).toBe(0);
          for (const lane of exit.value.lanes) {
            expect(
              lane.verified,
              `${replayHint(planIndex, plan)} lane ${lane.conversationId}`,
            ).toBe(true);
          }
          verifiedLanes += exit.value.lanes.length;
        }
        // Sanity: the sweep actually verified a meaningful population of lanes.
        expect(verifiedLanes).toBeGreaterThan(PLAN_COUNT);
      }),
    240_000,
  );

  it.effect("the ChaosPlan and ChaosPlanReport Schemas round-trip their encoded form", () =>
    Effect.gen(function* () {
      const [plan] = generateChaosPlans({ seed: ROOT_SEED, count: 1 });
      expect(plan).toBeDefined();
      if (plan === undefined) return;
      const encoded = yield* Schema.encodeEffect(ChaosPlan)(plan);
      const decoded = yield* Schema.decodeUnknownEffect(ChaosPlan)(encoded);
      expect(JSON.stringify(decoded)).toBe(JSON.stringify(plan));
      // Invalid shapes fail typed instead of decoding incorrectly (TEST-001).
      const invalid = yield* Effect.exit(
        Schema.decodeUnknownEffect(ChaosPlan)({ ...encoded, lanes: 0 }),
      );
      expect(Exit.isFailure(invalid)).toBe(true);
    }),
  );
});
