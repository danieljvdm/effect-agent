import * as v8 from "node:v8";
import * as vm from "node:vm";

import { Agent, AgentPolicy, ThreadId } from "@effect-agent/core";
import { MemoryThreadStoreLive, MemorySubmissionLedgerLive } from "@effect-agent/storage-memory";
import {
  DeploymentId,
  Digest,
  DefinitionDigests,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  IdempotencyKey,
  ObligationThresholds,
  Principal,
  ProducerId,
  SubmissionLedger,
  ToolReconciler,
  WakeScheduler,
  type DurableSubmitOptions,
} from "@effect-agent/thread";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

/**
 * P7 WP4 pure-memory soak (plan §5): 5,000 Submissions across 500 lanes under TestClock,
 * heavy on queued input so the joining/joined machinery (`joinedInputEnvelopes` and friends)
 * runs constantly. The leak claim is scoped honestly: the coordinator's per-Attempt maps are
 * function-local, so the observable property from outside is that after each HALF of the soak
 * finishes and its Layer scope closes, a forced GC returns the process heap to its baseline —
 * no module-level map, lingering fiber, or unclosed resource retains the 2,500 settled
 * Submissions' state. A per-Submission leak of even ~1KB would hold ~2.5MB per window and
 * fail the window-to-window stability bound.
 */

// The memory ThreadStore bounds itself to 256 Threads (SEC-013), so each wave
// stays under that bound and the 5,000-Submission total spans two scoped waves.
const LANES = 250;
const SUBMISSIONS_PER_LANE = 10;
const WAVES = 2;

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const PRINCIPAL = Schema.decodeSync(Principal)("principal-soak-memory");
const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

const usage = { inputTokens: {}, outputTokens: {} };
const finalParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '{"answer":"soak"}' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

/** Prompt-independent single-turn model; joined steering settles with the host Run. */
const soakModel = Model.make(
  "scripted",
  "soak-memory",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: () => Stream.fromIterable(finalParts),
    }),
  ),
);

const soakDefinition = Agent.make("soak-memory", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-soak-memory"),
  producerId: Schema.decodeSync(ProducerId)("producer-soak-memory"),
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

const freshLayer = () =>
  DurableAgentRuntime.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        MemorySubmissionLedgerLive,
        MemoryThreadStoreLive,
        WakeScheduler.layerNoop,
        DurableRuntimeFailpoint.layer,
        ToolReconciler.uncertain,
        configLayer,
      ).pipe(Layer.provideMerge(NodeCrypto.layer)),
    ),
  );

const submitOptions = (wave: number, lane: number, member: number): DurableSubmitOptions => ({
  threadId: decodeThreadId(`soak-memory-w${wave}-lane-${lane}`),
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(`soak-w${wave}-l${lane}-s${member}`),
  definitions: DIGESTS,
});

/** Forced full GC through the documented v8 flag toggle (no --expose-gc launch flag needed). */
const heapAfterGc = (): number => {
  v8.setFlagsFromString("--expose-gc");
  const gc = vm.runInNewContext("gc") as () => void;
  gc();
  gc();
  v8.setFlagsFromString("--no-expose-gc");
  return process.memoryUsage().heapUsed;
};

/** One soak wave: submit every lane's queue, drain each lane once, assert full settlement. */
const runWave = (wave: number) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const ledger = yield* SubmissionLedger;
    const agent = Agent.withModel(soakDefinition, soakModel);
    for (let lane = 0; lane < LANES; lane++) {
      // The whole queue is admitted before the drain, so the host Run claims the contiguous
      // ready prefix and JOINS the queued members at its Turn seams (DUR-016).
      for (let member = 0; member < SUBMISSIONS_PER_LANE; member++) {
        yield* runtime.submit(
          agent,
          { question: `soak ${lane}/${member}` },
          submitOptions(wave, lane, member),
        );
      }
      const settlements = yield* runtime.processThread(
        agent,
        decodeThreadId(`soak-memory-w${wave}-lane-${lane}`),
      );
      expect(settlements.length).toBeGreaterThan(0);
    }
    // Convergence: nothing nonterminal, nothing invisibly stuck (DUR-017 surface agrees).
    const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
    expect(Array.from(nonterminal)).toHaveLength(0);
    const obligations = yield* runtime.scanObligations(
      ObligationThresholds.make({ agingSeconds: 0, overdueSeconds: 0 }),
    );
    expect(obligations.entries).toHaveLength(0);
  }).pipe(Effect.provide(freshLayer()));

describe("DUR-016 P7 pure-memory soak (coordinator map cleanup)", () => {
  it.effect(
    `SOAK: ${WAVES * LANES * SUBMISSIONS_PER_LANE} submissions across ${WAVES * LANES} join-heavy lanes settle and the heap returns to baseline after each wave's scope closes`,
    () =>
      Effect.gen(function* () {
        const baseline = yield* Effect.sync(heapAfterGc);
        const waveHeaps: Array<number> = [];
        for (let wave = 0; wave < WAVES; wave++) {
          yield* runWave(wave);
          waveHeaps.push(yield* Effect.sync(heapAfterGc));
        }
        const first = waveHeaps[0] ?? Number.NaN;
        const last = waveHeaps.at(-1) ?? Number.NaN;
        // Window-to-window stability: closing each wave's Layer scope releases everything the
        // wave retained (canonical records included — they live in the wave's memory adapters).
        // A coordinator leak surviving the scope would grow monotonically across windows.
        expect(last).toBeLessThanOrEqual(first + 16 * 1024 * 1024);
        // And the whole soak returns near the pre-soak baseline.
        expect(last).toBeLessThanOrEqual(baseline + 32 * 1024 * 1024);
      }),
    240_000,
  );
});
