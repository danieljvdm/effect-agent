import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/MemorySubmissionLedger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { DurableRuntimeConfig } from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpoint } from "@effect-agent/thread/DurableFailpoint";
import { DeploymentId, ProducerId } from "@effect-agent/thread/Records";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Layer, Schema } from "effect";

import {
  measureHistory,
  seedHistory,
  type ReadFault,
  type ReadPhase,
} from "../../../test/fixtures/runtime-history-cost.ts";

const base = Layer.mergeAll(
  MemoryThreadStoreLive,
  MemorySubmissionLedgerLive,
  WakeScheduler.layerNoop,
  ToolReconciler.uncertain,
  DurableRuntimeFailpoint.layer,
  RunToolAuthorization.allowAll,
  DurableRuntimeConfig.layer({
    deploymentId: Schema.decodeSync(DeploymentId)("history-cost"),
    producerId: Schema.decodeSync(ProducerId)("history-cost"),
  }),
).pipe(Layer.provideMerge(NodeCrypto.layer));

const measure = Effect.fn("RuntimeHistoryCost.memory")(function* (
  historySize: number,
  fault?: { readonly kind: ReadFault; readonly phase: ReadPhase },
  raceAppend = false,
  withCompaction = false,
) {
  return yield* measureHistory(yield* seedHistory(historySize, withCompaction), fault, raceAppend);
});

it.effect("bounds startup history reads while retaining the complete fixed prefix", () =>
  Effect.gen(function* () {
    for (const historySize of [1, 11, 2051]) {
      const measurement = yield* measure(historySize).pipe(Effect.provide(base));

      expect(Exit.isSuccess(measurement.exit)).toBe(true);
      expect(measurement.measured).toBe(true);
      expect(measurement.modelCalls).toBe(1);
      expect(measurement.modelFinalizers).toBe(1);
      expect(measurement.openedPages).toBe(measurement.closedPages);
      expect(measurement.snapshot.ownership).toBeUndefined();
      expect(measurement.returnedRecords).toBeLessThanOrEqual(2 * historySize + 6);
      expect(measurement.totalReturnedRecords).toBeLessThanOrEqual(2 * historySize + 6);
    }
  }),
);

it.effect.each([
  { kind: "gap", phase: "prefix" },
  { kind: "short", phase: "prefix" },
  { kind: "failure", phase: "prefix" },
  { kind: "defect", phase: "prefix" },
  { kind: "interruption", phase: "prefix" },
  { kind: "gap", phase: "suffix" },
  { kind: "failure", phase: "suffix" },
  { kind: "interruption", phase: "suffix" },
  { kind: "gap", phase: "fold" },
  { kind: "short", phase: "fold" },
] satisfies ReadonlyArray<{ readonly kind: ReadFault; readonly phase: ReadPhase }>)(
  "releases startup reads and ownership after $phase $kind",
  (fault) =>
    Effect.gen(function* () {
      const result = yield* measure(1_025, fault).pipe(Effect.provide(base));

      expect(result.injected).toBe(true);
      expect(result.measured).toBe(false);
      expect(Exit.isFailure(result.exit)).toBe(true);
      expect(result.openedPages).toBe(result.closedPages);
      expect(result.snapshot.ownership).toBeUndefined();
      expect(result.snapshot.reservation).toBeUndefined();
      expect(result.requests.every((request) => request.limit <= 1_024)).toBe(true);
      if (Exit.isFailure(result.exit) && ["gap", "short", "failure"].includes(fault.kind))
        expect(Cause.hasFails(result.exit.cause)).toBe(true);
    }),
);

it.effect("captures the initial tail and incorporates racing appends through a later suffix", () =>
  Effect.gen(function* () {
    const result = yield* measure(1_025, undefined, true).pipe(Effect.provide(base));

    expect(result.raced).toBe(true);
    expect(result.measured).toBe(true);
    expect(Exit.isSuccess(result.exit)).toBe(true);
    expect(
      result.requests.slice(0, 2).map(({ afterSequence, limit }) => [afterSequence ?? 0, limit]),
    ).toEqual([
      [0, 1_024],
      [1_024, 1],
    ]);
    expect(result.returnedRecords).toBeLessThanOrEqual(2 * 1_026 + 6);
    expect(result.openedPages).toBe(result.closedPages);
    expect(result.snapshot.ownership).toBeUndefined();
  }),
);

it.effect("falls back to ordinary journal scans when any compaction metadata is present", () =>
  Effect.gen(function* () {
    const result = yield* measure(1_025, undefined, false, true).pipe(Effect.provide(base));

    expect(result.measured).toBe(true);
    expect(Exit.isSuccess(result.exit)).toBe(true);
    expect(result.returnedRecords).toBeGreaterThanOrEqual(3 * 1_025);
    expect(result.returnedRecords).toBeLessThanOrEqual(3 * 1_025 + 6);
    expect(result.openedPages).toBe(result.closedPages);
    expect(result.snapshot.ownership).toBeUndefined();
  }),
);
