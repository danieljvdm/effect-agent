import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { expect, it } from "vite-plus/test";

import {
  casesFor,
  completeBatch,
  FIXTURE_VERSION,
  summary,
  type Sample,
  type WorkerReport,
} from "../src/contracts.ts";
import { BenchmarkProgress } from "../src/evidence.ts";
import { runSample, SeedInitializerLive } from "../src/fixture.ts";
import { SeedTemplates } from "../src/seeds.ts";

it.each(casesFor("smoke"))(
  "validates equivalent completed work in $name",
  async (workload) => {
    const result = await Effect.runPromise(
      runSample(workload, 0, false).pipe(
        Effect.provide(
          Layer.merge(SeedTemplates.layer, BenchmarkProgress.silent).pipe(
            Layer.provide(SeedInitializerLive),
            Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
          ),
        ),
      ),
    );

    expect(result.failure).toBeNull();
    expect(result.status).toBe("passed");
    expect(result.modelCalls).toBe(result.finalizers);
    expect(result.checkpointCreationMs === null).toBe(workload.kind !== "recovery");
    expect(result.retainedPromptMessages).toBe(
      workload.kind === "durable" ? 2 * Math.max(0, Math.floor((workload.records - 1) / 3)) : 0,
    );
  },
  30_000,
);

it("rejects missing, duplicated, or unfinalized samples even if the subprocess exits successfully", () => {
  const sample: Sample = {
    case: "small-run",
    ordinal: 0,
    warmup: false,
    totalMs: 2,
    attemptMs: 3,
    setupMs: 0.5,
    failurePhase: null,
    modelEntryMs: 1,
    checkpointCreationMs: null,
    retainedPromptMessages: 0,
    modelCalls: 1,
    finalizers: 1,
    toolCalls: 0,
    outputBytes: 15,
    status: "passed",
    failure: null,
  };

  const report: WorkerReport = {
    fixture: FIXTURE_VERSION,
    profile: "smoke",
    runtime: "v24",
    platform: "test",
    architecture: "test",
    active: null,
    failure: null,
    samples: [sample],
  };

  const options = {
    cold: true,
    profile: "smoke" as const,
    warmups: 0,
    samples: 1,
    output: "unused",
  };

  expect(completeBatch(report, options)).toBe(true);
  expect(completeBatch({ ...report, samples: [] }, options)).toBe(false);
  expect(completeBatch({ ...report, samples: [sample, sample] }, options)).toBe(false);
  expect(completeBatch({ ...report, samples: [{ ...sample, finalizers: 0 }] }, options)).toBe(
    false,
  );
  expect(summary([1, 2, 3, 4, 100])).toEqual({
    count: 5,
    median: 3,
    q1: 2,
    q3: 4,
    min: 1,
    max: 100,
  });
});
