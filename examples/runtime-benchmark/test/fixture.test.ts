import { ThreadId } from "@effect-agent/core/Identifiers";
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import { ScriptedModel } from "@effect-agent/testing/ScriptedModel";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DeploymentId, DefinitionDigests, Digest, ProducerId } from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Effect, Exit, FileSystem, Layer, Schema } from "effect";
import { Agent } from "effect-agent";
import { Model, Toolkit } from "effect/unstable/ai";
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
import { assertCheckpointFault, runSample, SeedInitializerLive } from "../src/fixture.ts";
import { SeedTemplates } from "../src/seeds.ts";

it("reports failed durable Settlements even when processThread succeeds", async () => {
  let finalized = 0;

  const agent = Agent.make("checkpoint-diagnostic", {
    input: Schema.String,
    output: Schema.String,
    instructions: "Return the answer.",
    toolkit: Toolkit.empty,
  });

  const digest = Schema.decodeSync(Digest)("b".repeat(64));
  const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

  const model = Layer.mergeAll(
    ScriptedModel.layer([
      {
        _tag: "Stream",
        parts: [],
        termination: { _tag: "Fail", description: "diagnostic provider unavailable" },
        onStreamFinalize: Effect.sync(() => {
          finalized++;
        }),
      },
    ]),
    Layer.succeed(Model.ProviderName, "scripted"),
    Layer.succeed(Model.ModelName, "checkpoint-diagnostic"),
  );

  const { attempt, diagnostic } = await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "checkpoint-diagnostic-" });

      return yield* Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        const receipt = yield* runtime.submit({ definition: agent }, "Answer", {
          threadId: ThreadId.make("checkpoint-diagnostic"),
          principal: Principal.make("checkpoint-diagnostic"),
          idempotencyKey: IdempotencyKey.make("checkpoint-diagnostic"),
          definitions,
        });

        const attempt = yield* runtime
          .processThread({ definition: agent, model }, receipt.threadId)
          .pipe(Effect.exit);

        const diagnostic = yield* assertCheckpointFault(attempt, {
          compactionCommitted: false,
          checkpointCreationMs: null,
        }).pipe(Effect.flip);

        return { attempt, diagnostic };
      }).pipe(
        Effect.provide(
          NodeDurableAgentRuntime.layer({
            filename: `${directory}/thread.sqlite`,
            deploymentId: DeploymentId.make("checkpoint-diagnostic"),
            producerId: ProducerId.make("checkpoint-diagnostic"),
          }),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  expect(Exit.isSuccess(attempt)).toBe(true);
  const settlements = Exit.isSuccess(attempt) ? attempt.value : [];

  expect(settlements).toHaveLength(1);
  expect(settlements[0]?.outcome).toBe("failed");
  expect(settlements[0]?.failure?.errorTag).toBe("AiError");
  expect(settlements[0]?.failure?.message).toContain("diagnostic provider unavailable");
  expect(diagnostic._tag).toBe("BenchmarkError");
  expect(diagnostic.message).toContain('"outcome":"failed"');
  expect(diagnostic.message).toContain("AiError");
  expect(diagnostic.message).toContain("diagnostic provider unavailable");
  expect(diagnostic.message).toContain('"compactionCommitted":false');
  expect(diagnostic.message).toContain('"checkpointCreationMs":null');
  expect(finalized).toBe(1);
});

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
