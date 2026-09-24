import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/node-durable-agent-runtime";
import { ScriptedModel } from "@effect-agent/testing/scripted-model";
import { NodeServices } from "@effect/platform-node";
import { Effect, Exit, FileSystem, Layer, Schema } from "effect";
import { Agent } from "effect-agent";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ThreadId } from "effect-agent/identifiers";
import { DeploymentId, DefinitionDigests, Digest, ProducerId } from "effect-agent/records";
import { IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { Model, Toolkit } from "effect/unstable/ai";
import { expect, it } from "vite-plus/test";

import { assertCheckpointFault } from "../src/fixture.ts";

it("reports failed durable Settlements even when processThread succeeds", async () => {
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
  expect(diagnostic._tag).toBe("BenchmarkError");
});
