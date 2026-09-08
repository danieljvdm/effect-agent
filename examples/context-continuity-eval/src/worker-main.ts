import process from "node:process";

import { OpenAiClient } from "@effect/ai-openai";
import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Effect, FileSystem, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { EvaluationError } from "./contracts.ts";
import { runEvaluation } from "./evaluate.ts";
import { WorkerOptions } from "./process-host.ts";

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const file = yield* Config.string("CONTEXT_EVAL_WORKER_OPTIONS");

  const options = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(WorkerOptions))(
    yield* fs.readFileString(file),
  );

  const report = yield* runEvaluation({ ...options, processId: process.pid });

  if (report.status !== "passed")
    return yield* EvaluationError.make({ stage: "gate", message: "Worker evaluation failed" });
}).pipe(
  Effect.scoped,
  Effect.provide(
    Layer.mergeAll(
      OpenAiClient.layerConfig({ apiKey: Config.redacted("OPENAI_API_KEY") }).pipe(
        Layer.provide(FetchHttpClient.layer),
      ),
      NodeServices.layer,
      NodeCrypto.layer,
    ),
  ),
);

NodeRuntime.runMain(program);
