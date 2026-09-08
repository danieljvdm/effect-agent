import process from "node:process";

import { CanonicalRecordEnvelope } from "@effect-agent/thread/Records";
import { OpenAiClient } from "@effect/ai-openai";
import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Layer, Redacted, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { runEvaluation } from "../src/evaluate.ts";
import { RequestAudit } from "../src/live-model.ts";
import { WorkerOptions } from "../src/process-host.ts";
import { scriptedResponse } from "./scripted-model.ts";

// Test-only provider. No network transport is installed. It exercises the actual
// native streaming decoder, budget ledger, runtime, SQLite, and supervisor.
const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const file = yield* Config.string("CONTEXT_EVAL_WORKER_OPTIONS");

  const options = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(WorkerOptions))(
    yield* fs.readFileString(file),
  );

  let countedInput = 0;

  const http = HttpClient.make((request) =>
    Effect.gen(function* () {
      if (request.url.endsWith("/input_tokens")) {
        if (request.body._tag !== "Uint8Array")
          return yield* Effect.die("Expected token count request");
        countedInput = Math.ceil(request.body.body.byteLength / 4);

        return HttpClientResponse.fromWeb(
          request,
          Response.json({ object: "response.input_tokens", input_tokens: countedInput }),
        );
      }
      if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON body");

      const audit = (yield* fs.readFileString(`${options.outputDirectory}/requests.ndjson`))
        .trim()
        .split("\n")
        .map((line) => Schema.decodeSync(Schema.fromJsonString(RequestAudit))(line));

      const phase = audit.at(-1)?.phase ?? 0;
      const ordinal = audit.filter((a) => a.phase === phase && a.kind === "request").length - 1;

      const originalLog =
        phase === 0
          ? []
          : (yield* fs.readFileString(`${options.outputDirectory}/canonical.ndjson`))
              .trim()
              .split("\n")
              .map((line) =>
                Schema.decodeSync(Schema.fromJsonString(CanonicalRecordEnvelope))(line),
              );

      const original = originalLog.find(
        ({ record }) =>
          record.payload._tag === "ModelResponseRecorded" &&
          JSON.stringify(record.payload).includes("Archive document HARBOR-RECEIPTS follows"),
      )?.record.recordId;

      const { response } = yield* scriptedResponse(
        new TextDecoder().decode(request.body.body),
        phase,
        ordinal,
        countedInput,
        options.model,
        options.seed,
        original,
      );

      return HttpClientResponse.fromWeb(request, response);
    }).pipe(
      Effect.tapError((error) => Console.error(error.message)),
      Effect.orDie,
    ),
  );

  const report = yield* runEvaluation({ ...options, processId: process.pid }).pipe(
    Effect.provide(
      OpenAiClient.layer({ apiKey: Redacted.make("test-only") }).pipe(
        Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
      ),
    ),
  );

  if (report.status !== "passed")
    return yield* Effect.die(
      report.failure ?? {
        windows: report.windows.length,
        checks: report.phases.flatMap((p) => p.checks.filter((c) => !c.passed)),
        gate: report.checks.filter((c) => !c.passed),
      },
    );
}).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)));

NodeRuntime.runMain(program);
