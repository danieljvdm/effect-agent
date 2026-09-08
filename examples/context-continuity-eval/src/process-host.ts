import { Config, Console, Effect, Exit, FileSystem, Path, Redacted, Schema } from "effect";
import { ChildProcess } from "effect/unstable/process";

import { EvaluationError, EvaluationReport, KillWitness, ResumeCheckpoint } from "./contracts.ts";
import { ModelId, ReasoningEffort } from "./live-model.ts";
import { RESTARTS } from "./scenario.ts";

export const WorkerOptions = Schema.Struct({
  model: ModelId,
  reasoningEffort: ReasoningEffort,
  seed: Schema.Natural,
  outputDirectory: Schema.String,
  sourceCommit: Schema.String,
  dirtyWorkingTree: Schema.Boolean,
  maxCostMicrousd: Schema.Natural,
  profile: Schema.Literal("pressure-restart-sqlite-v1"),
});

/** Only the two named, acknowledged barriers authorize a new child. Unexpected exits stop. */
export const supervise = Effect.fn("ContextContinuity.supervise")(
  function* (options: typeof WorkerOptions.Type, workerFile?: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const optionsPath = path.join(options.outputDirectory, "worker-options.json");

    yield* fs.writeFileString(
      optionsPath,
      yield* Schema.encodeEffect(Schema.fromJsonString(WorkerOptions))(options),
    );

    const entry =
      workerFile ?? (yield* path.fromFileUrl(new URL("./worker-main.ts", import.meta.url)));

    const apiKey = yield* Config.redacted("OPENAI_API_KEY");

    for (let incarnation = 0; incarnation <= RESTARTS.length; incarnation++) {
      const child = yield* ChildProcess.make("node", ["--experimental-transform-types", entry], {
        env: { CONTEXT_EVAL_WORKER_OPTIONS: optionsPath, OPENAI_API_KEY: Redacted.value(apiKey) },
        extendEnv: true,
        stdout: "inherit",
        stderr: "inherit",
      });

      const boundary = RESTARTS[incarnation];

      if (boundary !== undefined) {
        const barrierPath = path.join(options.outputDirectory, `barrier-${boundary.phase}.json`);

        while (!(yield* fs.exists(barrierPath))) {
          if (!(yield* child.isRunning))
            return yield* EvaluationError.make({
              stage: "restart",
              message: "Worker exited before its planned kill barrier; no retry",
            });
          yield* Effect.sleep("50 millis");
        }

        const barrier = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ResumeCheckpoint))(
          yield* fs.readFileString(barrierPath),
        );

        if (
          barrier.phase !== boundary.phase ||
          barrier.processId !== child.pid ||
          barrier.report.sourceCommit !== options.sourceCommit
        )
          return yield* EvaluationError.make({
            stage: "restart",
            message: "Kill barrier identity mismatch",
          });
        yield* child.kill({ killSignal: "SIGKILL" });
        const killedExit = yield* child.exitCode.pipe(Effect.exit);

        if (Exit.isSuccess(killedExit))
          return yield* EvaluationError.make({
            stage: "restart",
            message: "Worker exited normally instead of dying from SIGKILL",
          });
        if (yield* child.isRunning)
          return yield* EvaluationError.make({
            stage: "restart",
            message: "Killed worker is still running",
          });
        yield* fs.writeFileString(
          path.join(options.outputDirectory, `kill-${boundary.phase}.json`),
          yield* Schema.encodeEffect(Schema.fromJsonString(KillWitness))({
            phase: boundary.phase,
            processId: child.pid,
            signal: "SIGKILL",
            exited: true,
          }),
        );
        yield* Console.error(
          `Context continuity: SIGKILL confirmed at ${boundary.location}, pid ${child.pid}`,
        );
        // The dead owner's lease must expire; no surviving runtime is used to release it.
        yield* Effect.sleep("2100 millis");
      } else {
        const exitCode = yield* child.exitCode;

        const report = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(EvaluationReport))(
          yield* fs.readFileString(path.join(options.outputDirectory, "report.json")),
        );

        if (exitCode !== 0 || report.status !== "passed")
          return yield* EvaluationError.make({
            stage: "gate",
            message: "Supervised evaluation failed; inspect preserved report and requests",
          });

        return report;
      }
    }

    return yield* EvaluationError.make({ stage: "restart", message: "No final worker outcome" });
  },
  Effect.scoped,
  Effect.timeout("40 minutes"),
);
