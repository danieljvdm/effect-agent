import { OpenAiClient } from "@effect/ai-openai";
import {
  Config,
  ConfigProvider,
  Console,
  Effect,
  FileSystem,
  Layer,
  Option,
  Path,
  Schema,
} from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { EvaluationError, EvaluationReport } from "./contracts.ts";
import { runEvaluation } from "./evaluate.ts";
import { ModelId, MODEL_IDS } from "./live-model.ts";
import {
  makeScenario,
  REQUIRED_ROLLOVERS,
  RESTARTS,
  ScenarioPhase,
  SCENARIO_VERSION,
} from "./scenario.ts";

const provider = OpenAiClient.layerConfig({ apiKey: Config.redacted("OPENAI_API_KEY") }).pipe(
  Layer.provide(FetchHttpClient.layer),
);

export const command = Command.make(
  "context-continuity-eval",
  {
    model: Flag.choice("model", MODEL_IDS).pipe(
      Flag.optional,
      Flag.withDescription("Required model, or CONTEXT_EVAL_MODEL. No model fallback."),
    ),
    effort: Flag.choice("effort", ["low", "medium", "high"]).pipe(
      Flag.withDefault("low"),
      Flag.withDescription("Reasoning effort recorded with the result."),
    ),
    seed: Flag.integer("seed").pipe(
      Flag.withSchema(Schema.Natural.check(Schema.isLessThanOrEqualTo(1_000_000))),
      Flag.withDefault(17),
      Flag.withDescription("Seed for the frozen conversation and receipt codes."),
    ),
    maxCostUsd: Flag.float("max-cost-usd").pipe(
      Flag.withSchema(Schema.Finite.check(Schema.isBetween({ minimum: 0.1, maximum: 100 }))),
      Flag.withDefault(10),
      Flag.withDescription(
        "Suite-wide conservative USD ceiling; reserve each request before inference.",
      ),
    ),
    outputDirectory: Flag.directory("output-dir").pipe(
      Flag.withDefault(".context-continuity-eval/run"),
      Flag.withDescription("New artifact directory. Existing runs are never overwritten."),
    ),
    envFile: Flag.file("env-file").pipe(
      Flag.optional,
      Flag.withDescription(
        "Optional existing dotenv file. Exported environment values take precedence.",
      ),
    ),
    requireClean: Flag.boolean("require-clean").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Reject tracked or untracked source changes; required for publication."),
    ),
    validate: Flag.boolean("validate").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Validate the scenario without credentials or model calls. This is not a passing live eval.",
      ),
    ),
  },
  Effect.fn("ContextContinuity.command")(function* (options) {
    if (options.validate) {
      const phases = yield* Schema.decodeUnknownEffect(Schema.Array(ScenarioPhase))(
        makeScenario(options.seed),
      );

      yield* Console.log(
        JSON.stringify({
          scenario: SCENARIO_VERSION,
          updates: phases.length,
          requiredNativeRollovers: REQUIRED_ROLLOVERS,
          recoveryBoundaries: RESTARTS,
          liveEvaluation: false,
        }),
      );

      return;
    }

    const run = Effect.gen(function* () {
      const enabled = yield* Config.string("EFFECT_AGENT_LIVE").pipe(Config.withDefault("0"));

      if (enabled !== "1")
        return yield* EvaluationError.make({
          stage: "configuration",
          message:
            "Set EFFECT_AGENT_LIVE=1 to authorize paid model calls. Use --validate for the offline scenario check.",
        });

      const model = Option.isSome(options.model)
        ? options.model.value
        : yield* Config.schema(ModelId, "CONTEXT_EVAL_MODEL");

      const path = yield* Path.Path;
      const fs = yield* FileSystem.FileSystem;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

      const root = path.resolve(
        path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
        "../../..",
      );

      const sourceCommit = (yield* spawner.string(
        ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: root }),
      )).trim();

      const status = yield* spawner.string(
        ChildProcess.make("git", ["status", "--porcelain", "--untracked-files=normal"], {
          cwd: root,
        }),
      );

      const dirtyWorkingTree = status.trim().length > 0;

      if (options.requireClean && dirtyWorkingTree)
        return yield* EvaluationError.make({
          stage: "source",
          message: "Publication evaluation requires a clean checkout of the candidate commit.",
        });
      const outputDirectory = path.resolve(options.outputDirectory);

      if (yield* fs.exists(outputDirectory))
        return yield* EvaluationError.make({
          stage: "evidence",
          message:
            "Output directory already exists. Choose a new --output-dir to preserve the first attempt.",
        });
      yield* fs.makeDirectory(outputDirectory, { recursive: true });

      const report = yield* runEvaluation({
        model,
        reasoningEffort: options.effort,
        seed: options.seed,
        outputDirectory,
        sourceCommit,
        dirtyWorkingTree,
        maxCostMicrousd: Math.floor(options.maxCostUsd * 1_000_000),
      }).pipe(Effect.provide(provider));

      yield* Console.log(
        yield* Schema.encodeEffect(Schema.fromJsonString(EvaluationReport))(report),
      );
      if (report.status !== "passed")
        return yield* EvaluationError.make({
          stage: "gate",
          message: `Continuity evaluation failed. Inspect ${path.join(outputDirectory, "report.json")}; a partial or failed run cannot approve publication.`,
        });
    });

    if (Option.isNone(options.envFile)) return yield* run;

    const localConfig = ConfigProvider.layerAdd(
      ConfigProvider.fromDotEnv({ path: options.envFile.value }),
    );

    return yield* run.pipe(Effect.provide(localConfig));
  }),
).pipe(
  Command.withDescription(
    "Run a real OpenAI conversation through native notes, history, 12 rollovers, and SQLite recovery. Requires EFFECT_AGENT_LIVE=1 and OPENAI_API_KEY.",
  ),
);
