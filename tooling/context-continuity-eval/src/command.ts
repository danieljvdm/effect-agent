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

import { runCloudflareEvaluation } from "./cloudflare.ts";
import { EvaluationError, EvaluationReport } from "./contracts.ts";
import { runEvaluation } from "./evaluate.ts";
import { ModelId, MODEL_IDS, productionCostPlan } from "./live-model.ts";
import { pressureScenario } from "./pressure.ts";
import { supervise } from "./process-host.ts";
import { DEFAULT_PROFILE, PROFILE_IDS, profilePlan } from "./profiles.ts";
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
    cloudflareUrl: Flag.string("cloudflare-url").pipe(
      Flag.optional,
      Flag.withDescription(
        "Already deployed Cloudflare evaluation host; requires CONTEXT_EVAL_TOKEN.",
      ),
    ),
    profile: Flag.choice("profile", PROFILE_IDS).pipe(
      Flag.withDefault(DEFAULT_PROFILE),
      Flag.withDescription("One profile per attempt; production-capacity is preparation-only."),
    ),
    productionContextTokens: Flag.integer("production-context-tokens").pipe(
      Flag.withDefault(200_000),
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 32_001, maximum: 1_000_000 }))),
      Flag.withDescription("Planning target only; specify the actual application/model limit."),
    ),
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
      Flag.withSchema(Schema.Finite.check(Schema.isBetween({ minimum: 0.1, maximum: 10 }))),
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
      const phases = yield* Schema.decodeEffect(Schema.Array(ScenarioPhase))(
        options.profile === DEFAULT_PROFILE
          ? makeScenario(options.seed)
          : pressureScenario(options.seed),
      );

      yield* Console.log(
        JSON.stringify({
          scenario: SCENARIO_VERSION,
          updates: phases.length,
          requiredNativeRollovers: REQUIRED_ROLLOVERS,
          recoveryBoundaries: RESTARTS,
          liveEvaluation: false,
          plan: profilePlan(options.profile, options.productionContextTokens),
          ...(options.profile === "production-capacity-v1"
            ? { costPlanning: productionCostPlan(options.productionContextTokens) }
            : {}),
        }),
      );

      return;
    }

    const run = Effect.gen(function* () {
      if (options.profile === "production-capacity-v1")
        return yield* EvaluationError.make({
          stage: "configuration",
          message:
            "Production-capacity coverage is preparation-only. Use --validate with the real production-context-tokens and budget the full workload before enabling inference.",
        });
      if (options.profile === "pressure-cloudflare-v1" && Option.isNone(options.cloudflareUrl))
        return yield* EvaluationError.make({
          stage: "configuration",
          message:
            "The Cloudflare profile requires --cloudflare-url for its already deployed evaluation host.",
        });
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

      const evaluationOptions = {
        model,
        reasoningEffort: options.effort,
        seed: options.seed,
        outputDirectory,
        sourceCommit,
        dirtyWorkingTree,
        maxCostMicrousd: Math.floor(options.maxCostUsd * 1_000_000),
      };

      let report =
        options.profile === "pressure-restart-sqlite-v1"
          ? yield* supervise({ ...evaluationOptions, profile: options.profile })
          : options.profile === "pressure-cloudflare-v1" && Option.isSome(options.cloudflareUrl)
            ? yield* runCloudflareEvaluation(evaluationOptions, options.cloudflareUrl.value).pipe(
                Effect.provide(FetchHttpClient.layer),
              )
            : yield* runEvaluation({ ...evaluationOptions, profile: options.profile }).pipe(
                Effect.provide(provider),
              );

      const finalCommit = (yield* spawner.string(
        ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: root }),
      )).trim();

      const finalStatus = yield* spawner.string(
        ChildProcess.make("git", ["status", "--porcelain", "--untracked-files=normal"], {
          cwd: root,
        }),
      );

      if (finalCommit !== sourceCommit || (options.requireClean && finalStatus.trim().length > 0)) {
        report = { ...report, status: "failed", failure: "Candidate changed during evaluation" };
        yield* fs.writeFileString(
          path.join(outputDirectory, "report.json"),
          yield* Schema.encodeEffect(Schema.fromJsonString(EvaluationReport))(report),
        );
      }

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
