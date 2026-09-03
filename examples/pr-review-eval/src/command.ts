import { ReviewStrategy } from "@effect-agent/pr-review";
import { Config, Console, Effect, FileSystem, Option, Schema, Stream } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { configurationIdentity, freezeEvalSuite, validateFrozenComparison } from "./comparison.ts";
import {
  EvalCaseId,
  EvalConfigurationError,
  EvalDataError,
  EvalSuite,
  EvalTrialCount,
  EvalVariantId,
} from "./contracts.ts";
import { loadEvalSuite, writeObservations } from "./corpus.ts";
import { makeCurrentOpenAiVariant, openAiClientLayer } from "./openai-variant.ts";
import { loadJudgmentSet, loadObservationFiles, writeQualityReport } from "./report-files.ts";
import { makeQualityReport, renderQualityReport, rescoreEvalObservations } from "./report.ts";
import { runEvalSuite, selectEvalCases } from "./runner.ts";

const casesFile = Flag.file("cases").pipe(
  Flag.withDescription("Path to one schema-encoded eval suite JSON file."),
);

const selectedCases = Flag.string("case").pipe(
  Flag.withDescription("Select one case ID. Repeat this flag to select several cases."),
  Flag.atMost(50),
);

const root = Command.make("pr-review-eval").pipe(
  Command.withSharedFlags({ casesFile }),
  Command.withDescription("Validate and run the opt-in PR-review model evaluation bench."),
);

const SelectedCaseIds = Schema.Array(EvalCaseId).check(
  Schema.makeFilter((ids) => new Set(ids).size === ids.length, {
    title: "Selected case IDs are unique",
  }),
);

const decodeSelectedCases = (values: ReadonlyArray<string>) =>
  Schema.decodeUnknownEffect(SelectedCaseIds)(values).pipe(
    Effect.mapError(() =>
      EvalConfigurationError.make({
        message:
          "Case IDs must be unique and use lowercase letters, digits, dots, underscores, and hyphens",
      }),
    ),
  );

const validateCommand = Command.make(
  "validate",
  { selectedCases },
  Effect.fn("PrReviewEval.validateCommand")(function* (options) {
    const shared = yield* root;
    const suite = yield* loadEvalSuite(shared.casesFile);
    const selected = yield* decodeSelectedCases(options.selectedCases);
    const cases = yield* selectEvalCases(suite, selected);

    if (suite.comparison !== undefined) yield* validateFrozenComparison(suite);

    yield* Console.log(`Validated ${cases.length} eval case(s).`);
  }),
).pipe(
  Command.withDescription("Decode cases and verify their input digests without calling a model."),
);

const output = Flag.file("output").pipe(
  Flag.withDescription("New JSONL file to receive one observation per trial."),
);

const trials = Flag.integer("trials").pipe(
  Flag.withDefault(3),
  Flag.withSchema(EvalTrialCount),
  Flag.withDescription("Trials per strategy; frozen comparisons require three."),
);

const concurrency = Flag.integer("concurrency").pipe(
  Flag.withDefault(1),
  Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4 }))),
  Flag.withDescription("Frozen comparisons require serial execution, concurrency 1."),
);

const guidance = Flag.file("guidance").pipe(
  Flag.withDescription("Optional repository guidance file, capped at 20,000 characters."),
  Flag.optional,
);

const variantId = Flag.string("variant").pipe(
  Flag.withDefault("comparison"),
  Flag.withSchema(EvalVariantId),
  Flag.withDescription("Label for the frozen comparison; does not select implementation."),
);

const strategies = Flag.choice("strategy", ["baseline", "verified"]).pipe(
  Flag.atMost(2),
  Flag.withDescription(
    "Select the actual implementation. Frozen paired runs require both; omit to select both.",
  ),
);

const readGuidance = Effect.fn("PrReviewEval.readGuidance")(function* (
  file: Option.Option<string>,
) {
  if (Option.isNone(file)) return undefined;
  const fs = yield* FileSystem.FileSystem;

  const text = yield* fs.readFileString(file.value).pipe(
    Effect.mapError((cause) =>
      EvalDataError.make({
        operation: "read guidance",
        path: file.value,
        message: "Could not read repository guidance",
        cause,
      }),
    ),
  );

  if (text.length > 20_000)
    return yield* EvalConfigurationError.make({
      message: "Repository guidance exceeds the 20,000 character limit",
    });

  return text;
});

const reviewerRevision = Effect.fn("PrReviewEval.reviewerRevision")(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const git = Effect.fn("PrReviewEval.gitIdentity")(
    function* (args: ReadonlyArray<string>, cwd: string) {
      const handle = yield* spawner.spawn(ChildProcess.make("git", args, { cwd }));
      const text = yield* handle.all.pipe(Stream.decodeText(), Stream.mkString);
      const exitCode = yield* handle.exitCode;

      if (exitCode !== 0)
        return yield* EvalConfigurationError.make({
          message: "Git could not verify the reviewer checkout identity",
        });

      return text;
    },
    Effect.scoped,
    Effect.mapError((cause) =>
      EvalDataError.make({
        operation: "read reviewer revision",
        message: "Could not inspect the reviewer checkout",
        cause,
      }),
    ),
  );

  const repository = (yield* git(["rev-parse", "--show-toplevel"], ".")).trim();
  const dirty = yield* git(["status", "--porcelain", "--untracked-files=all"], repository);

  if (dirty.trim() !== "")
    return yield* EvalConfigurationError.make({
      message: "Commit or remove checkout changes before freezing or running a comparison",
    });
  const revision = (yield* git(["rev-parse", "HEAD"], repository)).trim();

  return yield* Schema.decodeUnknownEffect(Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)))(
    revision,
  ).pipe(
    Effect.mapError(() =>
      EvalConfigurationError.make({ message: "Reviewer revision must be an immutable Git commit" }),
    ),
  );
});

const freezeCommand = Command.make(
  "freeze",
  {
    guidance,
    output: output.pipe(
      Flag.withDescription("New JSON file to receive the frozen comparison manifest."),
    ),
    variantId,
  },
  Effect.fn("PrReviewEval.freezeCommand")(function* (options) {
    const shared = yield* root;
    const suite = yield* loadEvalSuite(shared.casesFile);
    const text = yield* readGuidance(options.guidance);
    const revision = yield* reviewerRevision();

    const variants = yield* Effect.forEach(ReviewStrategy.literals, (strategy) =>
      makeCurrentOpenAiVariant({
        id: `${options.variantId}-${strategy}`,
        strategy,
        reviewerRevision: revision,
        ...(text === undefined ? {} : { guidance: text }),
      }),
    );

    const frozen = yield* freezeEvalSuite(
      suite,
      variants.map((variant) => variant.configuration),
      options.variantId,
    );

    const fs = yield* FileSystem.FileSystem;

    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(EvalSuite))(frozen).pipe(
      Effect.mapError((cause) =>
        EvalDataError.make({
          operation: "encode freeze",
          message: "Could not encode the comparison",
          cause,
        }),
      ),
    );

    yield* fs.writeFileString(options.output, `${encoded}\n`, { flag: "wx", mode: 0o600 }).pipe(
      Effect.mapError((cause) =>
        EvalDataError.make({
          operation: "write freeze",
          path: options.output,
          message: "Could not create a new comparison manifest",
          cause,
        }),
      ),
    );
    yield* Console.log(
      `Froze ${frozen.comparison?.plannedRuns} trials at a maximum ${frozen.comparison?.maximumCostMicrousd} microdollars. No model calls made.`,
    );
  }),
).pipe(
  Command.withDescription(
    "Freeze both strategies, source snapshots, oracle, and effective configuration without model calls.",
  ),
);

const runCommand = Command.make(
  "run",
  { concurrency, guidance, output, selectedCases, trials, strategies },
  Effect.fn("PrReviewEval.runCommand")(function* (options) {
    const liveGate = yield* Config.string("EFFECT_AGENT_LIVE").pipe(Config.withDefault(""));

    if (liveGate !== "1") {
      return yield* EvalConfigurationError.make({
        message: "Set EFFECT_AGENT_LIVE=1 to authorize live model evaluation",
      });
    }
    const shared = yield* root;
    const suite = yield* loadEvalSuite(shared.casesFile);
    const comparison = yield* validateFrozenComparison(suite);
    const caseIds = yield* decodeSelectedCases(options.selectedCases);

    yield* selectEvalCases(suite, caseIds);
    if (
      options.strategies.length > 0 &&
      [...options.strategies].sort().join(",") !== "baseline,verified"
    )
      return yield* EvalConfigurationError.make({
        message: "Frozen paired runs require both baseline and verified strategies",
      });
    const guidanceText = yield* readGuidance(options.guidance);
    const revision = yield* reviewerRevision();

    const variants = yield* Effect.forEach(comparison.configurations, (configuration) =>
      makeCurrentOpenAiVariant({
        id: configuration.id,
        strategy: configuration.strategy ?? "baseline",
        reviewerRevision: revision,
        ...(guidanceText === undefined ? {} : { guidance: guidanceText }),
      }),
    );

    if (
      variants.some(
        (variant) =>
          !comparison.configurations.some(
            (configuration) =>
              configurationIdentity(variant.configuration) === configurationIdentity(configuration),
          ),
      )
    )
      return yield* EvalConfigurationError.make({
        message:
          "Effective reviewer configuration differs from the freeze; create a new comparison",
      });

    const count = yield* writeObservations(
      options.output,
      runEvalSuite(suite, variants, {
        trials: options.trials,
        concurrency: options.concurrency,
        caseIds,
      }),
    ).pipe(Effect.provide(openAiClientLayer));

    yield* Console.log(`Wrote ${count} eval observation(s) to ${options.output}.`);
  }),
).pipe(
  Command.withDescription(
    "Run a frozen paired comparison in balanced order and retain every completed trial.",
  ),
  Command.withExamples([
    {
      command: "pr-review-eval --cases ./data/frozen.json run --output ./results/comparison.jsonl",
      description: "Run both frozen strategies, three trials each, without warmups or reruns.",
    },
  ]),
);

const observationFiles = Flag.file("observations").pipe(
  Flag.withDescription("JSONL observations; repeat once per baseline or candidate."),
  Flag.between(1, 8),
);

const judgmentsFile = Flag.file("judgments").pipe(
  Flag.withDescription("Optional schema-encoded judgment set with named adjudicators."),
  Flag.optional,
);

const rescoreCommand = Command.make(
  "rescore",
  {
    previousCases: Flag.file("previous-cases").pipe(
      Flag.withDescription("Original frozen suite used by the complete paired observations."),
    ),
    observationFiles,
    output,
    frozenOutput: Flag.file("frozen-output").pipe(
      Flag.withDescription("New corrected frozen suite JSON file."),
    ),
    correctionId: Flag.string("correction-id").pipe(
      Flag.withSchema(EvalVariantId),
      Flag.withDescription("New identity for this versioned oracle correction."),
    ),
  },
  Effect.fn("PrReviewEval.rescoreCommand")(function* (options) {
    const shared = yield* root;
    const previous = yield* loadEvalSuite(options.previousCases);
    const corrected = yield* loadEvalSuite(shared.casesFile);
    const observations = yield* loadObservationFiles(options.observationFiles);

    const rebound = yield* rescoreEvalObservations(
      previous,
      corrected,
      observations,
      options.correctionId,
    );

    const fs = yield* FileSystem.FileSystem;

    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(EvalSuite))(
      rebound.suite,
    ).pipe(
      Effect.mapError((cause) =>
        EvalDataError.make({
          operation: "encode correction",
          message: "Could not encode corrected freeze",
          cause,
        }),
      ),
    );

    yield* fs
      .writeFileString(options.frozenOutput, `${encoded}\n`, { flag: "wx", mode: 0o600 })
      .pipe(
        Effect.mapError((cause) =>
          EvalDataError.make({
            operation: "write correction",
            path: options.frozenOutput,
            message: "Could not create corrected freeze",
            cause,
          }),
        ),
      );
    yield* writeObservations(options.output, Stream.fromIterable(rebound.observations));
    yield* Console.log(
      `Rebound ${rebound.observations.length} original observations to the corrected oracle. Re-adjudicate candidate judgments against the new digests. No model calls made.`,
    );
  }),
).pipe(
  Command.withDescription(
    "Rescore both strategies under a versioned oracle correction, retaining original observation digests.",
  ),
);

const reportOutput = Flag.file("output").pipe(
  Flag.withDescription("New JSON file to receive the deterministic quality report."),
);

const reportTrials = Flag.integer("trials").pipe(
  Flag.withSchema(EvalTrialCount),
  Flag.withDescription("Expected trials per case and variant; must match the original run."),
);

const reportCommand = Command.make(
  "report",
  { judgmentsFile, observationFiles, reportOutput, reportTrials, selectedCases },
  Effect.fn("PrReviewEval.reportCommand")(function* (options) {
    const shared = yield* root;
    const suite = yield* loadEvalSuite(shared.casesFile);
    const caseIds = yield* decodeSelectedCases(options.selectedCases);

    const selectedSuite = EvalSuite.make({
      ...suite,
      cases: yield* selectEvalCases(suite, caseIds),
    });

    const observations = yield* loadObservationFiles(options.observationFiles);

    const judgments = yield* Option.match(options.judgmentsFile, {
      onNone: () => Effect.succeed(undefined),
      onSome: loadJudgmentSet,
    });

    const report = yield* makeQualityReport(
      selectedSuite,
      observations,
      options.reportTrials,
      judgments,
    );

    yield* writeQualityReport(options.reportOutput, report);
    yield* Console.log(renderQualityReport(report));
    if (judgments === undefined) {
      yield* Console.log(
        `No judgment file was supplied; ${report.unjudgedFindings.length} finding(s) remain explicit in the report.`,
      );
    }
  }),
).pipe(
  Command.withDescription("Reduce saved observations and human judgments without model calls."),
);

export const command = root.pipe(
  Command.withSubcommands([
    validateCommand,
    freezeCommand,
    runCommand,
    rescoreCommand,
    reportCommand,
  ]),
);
