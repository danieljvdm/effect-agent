import { Config, Console, Effect, FileSystem, Option, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";

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
import { makeQualityReport, renderQualityReport } from "./report.ts";
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
    yield* Console.log(`Validated ${cases.length} eval case(s).`);
  }),
).pipe(
  Command.withDescription("Decode cases and verify their input digests without calling a model."),
);

const output = Flag.file("output").pipe(
  Flag.withDescription("New JSONL file to receive one observation per trial."),
);
const trials = Flag.integer("trials").pipe(
  Flag.withDefault(2),
  Flag.withSchema(EvalTrialCount),
  Flag.withDescription("Independent trials per case and variant, between 1 and 20."),
);
const concurrency = Flag.integer("concurrency").pipe(
  Flag.withDefault(1),
  Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4 }))),
  Flag.withDescription("Maximum concurrent reviewer invocations, between 1 and 4."),
);
const model = Flag.string("model").pipe(
  Flag.withDefault("gpt-5.6-sol"),
  Flag.withSchema(Schema.NonEmptyString.check(Schema.isMaxLength(200))),
  Flag.withDescription("OpenAI model ID. Defaults to the current PR-review Action model."),
);
const effort = Flag.choice("effort", ["low", "medium", "high", "xhigh"]).pipe(
  Flag.withDefault("xhigh"),
  Flag.withDescription("Reasoning effort. Defaults to the current PR-review Action effort."),
);
const guidance = Flag.file("guidance").pipe(
  Flag.withDescription("Optional repository guidance file, capped at 20,000 characters."),
  Flag.optional,
);
const variantId = Flag.string("variant").pipe(
  Flag.withDefault("current"),
  Flag.withSchema(EvalVariantId),
  Flag.withDescription("Stable ID for this baseline or candidate configuration."),
);

const runCommand = Command.make(
  "run",
  { concurrency, effort, guidance, model, output, selectedCases, trials, variantId },
  Effect.fn("PrReviewEval.runCommand")(function* (options) {
    const liveGate = yield* Config.string("EFFECT_AGENT_LIVE").pipe(Config.withDefault(""));
    if (liveGate !== "1") {
      return yield* EvalConfigurationError.make({
        message: "Set EFFECT_AGENT_LIVE=1 to authorize live model evaluation",
      });
    }
    const shared = yield* root;
    const suite = yield* loadEvalSuite(shared.casesFile);
    const caseIds = yield* decodeSelectedCases(options.selectedCases);
    yield* selectEvalCases(suite, caseIds);
    const fs = yield* FileSystem.FileSystem;
    const guidanceText = yield* Option.match(options.guidance, {
      onNone: () => Effect.succeed(undefined),
      onSome: (path) =>
        Effect.gen(function* () {
          const text = yield* fs.readFileString(path).pipe(
            Effect.mapError((cause) =>
              EvalDataError.make({
                operation: "read guidance",
                path,
                message: `Could not read repository guidance at ${path}`,
                cause,
              }),
            ),
          );
          if (text.length > 20_000) {
            return yield* EvalConfigurationError.make({
              message: "Repository guidance exceeds the 20,000 character limit",
            });
          }
          return text;
        }),
    });
    const variant = yield* makeCurrentOpenAiVariant({
      id: options.variantId,
      model: options.model,
      reasoningEffort: options.effort,
      ...(guidanceText === undefined ? {} : { guidance: guidanceText }),
    });
    const count = yield* writeObservations(
      options.output,
      runEvalSuite(suite, [variant], {
        trials: options.trials,
        concurrency: options.concurrency,
        caseIds,
      }),
    ).pipe(Effect.provide(openAiClientLayer));
    yield* Console.log(`Wrote ${count} eval observation(s) to ${options.output}.`);
  }),
).pipe(
  Command.withDescription("Run the current reviewer against selected cases and write JSONL."),
  Command.withExamples([
    {
      command:
        "pr-review-eval --cases ./data/cases.json run --variant current --output ./results/current.jsonl --trials 5",
      description: "Run five independent trials for every case with the production defaults.",
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
  Command.withSubcommands([validateCommand, runCommand, reportCommand]),
);
