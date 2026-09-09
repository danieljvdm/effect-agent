import { Schema } from "effect";

export const fixtureVersion = "tool-discovery-v1";
export const modes = ["eager", "native", "code-mode"] as const;
export const workloads = ["common", "uncommon", "composed"] as const;
export const Mode = Schema.Literals(modes);
export const Workload = Schema.Literals(workloads);
export type Mode = typeof Mode.Type;
export type Workload = typeof Workload.Type;
export const seeds = [17, 29, 43, 61, 79] as const;

export const thresholds = {
  improvementFraction: 0.15,
  improvementMillis: 500,
  commonRegressionFraction: 0.1,
  commonRegressionMillis: 500,
  cohorts: 2,
  blocksPerCohort: 5,
} as const;

export const settings = {
  reasoning: { effort: "low" },
  service_tier: "default",
  max_output_tokens: 2_048,
  store: false,
  strictJsonSchema: true,
  truncation: "disabled",
} as const;

const Nonnegative = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));

export const Prices = Schema.Struct({
  input: Nonnegative,
  cachedInput: Nonnegative,
  output: Nonnegative,
});

export const Output = Schema.Struct({
  amountCents: Schema.Int,
  receipts: Schema.Array(Schema.String),
});

export const Request = Schema.Struct({
  mode: Mode,
  workload: Workload,
  seed: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1_000_000 })),
  live: Schema.Boolean,
  model: Schema.NonEmptyString,
  prices: Prices,
  remainingMicrousd: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 10_000_000 })),
});

export type Request = typeof Request.Type;

export const Audit = Schema.Struct({
  ordinal: Schema.Natural,
  toolNames: Schema.Array(Schema.String),
  toolBytes: Schema.Natural,
  requestBytes: Schema.Natural,
  reservedInputTokens: Schema.Natural,
  inputTokens: Schema.NullOr(Schema.Natural),
  cachedInputTokens: Schema.NullOr(Schema.Natural),
  outputTokens: Schema.NullOr(Schema.Natural),
  reasoningTokens: Schema.NullOr(Schema.Natural),
  returnedModel: Schema.NullOr(Schema.String),
  returnedTier: Schema.NullOr(Schema.String),
  completed: Schema.Boolean,
});

export type Audit = typeof Audit.Type;

export const Result = Schema.Struct({
  passed: Schema.Boolean,
  failure: Schema.NullOr(Schema.String),
  output: Schema.NullOr(Output),
  firstUsefulActionMillis: Schema.NullOr(Nonnegative),
  firstUsefulResultMillis: Schema.NullOr(Nonnegative),
  modelCalls: Schema.Natural,
  modelFinalizers: Schema.Natural,
  businessCalls: Schema.Array(Schema.String),
  discoveryCalls: Schema.Natural,
  codeCalls: Schema.Natural,
  audits: Schema.Array(Audit),
  costMicrousd: Schema.Natural,
  pendingMicrousd: Schema.Natural,
  scopeClosed: Schema.Boolean,
});

export type Result = typeof Result.Type;

export const Sample = Schema.Struct({
  cohort: Schema.Int,
  block: Schema.Int,
  mode: Mode,
  workload: Workload,
  seed: Schema.Int,
  taskMillis: Nonnegative,
  result: Result,
});

export type Sample = typeof Sample.Type;

export class BenchmarkError extends Schema.TaggedError<BenchmarkError>()("ToolBenchmarkError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const median = (values: ReadonlyArray<number>): number => {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);

  return sorted.length === 0
    ? 0
    : sorted.length % 2 === 0
      ? ((sorted[middle - 1] ?? 0) + (sorted[middle] ?? 0)) / 2
      : (sorted[middle] ?? 0);
};

/** Every expected cell must succeed once. Missing, duplicated, or failed samples never disappear. */
export const grade = (samples: ReadonlyArray<Sample>) => {
  const expected = new Set(
    Array.from({ length: 2 }, (_, cohort) =>
      seeds.flatMap((_, block) =>
        modes.flatMap((mode) =>
          workloads.map((workload) => `${cohort}:${block}:${mode}:${workload}`),
        ),
      ),
    ).flat(),
  );

  const complete =
    samples.length === expected.size &&
    samples.every(
      (sample) =>
        expected.delete(`${sample.cohort}:${sample.block}:${sample.mode}:${sample.workload}`) &&
        sample.seed === seeds[sample.block] &&
        sample.result.passed &&
        sample.result.scopeClosed &&
        sample.result.pendingMicrousd === 0,
    );

  const comparisons = [0, 1].flatMap((cohort) =>
    ["native", "code-mode"].map((candidate) => {
      const times = (mode: string, workload?: Workload) =>
        samples.filter(
          (s) =>
            s.cohort === cohort &&
            s.mode === mode &&
            (workload === undefined || s.workload === workload),
        );

      const baseline = median(times("eager").map((s) => s.taskMillis));
      const observed = median(times(candidate).map((s) => s.taskMillis));
      const commonBaseline = median(times("eager", "common").map((s) => s.taskMillis));
      const commonObserved = median(times(candidate, "common").map((s) => s.taskMillis));
      const commonRegression = commonObserved - commonBaseline;
      const commonPassed = !(commonRegression > 500 && commonObserved > commonBaseline * 1.1);

      return {
        cohort,
        candidate,
        baselineMedianMillis: baseline,
        candidateMedianMillis: observed,
        improvementMillis: baseline - observed,
        commonRegressionMillis: commonRegression,
        passed:
          complete && baseline - observed >= 500 && observed <= baseline * 0.85 && commonPassed,
        workflows: workloads.map((workload) => ({
          workload,
          baselineMedianMillis: median(times("eager", workload).map((s) => s.taskMillis)),
          candidateMedianMillis: median(times(candidate, workload).map((s) => s.taskMillis)),
        })),
      };
    }),
  );

  return {
    complete,
    nativePassed:
      complete && comparisons.filter((c) => c.candidate === "native").every((c) => c.passed),
    codeModePassed:
      complete && comparisons.filter((c) => c.candidate === "code-mode").every((c) => c.passed),
    comparisons,
  };
};
