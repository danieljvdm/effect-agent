import { Effect, Schema } from "effect";

export const FIXTURE_VERSION = "runtime-v1";

export const REFERENCE = {
  version: "audit-beta67-node24-v1",
  revision: "596cffba70716b1211ac02de949c4d0f31734b2f",
} as const;

export class BenchmarkError extends Schema.TaggedError<BenchmarkError>()("BenchmarkError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const check = Effect.fn("benchmark.check")(function* (condition: boolean, message: string) {
  if (!condition) return yield* BenchmarkError.make({ message });
});

export const Profile = Schema.Literals(["smoke", "pr", "extended", "archive"]);
export type Profile = typeof Profile.Type;

export const Case = Schema.Struct({
  name: Schema.String,
  kind: Schema.Literals(["run", "stream", "tools", "durable", "recovery", "ledger"]),
  chunks: Schema.Natural,
  outputBytes: Schema.Natural,
  historyBytes: Schema.Natural,
  records: Schema.Natural,
  rounds: Schema.Natural,
});

export type Case = typeof Case.Type;

const workload = (
  name: string,
  kind: Case["kind"],
  options: Partial<Omit<Case, "name" | "kind">> = {},
): Case => ({
  name,
  kind,
  chunks: 1,
  outputBytes: 0,
  historyBytes: 0,
  records: 0,
  rounds: 0,
  ...options,
});

/** Profile sizes are fixed fixture inputs, never derived from the measured revision. */
export const casesFor = (profile: Profile): ReadonlyArray<Case> => [
  workload("small-run", "run"),
  workload("small-stream", "stream"),
  ...[1, 64, 1_024, 4_096].map((chunks) =>
    workload(`stream-64k-${chunks}`, "stream", { chunks, outputBytes: 65_536 }),
  ),
  ...[65_536, 1_048_576].map((historyBytes) =>
    workload(`history-${historyBytes}`, "run", { historyBytes }),
  ),
  workload("parallel-tools-8", "tools", { rounds: 1 }),
  workload("tool-rounds-4", "tools", { rounds: 4 }),
  ...[
    0,
    ...(profile === "smoke" ? [16] : [256, 2_048]),
    ...(profile === "extended" || profile === "archive" ? [8_192] : []),
    ...(profile === "archive" ? [100_000] : []),
  ].flatMap((records) => [
    workload(`durable-fresh-${records}`, "durable", { records }),
    workload(`checkpoint-recovery-${records}`, "recovery", { records }),
    workload(`settled-ledger-${records}`, "ledger", { records }),
  ]),
];

export const WorkerOptions = Schema.Struct({
  cold: Schema.Boolean,
  profile: Profile,
  warmups: Schema.Natural.check(Schema.isLessThanOrEqualTo(20)),
  samples: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
  output: Schema.String,
});

export const Sample = Schema.Struct({
  case: Schema.String,
  ordinal: Schema.Natural,
  warmup: Schema.Boolean,
  totalMs: Schema.Finite,
  modelEntryMs: Schema.NullOr(Schema.Finite),
  checkpointCreationMs: Schema.NullOr(Schema.Finite),
  retainedPromptMessages: Schema.Natural,
  modelCalls: Schema.Natural,
  finalizers: Schema.Natural,
  toolCalls: Schema.Natural,
  outputBytes: Schema.Natural,
  status: Schema.Literals(["passed", "failed"]),
  failure: Schema.NullOr(Schema.String),
});

export type Sample = typeof Sample.Type;

export const WorkerReport = Schema.Struct({
  fixture: Schema.Literal(FIXTURE_VERSION),
  profile: Profile,
  runtime: Schema.String,
  platform: Schema.String,
  architecture: Schema.String,
  samples: Schema.Array(Sample),
});

export type WorkerReport = typeof WorkerReport.Type;

export const completeBatch = (
  report: WorkerReport,
  options: typeof WorkerOptions.Type,
): boolean => {
  const workloads = options.cold
    ? casesFor(options.profile).slice(0, 1)
    : casesFor(options.profile);

  const expected = new Set(
    workloads.flatMap((workload) =>
      Array.from(
        { length: options.warmups + options.samples },
        (_, ordinal) => `${workload.name}:${ordinal}`,
      ),
    ),
  );

  if (report.profile !== options.profile || report.samples.length !== expected.size) return false;

  return report.samples.every(
    (sample) =>
      expected.delete(`${sample.case}:${sample.ordinal}`) &&
      sample.warmup === sample.ordinal < options.warmups &&
      sample.status === "passed" &&
      sample.totalMs >= 0 &&
      sample.modelEntryMs !== null &&
      sample.modelEntryMs >= 0 &&
      sample.modelEntryMs <= sample.totalMs &&
      sample.modelCalls === sample.finalizers,
  );
};

export const summary = (values: ReadonlyArray<number>) => {
  const sorted = [...values].sort((a, b) => a - b);

  const quantile = (p: number) => {
    const index = (sorted.length - 1) * p;
    const lower = sorted[Math.floor(index)] ?? 0;

    return lower + ((sorted[Math.ceil(index)] ?? lower) - lower) * (index % 1);
  };

  return {
    count: sorted.length,
    median: quantile(0.5),
    q1: quantile(0.25),
    q3: quantile(0.75),
    min: sorted[0] ?? 0,
    max: sorted.at(-1) ?? 0,
  };
};
