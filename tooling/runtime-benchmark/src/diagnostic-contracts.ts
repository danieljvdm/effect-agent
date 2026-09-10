import type { Effect } from "effect";
import { Context, Schema } from "effect";

import type { BenchmarkError } from "./contracts.js";

export const DIAGNOSTIC_VERSION = "runtime-diagnostic-v1";
export const MAX_DIAGNOSTIC_MARKS = 512;
export const DIAGNOSTIC_SIZES = { cohorts: 2, warmups: 2, samples: 5 } as const;

const Name = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(96));
const Millis = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const Failure = Schema.NullOr(Schema.String.check(Schema.isMaxLength(8_192)));

export const DiagnosticCase = Schema.Struct({
  name: Name,
  family: Schema.Literals(["policy", "history", "memory", "mcp", "subagent", "ledger", "fairness"]),
  parameters: Schema.Record(Name, Schema.Finite).check(Schema.isMaxProperties(16)),
});

export type DiagnosticCase = typeof DiagnosticCase.Type;

export const DiagnosticResult = Schema.Struct({
  totalMs: Millis,
  metrics: Schema.Array(Schema.Struct({ name: Name, value: Millis })).check(Schema.isMaxLength(64)),
  counters: Schema.Array(Schema.Struct({ name: Name, value: Schema.Natural })).check(
    Schema.isMaxLength(64),
  ),
});

export type DiagnosticResult = typeof DiagnosticResult.Type;

export const DiagnosticPhase = Schema.Literals(["setup", "operation", "verification"]);
export type DiagnosticPhase = typeof DiagnosticPhase.Type;
export const DiagnosticMark = Schema.Struct({ name: Name, elapsedMs: Millis });
export type DiagnosticMark = typeof DiagnosticMark.Type;

export const DiagnosticMarks = Schema.Array(DiagnosticMark).check(
  Schema.isMaxLength(MAX_DIAGNOSTIC_MARKS),
);

/** Phase persistence is outside operation clocks; bounded marks are in-memory and inclusive. */
export class DiagnosticProgress extends Context.Service<
  DiagnosticProgress,
  {
    readonly phase: (phase: DiagnosticPhase) => Effect.Effect<void, BenchmarkError>;
    readonly mark: (mark: DiagnosticMark) => Effect.Effect<void, BenchmarkError>;
  }
>()("runtime-benchmark/DiagnosticProgress") {}

export const DiagnosticWorkerOptions = Schema.Struct({
  output: Schema.String,
  warmups: Schema.Natural.check(Schema.isLessThanOrEqualTo(2)),
  samples: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 5 })),
  timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 120_000 })),
});

export type DiagnosticWorkerOptions = typeof DiagnosticWorkerOptions.Type;

export const DiagnosticActive = Schema.Struct({
  case: Name,
  ordinal: Schema.Natural,
  warmup: Schema.Boolean,
  phase: DiagnosticPhase,
  elapsedMs: Millis,
  marks: DiagnosticMarks,
});

export type DiagnosticActive = typeof DiagnosticActive.Type;

export const DiagnosticSample = Schema.Struct({
  case: Name,
  ordinal: Schema.Natural,
  warmup: Schema.Boolean,
  attemptMs: Millis,
  phase: DiagnosticPhase,
  result: Schema.NullOr(DiagnosticResult),
  marks: DiagnosticMarks,
  status: Schema.Literals(["passed", "failed"]),
  failure: Failure,
});

export type DiagnosticSample = typeof DiagnosticSample.Type;

export const DiagnosticWorkerReport = Schema.Struct({
  fixture: Schema.Literal(DIAGNOSTIC_VERSION),
  runtime: Schema.String,
  platform: Schema.String,
  architecture: Schema.String,
  active: Schema.NullOr(DiagnosticActive),
  failure: Failure,
  samples: Schema.Array(DiagnosticSample).check(Schema.isMaxLength(448)),
});

export type DiagnosticWorkerReport = typeof DiagnosticWorkerReport.Type;

/** Failed, duplicate, absent, or mismatched samples cannot become comparative evidence. */
export const completeDiagnosticBatch = (
  report: DiagnosticWorkerReport,
  options: DiagnosticWorkerOptions,
  workloads: ReadonlyArray<DiagnosticCase>,
): boolean => {
  const expected = new Set(
    workloads.flatMap((workload) =>
      Array.from(
        { length: options.warmups + options.samples },
        (_, ordinal) => `${workload.name}:${ordinal}`,
      ),
    ),
  );

  if (report.active !== null || report.failure !== null || report.samples.length !== expected.size)
    return false;

  return report.samples.every(
    (sample) =>
      expected.delete(`${sample.case}:${sample.ordinal}`) &&
      sample.warmup === sample.ordinal < options.warmups &&
      sample.status === "passed" &&
      sample.failure === null &&
      sample.phase === "verification" &&
      sample.result !== null &&
      sample.attemptMs >= sample.result.totalMs &&
      new Set(sample.result.metrics.map(({ name }) => name)).size ===
        sample.result.metrics.length &&
      new Set(sample.result.counters.map(({ name }) => name)).size ===
        sample.result.counters.length,
  );
};
