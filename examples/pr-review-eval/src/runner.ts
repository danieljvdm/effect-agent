import { type ReviewOutcome, type ReviewRequest } from "@effect-agent/pr-review";
import { Clock, DateTime, Effect, Result, Schema } from "effect";

import {
  CURRENT_RUNNER_VERSION,
  type EvalCase,
  EvalCaseId,
  EvalConfigurationError,
  EvalObservation,
  type EvalReviewerFailure,
  type EvalSuite,
  EvalTrialFailed,
  EvalTrialSucceeded,
  type EvalVariantConfiguration,
} from "./contracts.ts";

const RunnerOptions = Schema.Struct({
  trials: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 })),
  concurrency: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4 })),
  caseIds: Schema.Array(EvalCaseId).check(Schema.isMaxLength(50)),
});

export interface EvalVariant<Requirements> {
  readonly configuration: EvalVariantConfiguration;
  readonly review: (
    request: ReviewRequest,
  ) => Effect.Effect<ReviewOutcome, EvalReviewerFailure, Requirements>;
}

export interface EvalRunnerOptions {
  readonly trials: number;
  readonly concurrency: number;
  readonly caseIds: ReadonlyArray<EvalCaseId>;
}

interface EvalJob<Requirements> {
  readonly evalCase: EvalCase;
  readonly variant: EvalVariant<Requirements>;
  readonly trial: number;
}

const decodeRunnerOptions = Schema.decodeUnknownEffect(RunnerOptions);

const selectCases = (
  suite: EvalSuite,
  caseIds: ReadonlyArray<EvalCaseId>,
): Effect.Effect<ReadonlyArray<EvalCase>, EvalConfigurationError> => {
  if (new Set(caseIds).size !== caseIds.length) {
    return Effect.fail(
      EvalConfigurationError.make({ message: "Each selected case ID may appear only once" }),
    );
  }
  if (caseIds.length === 0) return Effect.succeed(suite.cases);
  const selected = new Set(caseIds);
  const unknown = caseIds.filter((id) => !suite.cases.some((evalCase) => evalCase.id === id));
  if (unknown.length > 0) {
    return Effect.fail(
      EvalConfigurationError.make({
        message: `Unknown eval case ID(s): ${unknown.join(", ")}`,
      }),
    );
  }
  return Effect.succeed(suite.cases.filter((evalCase) => selected.has(evalCase.id)));
};

const elapsedMillis = (startedAt: bigint, finishedAt: bigint): number =>
  Number((finishedAt - startedAt + 999_999n) / 1_000_000n);

const runJob = Effect.fn("PrReviewEval.runJob")(function* <Requirements>(
  job: EvalJob<Requirements>,
) {
  const clock = yield* Clock.Clock;
  const recordedAt = yield* DateTime.now;
  const startedAt = yield* clock.monotonicTimeNanos;
  const result = yield* Effect.result(job.variant.review(job.evalCase.request));
  const finishedAt = yield* clock.monotonicTimeNanos;
  return EvalObservation.make({
    version: 1,
    runnerVersion: CURRENT_RUNNER_VERSION,
    caseId: job.evalCase.id,
    caseVersion: job.evalCase.version,
    inputDigest: job.evalCase.inputDigest,
    variant: job.variant.configuration,
    trial: job.trial,
    recordedAt,
    elapsedMillis: elapsedMillis(startedAt, finishedAt),
    result: Result.isSuccess(result)
      ? EvalTrialSucceeded.make({ outcome: result.success })
      : EvalTrialFailed.make({
          errorTag: result.failure.errorTag,
          message: result.failure.message,
        }),
  });
});

export const runEvalSuite = Effect.fn("PrReviewEval.runEvalSuite")(function* <Requirements>(
  suite: EvalSuite,
  variants: ReadonlyArray<EvalVariant<Requirements>>,
  options: EvalRunnerOptions,
) {
  const decodedOptions = yield* decodeRunnerOptions(options).pipe(
    Effect.mapError(() =>
      EvalConfigurationError.make({
        message: "Trials must be 1-20, concurrency 1-4, and at most 50 cases may be selected",
      }),
    ),
  );
  if (variants.length === 0 || variants.length > 8) {
    return yield* EvalConfigurationError.make({
      message: "Select between one and eight eval variants",
    });
  }
  const variantIds = variants.map((variant) => variant.configuration.id);
  if (new Set(variantIds).size !== variantIds.length) {
    return yield* EvalConfigurationError.make({ message: "Eval variant IDs must be unique" });
  }
  const selectedCases = yield* selectCases(suite, decodedOptions.caseIds);
  const jobs: Array<EvalJob<Requirements>> = [];
  for (const evalCase of selectedCases) {
    for (const variant of variants) {
      for (let trial = 1; trial <= decodedOptions.trials; trial += 1) {
        jobs.push({ evalCase, variant, trial });
      }
    }
  }
  return yield* Effect.forEach(jobs, runJob, { concurrency: decodedOptions.concurrency });
});
