import {
  ReviewDiagnostics,
  ReviewDiagnosticsSink,
  type ReviewOutcome,
  type ReviewRequest,
} from "@effect-agent/pr-review";
import {
  Cause,
  Clock,
  Console,
  Crypto,
  DateTime,
  Effect,
  Exit,
  Layer,
  Ref,
  Result,
  Schema,
  Stream,
} from "effect";

import {
  configurationIdentity,
  validateFrozenComparison,
  validateFrozenRun,
} from "./comparison.ts";
import {
  CURRENT_RUNNER_VERSION,
  type EvalCase,
  EvalCaseId,
  EvalConfigurationError,
  EvalDataError,
  type EvalInputDigest,
  EvalObservation,
  type EvalReviewerFailure,
  type EvalSuite,
  EvalTrialCount,
  EvalTrialFailed,
  EvalTrialSucceeded,
  type EvalVariantConfiguration,
} from "./contracts.ts";
import { digestEvalOracle, digestText, validateEvalSuite } from "./corpus.ts";
import { repositoryLayer } from "./repository.ts";

const RunnerOptions = Schema.Struct({
  trials: EvalTrialCount,
  concurrency: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4 })),
  caseIds: Schema.Array(EvalCaseId).check(Schema.isMaxLength(50)),
});

export interface EvalVariant<Requirements> {
  readonly configuration: EvalVariantConfiguration;
  readonly review: (
    request: ReviewRequest,
    trial?: EvalTrialContext,
  ) => Effect.Effect<ReviewOutcome, EvalReviewerFailure, Requirements>;
}

export interface EvalTrialContext {
  readonly runId: string;
  readonly cacheNamespace: EvalInputDigest;
}

const FinalizationDiagnostic = Schema.Struct({
  type: Schema.Literal("eval-trial-finalization"),
  caseId: EvalCaseId,
  variantId: Schema.String,
  trial: Schema.Natural,
  runId: Schema.String,
  cacheNamespace: Schema.String,
  stop: Schema.Literals(["defect", "interrupted"]),
  diagnostics: ReviewDiagnostics,
});

export interface EvalRunnerOptions {
  readonly trials: number;
  readonly concurrency: number;
  readonly caseIds: ReadonlyArray<EvalCaseId>;
}

interface EvalJob<Requirements> {
  readonly evalCase: EvalCase;
  readonly variant: EvalVariant<Requirements>;
  readonly trial: number;
  readonly sequence: number;
  readonly runId: string;
  readonly comparisonDigest?: EvalInputDigest;
  readonly frozenRunDigest?: EvalInputDigest;
}

const decodeRunnerOptions = Schema.decodeUnknownEffect(RunnerOptions);

export const selectEvalCases = (
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
  const oracleDigest = yield* digestEvalOracle(job.evalCase);

  const cacheNamespace = yield* digestText(
    `${job.runId}:${job.evalCase.id}:${job.variant.configuration.strategy ?? job.variant.configuration.id}:${job.trial}`,
  );

  const latestDiagnostics = yield* Ref.make<ReviewDiagnostics | undefined>(undefined);

  const result = yield* Effect.result(
    job.variant
      .review(job.evalCase.request, {
        runId: job.runId,
        cacheNamespace,
      })
      .pipe(
        Effect.provide([
          repositoryLayer(job.evalCase.repository),
          Layer.succeed(ReviewDiagnosticsSink, {
            record: (value) => Ref.set(latestDiagnostics, value),
          }),
        ]),
        Effect.onExit((exit) =>
          Effect.gen(function* () {
            if (
              Exit.isSuccess(exit) ||
              (!Cause.hasDies(exit.cause) && !Cause.hasInterrupts(exit.cause))
            )
              return;
            const diagnostics = yield* Ref.get(latestDiagnostics);

            if (diagnostics === undefined) return;
            // Preserve bounded host data on stderr without swallowing a defect or interruption.
            yield* Console.error(
              Schema.encodeSync(Schema.fromJsonString(FinalizationDiagnostic))({
                type: "eval-trial-finalization",
                caseId: job.evalCase.id,
                variantId: job.variant.configuration.id,
                trial: job.trial,
                runId: job.runId,
                cacheNamespace,
                stop: Cause.hasInterrupts(exit.cause) ? "interrupted" : "defect",
                diagnostics,
              }),
            );
          }),
        ),
      ),
  );

  const finishedAt = yield* clock.monotonicTimeNanos;

  const diagnostics = Result.isFailure(result)
    ? (result.failure.diagnostics ?? (yield* Ref.get(latestDiagnostics)))
    : undefined;

  return EvalObservation.make({
    version: 1,
    runnerVersion: CURRENT_RUNNER_VERSION,
    caseId: job.evalCase.id,
    caseVersion: job.evalCase.version,
    inputDigest: job.evalCase.inputDigest,
    oracleVersion: job.evalCase.oracleVersion ?? 1,
    oracleDigest,
    runId: job.runId,
    cacheNamespace,
    sequence: job.sequence,
    ...(job.comparisonDigest === undefined ? {} : { comparisonDigest: job.comparisonDigest }),
    ...(job.frozenRunDigest === undefined ? {} : { frozenRunDigest: job.frozenRunDigest }),
    ...(job.evalCase.repository === undefined
      ? {}
      : { repositoryDigest: job.evalCase.repository.digest }),
    variant: job.variant.configuration,
    trial: job.trial,
    recordedAt,
    elapsedMillis: elapsedMillis(startedAt, finishedAt),
    result: Result.isSuccess(result)
      ? EvalTrialSucceeded.make({ outcome: result.success })
      : EvalTrialFailed.make({
          errorTag: result.failure.errorTag,
          message: result.failure.message,
          ...(diagnostics === undefined ? {} : { diagnostics }),
          ...(result.failure.estimatedCostMicrousd === undefined
            ? {}
            : { estimatedCostMicrousd: result.failure.estimatedCostMicrousd }),
          ...(result.failure.reservedCostMicrousd === undefined
            ? {}
            : { reservedCostMicrousd: result.failure.reservedCostMicrousd }),
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

  const cacheSelectors = variants.map(
    ({ configuration }) => configuration.strategy ?? configuration.id,
  );

  if (new Set(cacheSelectors).size !== cacheSelectors.length) {
    return yield* EvalConfigurationError.make({
      message:
        "Eval variants share a cache namespace selector; run these configurations separately",
    });
  }
  const selectedCases = yield* selectEvalCases(suite, decodedOptions.caseIds);

  yield* validateEvalSuite(suite);

  const comparison =
    suite.comparison === undefined ? undefined : yield* validateFrozenComparison(suite);

  const frozenRun = suite.frozenRun === undefined ? undefined : yield* validateFrozenRun(suite);

  if (
    frozenRun !== undefined &&
    (decodedOptions.trials !== frozenRun.trials ||
      decodedOptions.concurrency !== 1 ||
      selectedCases.length !== suite.cases.length ||
      variants.length !== 1 ||
      variants.some(
        (variant) =>
          configurationIdentity(variant.configuration) !==
          configurationIdentity(frozenRun.configuration),
      ))
  ) {
    return yield* EvalConfigurationError.make({
      message:
        "Frozen baseline runs require the exact configuration, all cases and trials, and serial execution",
    });
  }

  if (
    comparison !== undefined &&
    (decodedOptions.trials !== 3 ||
      decodedOptions.concurrency !== 1 ||
      selectedCases.length !== suite.cases.length ||
      variants.length !== 2 ||
      variants.some(
        (variant) =>
          !comparison.configurations.some(
            (configuration) =>
              configurationIdentity(configuration) === configurationIdentity(variant.configuration),
          ),
      ))
  ) {
    return yield* EvalConfigurationError.make({
      message:
        "Frozen runs require the complete paired configuration, all cases, three trials, and serial balanced execution",
    });
  }
  const crypto = yield* Crypto.Crypto;

  const runId = yield* crypto.randomUUIDv4.pipe(
    Effect.mapError((cause) =>
      EvalDataError.make({
        operation: "allocate run",
        message: "Could not allocate an isolated eval run identity",
        cause,
      }),
    ),
  );

  const jobs: Array<EvalJob<Requirements>> = [];

  if (comparison !== undefined) {
    const orderedVariants = [...variants].sort((left, right) =>
      (left.configuration.strategy ?? "").localeCompare(right.configuration.strategy ?? ""),
    );

    const orderedCases = [...selectedCases].sort((left, right) => left.id.localeCompare(right.id));
    let pair = 0;

    for (let trial = 1; trial <= 3; trial += 1) {
      for (const evalCase of orderedCases) {
        const order = pair++ % 2 === 0 ? orderedVariants : [...orderedVariants].reverse();

        for (const variant of order)
          jobs.push({
            evalCase,
            variant,
            trial,
            runId,
            sequence: jobs.length,
            comparisonDigest: comparison.digest,
          });
      }
    }
  } else {
    for (const evalCase of selectedCases) {
      for (const variant of variants) {
        for (let trial = 1; trial <= decodedOptions.trials; trial += 1) {
          jobs.push({
            evalCase,
            variant,
            trial,
            runId,
            sequence: jobs.length,
            ...(frozenRun === undefined ? {} : { frozenRunDigest: frozenRun.digest }),
          });
        }
      }
    }
  }

  return Stream.fromIterable(jobs).pipe(
    Stream.mapEffect(runJob, { concurrency: decodedOptions.concurrency, unordered: true }),
  );
}, Stream.unwrap);
