import { ReviewFinding, type ReviewOutcome } from "@effect-agent/pr-review/Review";
import { Effect, Schema } from "effect";

import {
  type EvalCase,
  EvalCaseId,
  EvalCaseKind,
  EvalDefectId,
  EvalInputDigest,
  EvalObservation,
  EvalObservationSetDigest,
  EvalRunnerVersion,
  type EvalSuite,
  EvalTrialCount,
  EvalVariantConfiguration,
  EvalVariantId,
} from "./contracts.ts";
import { digestText } from "./corpus.ts";
import type { EvalFindingJudgment, EvalJudgmentSet } from "./judgments.ts";

export class EvalReportError extends Schema.TaggedError<EvalReportError>()("EvalReportError", {
  message: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
}) {}

export const EvalMetricStatus = Schema.Literals(["measured", "unresolved", "not-applicable"]);
export type EvalMetricStatus = typeof EvalMetricStatus.Type;

const RateFields = Schema.Struct({
  numerator: Schema.Natural,
  denominator: Schema.Natural,
  status: EvalMetricStatus,
}).check(
  Schema.makeFilter(
    (rate) =>
      rate.numerator <= rate.denominator &&
      (rate.status === "measured"
        ? rate.denominator > 0
        : rate.status === "not-applicable"
          ? rate.denominator === 0
          : true),
    { title: "Metric rate counts and status are consistent" },
  ),
);

export class EvalRate extends Schema.Class<EvalRate>(
  "@effect-agent/example-pr-review-eval/EvalRate",
)(RateFields) {}

const FindingQualityFields = Schema.Struct({
  valid: Schema.Natural,
  invalid: Schema.Natural,
  unclear: Schema.Natural,
  unjudged: Schema.Natural,
  precision: EvalRate,
}).check(
  Schema.makeFilter(
    (quality) => {
      const denominator = quality.valid + quality.invalid;

      const expectedStatus =
        quality.unclear + quality.unjudged > 0
          ? "unresolved"
          : denominator === 0
            ? "not-applicable"
            : "measured";

      return (
        quality.precision.numerator === quality.valid &&
        quality.precision.denominator === denominator &&
        quality.precision.status === expectedStatus
      );
    },
    { title: "Finding counts and precision are consistent" },
  ),
);

export class EvalFindingQuality extends Schema.Class<EvalFindingQuality>(
  "@effect-agent/example-pr-review-eval/EvalFindingQuality",
)(FindingQualityFields) {}

const BlockingFindingQualityFields = Schema.Struct({
  aligned: Schema.Natural,
  overstated: Schema.Natural,
  unresolved: Schema.Natural,
  precision: EvalRate,
}).check(
  Schema.makeFilter(
    (quality) => {
      const denominator = quality.aligned + quality.overstated;

      const expectedStatus =
        quality.unresolved > 0 ? "unresolved" : denominator === 0 ? "not-applicable" : "measured";

      return (
        quality.precision.numerator === quality.aligned &&
        quality.precision.denominator === denominator &&
        quality.precision.status === expectedStatus
      );
    },
    { title: "Blocking-finding counts and precision are consistent" },
  ),
);

export class EvalBlockingFindingQuality extends Schema.Class<EvalBlockingFindingQuality>(
  "@effect-agent/example-pr-review-eval/EvalBlockingFindingQuality",
)(BlockingFindingQualityFields) {}

export class EvalFailureCount extends Schema.Class<EvalFailureCount>(
  "@effect-agent/example-pr-review-eval/EvalFailureCount",
)({
  errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  count: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

const ResourceSummaryFields = Schema.Struct({
  attemptedTrials: Schema.Natural,
  succeededTrials: Schema.Natural,
  incompleteTrials: Schema.Natural,
  failedTrials: Schema.Natural,
  failuresByTag: Schema.Array(EvalFailureCount).check(Schema.isMaxLength(100)),
  turns: Schema.Natural,
  inputTokens: Schema.Natural,
  uncachedInputTokens: Schema.Natural,
  cachedInputTokens: Schema.Natural,
  cacheWriteInputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
  costedSucceededTrials: Schema.Natural,
  costedIncompleteTrials: Schema.Natural,
  costedFailedTrials: Schema.Natural,
  uncostedSucceededTrials: Schema.Natural,
  uncostedIncompleteTrials: Schema.Natural,
  estimatedCostMicrousd: Schema.Natural,
  elapsedMillis: Schema.Natural,
}).check(
  Schema.makeFilter(
    (resources) => {
      const failureTags = resources.failuresByTag.map((failure) => failure.errorTag);

      return (
        resources.attemptedTrials ===
          resources.succeededTrials + resources.incompleteTrials + resources.failedTrials &&
        resources.failedTrials ===
          resources.failuresByTag.reduce((total, failure) => total + failure.count, 0) &&
        new Set(failureTags).size === failureTags.length &&
        resources.inputTokens ===
          resources.uncachedInputTokens +
            resources.cachedInputTokens +
            resources.cacheWriteInputTokens &&
        resources.succeededTrials ===
          resources.costedSucceededTrials + resources.uncostedSucceededTrials &&
        resources.incompleteTrials ===
          resources.costedIncompleteTrials + resources.uncostedIncompleteTrials &&
        resources.costedFailedTrials <= resources.failedTrials
      );
    },
    { title: "Resource summary counts and token classes are consistent" },
  ),
);

export class EvalResourceSummary extends Schema.Class<EvalResourceSummary>(
  "@effect-agent/example-pr-review-eval/EvalResourceSummary",
)(ResourceSummaryFields) {}

export class EvalFindingReference extends Schema.Class<EvalFindingReference>(
  "@effect-agent/example-pr-review-eval/EvalFindingReference",
)({
  caseId: EvalCaseId,
  caseVersion: Schema.Literal(1),
  inputDigest: EvalInputDigest,
  variantId: EvalVariantId,
  trial: Schema.Int.check(Schema.isGreaterThan(0)),
  findingIndex: Schema.Natural,
  finding: ReviewFinding,
}) {}

export class EvalLaterBlocker extends Schema.Class<EvalLaterBlocker>(
  "@effect-agent/example-pr-review-eval/EvalLaterBlocker",
)({
  caseId: EvalCaseId,
  defectId: EvalDefectId,
  firstFoundTrial: Schema.Int.check(Schema.isGreaterThan(1)),
}) {}

export const EvalBlockerCaseStatus = Schema.Literals([
  "complete",
  "incomplete",
  "unresolved",
  "not-applicable",
]);

export type EvalBlockerCaseStatus = typeof EvalBlockerCaseStatus.Type;

export class EvalCaseQualityReport extends Schema.Class<EvalCaseQualityReport>(
  "@effect-agent/example-pr-review-eval/EvalCaseQualityReport",
)({
  caseId: EvalCaseId,
  caseVersion: Schema.Literal(1),
  inputDigest: EvalInputDigest,
  kind: EvalCaseKind,
  blockerDetection: EvalRate,
  blockerRecall: EvalRate,
  blockerStatus: EvalBlockerCaseStatus,
  cleanControlPassed: Schema.optionalKey(Schema.Boolean),
  laterOnlyBlockingDefects: Schema.Array(EvalLaterBlocker),
  firstTrialFindings: EvalFindingQuality,
  allTrialFindings: EvalFindingQuality,
  firstTrialBlockingFindings: EvalBlockingFindingQuality,
  firstTrialFailureTag: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(200))),
  resources: EvalResourceSummary,
}) {}

const CaseCompletionFields = Schema.Struct({
  complete: Schema.Natural,
  incomplete: Schema.Natural,
  unresolved: Schema.Natural,
  total: Schema.Natural,
}).check(
  Schema.makeFilter(
    (summary) => summary.complete + summary.incomplete + summary.unresolved === summary.total,
    { title: "Case completion counts equal the total" },
  ),
);

export class EvalCaseCompletionSummary extends Schema.Class<EvalCaseCompletionSummary>(
  "@effect-agent/example-pr-review-eval/EvalCaseCompletionSummary",
)(CaseCompletionFields) {}

const CleanControlFields = Schema.Struct({
  passed: Schema.Natural,
  total: Schema.Natural,
}).check(
  Schema.makeFilter((summary) => summary.passed <= summary.total, {
    title: "Clean-control passes do not exceed the total",
  }),
);

export class EvalCleanControlSummary extends Schema.Class<EvalCleanControlSummary>(
  "@effect-agent/example-pr-review-eval/EvalCleanControlSummary",
)(CleanControlFields) {}

export class EvalVariantQualityReport extends Schema.Class<EvalVariantQualityReport>(
  "@effect-agent/example-pr-review-eval/EvalVariantQualityReport",
)({
  configuration: EvalVariantConfiguration,
  blockerDetection: EvalRate,
  blockerRecall: EvalRate,
  blockerCases: EvalCaseCompletionSummary,
  cleanControls: EvalCleanControlSummary,
  laterOnlyBlockingDefects: Schema.Array(EvalLaterBlocker),
  firstTrialFindings: EvalFindingQuality,
  allTrialFindings: EvalFindingQuality,
  firstTrialBlockingFindings: EvalBlockingFindingQuality,
  firstTrialFailures: Schema.Natural,
  resources: EvalResourceSummary,
  cases: Schema.Array(EvalCaseQualityReport),
}) {}

export class EvalCaseIdentity extends Schema.Class<EvalCaseIdentity>(
  "@effect-agent/example-pr-review-eval/EvalCaseIdentity",
)({
  id: EvalCaseId,
  version: Schema.Literal(1),
  inputDigest: EvalInputDigest,
  repositoryDigest: Schema.optionalKey(EvalInputDigest),
}) {}

export class EvalQualityReport extends Schema.Class<EvalQualityReport>(
  "@effect-agent/example-pr-review-eval/EvalQualityReport",
)({
  version: Schema.Literal(3),
  observationSetDigest: EvalObservationSetDigest,
  runnerVersion: EvalRunnerVersion,
  trialCount: Schema.Int.check(Schema.isGreaterThan(0)),
  caseSet: Schema.Array(EvalCaseIdentity).check(Schema.isMinLength(1)),
  variants: Schema.Array(EvalVariantQualityReport).check(Schema.isMinLength(1)),
  unjudgedFindings: Schema.Array(EvalFindingReference),
  unmappedValidFindings: Schema.Array(EvalFindingReference),
}) {}

const encodeObservationArray = Schema.encodeEffect(
  Schema.fromJsonString(Schema.Array(EvalObservation)),
);

const trialKey = (caseId: EvalCaseId, variantId: EvalVariantId, trial: number): string =>
  `${caseId}\0${variantId}\0${trial}`;

const observationKey = (observation: EvalObservation): string =>
  trialKey(observation.caseId, observation.variant.id, observation.trial);

const judgmentKey = (judgment: EvalFindingJudgment): string =>
  `${judgment.caseId}\0${judgment.variantId}\0${judgment.trial}\0${judgment.findingIndex}`;

const findingKey = (
  caseId: EvalCaseId,
  variantId: EvalVariantId,
  trial: number,
  findingIndex: number,
): string => `${caseId}\0${variantId}\0${trial}\0${findingIndex}`;

const sortedObservations = (observations: ReadonlyArray<EvalObservation>) =>
  [...observations].sort(
    (left, right) =>
      left.caseId.localeCompare(right.caseId) ||
      left.variant.id.localeCompare(right.variant.id) ||
      left.trial - right.trial,
  );

export const digestObservationSet = Effect.fn("PrReviewEval.digestObservationSet")(function* (
  observations: ReadonlyArray<EvalObservation>,
) {
  const encoded = yield* encodeObservationArray(sortedObservations(observations)).pipe(
    Effect.mapError(() =>
      EvalReportError.make({ message: "Eval observations failed canonical encoding" }),
    ),
  );

  const digest = yield* digestText(encoded);

  return yield* Schema.decodeUnknownEffect(EvalObservationSetDigest)(digest).pipe(
    Effect.mapError(() =>
      EvalReportError.make({ message: "Observation set digest failed validation" }),
    ),
  );
});

const makeRate = (numerator: number, denominator: number, unresolved: boolean): EvalRate =>
  EvalRate.make({
    numerator,
    denominator,
    status: unresolved ? "unresolved" : denominator === 0 ? "not-applicable" : "measured",
  });

const findingQuality = (findings: ReadonlyArray<IndexedFinding>): EvalFindingQuality => {
  let valid = 0;
  let invalid = 0;
  let unclear = 0;
  let unjudged = 0;

  for (const indexed of findings) {
    switch (indexed.judgment?.label) {
      case "matches-expected":
      case "new-valid":
        valid += 1;
        break;
      case "invalid":
        invalid += 1;
        break;
      case "unclear":
        unclear += 1;
        break;
      case undefined:
        unjudged += 1;
        break;
    }
  }

  return EvalFindingQuality.make({
    valid,
    invalid,
    unclear,
    unjudged,
    precision: makeRate(valid, valid + invalid, unclear + unjudged > 0),
  });
};

const blockingFindingQuality = (
  findings: ReadonlyArray<IndexedFinding>,
  evalCase: EvalCase,
): EvalBlockingFindingQuality => {
  const expectedSeverity = new Map(
    evalCase.expectedDefects.map((defect) => [defect.id, defect.severity] as const),
  );

  let aligned = 0;
  let overstated = 0;
  let unresolved = 0;

  for (const indexed of findings) {
    if (indexed.reference.finding.severity !== "blocking") continue;
    switch (indexed.judgment?.label) {
      case "matches-expected":
        if (
          indexed.judgment.matchedDefectIds.some(
            (defectId) => expectedSeverity.get(defectId) === "blocking",
          )
        ) {
          aligned += 1;
        } else {
          overstated += 1;
        }
        break;
      case "invalid":
        overstated += 1;
        break;
      case "new-valid":
      case "unclear":
      case undefined:
        unresolved += 1;
        break;
    }
  }

  return EvalBlockingFindingQuality.make({
    aligned,
    overstated,
    unresolved,
    precision: makeRate(aligned, aligned + overstated, unresolved > 0),
  });
};

const isIncompleteReview = (outcome: ReviewOutcome): boolean =>
  outcome.incomplete === true || outcome.exhausted !== undefined;

const resourceSummary = (observations: ReadonlyArray<EvalObservation>): EvalResourceSummary => {
  let succeededTrials = 0;
  let incompleteTrials = 0;
  let failedTrials = 0;
  let turns = 0;
  let inputTokens = 0;
  let uncachedInputTokens = 0;
  let cachedInputTokens = 0;
  let cacheWriteInputTokens = 0;
  let outputTokens = 0;
  let costedSucceededTrials = 0;
  let costedIncompleteTrials = 0;
  let costedFailedTrials = 0;
  let estimatedCostMicrousd = 0;
  let elapsedMillis = 0;
  const failureCounts = new Map<string, number>();

  for (const observation of observations) {
    elapsedMillis += observation.elapsedMillis;
    if (observation.result._tag === "Failed") {
      failedTrials += 1;
      failureCounts.set(
        observation.result.errorTag,
        (failureCounts.get(observation.result.errorTag) ?? 0) + 1,
      );
      if (observation.result.estimatedCostMicrousd !== undefined) {
        costedFailedTrials += 1;
        estimatedCostMicrousd += observation.result.estimatedCostMicrousd;
      }
      continue;
    }
    const incomplete = isIncompleteReview(observation.result.outcome);

    if (incomplete) incompleteTrials += 1;
    else succeededTrials += 1;
    const { usage } = observation.result.outcome;

    turns += observation.result.outcome.turns;
    inputTokens += usage.inputTokens;
    uncachedInputTokens += usage.uncachedInputTokens;
    cachedInputTokens += usage.cachedInputTokens;
    cacheWriteInputTokens += usage.cacheWriteInputTokens;
    outputTokens += usage.outputTokens;
    if (usage.estimatedCostMicrousd !== undefined) {
      if (incomplete) costedIncompleteTrials += 1;
      else costedSucceededTrials += 1;
      estimatedCostMicrousd += usage.estimatedCostMicrousd;
    }
  }

  return EvalResourceSummary.make({
    attemptedTrials: observations.length,
    succeededTrials,
    incompleteTrials,
    failedTrials,
    failuresByTag: [...failureCounts]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([errorTag, count]) => EvalFailureCount.make({ errorTag, count })),
    turns,
    inputTokens,
    uncachedInputTokens,
    cachedInputTokens,
    cacheWriteInputTokens,
    outputTokens,
    costedSucceededTrials,
    costedIncompleteTrials,
    costedFailedTrials,
    uncostedSucceededTrials: succeededTrials - costedSucceededTrials,
    uncostedIncompleteTrials: incompleteTrials - costedIncompleteTrials,
    estimatedCostMicrousd,
    elapsedMillis,
  });
};

interface IndexedFinding {
  readonly reference: EvalFindingReference;
  readonly judgment: EvalFindingJudgment | undefined;
}

interface ValidatedInputs {
  readonly observationSetDigest: EvalObservationSetDigest;
  readonly runnerVersion: EvalRunnerVersion;
  readonly trialCount: number;
  readonly cases: ReadonlyArray<EvalCase>;
  readonly configurations: ReadonlyMap<EvalVariantId, EvalVariantConfiguration>;
  readonly observations: ReadonlyMap<string, EvalObservation>;
  readonly judgments: ReadonlyMap<string, EvalFindingJudgment>;
}

const validateInputs = Effect.fn("PrReviewEval.validateReportInputs")(function* (
  suite: EvalSuite,
  observations: ReadonlyArray<EvalObservation>,
  expectedTrialCount: number,
  judgmentSet: EvalJudgmentSet | undefined,
) {
  const trialCount = yield* Schema.decodeUnknownEffect(EvalTrialCount)(expectedTrialCount).pipe(
    Effect.mapError(() =>
      EvalReportError.make({ message: "Declare the expected trial count between 1 and 20" }),
    ),
  );

  if (observations.length === 0) {
    return yield* EvalReportError.make({ message: "At least one eval observation is required" });
  }

  const cases = new Map(suite.cases.map((evalCase) => [evalCase.id, evalCase]));
  const byKey = new Map<string, EvalObservation>();
  const configurations = new Map<EvalVariantId, EvalVariantConfiguration>();
  const encodedConfigurations = new Map<EvalVariantId, string>();
  const runnerVersions = new Set<EvalRunnerVersion>();

  for (const observation of observations) {
    const evalCase = cases.get(observation.caseId);

    if (
      evalCase === undefined ||
      observation.caseVersion !== evalCase.version ||
      observation.inputDigest !== evalCase.inputDigest ||
      observation.repositoryDigest !== evalCase.repository?.digest
    ) {
      return yield* EvalReportError.make({
        message: `Observation case identity is incompatible for ${observation.caseId}`,
      });
    }
    const key = observationKey(observation);

    if (byKey.has(key)) {
      return yield* EvalReportError.make({ message: `Duplicate eval observation ${key}` });
    }
    byKey.set(key, observation);
    runnerVersions.add(observation.runnerVersion);

    const encoded = JSON.stringify(
      Schema.encodeSync(EvalVariantConfiguration)(observation.variant),
    );

    const existing = encodedConfigurations.get(observation.variant.id);

    if (existing !== undefined && existing !== encoded) {
      return yield* EvalReportError.make({
        message: `Variant ${observation.variant.id} has incompatible model configurations`,
      });
    }
    encodedConfigurations.set(observation.variant.id, encoded);
    configurations.set(observation.variant.id, observation.variant);
  }

  if (runnerVersions.size !== 1) {
    return yield* EvalReportError.make({
      message: "A quality report cannot combine different runner versions",
    });
  }

  for (const variantId of configurations.keys()) {
    for (const evalCase of suite.cases) {
      const trials = observations
        .filter(
          (observation) =>
            observation.variant.id === variantId && observation.caseId === evalCase.id,
        )
        .map((observation) => observation.trial)
        .sort((left, right) => left - right);

      if (trials.length !== trialCount || trials.some((trial, index) => trial !== index + 1)) {
        return yield* EvalReportError.make({
          message: `Variant ${variantId} does not have the expected ${trialCount} trials for ${evalCase.id}`,
        });
      }
    }
  }

  const observationSetDigest = yield* digestObservationSet(observations);

  if (judgmentSet !== undefined && judgmentSet.observationSetDigest !== observationSetDigest) {
    return yield* EvalReportError.make({
      message: "Judgments do not match the exact eval observation set",
    });
  }

  const judgments = new Map<string, EvalFindingJudgment>();

  for (const judgment of judgmentSet?.judgments ?? []) {
    const key = judgmentKey(judgment);
    const matchedIds = judgment.matchedDefectIds;

    if (
      judgments.has(key) ||
      !Number.isSafeInteger(judgment.trial) ||
      judgment.trial < 1 ||
      !Number.isSafeInteger(judgment.findingIndex) ||
      judgment.findingIndex < 0 ||
      new Set(matchedIds).size !== matchedIds.length ||
      (judgment.label === "matches-expected" ? matchedIds.length === 0 : matchedIds.length > 0)
    ) {
      return yield* EvalReportError.make({
        message: `Invalid or duplicate finding judgment ${key}`,
      });
    }
    const observation = byKey.get(trialKey(judgment.caseId, judgment.variantId, judgment.trial));
    const evalCase = cases.get(judgment.caseId);

    if (
      observation === undefined ||
      evalCase === undefined ||
      judgment.caseVersion !== evalCase.version ||
      judgment.inputDigest !== evalCase.inputDigest ||
      observation.result._tag !== "Succeeded" ||
      judgment.findingIndex >= observation.result.outcome.report.findings.length
    ) {
      return yield* EvalReportError.make({
        message: `Finding judgment does not identify an emitted finding: ${key}`,
      });
    }
    const expectedIds = new Set(evalCase.expectedDefects.map((defect) => defect.id));

    if (
      judgment.label === "matches-expected" &&
      judgment.matchedDefectIds.some((id) => !expectedIds.has(id))
    ) {
      return yield* EvalReportError.make({
        message: `Finding judgment ${key} names an unknown expected defect`,
      });
    }
    judgments.set(key, judgment);
  }

  const runnerVersion = runnerVersions.values().next().value;

  if (runnerVersion === undefined) {
    return yield* EvalReportError.make({ message: "Eval observation grid is empty" });
  }

  return {
    observationSetDigest,
    runnerVersion,
    trialCount,
    cases: suite.cases,
    configurations,
    observations: byKey,
    judgments,
  } satisfies ValidatedInputs;
});

const indexFindings = (
  observation: EvalObservation,
  judgments: ReadonlyMap<string, EvalFindingJudgment>,
): ReadonlyArray<IndexedFinding> => {
  if (observation.result._tag === "Failed") return [];

  return observation.result.outcome.report.findings.map((finding, findingIndex) => {
    const key = findingKey(
      observation.caseId,
      observation.variant.id,
      observation.trial,
      findingIndex,
    );

    return {
      reference: EvalFindingReference.make({
        caseId: observation.caseId,
        caseVersion: observation.caseVersion,
        inputDigest: observation.inputDigest,
        variantId: observation.variant.id,
        trial: observation.trial,
        findingIndex,
        finding,
      }),
      judgment: judgments.get(key),
    };
  });
};

const matchedBlockingDefects = (
  findings: ReadonlyArray<IndexedFinding>,
  evalCase: EvalCase,
  requireBlockingSeverity: boolean,
): ReadonlySet<EvalDefectId> => {
  const blockers = new Set(
    evalCase.expectedDefects
      .filter((defect) => defect.severity === "blocking")
      .map((defect) => defect.id),
  );

  const matched = new Set<EvalDefectId>();

  for (const indexed of findings) {
    if (
      (requireBlockingSeverity && indexed.reference.finding.severity !== "blocking") ||
      indexed.judgment?.label !== "matches-expected"
    ) {
      continue;
    }
    for (const defectId of indexed.judgment.matchedDefectIds) {
      if (blockers.has(defectId)) matched.add(defectId);
    }
  }

  return matched;
};

const caseReport = (
  evalCase: EvalCase,
  firstObservation: EvalObservation,
  observations: ReadonlyArray<EvalObservation>,
  judgments: ReadonlyMap<string, EvalFindingJudgment>,
): EvalCaseQualityReport => {
  const indexed = observations.flatMap((observation) => indexFindings(observation, judgments));
  const firstFindings = indexed.filter((finding) => finding.reference.trial === 1);
  const firstDetected = matchedBlockingDefects(firstFindings, evalCase, false);
  const firstMatched = matchedBlockingDefects(firstFindings, evalCase, true);

  const expectedBlockers = evalCase.expectedDefects.filter(
    (defect) => defect.severity === "blocking",
  );

  const firstQuality = findingQuality(firstFindings);

  const unresolvedBlockingFindings = firstFindings.filter(
    (finding) =>
      finding.reference.finding.severity === "blocking" &&
      (finding.judgment === undefined || finding.judgment.label === "unclear"),
  );

  const firstUnresolved =
    firstMatched.size < expectedBlockers.length &&
    firstObservation.result._tag === "Succeeded" &&
    unresolvedBlockingFindings.length > 0;

  const unresolvedDetectionFindings = firstFindings.filter(
    (finding) => finding.judgment === undefined || finding.judgment.label === "unclear",
  );

  const firstDetectionUnresolved =
    firstDetected.size < expectedBlockers.length &&
    firstObservation.result._tag === "Succeeded" &&
    unresolvedDetectionFindings.length > 0;

  const firstIncomplete =
    firstObservation.result._tag === "Succeeded" &&
    isIncompleteReview(firstObservation.result.outcome);

  const blockerStatus: EvalBlockerCaseStatus =
    expectedBlockers.length === 0
      ? "not-applicable"
      : firstObservation.result._tag === "Failed" || firstIncomplete
        ? "incomplete"
        : firstMatched.size === expectedBlockers.length
          ? "complete"
          : firstUnresolved
            ? "unresolved"
            : "incomplete";

  const laterOnlyBlockingDefects: Array<EvalLaterBlocker> = [];

  const firstTrialIsResolved =
    firstObservation.result._tag === "Failed" ||
    unresolvedBlockingFindings.length === 0 ||
    firstMatched.size === expectedBlockers.length;

  if (firstTrialIsResolved) {
    for (const defect of expectedBlockers) {
      if (firstMatched.has(defect.id)) continue;
      for (let trial = 2; trial <= observations.length; trial += 1) {
        const trialMatched = matchedBlockingDefects(
          indexed.filter((finding) => finding.reference.trial === trial),
          evalCase,
          true,
        );

        if (trialMatched.has(defect.id)) {
          laterOnlyBlockingDefects.push(
            EvalLaterBlocker.make({
              caseId: evalCase.id,
              defectId: defect.id,
              firstFoundTrial: trial,
            }),
          );
          break;
        }
      }
    }
  }

  return EvalCaseQualityReport.make({
    caseId: evalCase.id,
    caseVersion: evalCase.version,
    inputDigest: evalCase.inputDigest,
    kind: evalCase.kind,
    blockerDetection: makeRate(
      firstDetected.size,
      expectedBlockers.length,
      firstDetectionUnresolved,
    ),
    blockerRecall: makeRate(firstMatched.size, expectedBlockers.length, firstUnresolved),
    blockerStatus,
    ...(evalCase.kind === "clean-control"
      ? {
          cleanControlPassed:
            firstObservation.result._tag === "Succeeded" &&
            !firstIncomplete &&
            firstFindings.length === 0,
        }
      : {}),
    laterOnlyBlockingDefects,
    firstTrialFindings: firstQuality,
    allTrialFindings: findingQuality(indexed),
    firstTrialBlockingFindings: blockingFindingQuality(firstFindings, evalCase),
    ...(firstObservation.result._tag === "Failed"
      ? { firstTrialFailureTag: firstObservation.result.errorTag }
      : {}),
    resources: resourceSummary(observations),
  });
};

const aggregateFindingQuality = (
  reports: ReadonlyArray<EvalCaseQualityReport>,
  field: "firstTrialFindings" | "allTrialFindings",
): EvalFindingQuality => {
  const totals = reports.reduce(
    (accumulator, report) => ({
      valid: accumulator.valid + report[field].valid,
      invalid: accumulator.invalid + report[field].invalid,
      unclear: accumulator.unclear + report[field].unclear,
      unjudged: accumulator.unjudged + report[field].unjudged,
    }),
    { valid: 0, invalid: 0, unclear: 0, unjudged: 0 },
  );

  return EvalFindingQuality.make({
    ...totals,
    precision: makeRate(
      totals.valid,
      totals.valid + totals.invalid,
      totals.unclear + totals.unjudged > 0,
    ),
  });
};

const aggregateBlockingFindingQuality = (
  reports: ReadonlyArray<EvalCaseQualityReport>,
): EvalBlockingFindingQuality => {
  const totals = reports.reduce(
    (accumulator, report) => ({
      aligned: accumulator.aligned + report.firstTrialBlockingFindings.aligned,
      overstated: accumulator.overstated + report.firstTrialBlockingFindings.overstated,
      unresolved: accumulator.unresolved + report.firstTrialBlockingFindings.unresolved,
    }),
    { aligned: 0, overstated: 0, unresolved: 0 },
  );

  return EvalBlockingFindingQuality.make({
    ...totals,
    precision: makeRate(totals.aligned, totals.aligned + totals.overstated, totals.unresolved > 0),
  });
};

export const makeQualityReport = Effect.fn("PrReviewEval.makeQualityReport")(function* (
  suite: EvalSuite,
  observations: ReadonlyArray<EvalObservation>,
  expectedTrialCount: number,
  judgmentSet?: EvalJudgmentSet,
) {
  const validated = yield* validateInputs(suite, observations, expectedTrialCount, judgmentSet);
  const variants: Array<EvalVariantQualityReport> = [];
  const unjudgedFindings: Array<EvalFindingReference> = [];
  const unmappedValidFindings: Array<EvalFindingReference> = [];

  for (const [variantId, configuration] of [...validated.configurations].sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    const variantObservations = sortedObservations(
      observations.filter((observation) => observation.variant.id === variantId),
    );

    const cases: Array<EvalCaseQualityReport> = [];

    for (const evalCase of validated.cases) {
      const caseObservations = variantObservations.filter(
        (observation) => observation.caseId === evalCase.id,
      );

      const firstObservation = caseObservations[0];

      if (firstObservation === undefined) {
        return yield* EvalReportError.make({
          message: `Variant ${variantId} has no observations for ${evalCase.id}`,
        });
      }
      for (const observation of caseObservations) {
        for (const indexed of indexFindings(observation, validated.judgments)) {
          if (indexed.judgment === undefined) unjudgedFindings.push(indexed.reference);
          if (indexed.judgment?.label === "new-valid") {
            unmappedValidFindings.push(indexed.reference);
          }
        }
      }
      cases.push(caseReport(evalCase, firstObservation, caseObservations, validated.judgments));
    }

    const blockerDetected = cases.reduce(
      (total, report) => total + report.blockerDetection.numerator,
      0,
    );

    const blockerFound = cases.reduce((total, report) => total + report.blockerRecall.numerator, 0);

    const blockerTotal = cases.reduce(
      (total, report) => total + report.blockerRecall.denominator,
      0,
    );

    const blockerUnresolved = cases.some((report) => report.blockerRecall.status === "unresolved");

    const detectionUnresolved = cases.some(
      (report) => report.blockerDetection.status === "unresolved",
    );

    const eligibleCases = cases.filter((report) => report.blockerStatus !== "not-applicable");
    const cleanControls = cases.filter((report) => report.kind === "clean-control");

    variants.push(
      EvalVariantQualityReport.make({
        configuration,
        blockerDetection: makeRate(blockerDetected, blockerTotal, detectionUnresolved),
        blockerRecall: makeRate(blockerFound, blockerTotal, blockerUnresolved),
        blockerCases: EvalCaseCompletionSummary.make({
          complete: eligibleCases.filter((report) => report.blockerStatus === "complete").length,
          incomplete: eligibleCases.filter((report) => report.blockerStatus === "incomplete")
            .length,
          unresolved: eligibleCases.filter((report) => report.blockerStatus === "unresolved")
            .length,
          total: eligibleCases.length,
        }),
        cleanControls: EvalCleanControlSummary.make({
          passed: cleanControls.filter((report) => report.cleanControlPassed === true).length,
          total: cleanControls.length,
        }),
        laterOnlyBlockingDefects: cases.flatMap((report) => report.laterOnlyBlockingDefects),
        firstTrialFindings: aggregateFindingQuality(cases, "firstTrialFindings"),
        allTrialFindings: aggregateFindingQuality(cases, "allTrialFindings"),
        firstTrialBlockingFindings: aggregateBlockingFindingQuality(cases),
        firstTrialFailures: cases.filter((report) => report.firstTrialFailureTag !== undefined)
          .length,
        resources: resourceSummary(variantObservations),
        cases,
      }),
    );
  }

  return EvalQualityReport.make({
    version: 3,
    observationSetDigest: validated.observationSetDigest,
    runnerVersion: validated.runnerVersion,
    trialCount: validated.trialCount,
    caseSet: validated.cases.map((evalCase) =>
      EvalCaseIdentity.make({
        id: evalCase.id,
        version: evalCase.version,
        inputDigest: evalCase.inputDigest,
        ...(evalCase.repository === undefined
          ? {}
          : { repositoryDigest: evalCase.repository.digest }),
      }),
    ),
    variants,
    unjudgedFindings,
    unmappedValidFindings,
  });
});

const renderRate = (rate: EvalRate): string =>
  rate.status === "measured"
    ? `${rate.numerator}/${rate.denominator}`
    : `${rate.numerator}/${rate.denominator} (${rate.status})`;

export const renderQualityReport = (report: EvalQualityReport): string =>
  report.variants
    .map((variant) => {
      const quality = variant.firstTrialFindings;
      const blockingQuality = variant.firstTrialBlockingFindings;

      return [
        `${variant.configuration.id}: blocking-recall ${renderRate(variant.blockerRecall)}`,
        `detected ${renderRate(variant.blockerDetection)}`,
        `complete ${variant.blockerCases.complete}/${variant.blockerCases.total}`,
        `precision ${renderRate(quality.precision)}`,
        `blocking-precision ${renderRate(blockingQuality.precision)}`,
        `overstated-blocking ${blockingQuality.overstated}`,
        `invalid ${quality.invalid}`,
        `unclear ${quality.unclear}`,
        `unjudged ${quality.unjudged}`,
        `later-only ${variant.laterOnlyBlockingDefects.length}`,
        `incomplete ${variant.resources.incompleteTrials}/${variant.resources.attemptedTrials}`,
        `failures ${variant.resources.failedTrials}/${variant.resources.attemptedTrials}`,
        `tokens ${variant.resources.inputTokens} in/${variant.resources.outputTokens} out`,
        variant.resources.costedSucceededTrials +
          variant.resources.costedIncompleteTrials +
          variant.resources.costedFailedTrials ===
        0
          ? "cost unavailable"
          : `cost ${variant.resources.estimatedCostMicrousd}µUSD (${variant.resources.costedSucceededTrials} succeeded + ${variant.resources.costedIncompleteTrials} incomplete + ${variant.resources.costedFailedTrials} failed costed)`,
        `elapsed ${variant.resources.elapsedMillis}ms`,
      ].join("; ");
    })
    .join("\n");
