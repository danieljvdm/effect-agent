import {
  ReviewCandidate,
  ReviewFinding,
  type ReviewOutcome,
  type ReviewSeverity,
} from "@effect-agent/pr-review";
import { Effect, Schema } from "effect";

import { configurationIdentity, freezeEvalSuite, validateFrozenComparison } from "./comparison.ts";
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
import { digestEvalOracle, digestObservation, digestText, validateEvalSuite } from "./corpus.ts";
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
  candidateId: Schema.optionalKey(ReviewCandidate.fields.id),
  observationDigest: Schema.optionalKey(EvalInputDigest),
  oracleDigest: Schema.optionalKey(EvalInputDigest),
  candidate: Schema.optionalKey(ReviewCandidate),
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
  /** Expected blockers absent after all possible first-trial matches have been adjudicated. */
  discoveryMisses: Schema.Natural,
  /** Unmatched expected blockers that an unjudged or unclear first-trial candidate may cover. */
  unresolvedDiscoveryMisses: Schema.Natural,
  validCandidatesRefuted: Schema.Natural,
  validCandidatesWithheld: Schema.Natural,
  validBlockingCandidatesRefuted: Schema.Natural,
  validBlockingCandidatesWithheld: Schema.Natural,
  firstTrialIncomplete: Schema.Boolean,
  repeatedTrialInstability: Schema.Boolean,
  unresolvedRepeatedTrialInstability: Schema.Boolean,
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
  discoveryMisses: Schema.Natural,
  unresolvedDiscoveryMisses: Schema.Natural,
  validCandidatesRefuted: Schema.Natural,
  validCandidatesWithheld: Schema.Natural,
  validBlockingCandidatesRefuted: Schema.Natural,
  validBlockingCandidatesWithheld: Schema.Natural,
  repeatedTrialInstability: Schema.Natural,
  unresolvedRepeatedTrialInstability: Schema.Natural,
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
  oracleDigest: Schema.optionalKey(EvalInputDigest),
  oracleVersion: Schema.optionalKey(Schema.Natural),
}) {}

export class EvalRolloutDecision extends Schema.Class<EvalRolloutDecision>(
  "@effect-agent/example-pr-review-eval/EvalRolloutDecision",
)({
  decision: Schema.Literals(["eligible", "experimental"]),
  basis: Schema.Literal("frozen-heldout-first-trials"),
  heldoutCases: Schema.Natural,
  baselineFalseBlockers: Schema.Natural,
  verifiedFalseBlockers: Schema.Natural,
  reasons: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(20),
  ),
}) {}

export class EvalQualityReport extends Schema.Class<EvalQualityReport>(
  "@effect-agent/example-pr-review-eval/EvalQualityReport",
)({
  version: Schema.Literal(5),
  observationSetDigest: EvalObservationSetDigest,
  runnerVersion: EvalRunnerVersion,
  trialCount: Schema.Int.check(Schema.isGreaterThan(0)),
  caseSet: Schema.Array(EvalCaseIdentity).check(Schema.isMinLength(1)),
  variants: Schema.Array(EvalVariantQualityReport).check(Schema.isMinLength(1)),
  unjudgedFindings: Schema.Array(EvalFindingReference),
  unmappedValidFindings: Schema.Array(EvalFindingReference),
  rollout: EvalRolloutDecision,
}) {}

const encodeObservationArray = Schema.encodeEffect(
  Schema.fromJsonString(Schema.Array(EvalObservation)),
);

const trialKey = (caseId: EvalCaseId, variantId: EvalVariantId, trial: number): string =>
  `${caseId}\0${variantId}\0${trial}`;

const observationKey = (observation: EvalObservation): string =>
  trialKey(observation.caseId, observation.variant.id, observation.trial);

const judgmentKey = (judgment: EvalFindingJudgment): string =>
  `${judgment.caseId}\0${judgment.variantId}\0${judgment.trial}\0${judgment.candidateId ?? judgment.findingIndex}`;

const findingKey = (
  caseId: EvalCaseId,
  variantId: EvalVariantId,
  trial: number,
  findingIndex: string | number,
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

const isIncompleteReview = (outcome: ReviewOutcome, evalCase?: EvalCase): boolean =>
  outcome.incomplete === true ||
  outcome.exhausted !== undefined ||
  (outcome.pendingPaths?.length ?? 0) > 0 ||
  (evalCase?.request.unreviewedPaths.length ?? 0) > 0;

const resourceSummary = (
  observations: ReadonlyArray<EvalObservation>,
  cases: ReadonlyArray<EvalCase>,
): EvalResourceSummary => {
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

    const incomplete = isIncompleteReview(
      observation.result.outcome,
      cases.find((evalCase) => evalCase.id === observation.caseId),
    );

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

const observationCandidates = (observation: EvalObservation) =>
  observation.result._tag === "Succeeded"
    ? observation.result.outcome.diagnostics?.candidates
    : observation.result.diagnostics?.candidates;

const isPublished = (finding: IndexedFinding): boolean =>
  finding.reference.candidate === undefined ||
  finding.reference.candidate.publication === "published";

const isValid = (finding: IndexedFinding): boolean =>
  finding.judgment?.label === "matches-expected" || finding.judgment?.label === "new-valid";

const judgedSeverity = (
  finding: IndexedFinding,
  evalCase: EvalCase,
): ReviewSeverity | undefined => {
  if (finding.judgment?.label === "new-valid") return finding.judgment.severity;
  if (finding.judgment?.label !== "matches-expected") return undefined;

  const severities = evalCase.expectedDefects
    .filter((defect) => finding.judgment?.matchedDefectIds.includes(defect.id))
    .map((defect) => defect.severity);

  return severities.includes("blocking")
    ? "blocking"
    : severities.includes("important")
      ? "important"
      : "nit";
};

interface ValidatedInputs {
  readonly observationSetDigest: EvalObservationSetDigest;
  readonly runnerVersion: EvalRunnerVersion;
  readonly trialCount: number;
  readonly cases: ReadonlyArray<EvalCase>;
  readonly configurations: ReadonlyMap<EvalVariantId, EvalVariantConfiguration>;
  readonly observations: ReadonlyMap<string, EvalObservation>;
  readonly judgments: ReadonlyMap<string, EvalFindingJudgment>;
  readonly observationDigests: ReadonlyMap<string, EvalInputDigest>;
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

  yield* validateEvalSuite(suite);

  const comparison =
    suite.comparison === undefined ? undefined : yield* validateFrozenComparison(suite);

  if (observations.length === 0) {
    return yield* EvalReportError.make({ message: "At least one eval observation is required" });
  }

  const cases = new Map(suite.cases.map((evalCase) => [evalCase.id, evalCase]));
  const byKey = new Map<string, EvalObservation>();
  const configurations = new Map<EvalVariantId, EvalVariantConfiguration>();
  const encodedConfigurations = new Map<EvalVariantId, string>();
  const runnerVersions = new Set<EvalRunnerVersion>();
  const observationDigests = new Map<string, EvalInputDigest>();
  const oracleDigests = new Map<EvalCaseId, EvalInputDigest>();

  for (const evalCase of suite.cases)
    oracleDigests.set(evalCase.id, yield* digestEvalOracle(evalCase));

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
    if (
      (observation.oracleDigest !== undefined &&
        (observation.oracleDigest !== oracleDigests.get(evalCase.id) ||
          observation.oracleVersion !== (evalCase.oracleVersion ?? 1))) ||
      (comparison !== undefined &&
        (observation.oracleDigest === undefined ||
          observation.comparisonDigest !== comparison.digest ||
          observation.cacheNamespace === undefined ||
          observation.runId === undefined ||
          observation.sequence === undefined ||
          !comparison.configurations.some(
            (configuration) =>
              configurationIdentity(configuration) === configurationIdentity(observation.variant),
          )))
    ) {
      return yield* EvalReportError.make({
        message: `Observation oracle or frozen configuration identity is incompatible for ${observation.caseId}; version corrections and rescore both strategies explicitly`,
      });
    }
    const key = observationKey(observation);

    const diagnostics =
      observation.result._tag === "Succeeded"
        ? observation.result.outcome.diagnostics
        : observation.result.diagnostics;

    if (
      diagnostics !== undefined &&
      (diagnostics.requestDigest !== observation.inputDigest ||
        diagnostics.strategy !== (observation.variant.strategy ?? "baseline") ||
        diagnostics.candidates.some(
          (candidate, index) =>
            candidate.id !== `${observation.inputDigest}:${index + 1}` ||
            candidate.requestDigest !== observation.inputDigest ||
            candidate.baseRevision !== evalCase.request.baseRevision ||
            candidate.headRevision !== evalCase.request.headRevision,
        ))
    ) {
      return yield* EvalReportError.make({
        message: `Candidate identity does not match the immutable review input for ${observation.caseId}`,
      });
    }
    if (
      comparison !== undefined &&
      observation.result._tag === "Succeeded" &&
      diagnostics === undefined
    ) {
      return yield* EvalReportError.make({
        message:
          "Frozen observations must retain every original candidate and its publication disposition",
      });
    }

    if (byKey.has(key)) {
      return yield* EvalReportError.make({ message: `Duplicate eval observation ${key}` });
    }
    byKey.set(key, observation);
    observationDigests.set(key, yield* digestObservation(observation));
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

  if (comparison !== undefined) {
    if (
      observations.length !== comparison.plannedRuns ||
      trialCount !== 3 ||
      new Set(observations.map((observation) => observation.runId)).size !== 1 ||
      new Set(observations.map((observation) => observation.cacheNamespace)).size !==
        observations.length
    ) {
      return yield* EvalReportError.make({
        message:
          "Frozen comparison requires its complete run budget, one run identity, and isolated trial cache namespaces",
      });
    }
    const expected: Array<string> = [];
    let pair = 0;

    for (let trial = 1; trial <= 3; trial += 1) {
      for (const evalCase of comparison.cases) {
        const order =
          pair++ % 2 === 0 ? comparison.configurations : [...comparison.configurations].reverse();

        for (const configuration of order)
          expected.push(trialKey(evalCase.id, configuration.id, trial));
      }
    }
    if (
      new Set(observations.map((observation) => observation.sequence)).size !==
        observations.length ||
      observations.some(
        (observation) =>
          observation.sequence === undefined ||
          expected[observation.sequence] !== observationKey(observation),
      )
    ) {
      return yield* EvalReportError.make({
        message: "Observations do not match the fixed balanced execution order",
      });
    }
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
      (judgment.findingIndex !== undefined &&
        (!Number.isSafeInteger(judgment.findingIndex) || judgment.findingIndex < 0)) ||
      (judgment.findingIndex === undefined && judgment.candidateId === undefined) ||
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
      (observationCandidates(observation) === undefined
        ? observation.result._tag !== "Succeeded" ||
          judgment.findingIndex === undefined ||
          judgment.findingIndex >= observation.result.outcome.report.findings.length
        : judgment.candidateId === undefined ||
          !observationCandidates(observation)?.some(
            (candidate) => candidate.id === judgment.candidateId,
          ))
    ) {
      return yield* EvalReportError.make({
        message: `Finding judgment does not identify an emitted finding: ${key}`,
      });
    }
    if (
      observationCandidates(observation) !== undefined &&
      (judgment.observationDigest !== observationDigests.get(observationKey(observation)) ||
        judgment.oracleDigest !== oracleDigests.get(evalCase.id))
    ) {
      return yield* EvalReportError.make({
        message: `Candidate judgment must bind the exact observation and oracle digests: ${key}`,
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
    observationDigests,
  } satisfies ValidatedInputs;
});

const indexFindings = (
  observation: EvalObservation,
  judgments: ReadonlyMap<string, EvalFindingJudgment>,
  observationDigests?: ReadonlyMap<string, EvalInputDigest>,
): ReadonlyArray<IndexedFinding> => {
  const candidates = observationCandidates(observation);

  const findings =
    candidates?.map((candidate) => candidate.finding) ??
    (observation.result._tag === "Failed" ? [] : observation.result.outcome.report.findings);

  return findings.map((finding, findingIndex) => {
    const candidate = candidates?.[findingIndex];

    const key = findingKey(
      observation.caseId,
      observation.variant.id,
      observation.trial,
      candidate?.id ?? findingIndex,
    );

    return {
      reference: EvalFindingReference.make({
        caseId: observation.caseId,
        caseVersion: observation.caseVersion,
        inputDigest: observation.inputDigest,
        variantId: observation.variant.id,
        trial: observation.trial,
        findingIndex,
        ...(candidate === undefined ? {} : { candidate, candidateId: candidate.id }),
        ...(observation.oracleDigest === undefined
          ? {}
          : { oracleDigest: observation.oracleDigest }),
        ...(observationDigests?.get(observationKey(observation)) === undefined
          ? {}
          : { observationDigest: observationDigests.get(observationKey(observation)) }),
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
  const firstMatched = matchedBlockingDefects(firstFindings.filter(isPublished), evalCase, true);

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
    firstMatched.size < expectedBlockers.length && unresolvedBlockingFindings.length > 0;

  const unresolvedDetectionFindings = firstFindings.filter(
    (finding) => finding.judgment === undefined || finding.judgment.label === "unclear",
  );

  const unmatchedBlockers = expectedBlockers.length - firstDetected.size;

  const firstDetectionUnresolved = unmatchedBlockers > 0 && unresolvedDetectionFindings.length > 0;

  const firstIncomplete =
    firstObservation.result._tag === "Succeeded" &&
    isIncompleteReview(firstObservation.result.outcome, evalCase);

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
    unresolvedBlockingFindings.length === 0 || firstMatched.size === expectedBlockers.length;

  if (firstTrialIsResolved) {
    for (const defect of expectedBlockers) {
      if (firstMatched.has(defect.id)) continue;
      for (let trial = 2; trial <= observations.length; trial += 1) {
        const trialMatched = matchedBlockingDefects(
          indexed.filter((finding) => finding.reference.trial === trial && isPublished(finding)),
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

  const trialStatus = (observation: EvalObservation) =>
    observation.result._tag === "Failed"
      ? "failed"
      : isIncompleteReview(observation.result.outcome, evalCase)
        ? "incomplete"
        : "complete";

  const operationalInstability = new Set(observations.map(trialStatus)).size > 1;

  const unresolvedRepeatedTrialInstability =
    observations.length > 1 &&
    !operationalInstability &&
    indexed.some(
      (finding) =>
        finding.judgment === undefined ||
        finding.judgment.label === "unclear" ||
        finding.judgment.label === "new-valid",
    );

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
            firstFindings.filter(isPublished).length === 0,
        }
      : {}),
    laterOnlyBlockingDefects,
    firstTrialFindings: firstQuality,
    allTrialFindings: findingQuality(indexed),
    firstTrialBlockingFindings: blockingFindingQuality(firstFindings.filter(isPublished), evalCase),
    discoveryMisses: firstDetectionUnresolved ? 0 : unmatchedBlockers,
    unresolvedDiscoveryMisses: firstDetectionUnresolved ? unmatchedBlockers : 0,
    validCandidatesRefuted: firstFindings.filter(
      (finding) => isValid(finding) && finding.reference.candidate?.disposition === "refuted",
    ).length,
    validCandidatesWithheld: firstFindings.filter(
      (finding) => isValid(finding) && !isPublished(finding),
    ).length,
    validBlockingCandidatesRefuted: firstFindings.filter(
      (finding) =>
        judgedSeverity(finding, evalCase) === "blocking" &&
        finding.reference.candidate?.disposition === "refuted",
    ).length,
    validBlockingCandidatesWithheld: firstFindings.filter(
      (finding) => judgedSeverity(finding, evalCase) === "blocking" && !isPublished(finding),
    ).length,
    firstTrialIncomplete: firstIncomplete,
    unresolvedRepeatedTrialInstability,
    repeatedTrialInstability:
      operationalInstability ||
      (!unresolvedRepeatedTrialInstability &&
        new Set(
          observations.map((observation) =>
            JSON.stringify({
              status: trialStatus(observation),
              blockers: [
                ...matchedBlockingDefects(
                  indexed.filter(
                    (finding) =>
                      finding.reference.trial === observation.trial && isPublished(finding),
                  ),
                  evalCase,
                  true,
                ),
              ].sort(),
              falseBlockers: blockingFindingQuality(
                indexed.filter(
                  (finding) =>
                    finding.reference.trial === observation.trial && isPublished(finding),
                ),
                evalCase,
              ).overstated,
            }),
          ),
        ).size > 1),
    ...(firstObservation.result._tag === "Failed"
      ? { firstTrialFailureTag: firstObservation.result.errorTag }
      : {}),
    resources: resourceSummary(observations, [evalCase]),
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

const rolloutDecision = (
  suite: EvalSuite,
  validated: ValidatedInputs,
  variants: ReadonlyArray<EvalVariantQualityReport>,
): EvalRolloutDecision => {
  const reasons = new Set<string>();

  const heldout = suite.cases.filter(
    (evalCase) => evalCase.split === "heldout" && evalCase.kind !== "unadjudicated",
  );

  const baseline = variants.find((variant) => variant.configuration.strategy === "baseline");
  const verified = variants.find((variant) => variant.configuration.strategy === "verified");

  if (suite.comparison === undefined) reasons.add("No frozen paired comparison is available.");
  if (baseline === undefined || verified === undefined)
    reasons.add("Both baseline and verified observations are required.");
  if (
    !heldout.some((evalCase) => evalCase.kind === "clean-control") ||
    !heldout.some((evalCase) =>
      evalCase.expectedDefects.some((defect) => defect.severity === "blocking"),
    )
  )
    reasons.add("Heldout first trials need independently established clean controls and blockers.");
  let baselineFalseBlockers = 0;
  let verifiedFalseBlockers = 0;

  if (baseline !== undefined && verified !== undefined) {
    for (const evalCase of heldout) {
      const left = validated.observations.get(trialKey(evalCase.id, baseline.configuration.id, 1));
      const right = validated.observations.get(trialKey(evalCase.id, verified.configuration.id, 1));

      if (left === undefined || right === undefined) {
        reasons.add("Heldout first trials are missing.");
        continue;
      }
      const leftFindings = indexFindings(left, validated.judgments);
      const rightFindings = indexFindings(right, validated.judgments);
      const all = [...leftFindings, ...rightFindings];

      if (
        all.some(
          (finding) => finding.judgment === undefined || finding.judgment.label === "unclear",
        )
      )
        reasons.add(
          "Resolve all heldout first-trial candidate judgments independently of verifier outcomes.",
        );
      if (all.some((finding) => finding.judgment?.label === "new-valid"))
        reasons.add(
          "New valid defects require a versioned oracle correction and rescoring both strategies.",
        );
      baselineFalseBlockers += blockingFindingQuality(
        leftFindings.filter(isPublished),
        evalCase,
      ).overstated;
      verifiedFalseBlockers += blockingFindingQuality(
        rightFindings.filter(isPublished),
        evalCase,
      ).overstated;
      const detected = matchedBlockingDefects(leftFindings, evalCase, false);
      const retained = matchedBlockingDefects(rightFindings.filter(isPublished), evalCase, false);
      const blocking = matchedBlockingDefects(leftFindings.filter(isPublished), evalCase, true);

      const retainedBlocking = matchedBlockingDefects(
        rightFindings.filter(isPublished),
        evalCase,
        true,
      );

      if ([...detected].some((id) => !retained.has(id)))
        reasons.add("Verification did not preserve every baseline-detected blocker.");
      if ([...blocking].some((id) => !retainedBlocking.has(id)))
        reasons.add("A baseline blocking finding was withheld or downgraded.");
      if (
        rightFindings.some(
          (finding) =>
            judgedSeverity(finding, evalCase) === "blocking" &&
            finding.reference.candidate?.disposition === "refuted",
        )
      )
        reasons.add("Verification wrongly refuted a valid blocking candidate.");

      const leftIncomplete =
        left.result._tag === "Failed" || isIncompleteReview(left.result.outcome, evalCase);

      const rightIncomplete =
        right.result._tag === "Failed" || isIncompleteReview(right.result.outcome, evalCase);

      if (!leftIncomplete && rightIncomplete)
        reasons.add("Verification added an incomplete or failed heldout first trial.");
      if (left.result._tag !== "Failed" && right.result._tag === "Failed")
        reasons.add("Verification added a failed heldout first trial.");
    }
  }
  if (verifiedFalseBlockers >= baselineFalseBlockers)
    reasons.add("Fewer false blocking findings have not been established.");

  return EvalRolloutDecision.make({
    decision: reasons.size === 0 ? "eligible" : "experimental",
    basis: "frozen-heldout-first-trials",
    heldoutCases: heldout.length,
    baselineFalseBlockers,
    verifiedFalseBlockers,
    reasons: [...reasons],
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
        for (const indexed of indexFindings(
          observation,
          validated.judgments,
          validated.observationDigests,
        )) {
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
        discoveryMisses: cases.reduce((sum, report) => sum + report.discoveryMisses, 0),
        unresolvedDiscoveryMisses: cases.reduce(
          (sum, report) => sum + report.unresolvedDiscoveryMisses,
          0,
        ),
        validCandidatesRefuted: cases.reduce(
          (sum, report) => sum + report.validCandidatesRefuted,
          0,
        ),
        validCandidatesWithheld: cases.reduce(
          (sum, report) => sum + report.validCandidatesWithheld,
          0,
        ),
        validBlockingCandidatesRefuted: cases.reduce(
          (sum, report) => sum + report.validBlockingCandidatesRefuted,
          0,
        ),
        validBlockingCandidatesWithheld: cases.reduce(
          (sum, report) => sum + report.validBlockingCandidatesWithheld,
          0,
        ),
        repeatedTrialInstability: cases.filter((report) => report.repeatedTrialInstability).length,
        unresolvedRepeatedTrialInstability: cases.filter(
          (report) => report.unresolvedRepeatedTrialInstability,
        ).length,
        resources: resourceSummary(variantObservations, validated.cases),
        cases,
      }),
    );
  }

  return EvalQualityReport.make({
    version: 5,
    observationSetDigest: validated.observationSetDigest,
    runnerVersion: validated.runnerVersion,
    trialCount: validated.trialCount,
    caseSet: validated.cases.map((evalCase) =>
      EvalCaseIdentity.make({
        id: evalCase.id,
        version: evalCase.version,
        inputDigest: evalCase.inputDigest,
        ...(evalCase.oracleDigest === undefined ? {} : { oracleDigest: evalCase.oracleDigest }),
        ...(evalCase.oracleVersion === undefined ? {} : { oracleVersion: evalCase.oracleVersion }),
        ...(evalCase.repository === undefined
          ? {}
          : { repositoryDigest: evalCase.repository.digest }),
      }),
    ),
    variants,
    unjudgedFindings,
    unmappedValidFindings,
    rollout: rolloutDecision(suite, validated, variants),
  });
});

/** Rebind a complete paired run to an explicitly versioned correction without replacing evidence. */
export const rescoreEvalObservations = Effect.fn("PrReviewEval.rescoreEvalObservations")(function* (
  previousSuite: EvalSuite,
  correctedCases: EvalSuite,
  observations: ReadonlyArray<EvalObservation>,
  correctionId: string,
) {
  const previous = yield* validateFrozenComparison(previousSuite);

  yield* validateInputs(previousSuite, observations, 3, undefined);
  if (correctedCases.cases.length !== previousSuite.cases.length)
    return yield* EvalReportError.make({
      message: "Oracle correction must retain the complete original case set",
    });
  let corrections = 0;

  for (const evalCase of correctedCases.cases) {
    const original = previousSuite.cases.find((entry) => entry.id === evalCase.id);

    if (
      original === undefined ||
      original.inputDigest !== evalCase.inputDigest ||
      original.repository?.digest !== evalCase.repository?.digest ||
      original.split !== evalCase.split ||
      original.relatedGroup !== evalCase.relatedGroup
    ) {
      return yield* EvalReportError.make({
        message:
          "Oracle correction cannot change request, source snapshot, split, or related group",
      });
    }
    const changed = (yield* digestEvalOracle(original)) !== (yield* digestEvalOracle(evalCase));

    if (changed) {
      if (
        evalCase.oracleVersion === undefined ||
        evalCase.oracleVersion <= (original.oracleVersion ?? 1)
      )
        return yield* EvalReportError.make({
          message: `Oracle correction for ${evalCase.id} requires a higher oracleVersion`,
        });
      corrections += 1;
    }
  }
  if (corrections === 0 || correctionId === previous.id)
    return yield* EvalReportError.make({
      message: "Use a new correction ID and at least one explicitly versioned oracle correction",
    });
  const suite = yield* freezeEvalSuite(correctedCases, previous.configurations, correctionId);
  const comparison = yield* validateFrozenComparison(suite);

  const rebound = yield* Effect.forEach(
    observations,
    Effect.fn("PrReviewEval.rebindOracle")(function* (observation) {
      const evalCase = suite.cases.find((entry) => entry.id === observation.caseId);

      if (evalCase === undefined)
        return yield* EvalReportError.make({ message: "Oracle correction lost an observed case" });

      return EvalObservation.make({
        ...observation,
        oracleVersion: evalCase.oracleVersion,
        oracleDigest: evalCase.oracleDigest,
        comparisonDigest: comparison.digest,
        previousObservationDigest: yield* digestObservation(observation),
      });
    }),
  );

  return { suite, observations: rebound };
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
        `blocker-cases-complete ${variant.blockerCases.complete}/${variant.blockerCases.total}`,
        `precision ${renderRate(quality.precision)}`,
        `blocking-precision ${renderRate(blockingQuality.precision)}`,
        `overstated-blocking ${blockingQuality.overstated} adjudicated`,
        `invalid ${quality.invalid} adjudicated`,
        `unclear ${quality.unclear}`,
        `unjudged ${quality.unjudged}`,
        `later-only ${variant.laterOnlyBlockingDefects.length} confirmed`,
        `discovery-misses ${variant.discoveryMisses} confirmed; ${variant.unresolvedDiscoveryMisses} awaiting judgment`,
        `valid-refuted ${variant.validCandidatesRefuted} confirmed`,
        `valid-withheld ${variant.validCandidatesWithheld} confirmed`,
        `unstable-cases ${variant.repeatedTrialInstability} confirmed; ${variant.unresolvedRepeatedTrialInstability} awaiting judgment`,
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
    .join("\n") + `\nRollout: ${report.rollout.decision}. ${report.rollout.reasons.join(" ")}`;
