import {
  ReviewDiagnostics,
  ReviewOutcome,
  ReviewRequest,
  ReviewSeverity,
  ReviewStrategy,
} from "@effect-agent/pr-review";
import { Schema } from "effect";

const BoundedIdentifier = Schema.NonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[a-z0-9][a-z0-9._-]*$/),
);

const BoundedText = Schema.NonEmptyString.check(Schema.isMaxLength(4_096));
const BoundedPath = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));

export const EvalRunnerVersion = Schema.String.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^\d+\.\d+\.\d+(?:-[0-9a-z.-]+)?$/),
);

export type EvalRunnerVersion = typeof EvalRunnerVersion.Type;

export const CURRENT_RUNNER_VERSION = Schema.decodeSync(EvalRunnerVersion)("0.3.0");

export const EvalCaseId = BoundedIdentifier.pipe(
  Schema.brand("@effect-agent/example-pr-review-eval/EvalCaseId"),
);

export type EvalCaseId = typeof EvalCaseId.Type;

export const EvalTrialCount = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 }));

export const EvalDefectId = BoundedIdentifier.pipe(
  Schema.brand("@effect-agent/example-pr-review-eval/EvalDefectId"),
);

export type EvalDefectId = typeof EvalDefectId.Type;

export const EvalInputDigest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)).pipe(
  Schema.brand("@effect-agent/example-pr-review-eval/EvalInputDigest"),
);

export type EvalInputDigest = typeof EvalInputDigest.Type;

export const EvalObservationSetDigest = Schema.String.check(
  Schema.isPattern(/^[a-f0-9]{64}$/),
).pipe(Schema.brand("@effect-agent/example-pr-review-eval/EvalObservationSetDigest"));

export type EvalObservationSetDigest = typeof EvalObservationSetDigest.Type;

export const EvalVariantId = BoundedIdentifier;
export type EvalVariantId = typeof EvalVariantId.Type;

export class EvalEvidence extends Schema.Class<EvalEvidence>(
  "@effect-agent/example-pr-review-eval/EvalEvidence",
)({
  path: BoundedPath,
  line: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  description: BoundedText,
}) {}

export class EvalExpectedDefect extends Schema.Class<EvalExpectedDefect>(
  "@effect-agent/example-pr-review-eval/EvalExpectedDefect",
)({
  id: EvalDefectId,
  severity: ReviewSeverity,
  invariant: BoundedText,
  evidence: Schema.Array(EvalEvidence).check(Schema.isMinLength(1), Schema.isMaxLength(20)),
}) {}

export class EvalRepositoryFile extends Schema.Class<EvalRepositoryFile>(
  "@effect-agent/example-pr-review-eval/EvalRepositoryFile",
)({
  path: BoundedPath,
  revision: Schema.Literals(["base", "head"]),
  content: Schema.String.check(Schema.isMaxLength(512_000)),
}) {}

const EvalRepositorySnapshotUnsigned = Schema.Struct({
  version: Schema.Literal(1),
  files: Schema.Array(EvalRepositoryFile).check(Schema.isMaxLength(200)),
});

export class EvalRepositorySnapshot extends Schema.Class<EvalRepositorySnapshot>(
  "@effect-agent/example-pr-review-eval/EvalRepositorySnapshot",
)({
  ...EvalRepositorySnapshotUnsigned.fields,
  digest: EvalInputDigest,
}) {}

export const EvalCaseKind = Schema.Literals(["known-defects", "clean-control", "unadjudicated"]);
export type EvalCaseKind = typeof EvalCaseKind.Type;
export const EvalSplit = Schema.Literals(["development", "heldout"]);

const EvalCaseFields = Schema.Struct({
  version: Schema.Literal(1),
  id: EvalCaseId,
  kind: EvalCaseKind,
  provenance: BoundedText,
  sourceUrl: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(2_048))),
  inputDigest: EvalInputDigest,
  request: ReviewRequest,
  repository: Schema.optionalKey(EvalRepositorySnapshot),
  expectedDefects: Schema.Array(EvalExpectedDefect).check(Schema.isMaxLength(12)),
  oracleVersion: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  oracleDigest: Schema.optionalKey(EvalInputDigest),
  split: Schema.optionalKey(EvalSplit),
  relatedGroup: Schema.optionalKey(BoundedIdentifier),
}).check(
  Schema.makeFilter(
    (evalCase) => {
      const defectIds = evalCase.expectedDefects.map((defect) => defect.id);

      const repositoryKeys = (evalCase.repository?.files ?? []).map(
        (file) => `${file.revision}\0${file.path}`,
      );

      const admittedPaths = new Set(evalCase.request.changes.map((change) => change.path));

      return (
        new Set(defectIds).size === defectIds.length &&
        new Set(repositoryKeys).size === repositoryKeys.length &&
        (evalCase.kind === "known-defects"
          ? evalCase.expectedDefects.length > 0
          : evalCase.expectedDefects.length === 0) &&
        evalCase.expectedDefects.every((defect) =>
          defect.evidence.every((evidence) => admittedPaths.has(evidence.path)),
        )
      );
    },
    {
      title:
        "Defect IDs and repository files are unique, case kind matches expected defects, and evidence uses admitted paths",
    },
  ),
);

export class EvalCase extends Schema.Class<EvalCase>(
  "@effect-agent/example-pr-review-eval/EvalCase",
)(EvalCaseFields) {}

export const EvalReasoningEffort = Schema.Literals(["low", "medium", "high", "xhigh"]);
export type EvalReasoningEffort = typeof EvalReasoningEffort.Type;

export class EvalEffectiveConfiguration extends Schema.Class<EvalEffectiveConfiguration>(
  "@effect-agent/example-pr-review-eval/EvalEffectiveConfiguration",
)({
  reviewerRevision: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/))),
  discoveryPromptDigest: EvalInputDigest,
  verificationPromptDigest: EvalInputDigest,
  serviceTier: Schema.Literal("default"),
  discoveryLimitMicrousd: Schema.Natural,
  verificationReserveMicrousd: Schema.Natural,
  maxInputTokens: Schema.Natural,
  maxTurns: Schema.Natural,
  maxToolCalls: Schema.Natural,
  maxDurationMillis: Schema.Natural,
  contextTokenLimit: Schema.Natural,
  completionReserveTokens: Schema.Natural,
  candidateCapacity: Schema.Natural,
  patchBatchCharacters: Schema.Natural,
  pricingVersion: BoundedIdentifier,
  pricingValidUntil: Schema.Natural,
  pricing: Schema.Struct({
    input: Schema.Natural,
    read: Schema.Natural,
    write: Schema.Natural,
    output: Schema.Natural,
  }),
  cache: Schema.Struct({
    mode: Schema.Literal("explicit"),
    ttl: Schema.Literal("30m"),
    namespacePolicy: Schema.Literal("isolated-case-strategy-trial-v1"),
  }),
}) {}

export class EvalVariantConfiguration extends Schema.Class<EvalVariantConfiguration>(
  "@effect-agent/example-pr-review-eval/EvalVariantConfiguration",
)({
  id: EvalVariantId,
  reviewerProfile: BoundedIdentifier,
  provider: Schema.Literal("openai"),
  model: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  reasoningEffort: EvalReasoningEffort,
  maxOutputTokens: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100_000 })),
  strictJsonSchema: Schema.Literal(true),
  store: Schema.Literal(false),
  costLimitMicrousd: Schema.optionalKey(Schema.Natural),
  guidanceDigest: Schema.optionalKey(EvalInputDigest),
  strategy: Schema.optionalKey(ReviewStrategy),
  effective: Schema.optionalKey(EvalEffectiveConfiguration),
}) {}

export class EvalFrozenCaseIdentity extends Schema.Class<EvalFrozenCaseIdentity>(
  "@effect-agent/example-pr-review-eval/EvalFrozenCaseIdentity",
)({
  id: EvalCaseId,
  inputDigest: EvalInputDigest,
  repositoryDigest: EvalInputDigest,
  oracleVersion: Schema.Int.check(Schema.isGreaterThan(0)),
  oracleDigest: EvalInputDigest,
  split: EvalSplit,
  relatedGroup: BoundedIdentifier,
}) {}

export const EvalComparisonPayload = Schema.Struct({
  version: Schema.Literal(1),
  id: BoundedIdentifier,
  cases: Schema.Array(EvalFrozenCaseIdentity).check(Schema.isMinLength(1), Schema.isMaxLength(20)),
  configurations: Schema.Array(EvalVariantConfiguration).check(Schema.isLengthBetween(2, 2)),
  trials: Schema.Literal(3),
  plannedRuns: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 120 })),
  maximumCostMicrousd: Schema.Natural.check(Schema.isLessThanOrEqualTo(119_999_880)),
  order: Schema.Literal("alternating-pairs-v1"),
});

export class EvalComparison extends Schema.Class<EvalComparison>(
  "@effect-agent/example-pr-review-eval/EvalComparison",
)({ ...EvalComparisonPayload.fields, digest: EvalInputDigest }) {}

const EvalSuiteFields = Schema.Struct({
  version: Schema.Literal(1),
  cases: Schema.Array(EvalCase).check(Schema.isMinLength(1), Schema.isMaxLength(50)),
  comparison: Schema.optionalKey(EvalComparison),
}).check(
  Schema.makeFilter(
    (suite) => new Set(suite.cases.map((evalCase) => evalCase.id)).size === suite.cases.length,
    { title: "Eval case IDs are unique" },
  ),
);

export class EvalSuite extends Schema.Class<EvalSuite>(
  "@effect-agent/example-pr-review-eval/EvalSuite",
)(EvalSuiteFields) {}

export class EvalReviewerFailure extends Schema.TaggedError<EvalReviewerFailure>()(
  "EvalReviewerFailure",
  {
    errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
    message: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
    estimatedCostMicrousd: Schema.optionalKey(Schema.Natural),
    /** Maximum additional charge for unsettled requests; absence means unavailable. */
    reservedCostMicrousd: Schema.optionalKey(Schema.Natural),
    diagnostics: Schema.optionalKey(ReviewDiagnostics),
  },
) {}

/** The invocation returned an outcome; its incomplete/exhausted flags still limit review coverage. */
export class EvalTrialSucceeded extends Schema.TaggedClass<EvalTrialSucceeded>()("Succeeded", {
  outcome: ReviewOutcome,
}) {}

export class EvalTrialFailed extends Schema.TaggedClass<EvalTrialFailed>()("Failed", {
  errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  message: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
  estimatedCostMicrousd: Schema.optionalKey(Schema.Natural),
  /** Maximum additional charge for unsettled requests; absence means unavailable. */
  reservedCostMicrousd: Schema.optionalKey(Schema.Natural),
  diagnostics: Schema.optionalKey(ReviewDiagnostics),
}) {}

export const EvalTrialResult = Schema.Union([EvalTrialSucceeded, EvalTrialFailed]);
export type EvalTrialResult = typeof EvalTrialResult.Type;

export class EvalObservation extends Schema.Class<EvalObservation>(
  "@effect-agent/example-pr-review-eval/EvalObservation",
)({
  version: Schema.Literal(1),
  runnerVersion: EvalRunnerVersion,
  caseId: EvalCaseId,
  caseVersion: Schema.Literal(1),
  inputDigest: EvalInputDigest,
  repositoryDigest: Schema.optionalKey(EvalInputDigest),
  oracleVersion: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  oracleDigest: Schema.optionalKey(EvalInputDigest),
  comparisonDigest: Schema.optionalKey(EvalInputDigest),
  previousObservationDigest: Schema.optionalKey(EvalInputDigest),
  runId: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(128))),
  cacheNamespace: Schema.optionalKey(EvalInputDigest),
  sequence: Schema.optionalKey(Schema.Natural),
  variant: EvalVariantConfiguration,
  trial: Schema.Int.check(Schema.isGreaterThan(0)),
  recordedAt: Schema.DateTimeUtcFromString,
  elapsedMillis: Schema.Natural,
  result: EvalTrialResult,
}) {}

export class EvalDataError extends Schema.TaggedError<EvalDataError>()("EvalDataError", {
  operation: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  path: Schema.optionalKey(BoundedPath),
  message: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export class EvalConfigurationError extends Schema.TaggedError<EvalConfigurationError>()(
  "EvalConfigurationError",
  {
    message: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
  },
) {}
