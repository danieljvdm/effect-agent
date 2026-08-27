import { ReviewOutcome, ReviewRequest, ReviewSeverity } from "@effect-agent/pr-review";
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

export const CURRENT_RUNNER_VERSION = Schema.decodeSync(EvalRunnerVersion)("0.1.1");
export const CURRENT_REVIEWER_PROFILE = "source-review-v9";

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
  content: Schema.String.check(Schema.isMaxLength(200_000)),
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

export const EvalCaseKind = Schema.Literals(["known-defects", "clean-control"]);
export type EvalCaseKind = typeof EvalCaseKind.Type;

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
        (evalCase.kind === "clean-control"
          ? evalCase.expectedDefects.length === 0
          : evalCase.expectedDefects.length > 0) &&
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

const EvalSuiteFields = Schema.Struct({
  version: Schema.Literal(1),
  cases: Schema.Array(EvalCase).check(Schema.isMinLength(1), Schema.isMaxLength(50)),
}).check(
  Schema.makeFilter(
    (suite) => new Set(suite.cases.map((evalCase) => evalCase.id)).size === suite.cases.length,
    { title: "Eval case IDs are unique" },
  ),
);

export class EvalSuite extends Schema.Class<EvalSuite>(
  "@effect-agent/example-pr-review-eval/EvalSuite",
)(EvalSuiteFields) {}

export const EvalReasoningEffort = Schema.Literals(["low", "medium", "high", "xhigh", "max"]);
export type EvalReasoningEffort = typeof EvalReasoningEffort.Type;

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
  guidanceDigest: Schema.optionalKey(EvalInputDigest),
}) {}

export class EvalReviewerFailure extends Schema.TaggedError<EvalReviewerFailure>()(
  "EvalReviewerFailure",
  {
    errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
    message: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
    estimatedCostMicrousd: Schema.optionalKey(Schema.Natural),
  },
) {}

export class EvalTrialSucceeded extends Schema.TaggedClass<EvalTrialSucceeded>()("Succeeded", {
  outcome: ReviewOutcome,
}) {}

export class EvalTrialFailed extends Schema.TaggedClass<EvalTrialFailed>()("Failed", {
  errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  message: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
  estimatedCostMicrousd: Schema.optionalKey(Schema.Natural),
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
