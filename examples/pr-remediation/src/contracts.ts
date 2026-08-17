import {
  GitCommitSha,
  ReviewFindingId,
  ReviewHandoffDigest,
  ReviewHandoffEnvelope,
  ReviewRunOutcome,
} from "@effect-agent/pr-review";
import { Effect, Schema } from "effect";

export const WorkspacePath = Schema.NonEmptyString.check(Schema.isMaxLength(512));
export type WorkspacePath = typeof WorkspacePath.Type;

export const PatchDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)).pipe(
  Schema.brand("@effect-agent/example-pr-remediation/PatchDigest"),
);
export type PatchDigest = typeof PatchDigest.Type;

/** Authenticated host trigger; schemas validate shape, the host validates authority. */
export class RemediationTrigger extends Schema.Class<RemediationTrigger>(
  "@effect-agent/example-pr-remediation/RemediationTrigger",
)({
  version: Schema.Literal(1),
  triggerId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  label: Schema.Literal("pr-remediate"),
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  requestedHeadSha: GitCommitSha,
  trust: Schema.Literal("same-repository"),
}) {}

export class RemediationTriggerRejected extends Schema.TaggedError<RemediationTriggerRejected>()(
  "RemediationTriggerRejected",
  {
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class RemediationAttemptAlreadyClaimed extends Schema.TaggedError<RemediationAttemptAlreadyClaimed>()(
  "RemediationAttemptAlreadyClaimed",
  {
    repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
    pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
    reviewedHeadSha: GitCommitSha,
  },
) {}

export class NoRemediationFindings extends Schema.TaggedError<NoRemediationFindings>()(
  "NoRemediationFindings",
  {
    reviewedHeadSha: GitCommitSha,
  },
) {}

export class WorkspaceViolation extends Schema.TaggedError<WorkspaceViolation>()(
  "WorkspaceViolation",
  {
    path: Schema.String.check(Schema.isMaxLength(1_024)),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class WorkspaceOperationFailure extends Schema.TaggedError<WorkspaceOperationFailure>()(
  "WorkspaceOperationFailure",
  {
    operation: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
  },
) {}

export const CheckStatus = Schema.Literals(["passed", "failed"]);
export type CheckStatus = typeof CheckStatus.Type;

export class RemediationCheckResult extends Schema.Class<RemediationCheckResult>(
  "@effect-agent/example-pr-remediation/RemediationCheckResult",
)({
  name: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  status: CheckStatus,
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

export class PatchSnapshot extends Schema.Class<PatchSnapshot>(
  "@effect-agent/example-pr-remediation/PatchSnapshot",
)({
  digest: PatchDigest,
  changedPaths: Schema.Array(WorkspacePath).check(Schema.isMaxLength(100)),
  preview: Schema.String.check(Schema.isMaxLength(20_000)),
  truncated: Schema.Boolean,
}) {}

export const FindingResolutionStatus = Schema.Literals(["fixed", "not-applicable", "needs-human"]);
export type FindingResolutionStatus = typeof FindingResolutionStatus.Type;

export class FindingResolution extends Schema.Class<FindingResolution>(
  "@effect-agent/example-pr-remediation/FindingResolution",
)({
  findingId: ReviewFindingId,
  status: FindingResolutionStatus,
  rationale: Schema.NonEmptyString.check(Schema.isMaxLength(1_000)),
}) {}

/**
 * Schema-validated model settlement. It names the host-collected patch only
 * by digest; the host independently recollects and validates it.
 */
export class RemediationReport extends Schema.Class<RemediationReport>(
  "@effect-agent/example-pr-remediation/RemediationReport",
)({
  handoffDigest: ReviewHandoffDigest,
  reviewedHeadSha: GitCommitSha,
  resolutions: Schema.Array(FindingResolution).check(Schema.isMaxLength(20)),
  changedPaths: Schema.Array(WorkspacePath).check(Schema.isMaxLength(100)),
  checks: Schema.Array(RemediationCheckResult).check(Schema.isMaxLength(20)),
  patchDigest: PatchDigest,
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

export const RemediationValidationReason = Schema.Literals([
  "handoff-digest-mismatch",
  "reviewed-head-mismatch",
  "finding-accounting-mismatch",
  "finding-needs-human",
  "changed-paths-mismatch",
  "path-not-allowed",
  "patch-digest-mismatch",
  "check-results-mismatch",
  "empty-patch",
  "check-mutated-patch",
]);

export class RemediationValidationFailure extends Schema.TaggedError<RemediationValidationFailure>()(
  "RemediationValidationFailure",
  {
    reason: RemediationValidationReason,
    detail: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class RequiredCheckFailed extends Schema.TaggedError<RequiredCheckFailed>()(
  "RequiredCheckFailed",
  {
    check: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
    summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
  },
) {}

export class StalePullRequestHead extends Schema.TaggedError<StalePullRequestHead>()(
  "StalePullRequestHead",
  {
    expected: GitCommitSha,
    actual: GitCommitSha,
  },
) {}

export class PublishedRemediation extends Schema.Class<PublishedRemediation>(
  "@effect-agent/example-pr-remediation/PublishedRemediation",
)({
  previousHeadSha: GitCommitSha,
  publishedHeadSha: GitCommitSha,
  patchDigest: PatchDigest,
  changedPaths: Schema.Array(WorkspacePath).check(Schema.isMaxLength(100)),
  checks: Schema.Array(RemediationCheckResult).check(Schema.isMaxLength(20)),
}) {}

export class PrRemediationLoopOutcome extends Schema.Class<PrRemediationLoopOutcome>(
  "@effect-agent/example-pr-remediation/PrRemediationLoopOutcome",
)({
  initialReview: ReviewRunOutcome,
  handoff: ReviewHandoffEnvelope,
  remediation: RemediationReport,
  publication: PublishedRemediation,
  freshReview: ReviewRunOutcome,
}) {}

/** Reject path traversal and normalization ambiguity; never silently repair it. */
export const normalizeWorkspacePath = (
  path: string,
): Effect.Effect<WorkspacePath, WorkspaceViolation> => {
  const fail = (reason: string) => WorkspaceViolation.make({ path, reason });
  if (path.length === 0 || path.length > 512) {
    return Effect.fail(fail("path length is out of bounds"));
  }
  const hasControlCharacter = [...path].some((character) => {
    const code = character.charCodeAt(0);
    return code <= 31 || code === 127;
  });
  if (hasControlCharacter || path.includes("\\")) {
    return Effect.fail(fail("path contains a forbidden character"));
  }
  if (path.startsWith("/") || /^[A-Za-z]:/.test(path)) {
    return Effect.fail(fail("path must be repository-relative"));
  }
  const segments = path.split("/");
  if (segments.some((segment) => segment === "" || segment === "." || segment === "..")) {
    return Effect.fail(fail("path segments must not be empty, '.' or '..'"));
  }
  return Schema.decodeUnknownEffect(WorkspacePath)(segments.join("/")).pipe(
    Effect.mapError(() => fail("path is outside the supported workspace path schema")),
  );
};
