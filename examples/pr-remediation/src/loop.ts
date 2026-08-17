import {
  computeReviewHandoffDigest,
  GitCommitSha,
  makeReviewHandoff,
  ReviewHandoffAuthenticator,
  type ReviewRunOutcome,
  type PullRequestMetadata,
  type ChangedFile,
} from "@effect-agent/pr-review";
import { Effect, Schema } from "effect";

import {
  NoRemediationFindings,
  PrRemediationLoopOutcome,
  type RemediationCheckResult,
  RemediationValidationFailure,
  RequiredCheckFailed,
  StalePullRequestHead,
  type RemediationReport,
  type RemediationTrigger,
} from "./contracts.ts";
import { RemediationMission } from "./implementation-agent.ts";
import {
  type ImplementationWorkspace,
  RemediationAttemptPolicy,
  RemediationHost,
} from "./workspace.ts";

export interface ReviewEvidence {
  readonly outcome: ReviewRunOutcome;
  readonly metadata: PullRequestMetadata;
  readonly files: ReadonlyArray<ChangedFile>;
  readonly profileFingerprint: string;
}

const sorted = (values: Iterable<string>): ReadonlyArray<string> =>
  [...values].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));

const exactMultiset = (left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean =>
  left.length === right.length && sorted(left).join("\0") === sorted(right).join("\0");

const exactChecks = (
  reported: ReadonlyArray<RemediationCheckResult>,
  observed: ReadonlyArray<RemediationCheckResult>,
): boolean =>
  reported.length === observed.length &&
  reported.every((check, index) => {
    const actual = observed[index];
    return (
      actual !== undefined &&
      check.name === actual.name &&
      check.status === actual.status &&
      check.summary === actual.summary
    );
  });

/**
 * Deterministic host workflow: review H0, authenticate the handoff, run one
 * distinct implementation Agent in one Scope, validate and atomically
 * publish, then invoke a fresh reviewer exactly once at H1.
 */
export const runPrRemediationLoop = <
  ReviewError,
  ReviewRequirements,
  ImplementError,
  ImplementRequirements,
>(options: {
  readonly trigger: RemediationTrigger;
  readonly review: () => Effect.Effect<ReviewEvidence, ReviewError, ReviewRequirements>;
  readonly implement: (
    mission: RemediationMission,
    workspace: ImplementationWorkspace,
  ) => Effect.Effect<RemediationReport, ImplementError, ImplementRequirements>;
}) =>
  Effect.gen(function* () {
    const host = yield* RemediationHost;
    const attempts = yield* RemediationAttemptPolicy;
    const authenticator = yield* ReviewHandoffAuthenticator;

    yield* host.authorizeTrigger(options.trigger);
    const initial = yield* options.review();
    if (
      initial.metadata.repository !== options.trigger.repository ||
      initial.metadata.number !== options.trigger.pullRequestNumber
    ) {
      return yield* RemediationValidationFailure.make({
        reason: "reviewed-head-mismatch",
        detail: "review evidence belongs to a different repository or pull request",
      });
    }
    const initialHead = yield* Schema.decodeUnknownEffect(GitCommitSha)(
      initial.metadata.headSha,
    ).pipe(
      Effect.mapError(() =>
        RemediationValidationFailure.make({
          reason: "reviewed-head-mismatch",
          detail: "review source returned an invalid commit SHA",
        }),
      ),
    );
    if (initialHead !== options.trigger.requestedHeadSha) {
      return yield* StalePullRequestHead.make({
        expected: options.trigger.requestedHeadSha,
        actual: initialHead,
      });
    }
    const built = yield* makeReviewHandoff(initial);
    const envelope = yield* authenticator.sign(built);
    const handoff = yield* authenticator.verify(envelope);
    const handoffDigest = yield* computeReviewHandoffDigest(handoff);
    if (handoff.findings.length === 0) {
      return yield* NoRemediationFindings.make({ reviewedHeadSha: handoff.reviewedHeadSha });
    }
    yield* attempts.claim(handoff);

    const { publication, report } = yield* host.withWorktree(handoff, (worktree) =>
      Effect.gen(function* () {
        const report = yield* options.implement(
          RemediationMission.make({
            handoff,
            handoffDigest,
            requiredChecks: host.requiredChecks,
          }),
          worktree.modelWorkspace,
        );
        const observedChecks = yield* worktree.observedChecks;
        if (!exactChecks(report.checks, observedChecks)) {
          return yield* RemediationValidationFailure.make({
            reason: "check-results-mismatch",
            detail: "model-reported checks differ from host-observed check capability results",
          });
        }
        if (report.handoffDigest !== handoffDigest) {
          return yield* RemediationValidationFailure.make({
            reason: "handoff-digest-mismatch",
            detail: "implementation report does not name the authenticated handoff",
          });
        }
        if (report.reviewedHeadSha !== handoff.reviewedHeadSha) {
          return yield* RemediationValidationFailure.make({
            reason: "reviewed-head-mismatch",
            detail: "implementation report targets a different reviewed head",
          });
        }
        const expectedFindingIds = handoff.findings.map((finding) => finding.id);
        const reportedFindingIds = report.resolutions.map((resolution) => resolution.findingId);
        if (!exactMultiset(expectedFindingIds, reportedFindingIds)) {
          return yield* RemediationValidationFailure.make({
            reason: "finding-accounting-mismatch",
            detail: "implementation report must account for every finding exactly once",
          });
        }
        if (report.resolutions.some((resolution) => resolution.status === "needs-human")) {
          return yield* RemediationValidationFailure.make({
            reason: "finding-needs-human",
            detail: "a finding requires human resolution; no commit may be published",
          });
        }

        const patch = yield* worktree.inspectPatch;
        if (patch.changedPaths.length === 0) {
          return yield* RemediationValidationFailure.make({
            reason: "empty-patch",
            detail: "implementation produced no host-visible patch",
          });
        }
        if (patch.changedPaths.some((path) => !worktree.allowedPaths.has(path))) {
          return yield* RemediationValidationFailure.make({
            reason: "path-not-allowed",
            detail: "host-collected patch contains a path outside the remediation allowlist",
          });
        }
        if (!exactMultiset(report.changedPaths, patch.changedPaths)) {
          return yield* RemediationValidationFailure.make({
            reason: "changed-paths-mismatch",
            detail: "model-reported changed paths differ from the host-collected patch",
          });
        }
        if (report.patchDigest !== patch.digest) {
          return yield* RemediationValidationFailure.make({
            reason: "patch-digest-mismatch",
            detail: "model-reported patch digest differs from the host-collected patch",
          });
        }

        const checkResults = yield* Effect.forEach(host.requiredChecks, (name) =>
          worktree.runCheck(name),
        );
        const failed = checkResults.find((check) => check.status === "failed");
        if (failed !== undefined) {
          return yield* RequiredCheckFailed.make({ check: failed.name, summary: failed.summary });
        }
        const afterChecks = yield* worktree.inspectPatch;
        if (
          afterChecks.digest !== patch.digest ||
          !exactMultiset(afterChecks.changedPaths, patch.changedPaths)
        ) {
          return yield* RemediationValidationFailure.make({
            reason: "check-mutated-patch",
            detail: "a required check changed the patch after model settlement",
          });
        }
        const publication = yield* worktree.commitAndPublish({
          handoff,
          report,
          patch: afterChecks,
          checks: checkResults,
        });
        return { publication, report };
      }),
    );

    const fresh = yield* options.review();
    if (
      fresh.metadata.repository !== options.trigger.repository ||
      fresh.metadata.number !== options.trigger.pullRequestNumber ||
      fresh.metadata.headSha !== publication.publishedHeadSha ||
      fresh.outcome.plan.commitSha !== publication.publishedHeadSha
    ) {
      return yield* RemediationValidationFailure.make({
        reason: "reviewed-head-mismatch",
        detail: "fresh reviewer did not observe the host-published pull-request head",
      });
    }
    return PrRemediationLoopOutcome.make({
      initialReview: initial.outcome,
      handoff: envelope,
      remediation: report,
      publication,
      freshReview: fresh.outcome,
    });
  });
