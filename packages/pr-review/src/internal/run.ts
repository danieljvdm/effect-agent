import { Effect, Schema } from "effect";
import {
  makeUsageBudget,
  toRunBudgetHook,
  UsageBudgetLimits,
  UsageTotals,
  AgentRuntime,
  type RuntimeBinding,
} from "effect-agent";
import { type Tool } from "effect/unstable/ai";

import {
  collectReviewAdjudications,
  renderAdjudicationContextLine,
  renderPriorFindingContextLine,
  buildPriorReviewContext,
} from "./adjudication.ts";
import {
  assessFlatReview,
  fanOutInputCoverage,
  ReviewAssurance,
  ReviewInputCoverage,
} from "./coverage.ts";
import type { ChangedFile } from "./diff.ts";
import { runFanOutReview, type FileReviewerBinding } from "./fan-out.ts";
import { computeChangesetFingerprint } from "./fingerprint.ts";
import { PublishedReview, ReviewPublisher } from "./github.ts";
import { planPublication, ReviewPublicationPlan } from "./render.ts";
import {
  clampMaxFindings,
  CodeReview,
  ReviewConcern,
  ReviewFinding,
  ReviewMission,
} from "./review-agent.ts";
import {
  adjudicationIdentity,
  concernIdentity,
  findingIdentity,
  fromStoredConcern,
  fromStoredFinding,
  MAX_STORED_UNREVIEWED_PASSES,
  MAX_STORED_UNREVIEWED_PATHS,
  ReviewExecutionContext,
  ReviewState,
  StoredAdjudication,
  StoredUnreviewedPass,
  toStoredConcern,
  toStoredFinding,
} from "./review-state.ts";
import { rankAndDedupeConcerns, rankAndDedupeFindings, reviewConcernKey } from "./review-units.ts";
import { PullRequestSource, type PullRequestMetadata } from "./source.ts";

// ---------------------------------------------------------------------------
// One review run, end to end: read the pull request, run the bounded review
// (one flat agent, or the host-scheduled fan-out pipeline), validate the
// review against the real diff, then (optionally) publish. Publication
// happens strictly AFTER all model work so no model turn can observe or
// influence the mutation, and a failed run publishes nothing.
//
// Continuity is monotone: every completed run that can be signed advances the
// stored baseline, carrying genuinely-unsettled scope forward explicitly. A
// flaky pass therefore costs exactly its own scope on the next run — it can
// never freeze the baseline and reopen everything reviewed since.
// ---------------------------------------------------------------------------

/**
 * Run-level usage bounds on top of the definition's AgentPolicy. Real diffs
 * are token-heavy, so the input budget is research-sized with cost as the
 * safety net.
 */
export const reviewBudgetLimits = UsageBudgetLimits.make({
  maxInputTokens: 400_000,
  maxOutputTokens: 16_000,
  maxToolCalls: 24,
  maxCostMicrousd: 2_000_000,
  maxDurationMillis: 480_000,
});

/**
 * Run-level bounds for the fan-out pipeline. One budget observes EVERY child
 * pass, so the ceiling covers bounded parallel discovery and verification
 * plus the one-retry allowance.
 */
export const fanOutReviewBudgetLimits = UsageBudgetLimits.make({
  maxInputTokens: 600_000,
  maxOutputTokens: 32_000,
  maxToolCalls: 32,
  maxCostMicrousd: 2_000_000,
  maxDurationMillis: 1_200_000,
});

/** Everything one review run produced, publication receipt included. */
export class ReviewRunOutcome extends Schema.Class<ReviewRunOutcome>(
  "@effect-agent/pr-review/ReviewRunOutcome",
)({
  review: CodeReview,
  /** All currently unresolved findings, including unchanged carried scope. */
  activeFindings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(20)),
  /** All currently unresolved concerns, including concerns carried to final audit. */
  activeConcerns: Schema.Array(ReviewConcern).check(Schema.isMaxLength(10)),
  /** Exact path/evidence assignment, distinct from semantic review work. */
  inputCoverage: ReviewInputCoverage,
  /** Settlement of scheduled discovery, specialist, and verification work. */
  assurance: ReviewAssurance,
  /** Retryable scope this run could not settle; carried to the next run. */
  unreviewedPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  plan: ReviewPublicationPlan,
  published: Schema.optionalKey(PublishedReview),
  /** Total settled model turns (all child passes for the fan-out pipeline). */
  turns: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** The run budget's observed usage across the whole run. */
  usage: Schema.optionalKey(UsageTotals),
  reviewMode: Schema.optionalKey(Schema.Literals(["incremental", "full"])),
  reviewReason: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(1_000))),
  state: Schema.optionalKey(ReviewState),
  /** Maintainer adjudications standing against this run's identities. */
  adjudications: Schema.optionalKey(Schema.Array(StoredAdjudication).check(Schema.isMaxLength(20))),
}) {}

export interface ExecuteReviewOptions {
  /** Post the review to GitHub; `false` stops after planning (dry run). */
  readonly post: boolean;
  /** Map the model's verdict onto APPROVE/REQUEST_CHANGES instead of COMMENT. */
  readonly applyVerdict: boolean;
  /** Run-level usage bounds; defaults to the shape's packaged limits. */
  readonly limits?: UsageBudgetLimits | undefined;
  /**
   * Host-side findings bound (fail-closed backstop for the instruction-level
   * bound): a review carrying more findings is ranked by severity, deduped by
   * anchor, and trimmed — never published oversized. Clamped to the schema cap.
   */
  readonly maxFindings?: number | undefined;
  /**
   * Prompt signature for changeset fingerprinting. When present, the
   * changeset fingerprint is computed and embedded invisibly in the review
   * body so later runs can skip an unchanged changeset.
   */
  readonly signature?: ((mission: ReviewMission) => string) | undefined;
  /** Provider binding descriptor rendered into the review footer. */
  readonly modelLabel?: string | undefined;
  /** Workflow-run URL rendered into the review footer. */
  readonly runUrl?: string | undefined;
}

/**
 * Build the mission one review run frames from the source's snapshot. The
 * optional continuity context (adjudicated identities, prior-round findings
 * on re-reviewed scope) reaches only RUN missions — fingerprint missions stay
 * plain so an adjudication never invalidates skip-unchanged authority.
 */
export const buildReviewMission = (
  metadata: PullRequestMetadata,
  files: ReadonlyArray<ChangedFile>,
  context?: {
    readonly adjudicated?: ReadonlyArray<string> | undefined;
    readonly priorFindings?: ReadonlyArray<string> | undefined;
  },
): ReviewMission =>
  ReviewMission.make({
    repository: metadata.repository,
    number: metadata.number,
    title: metadata.title,
    body: metadata.body,
    baseRef: metadata.baseRef,
    headRef: metadata.headRef,
    changedFileCount: files.length,
    ...(context?.adjudicated !== undefined && context.adjudicated.length > 0
      ? { adjudicatedContext: context.adjudicated.slice(0, 20) }
      : {}),
    ...(context?.priorFindings !== undefined && context.priorFindings.length > 0
      ? { priorFindingContext: context.priorFindings.slice(0, 20) }
      : {}),
  });

/** Enforce the configured findings bound on an already-validated review. */
export const enforceFindingsBound = (review: CodeReview, maxFindings: number): CodeReview =>
  review.findings.length <= maxFindings
    ? review
    : CodeReview.make({
        summary: review.summary,
        verdict: review.verdict,
        findings: rankAndDedupeFindings(review.findings).slice(0, maxFindings),
        ...(review.concerns !== undefined ? { concerns: review.concerns } : {}),
        ...(review.walkthrough !== undefined ? { walkthrough: review.walkthrough } : {}),
      });

const findingKey = (finding: ReviewFinding): string =>
  `${finding.path}\u0000${finding.startLine}\u0000${finding.endLine}\u0000${finding.severity}\u0000${finding.title}`;

/**
 * Continuity inputs resolved BEFORE any model work: the standing maintainer
 * adjudications (fresh host listing merged later-wins over the prior state's
 * stored set) and the prior-round findings whose paths this run re-reviews.
 * The latter are dropped from the carry (the new round re-decides them) but
 * injected as prompt context so successive rounds do not silently contradict
 * each other — context ONLY, never auto-carried into active findings.
 */
const resolveReviewContinuityContext = Effect.fn("resolveReviewContinuityContext")(function* () {
  const executionContext = yield* ReviewExecutionContext;
  const priorState =
    executionContext.mode === "incremental" ? executionContext.priorState : undefined;
  const adjudications = yield* collectReviewAdjudications(priorState?.adjudications ?? []);
  const affectedPaths = new Set(executionContext.affectedPaths);
  const priorFindingsOnScope =
    priorState?.unresolvedFindings.filter((finding) => affectedPaths.has(finding.path)) ?? [];
  return { adjudications, priorFindingsOnScope };
});

/** One shape-specific review result, before the shared settlement tail. */
interface ReviewCore {
  readonly review: CodeReview;
  readonly inputCoverage: ReviewInputCoverage;
  readonly assurance: ReviewAssurance;
  readonly unreviewedPaths: ReadonlyArray<string>;
  readonly unreviewedPasses?: ReadonlyArray<StoredUnreviewedPass> | undefined;
  readonly turns: number;
}

/**
 * The shared settlement tail: carry unchanged prior scope, decide whether
 * this run's continuity state can be signed, plan the exact publication, and
 * (optionally) post it. Continuity requires only that the run COMPLETED with
 * a trustworthy full-surface fingerprint — never that every pass settled;
 * unsettled scope travels inside the state instead of freezing it.
 */
const settleReviewRun = (
  core: ReviewCore,
  context: {
    readonly metadata: PullRequestMetadata;
    readonly files: ReadonlyArray<ChangedFile>;
    readonly anchorFiles: ReadonlyArray<ChangedFile>;
    readonly fingerprint: string | undefined;
    readonly usage: UsageTotals | undefined;
    readonly adjudications?: ReadonlyArray<StoredAdjudication> | undefined;
  },
  options: ExecuteReviewOptions,
) =>
  Effect.gen(function* () {
    const { metadata, files, anchorFiles, fingerprint, usage } = context;
    const executionContext = yield* ReviewExecutionContext;
    const adjudications = context.adjudications ?? [];
    const adjudicatedIdentities = new Set(adjudications.map(adjudicationIdentity));
    const isAdjudicatedFinding = (finding: ReviewFinding): boolean =>
      adjudicatedIdentities.has(findingIdentity(finding));
    const isAdjudicatedConcern = (concern: ReviewConcern): boolean =>
      adjudicatedIdentities.has(concernIdentity(concern));
    // Suppress adjudicated model output before any ranking or bounding. A
    // suppressed blocker must never consume the slot of an active finding.
    const filteredReview =
      adjudicatedIdentities.size === 0
        ? core.review
        : CodeReview.make({
            summary: core.review.summary,
            verdict: core.review.verdict,
            findings: core.review.findings.filter((finding) => !isAdjudicatedFinding(finding)),
            ...(core.review.concerns === undefined
              ? {}
              : {
                  concerns: core.review.concerns.filter(
                    (concern) => !isAdjudicatedConcern(concern),
                  ),
                }),
            ...(core.review.walkthrough === undefined
              ? {}
              : { walkthrough: core.review.walkthrough }),
          });
    const reviewPaths = new Set(files.map((file) => file.path));
    const normalizedReview = CodeReview.make({
      ...filteredReview,
      ...(filteredReview.concerns === undefined
        ? {}
        : {
            concerns: filteredReview.concerns.map((concern) => {
              const evidencePaths = concern.evidencePaths;
              if (
                evidencePaths === undefined ||
                evidencePaths.some((path) => !reviewPaths.has(path))
              ) {
                const { evidencePaths: _invalid, ...pathless } = concern;
                return ReviewConcern.make(pathless);
              }
              return ReviewConcern.make({
                ...concern,
                evidencePaths: [...new Set(evidencePaths)].sort(),
              });
            }),
          }),
    });
    // Adjudicated identities leave the published review entirely: no inline
    // comment, no severity count, no verdict influence — they render only in
    // the plan's collapsed adjudicated section. Only identity-equal items are
    // suppressed; a materially different finding at the same location (a
    // different title) is untouched.
    const review = enforceFindingsBound(normalizedReview, clampMaxFindings(options.maxFindings));
    const { inputCoverage, assurance } = core;
    const unreviewedPaths = [...new Set(core.unreviewedPaths)].sort();
    const reviewTotalFiles = executionContext.totalFiles;
    const affectedPaths = new Set(executionContext.affectedPaths);
    const priorState =
      executionContext.mode === "incremental" ? executionContext.priorState : undefined;
    const carriedCandidates =
      priorState?.unresolvedFindings
        .filter((finding) => !affectedPaths.has(finding.path))
        .map(fromStoredFinding) ?? [];
    const eligibleCarriedCandidates = carriedCandidates.filter(
      (finding) => !isAdjudicatedFinding(finding),
    );
    const activeFindings = rankAndDedupeFindings([
      ...eligibleCarriedCandidates,
      ...review.findings.filter((finding) => !isAdjudicatedFinding(finding)),
    ]).slice(0, clampMaxFindings(options.maxFindings));
    const activeFindingKeys = new Set(activeFindings.map(findingKey));
    const currentFindingKeys = new Set(review.findings.map(findingKey));
    const carriedFindings = eligibleCarriedCandidates.filter(
      (finding) =>
        activeFindingKeys.has(findingKey(finding)) && !currentFindingKeys.has(findingKey(finding)),
    );
    // A concern remains active only while every host-validated evidence path
    // is unchanged. Touching or removing any one invalidates the old claim;
    // range selection reopens its remaining current paths for fresh context.
    const carriedConcernCandidates =
      priorState?.unresolvedConcerns
        .filter(
          (concern) =>
            concern.evidencePaths !== undefined &&
            concern.evidencePaths.every((path) => !affectedPaths.has(path)),
        )
        .map(fromStoredConcern) ?? [];
    const eligibleCarriedConcernCandidates = carriedConcernCandidates.filter(
      (concern) => !isAdjudicatedConcern(concern),
    );
    const activeConcerns = rankAndDedupeConcerns([
      ...eligibleCarriedConcernCandidates,
      ...(review.concerns ?? []).filter((concern) => !isAdjudicatedConcern(concern)),
    ]);
    const currentConcernKeys = new Set((review.concerns ?? []).map(reviewConcernKey));
    const activeConcernKeys = new Set(activeConcerns.map(reviewConcernKey));
    const carriedConcerns = eligibleCarriedConcernCandidates.filter((concern) => {
      const key = reviewConcernKey(concern);
      return activeConcernKeys.has(key) && !currentConcernKeys.has(key);
    });
    const settled =
      inputCoverage.status === "complete" &&
      assurance.status !== "incomplete" &&
      unreviewedPaths.length === 0;
    const concernsHaveEvidencePaths = activeConcerns.every(
      (concern) => concern.evidencePaths !== undefined,
    );
    // The fingerprint marker is standalone skip authority for fingerprint-only
    // harnesses, so it is embedded only for a fully settled run.
    const skipFingerprint = settled && concernsHaveEvidencePaths ? fingerprint : undefined;
    const carriedScopeFits = unreviewedPaths.length <= MAX_STORED_UNREVIEWED_PATHS;
    const stateCandidate =
      fingerprint !== undefined &&
      executionContext.profileFingerprint !== undefined &&
      metadata.baseSha !== undefined &&
      // The fingerprint and stored baseline describe the FULL pull-request
      // surface; a truncated anchor surface cannot make either claim.
      anchorFiles.length >= metadata.totalChangedFiles &&
      carriedScopeFits &&
      concernsHaveEvidencePaths &&
      executionContext.stateAuthenticator?.status === "available"
        ? ReviewState.make({
            version: 1,
            repository: metadata.repository,
            pullRequestNumber: metadata.number,
            baseRef: metadata.baseRef,
            baseSha: metadata.baseSha,
            headRef: metadata.headRef,
            reviewedHeadSha: metadata.headSha,
            profileFingerprint: executionContext.profileFingerprint,
            settledScopeFingerprint: fingerprint,
            reviewedPathCount: anchorFiles.length,
            unresolvedFindings: activeFindings.map(toStoredFinding),
            unresolvedConcerns: activeConcerns.map(toStoredConcern),
            unreviewedPaths,
            unreviewedPasses: (core.unreviewedPasses ?? []).slice(0, MAX_STORED_UNREVIEWED_PASSES),
            settled,
            lastReviewMode: executionContext.mode,
            ...(adjudications.length === 0 ? {} : { adjudications }),
          })
        : undefined;
    const continuity =
      stateCandidate === undefined || executionContext.stateAuthenticator === undefined
        ? {
            state: undefined,
            marker: undefined,
            notice: !carriedScopeFits
              ? `carried unreviewed scope (${unreviewedPaths.length} paths) exceeded the ${MAX_STORED_UNREVIEWED_PATHS}-path continuity bound`
              : !concernsHaveEvidencePaths
                ? "one or more review concerns lacked host-validated affected paths"
                : executionContext.stateAuthenticator?.status === "unavailable"
                  ? (executionContext.stateAuthenticator.unavailableReason ??
                    "authenticated continuity state is unavailable")
                  : undefined,
          }
        : yield* executionContext.stateAuthenticator.render(stateCandidate).pipe(
            Effect.match({
              onFailure: (error) => ({
                state: undefined,
                marker: undefined,
                notice:
                  error._tag === "ReviewStateMarkerTooLarge"
                    ? `authenticated continuity state exceeded its ${error.maximumChars}-character bound`
                    : `authenticated continuity state could not be signed: ${error.reason}`,
              }),
              onSuccess: (marker) => ({ state: stateCandidate, marker, notice: undefined }),
            }),
          );
    const plan = planPublication(review, anchorFiles, {
      applyVerdict: options.applyVerdict,
      headSha: metadata.headSha,
      totalChangedFiles: metadata.totalChangedFiles,
      baseRef: metadata.baseRef,
      headRef: metadata.headRef,
      modelLabel: options.modelLabel,
      runUrl: options.runUrl,
      usage,
      fingerprint: skipFingerprint,
      inputCoverage,
      assurance,
      unreviewedPaths,
      carriedFindings,
      carriedConcerns,
      reviewMode: executionContext.mode,
      reviewReason: executionContext.reason,
      baselineSha: executionContext.baselineSha,
      reviewFilesVisible: files.length,
      reviewTotalFiles,
      stateMarker: continuity.marker,
      stateNotice: continuity.notice,
      ...(adjudications.length === 0 ? {} : { adjudications }),
    });
    const shared = {
      review,
      activeFindings,
      activeConcerns,
      inputCoverage,
      assurance,
      unreviewedPaths,
      plan,
      turns: core.turns,
      ...(usage === undefined ? {} : { usage }),
      reviewMode: executionContext.mode,
      reviewReason: executionContext.reason,
      ...(continuity.state === undefined ? {} : { state: continuity.state }),
      ...(adjudications.length === 0 ? {} : { adjudications }),
    };
    if (!options.post) return ReviewRunOutcome.make(shared);
    const publisher = yield* ReviewPublisher;
    const published = yield* publisher.publish(plan);
    return ReviewRunOutcome.make({ ...shared, published });
  });

/**
 * Execute one flat review with any explicit Agent Binding whose contract is
 * `ReviewMission -> CodeReview`. The binding stays a parameter (D-027): tests
 * pass scripted models, hosts pass live provider bindings, and the model
 * Layer's requirements stay visible in this Effect's `R`.
 */
export const executeReview = <
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
>(
  binding: RuntimeBinding<
    typeof ReviewMission,
    typeof CodeReview,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires
  >,
  options: ExecuteReviewOptions,
) =>
  Effect.gen(function* () {
    const source = yield* PullRequestSource;
    const metadata = yield* source.metadata;
    const files = yield* source.changedFiles;
    const anchorFiles = yield* source.anchorFiles;
    const executionContext = yield* ReviewExecutionContext;
    const continuity = yield* resolveReviewContinuityContext();
    const mission = buildReviewMission(metadata, files, {
      adjudicated: continuity.adjudications.map(renderAdjudicationContextLine),
      priorFindings: continuity.priorFindingsOnScope.map(renderPriorFindingContextLine),
    });
    const fullMission = buildReviewMission(metadata, anchorFiles);
    const fingerprint =
      options.signature === undefined
        ? undefined
        : yield* computeChangesetFingerprint(anchorFiles, options.signature(fullMission));

    const budget = yield* makeUsageBudget(options.limits ?? reviewBudgetLimits);
    const detached = yield* AgentRuntime.start(binding, mission, {
      budget: toRunBudgetHook(budget),
      estimateCostMicrousd: () => Effect.succeed(500),
    });
    const result = yield* detached.await;
    const events = yield* detached.events;

    // The engine validated the terminal JSON against the output schema; this
    // decode recovers the typed value on this side of the generic boundary.
    const review = yield* Schema.decodeUnknownEffect(CodeReview)(result.output);
    const assessment = assessFlatReview({
      files,
      totalFiles: executionContext.totalFiles,
      anchorFiles,
      totalAnchorFiles: metadata.totalChangedFiles,
      events,
    });
    const usage = yield* budget.snapshot;
    return yield* settleReviewRun(
      {
        review,
        inputCoverage: assessment.inputCoverage,
        assurance: assessment.assurance,
        unreviewedPaths: assessment.unreviewedPaths,
        turns: result.turns,
      },
      { metadata, files, anchorFiles, fingerprint, usage, adjudications: continuity.adjudications },
      options,
    );
  });

/**
 * Execute one host-scheduled fan-out review: deterministic planning,
 * independent discovery and verification child passes with bounded retries,
 * and a host-composed review from verifier-confirmed candidates only. One
 * budget observes every child pass, so the reported usage is whole-run.
 */
export const executeFanOutReview = <Provider, ModelProvides, ModelRequires>(
  binding: FileReviewerBinding<Provider, ModelProvides, ModelRequires>,
  options: ExecuteReviewOptions,
) =>
  Effect.gen(function* () {
    const source = yield* PullRequestSource;
    const metadata = yield* source.metadata;
    const files = yield* source.changedFiles;
    const anchorFiles = yield* source.anchorFiles;
    const executionContext = yield* ReviewExecutionContext;
    const fullMission = buildReviewMission(metadata, anchorFiles);
    const fingerprint =
      options.signature === undefined
        ? undefined
        : yield* computeChangesetFingerprint(anchorFiles, options.signature(fullMission));

    const budget = yield* makeUsageBudget(options.limits ?? fanOutReviewBudgetLimits);
    const totalFiles = executionContext.totalFiles;
    const continuity = yield* resolveReviewContinuityContext();
    const pipeline = yield* runFanOutReview(binding, {
      files,
      anchorFiles,
      totalChangedFiles: totalFiles,
      maxFindings: options.maxFindings,
      budget: toRunBudgetHook(budget),
      ...(executionContext.retryPaths.length > 0
        ? {
            retry: {
              paths: executionContext.retryPaths,
              stages: executionContext.retryStages,
            },
          }
        : {}),
      ...(continuity.adjudications.length > 0 || continuity.priorFindingsOnScope.length > 0
        ? {
            priorContext: buildPriorReviewContext(
              continuity.adjudications,
              continuity.priorFindingsOnScope,
            ),
          }
        : {}),
    });
    const inputCoverage = fanOutInputCoverage({
      plan: pipeline.plan,
      files,
      totalFiles,
      anchorFiles,
      totalAnchorFiles: metadata.totalChangedFiles,
    });
    const usage = yield* budget.snapshot;
    return yield* settleReviewRun(
      {
        review: pipeline.review,
        inputCoverage,
        assurance: pipeline.assurance,
        unreviewedPaths: pipeline.unreviewedPaths,
        unreviewedPasses: pipeline.unreviewedPasses
          .slice(0, MAX_STORED_UNREVIEWED_PASSES)
          .map((pass) =>
            StoredUnreviewedPass.make({
              stage: pass.stage,
              paths: pass.paths.slice(0, 12),
            }),
          ),
        turns: pipeline.turns,
      },
      { metadata, files, anchorFiles, fingerprint, usage, adjudications: continuity.adjudications },
      options,
    );
  });
