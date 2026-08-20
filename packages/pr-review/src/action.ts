import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Layer, Option, Redacted, Schema } from "effect";
import { BudgetExceeded, UsageBudgetLimits } from "effect-agent";
import { FetchHttpClient } from "effect/unstable/http";

import { collectReviewAdjudications } from "./internal/adjudication.ts";
import type { ChangedFile } from "./internal/diff.ts";
import { InvalidEffortInput, parseEffortPosition, type EffortPosition } from "./internal/effort.ts";
import { PrReview, type RunReviewOptions } from "./internal/factory.ts";
import { readGitHubEvent, resolveReviewTarget, gitHubReviewLayers } from "./internal/github-env.ts";
import { PriorReviews } from "./internal/github.ts";
import { compactReviewLoggingLayer } from "./internal/logging.ts";
import { ReviewProgressReporter } from "./internal/progress.ts";
import {
  anthropicClientLayer,
  DEFAULT_PROVIDER,
  describeReviewModel,
  makeAnthropicReviewModel,
  makeOpenAiReviewModel,
  openAiClientLayer,
  type ReviewProvider,
} from "./internal/providers.ts";
import { retireStaleReviews } from "./internal/retirement.ts";
import {
  adjudicationIdentity,
  findingIdentity,
  ReviewExecutionContext,
  ReviewHeadComparison,
  ReviewStateAuthenticator,
  isLineageAncestor,
  type ReviewMode,
  type ReviewState,
  selectReviewRange,
  selectedPullRequestSourceLayer,
  unavailableReviewStateAuthenticatorLayer,
  validateReviewState,
  webCryptoReviewStateAuthenticatorLayer,
} from "./internal/review-state.ts";
import { fanOutReviewBudgetLimits, reviewBudgetLimits } from "./internal/run.ts";
import type { ReviewRunOutcome } from "./internal/run.ts";
import {
  normalizeRepoRelativePath,
  PullRequestSource,
  type PullRequestMetadata,
} from "./internal/source.ts";

// ---------------------------------------------------------------------------
// The GitHub Actions entrypoint (deployment class E: one bounded ephemeral
// run, no exactly-once posting). Bounded continuity state travels in GitHub
// review bodies and is validated before reuse. Inputs arrive as
// PR_REVIEW_* environment variables set by the action manifest; the target
// pull request comes from the standard Actions event environment. A non-PR
// or draft event is a typed skip — green job, nothing posted. Publication
// happens only after the run settles, so a failed or truncated run posts
// nothing.
// ---------------------------------------------------------------------------

export const ReviewCheckConclusion = Schema.Literals(["success", "blocking", "incomplete"]);
export type ReviewCheckConclusion = typeof ReviewCheckConclusion.Type;

/** The host-derived blocking or incomplete conclusion failed the check. */
export class ReviewGateFailed extends Schema.TaggedError<ReviewGateFailed>()("ReviewGateFailed", {
  conclusion: Schema.Literals(["blocking", "incomplete"]),
  reasons: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(1_000))).check(
    Schema.isMinLength(1),
  ),
}) {
  override get message() {
    return `Review check concluded '${this.conclusion}': ${this.reasons.join("; ")}`;
  }
}

/** A configured max-duration that cannot bound a run; configuration faults fail loudly. */
export class InvalidMaxDurationInput extends Schema.TaggedError<InvalidMaxDurationInput>()(
  "InvalidMaxDurationInput",
  {
    minutes: Schema.Int,
  },
) {
  override get message() {
    return `Invalid max-duration-minutes '${this.minutes}': expected a positive number of minutes.`;
  }
}

/** Everything the packaged action reads from its environment. */
export interface ResolvedActionInputs {
  readonly provider: ReviewProvider;
  readonly model: string | undefined;
  readonly effort: EffortPosition | undefined;
  readonly post: boolean;
  readonly applyVerdict: boolean;
  readonly fanOut: boolean;
  readonly guidance: string | undefined;
  readonly guidanceFile: string | undefined;
  readonly ignore: ReadonlyArray<string>;
  readonly maxFindings: number | undefined;
  readonly maxDurationMinutes: number | undefined;
  readonly reviewMode: ReviewMode;
  readonly skipUnchanged: boolean;
  readonly retireStaleReviews: boolean;
  readonly progressComment: boolean;
}

/** Read the PR_REVIEW_* input surface (all optional, all defaulted). */
export const resolveActionInputs = Effect.fn("resolveActionInputs")(function* () {
  const provider = yield* Config.literals(["openai", "anthropic"], "PR_REVIEW_PROVIDER").pipe(
    Config.withDefault<ReviewProvider>(DEFAULT_PROVIDER),
  );
  const model = yield* Config.option(Config.nonEmptyString("PR_REVIEW_MODEL"));
  const effortRaw = yield* Config.option(Config.nonEmptyString("PR_REVIEW_EFFORT"));
  let effort: EffortPosition | undefined;
  if (Option.isSome(effortRaw)) {
    effort = parseEffortPosition(effortRaw.value);
    if (effort === undefined) {
      return yield* InvalidEffortInput.make({ input: effortRaw.value });
    }
  }
  const maxDurationMinutes = Option.getOrUndefined(
    yield* Config.option(Config.int("PR_REVIEW_MAX_DURATION_MINUTES")),
  );
  if (maxDurationMinutes !== undefined && maxDurationMinutes <= 0) {
    return yield* InvalidMaxDurationInput.make({ minutes: maxDurationMinutes });
  }
  const post = yield* Config.boolean("PR_REVIEW_POST").pipe(Config.withDefault(true));
  const applyVerdict = yield* Config.boolean("PR_REVIEW_APPLY_VERDICT").pipe(
    Config.withDefault(false),
  );
  const fanOut = yield* Config.boolean("PR_REVIEW_FAN_OUT").pipe(Config.withDefault(true));
  const guidance = yield* Config.option(Config.nonEmptyString("PR_REVIEW_GUIDANCE"));
  const guidanceFile = yield* Config.option(Config.nonEmptyString("PR_REVIEW_GUIDANCE_FILE"));
  const ignoreRaw = yield* Config.string("PR_REVIEW_IGNORE").pipe(Config.withDefault(""));
  const maxFindings = yield* Config.option(Config.int("PR_REVIEW_MAX_FINDINGS"));
  const reviewMode = yield* Config.literals(["incremental", "final"], "PR_REVIEW_MODE").pipe(
    Config.withDefault<ReviewMode>("incremental"),
  );
  const skipUnchanged = yield* Config.boolean("PR_REVIEW_SKIP_UNCHANGED").pipe(
    Config.withDefault(true),
  );
  const retireStaleReviews = yield* Config.boolean("PR_REVIEW_RETIRE_STALE_REVIEWS").pipe(
    Config.withDefault(true),
  );
  const progressComment = yield* Config.boolean("PR_REVIEW_PROGRESS_COMMENT").pipe(
    Config.withDefault(true),
  );
  return {
    provider,
    model: Option.getOrUndefined(model),
    effort,
    post,
    applyVerdict,
    fanOut,
    guidance: Option.getOrUndefined(guidance),
    guidanceFile: Option.getOrUndefined(guidanceFile),
    ignore: ignoreRaw
      .split(",")
      .map((pattern) => pattern.trim())
      .filter((pattern) => pattern.length > 0),
    maxFindings: Option.getOrUndefined(maxFindings),
    maxDurationMinutes,
    reviewMode,
    skipUnchanged,
    retireStaleReviews,
    progressComment,
  } satisfies ResolvedActionInputs;
});

/** A guidance file larger than this is refused, never silently truncated. */
export const MAX_GUIDANCE_FILE_CHARS = 20_000;

/** A configured guidance file could not be used; configuration faults fail loudly. */
export class GuidanceFileUnreadable extends Schema.TaggedError<GuidanceFileUnreadable>()(
  "GuidanceFileUnreadable",
  {
    path: Schema.String,
    reason: Schema.String,
  },
) {
  override get message() {
    return `Cannot use guidance file '${this.path}': ${this.reason}`;
  }
}

/**
 * Resolve the effective review guidance: the committed guidance file (a
 * repository-owned review profile) first, with any inline guidance appended.
 * A configured-but-unreadable file fails typed — a review silently running
 * without its profile would be worse than a red job.
 */
export const resolveGuidance = Effect.fn("resolveGuidance")(function* (inputs: {
  readonly guidance: string | undefined;
  readonly guidanceFile: string | undefined;
}) {
  const filePath = inputs.guidanceFile;
  if (filePath === undefined) return inputs.guidance;
  // The profile path is operator configuration, but it stays fail-closed
  // like every other path in this package: workspace-relative only, and the
  // size bound is checked before the file is read into memory.
  const relative = yield* normalizeRepoRelativePath(filePath).pipe(
    Effect.mapError((violation) =>
      GuidanceFileUnreadable.make({ path: filePath, reason: violation.reason }),
    ),
  );
  const fs = yield* FileSystem.FileSystem;
  const unreadable = (error: { readonly _tag: string; readonly message: string }) =>
    GuidanceFileUnreadable.make({
      path: relative,
      reason: `${error._tag}: ${error.message}`.slice(0, 2_048),
    });
  const stat = yield* fs.stat(relative).pipe(Effect.mapError(unreadable));
  const oversized = GuidanceFileUnreadable.make({
    path: relative,
    reason: `File is larger than the ${MAX_GUIDANCE_FILE_CHARS}-character guidance bound.`,
  });
  if (stat.size > BigInt(MAX_GUIDANCE_FILE_CHARS) * 4n) {
    return yield* oversized;
  }
  const content = yield* fs.readFileString(relative).pipe(Effect.mapError(unreadable));
  if (content.length > MAX_GUIDANCE_FILE_CHARS) {
    return yield* oversized;
  }
  const combined = [content.trim(), inputs.guidance ?? ""]
    .filter((part) => part.length > 0)
    .join("\n");
  return combined.length > 0 ? combined : undefined;
});

/** One step-output line; values must be single-line by construction. */
const outputLine = (name: string, value: string): string =>
  `${name}=${value.replaceAll("\n", " ").slice(0, 1_000)}\n`;

/** Append step outputs to GITHUB_OUTPUT when present (no-op locally). */
export const writeActionOutputs = Effect.fn("writeActionOutputs")(function* (
  entries: ReadonlyArray<readonly [name: string, value: string]>,
) {
  const outputPath = yield* Config.string("GITHUB_OUTPUT").pipe(Config.withDefault(""));
  if (outputPath === "") return;
  const fs = yield* FileSystem.FileSystem;
  yield* fs.writeFileString(
    outputPath,
    entries.map(([name, value]) => outputLine(name, value)).join(""),
    {
      flag: "a",
    },
  );
});

/** Append markdown to the GITHUB_STEP_SUMMARY report when present (no-op locally). */
export const writeStepSummary = Effect.fn("writeStepSummary")(function* (
  lines: ReadonlyArray<string>,
) {
  const summaryPath = yield* Config.string("GITHUB_STEP_SUMMARY").pipe(Config.withDefault(""));
  if (summaryPath === "") return;
  const fs = yield* FileSystem.FileSystem;
  yield* fs.writeFileString(summaryPath, `${lines.join("\n")}\n`, { flag: "a" });
});

const outcomeOutputs = (
  outcome: ReviewRunOutcome,
  conclusion: ReviewCheckConclusion,
): ReadonlyArray<readonly [string, string]> => [
  ["skipped", "false"],
  ["conclusion", conclusion],
  ["verdict", outcome.review.verdict],
  ["input-coverage", outcome.inputCoverage.status],
  ["review-assurance", outcome.assurance.status],
  ["review-mode", outcome.reviewMode ?? "full"],
  ["review-reason", outcome.reviewReason ?? "direct full review"],
  ["inline-comments", String(outcome.plan.comments.length)],
  ["demoted-findings", String(outcome.plan.demoted.length)],
  ["concerns", String(outcome.review.concerns?.length ?? 0)],
  ["unreviewed-paths", String(outcome.unreviewedPaths.length)],
  ...(outcome.usage === undefined
    ? []
    : ([
        ["input-tokens", String(outcome.usage.inputTokens)],
        ["output-tokens", String(outcome.usage.outputTokens)],
      ] as const)),
  ["review-url", outcome.published?.url ?? ""],
];

const outcomeSummary = (
  outcome: ReviewRunOutcome,
  modelLabel: string | undefined,
  conclusion: ReviewCheckConclusion,
): ReadonlyArray<string> => [
  "### Pull-request review",
  `- Check conclusion: **${conclusion}**`,
  `- Verdict: **${outcome.review.verdict}**`,
  `- Input coverage: **${outcome.inputCoverage.status}** · scope: ${outcome.reviewMode ?? "full"}`,
  `- Review assurance: **${outcome.assurance.status}** · general discovery ${outcome.assurance.completedGeneralDiscoveryPasses}/${outcome.assurance.requiredGeneralDiscoveryPasses} · specialist ${outcome.assurance.completedSpecialistPasses}/${outcome.assurance.requiredSpecialistPasses} · verification ${outcome.assurance.completedVerificationPasses}/${outcome.assurance.requiredVerificationPasses}`,
  `- Inline comments: ${outcome.plan.comments.length} · demoted findings: ${outcome.plan.demoted.length} · concerns: ${outcome.review.concerns?.length ?? 0}`,
  ...(outcome.unreviewedPaths.length === 0
    ? []
    : [
        `- Carried forward: ${outcome.unreviewedPaths.length} unreviewed path(s) retried automatically on the next run (reviewer-side gap, not a code defect)`,
      ]),
  ...(modelLabel === undefined ? [] : [`- Model: \`${modelLabel}\``]),
  ...(outcome.usage === undefined
    ? []
    : [`- Tokens: ${outcome.usage.inputTokens} in / ${outcome.usage.outputTokens} out`]),
  ...(outcome.published === undefined
    ? ["- Dry run: nothing posted"]
    : [`- Posted: ${outcome.published.url}`]),
];

/** The result of one action invocation: a typed skip or a settled review. */
export type ReviewActionResult =
  | { readonly _tag: "Skipped"; readonly reason: string }
  | { readonly _tag: "Completed"; readonly outcome: ReviewRunOutcome };

const skip = (reason: string) =>
  Effect.gen(function* () {
    yield* Console.log(`Skipping review: ${reason}`);
    yield* writeActionOutputs([
      ["skipped", "true"],
      ["skip-reason", reason],
    ]);
    yield* writeStepSummary(["### Pull-request review skipped", `- Reason: ${reason}`]);
    return { _tag: "Skipped", reason } satisfies ReviewActionResult;
  });

/** The Actions run URL for the review footer, or undefined outside Actions. */
const resolveRunUrl = Effect.fn("resolveRunUrl")(function* () {
  const runId = yield* Config.string("GITHUB_RUN_ID").pipe(Config.withDefault(""));
  const repository = yield* Config.string("GITHUB_REPOSITORY").pipe(Config.withDefault(""));
  if (runId === "" || repository === "") return undefined;
  const server = yield* Config.string("GITHUB_SERVER_URL").pipe(
    Config.withDefault("https://github.com"),
  );
  return `${server}/${repository}/actions/runs/${runId}`;
});

/**
 * The reviewer surface the action harness drives. `PrReview.make` and
 * `PrReview.makeFanOut` provide the state-selection effects. A fingerprint is
 * skip authority only when a profile fingerprint and authenticated review
 * state bind it to settled assurance.
 */
interface HarnessedReviewerBase<E, R> {
  readonly run: (runOptions?: RunReviewOptions) => Effect.Effect<ReviewRunOutcome, E, R>;
}

export type HarnessedReviewer<E, R, FingerprintE, FingerprintR> = HarnessedReviewerBase<E, R> &
  (
    | {
        readonly fingerprint?: undefined;
        readonly profileFingerprint?: undefined;
        readonly snapshot?: undefined;
        readonly filterFiles?: undefined;
      }
    | {
        /** Current effective changeset fingerprint; not standalone skip authority. */
        readonly fingerprint: Effect.Effect<string, FingerprintE, FingerprintR>;
        readonly profileFingerprint: Effect.Effect<string, FingerprintE, FingerprintR>;
        readonly snapshot: Effect.Effect<
          {
            readonly metadata: PullRequestMetadata;
            readonly files: ReadonlyArray<ChangedFile>;
          },
          FingerprintE,
          FingerprintR
        >;
        readonly filterFiles?:
          | ((files: ReadonlyArray<ChangedFile>) => ReadonlyArray<ChangedFile>)
          | undefined;
      }
  );

const blockingReasons = (input: {
  readonly findings: ReadonlyArray<{ readonly severity: string; readonly title: string }>;
  readonly concerns: ReadonlyArray<{ readonly severity: string; readonly title: string }>;
}): ReadonlyArray<string> => [
  ...input.findings
    .filter((finding) => finding.severity === "blocking")
    .map((finding) => `blocking finding: ${finding.title}`),
  ...input.concerns
    .filter((concern) => concern.severity === "blocking")
    .map((concern) => `blocking concern: ${concern.title}`),
];

/**
 * Host-derived check conclusion; model verdict prose cannot weaken it.
 *
 * Blocking code findings outrank machinery gaps — they are the actionable
 * signal. A machinery gap (incomplete input, unsettled passes) concludes
 * `incomplete` with reasons that explicitly say the failure is reviewer-side
 * uncertainty carried forward for retry, never an invitation to change code.
 * The flat reviewer's constant `unverified` assurance is not a gap.
 */
export const concludeReviewOutcome = (
  outcome: ReviewRunOutcome,
): {
  readonly conclusion: ReviewCheckConclusion;
  readonly reasons: ReadonlyArray<string>;
} => {
  const machinery: Array<string> = [];
  if (outcome.inputCoverage.status === "incomplete" || outcome.assurance.status === "incomplete") {
    machinery.push(
      outcome.unreviewedPaths.length > 0
        ? `review infrastructure did not settle — a reviewer-side gap, not a code defect; ${outcome.unreviewedPaths.length} path(s) are carried forward and retried automatically on the next run`
        : "review infrastructure did not settle — a reviewer-side gap, not a code defect",
    );
    if (outcome.inputCoverage.status === "incomplete") {
      machinery.push(...outcome.inputCoverage.reasons);
    }
    if (outcome.assurance.status === "incomplete") {
      machinery.push(...outcome.assurance.reasons);
    }
  }
  const blocking = blockingReasons({
    findings: outcome.activeFindings,
    concerns: outcome.activeConcerns,
  });
  if (blocking.length > 0) {
    return { conclusion: "blocking", reasons: [...blocking, ...machinery] };
  }
  if (machinery.length > 0) return { conclusion: "incomplete", reasons: machinery };
  return { conclusion: "success", reasons: [] };
};

const concludeReviewState = (state: ReviewState, adjudicated: ReadonlySet<string>) => {
  const reasons = blockingReasons({
    findings: state.unresolvedFindings.filter(
      (finding) => !adjudicated.has(findingIdentity(finding)),
    ),
    concerns: state.unresolvedConcerns.filter((concern) => !adjudicated.has(concern.title)),
  });
  return reasons.length > 0
    ? ({ conclusion: "blocking", reasons } as const)
    : ({ conclusion: "success", reasons: [] } as const);
};

const skipCoveredReview = Effect.fn("skipCoveredReview")(function* (input: {
  readonly repository: string;
  readonly pullRequestNumber: number;
  readonly reason: string;
  readonly state: ReviewState;
}) {
  // A maintainer adjudication must lift a preserved blocking conclusion
  // without a code push, so the skip path re-reads adjudications (fail-open;
  // stored ones survive a listing fault) before enforcing the stored state.
  const adjudications = yield* collectReviewAdjudications(input.state.adjudications ?? []);
  const result = concludeReviewState(input.state, new Set(adjudications.map(adjudicationIdentity)));
  yield* Console.log(
    `Skipping review of ${input.repository}#${input.pullRequestNumber}: ${input.reason}.`,
  );
  yield* writeActionOutputs([
    ["skipped", "true"],
    ["skip-reason", input.reason],
    ["conclusion", result.conclusion],
    ["input-coverage", "complete"],
    ["review-assurance", "settled"],
    ["review-mode", "incremental"],
  ]);
  yield* writeStepSummary([
    "### Pull-request review skipped",
    `- Reason: ${input.reason}`,
    `- Preserved check conclusion: **${result.conclusion}**`,
  ]);
  if (result.conclusion === "blocking") {
    return yield* ReviewGateFailed.make({
      conclusion: "blocking",
      reasons: result.reasons,
    });
  }
  return { _tag: "Skipped", reason: input.reason } satisfies ReviewActionResult;
});

/**
 * Harness one already-built reviewer inside the Actions environment: resolve
 * the target from the event, provide the GitHub source/publisher/prior
 * reviews, validate/select bounded continuity scope, write step outputs, and apply
 * the host-derived coverage/blocker gate. Draft and non-PR skips are values;
 * an unchanged reviewed head preserves and enforces its stored conclusion. The reviewer's
 * remaining requirements — its model client, any extra tool handlers — stay
 * visible in `R` for the caller.
 */
export const runReviewAction = <E, R, FingerprintE = never, FingerprintR = never>(
  reviewer: HarnessedReviewer<E, R, FingerprintE, FingerprintR>,
  options: {
    readonly post?: boolean | undefined;
    /** Skip model execution when the current head already has complete stored coverage. */
    readonly skipUnchanged?: boolean | undefined;
    /** Incremental by default; `final` deliberately re-reviews the full PR diff. */
    readonly reviewMode?: ReviewMode | undefined;
    /** Model binding descriptor for the Actions step summary. */
    readonly modelLabel?: string | undefined;
    /** Explicit test/custom-host history override; GitHub owns the default adapter. */
    readonly priorReviews?: PriorReviews["Service"] | undefined;
    /** Retire marker-bearing prior reviews after a successful post (default true). */
    readonly retireStaleReviews?: boolean | undefined;
    /**
     * Maintain one sticky "review in progress" issue comment, updated in
     * place when the run settles. Default false here for custom-harness
     * compatibility; the packaged action enables it by default. Dry runs
     * (`post: false`) never post progress.
     */
    readonly progressComment?: boolean | undefined;
  } = {},
) =>
  Effect.gen(function* () {
    const event = yield* readGitHubEvent();
    if (Option.isSome(event)) {
      if (event.value.pull_request === undefined) {
        return yield* skip("the triggering event carries no pull request");
      }
      if (event.value.pull_request.draft === true) {
        return yield* skip("the pull request is a draft");
      }
    }
    const target = yield* resolveReviewTarget({});
    // One layer build for state selection AND the run, so both observe
    // the same cached pull-request snapshot.
    return yield* Effect.gen(function* () {
      let selection: ReturnType<typeof selectReviewRange> | undefined;
      if (reviewer.profileFingerprint !== undefined) {
        const [snapshot, profileFingerprint, currentFingerprint] = yield* Effect.all([
          reviewer.snapshot,
          reviewer.profileFingerprint,
          reviewer.fingerprint.pipe(
            Effect.map((fingerprint): string | undefined => fingerprint),
            Effect.orElseSucceed(() => undefined),
          ),
        ]);
        const { metadata, files: fullFiles } = snapshot;
        const history = options.priorReviews ?? (yield* PriorReviews);
        const stateAuthenticator = yield* ReviewStateAuthenticator;
        const recovered =
          stateAuthenticator.status === "unavailable"
            ? {
                state: undefined,
                failure:
                  stateAuthenticator.unavailableReason ??
                  "an authenticated review-state secret is not configured",
              }
            : yield* history.latestState.pipe(
                Effect.match({
                  onFailure: (failure) => ({ state: undefined, failure: failure.reason }),
                  onSuccess: (state) => ({
                    state: Option.getOrUndefined(state),
                    failure: undefined,
                  }),
                }),
              );
        const equivalentPatchState =
          (options.reviewMode ?? "incremental") === "incremental" &&
          options.skipUnchanged !== false &&
          recovered.state !== undefined &&
          // Only a fully settled run may be skipped over: an unsettled state
          // carries retryable scope the next run must actually retry.
          recovered.state.settled &&
          currentFingerprint !== undefined &&
          validateReviewState(recovered.state, metadata, profileFingerprint) === undefined &&
          recovered.state.settledScopeFingerprint === currentFingerprint
            ? recovered.state
            : undefined;
        if (equivalentPatchState !== undefined) {
          return yield* skipCoveredReview({
            repository: target.repository,
            pullRequestNumber: target.number,
            reason:
              equivalentPatchState.reviewedHeadSha === metadata.headSha
                ? "the current head already has settled stored review assurance"
                : "the effective pull-request patch is unchanged since the last settled review",
            state: equivalentPatchState,
          });
        }
        let comparison: ReviewHeadComparison | undefined;
        let baseComparison: ReviewHeadComparison | undefined;
        let contentComparison: ReviewHeadComparison | undefined;
        if (
          (options.reviewMode ?? "incremental") === "incremental" &&
          recovered.state !== undefined
        ) {
          if (recovered.state.reviewedHeadSha === metadata.headSha) {
            comparison = ReviewHeadComparison.make({
              status: "identical",
              baseSha: metadata.headSha,
              headSha: metadata.headSha,
              mergeBaseSha: metadata.headSha,
              files: [],
              truncated: false,
            });
          } else {
            comparison = yield* history
              .compareHeads(recovered.state.reviewedHeadSha, metadata.headSha)
              .pipe(Effect.orElseSucceed(() => undefined));
            if (comparison !== undefined && reviewer.filterFiles !== undefined) {
              comparison = ReviewHeadComparison.make({
                ...comparison,
                files: reviewer.filterFiles(comparison.files),
              });
            }
          }
          if (metadata.baseSha !== undefined && recovered.state.baseSha !== metadata.baseSha) {
            baseComparison = yield* history
              .compareHeads(recovered.state.baseSha, metadata.baseSha)
              .pipe(Effect.orElseSucceed(() => undefined));
            if (baseComparison !== undefined && reviewer.filterFiles !== undefined) {
              baseComparison = ReviewHeadComparison.make({
                ...baseComparison,
                files: reviewer.filterFiles(baseComparison.files),
              });
            }
          }
          if (
            recovered.state.reviewedHeadSha !== metadata.headSha &&
            (comparison === undefined ||
              !isLineageAncestor(comparison, recovered.state, metadata.headSha))
          ) {
            contentComparison = yield* history
              .compareTrees(recovered.state.reviewedHeadSha, metadata.headSha)
              .pipe(Effect.orElseSucceed(() => undefined));
            if (contentComparison !== undefined && reviewer.filterFiles !== undefined) {
              contentComparison = ReviewHeadComparison.make({
                ...contentComparison,
                files: reviewer.filterFiles(contentComparison.files),
              });
            }
          }
        }
        selection = {
          ...selectReviewRange({
            requestedMode: options.reviewMode ?? "incremental",
            current: metadata,
            fullFiles,
            profileFingerprint,
            priorState: recovered.state,
            comparison,
            baseComparison,
            contentComparison,
            lookupFailure: recovered.failure,
          }),
          stateAuthenticator,
        };
        if (
          options.skipUnchanged !== false &&
          selection.mode === "incremental" &&
          selection.files.length === 0 &&
          selection.priorState !== undefined &&
          selection.priorState.settled
        ) {
          const reason = "no changed review scope since the last settled review head";
          return yield* skipCoveredReview({
            repository: target.repository,
            pullRequestNumber: target.number,
            reason,
            state: selection.priorState,
          });
        }
      }
      yield* Console.log(
        `Reviewing ${target.repository}#${target.number} (${options.post === false ? "dry run" : "posting"})...`,
      );
      const runUrl = yield* resolveRunUrl();
      // Progress is a cosmetic, fail-open narration surface: it says a run is
      // working the moment execution starts and is overwritten in place when
      // the run settles. It never gates or delays the review itself.
      const progress =
        options.progressComment === true && options.post !== false
          ? Option.some(yield* ReviewProgressReporter)
          : Option.none<ReviewProgressReporter["Service"]>();
      if (Option.isSome(progress)) {
        const source = yield* PullRequestSource;
        const headMetadata = yield* source.metadata.pipe(Effect.orElseSucceed(() => undefined));
        yield* progress.value.begin({
          headSha: headMetadata?.headSha,
          reviewMode: selection?.mode,
          reviewReason: selection?.reason,
          filesInScope: selection?.files.length,
          modelLabel: options.modelLabel,
          runUrl,
        });
      }
      const runReview = reviewer.run({ post: options.post ?? true, runUrl });
      const reviewEffect = Option.isSome(progress)
        ? runReview.pipe(
            Effect.tapCause(() =>
              progress.value.settle({
                outcome: "failed",
                runUrl,
                modelLabel: options.modelLabel,
              }),
            ),
          )
        : runReview;
      const outcome = yield* selection === undefined
        ? reviewEffect
        : reviewEffect.pipe(
            Effect.provide(selectedPullRequestSourceLayer(selection)),
            Effect.provideService(ReviewExecutionContext, selection),
          );
      yield* Console.log(
        `Review finished in ${outcome.turns} turn(s): verdict ${outcome.review.verdict}, ` +
          `${outcome.plan.comments.length} inline comment(s), ${outcome.plan.demoted.length} demoted finding(s).`,
      );
      if (outcome.published !== undefined) {
        yield* Console.log(`Posted ${outcome.published.event} review: ${outcome.published.url}`);
        if (options.retireStaleReviews !== false && outcome.state !== undefined) {
          if (outcome.published.authorNodeId === null || outcome.published.submittedAt === null) {
            yield* Console.warn(
              "Skipping stale-review retirement because GitHub did not return the posted review's actor and submission time.",
            );
          } else {
            const report = yield* retireStaleReviews({
              currentReviewId: outcome.published.reviewId,
              currentReviewUrl: outcome.published.url,
              currentAuthorNodeId: outcome.published.authorNodeId,
              currentSubmittedAt: outcome.published.submittedAt,
              currentState: outcome.state,
            });
            yield* Console.log(
              `Review retirement: ${report.reviewsRetired} prior review(s), ` +
                `${report.findingsResolved} resolved finding(s), ` +
                `${report.commentsMinimized} minimized inline comment(s), ` +
                `${report.failures} failure(s).`,
            );
          }
        }
      }
      const check = concludeReviewOutcome(outcome);
      if (Option.isSome(progress)) {
        yield* progress.value.settle({
          outcome: "reviewed",
          conclusion: check.conclusion,
          verdict: outcome.review.verdict,
          inlineComments: outcome.plan.comments.length,
          reviewUrl: outcome.published?.url,
          runUrl,
          modelLabel: options.modelLabel,
        });
      }
      yield* writeActionOutputs(outcomeOutputs(outcome, check.conclusion));
      yield* writeStepSummary(outcomeSummary(outcome, options.modelLabel, check.conclusion));
      if (check.conclusion !== "success") {
        return yield* ReviewGateFailed.make({
          conclusion: check.conclusion,
          reasons: check.reasons,
        });
      }
      return { _tag: "Completed", outcome } satisfies ReviewActionResult;
    }).pipe(Effect.provide(gitHubReviewLayers(target)));
  });

/**
 * The packaged, environment-driven action program: inputs from PR_REVIEW_*,
 * reviewer built from the packaged factory, provider client from the
 * matching credential environment variable.
 */
export const reviewActionProgram = Effect.gen(function* () {
  const inputs = yield* resolveActionInputs();
  const stateSecret = Option.map(
    yield* Config.option(Config.nonEmptyString("PR_REVIEW_STATE_SECRET")),
    Redacted.make,
  );
  const stateAuthenticatorLayer = Option.match(stateSecret, {
    onNone: () =>
      unavailableReviewStateAuthenticatorLayer(
        "an authenticated review-state secret is not configured",
      ),
    onSome: webCryptoReviewStateAuthenticatorLayer,
  });
  const guidance = yield* resolveGuidance(inputs);
  const modelLabel = describeReviewModel(inputs.provider, inputs.model, inputs.effort);
  const defaults = inputs.fanOut ? fanOutReviewBudgetLimits : reviewBudgetLimits;
  const budget =
    inputs.maxDurationMinutes === undefined
      ? defaults
      : UsageBudgetLimits.make({
          ...defaults,
          maxDurationMillis: inputs.maxDurationMinutes * 60_000,
        });
  const shared = {
    guidance,
    ignore: inputs.ignore,
    maxFindings: inputs.maxFindings,
    applyVerdict: inputs.applyVerdict,
    modelLabel,
    budget,
  };
  const harness = {
    post: inputs.post,
    skipUnchanged: inputs.skipUnchanged,
    reviewMode: inputs.reviewMode,
    retireStaleReviews: inputs.retireStaleReviews,
    progressComment: inputs.progressComment,
    modelLabel,
  };
  if (inputs.provider === "anthropic") {
    const model = makeAnthropicReviewModel(inputs.model, inputs.effort);
    const reviewer = inputs.fanOut
      ? PrReview.makeFanOut({ ...shared, model })
      : PrReview.make({ ...shared, model });
    return yield* runReviewAction(reviewer, harness).pipe(
      Effect.provide(Layer.merge(stateAuthenticatorLayer, anthropicClientLayer)),
    );
  }
  const model = makeOpenAiReviewModel(inputs.model, inputs.effort);
  const reviewer = inputs.fanOut
    ? PrReview.makeFanOut({ ...shared, model })
    : PrReview.make({ ...shared, model });
  return yield* runReviewAction(reviewer, harness).pipe(
    Effect.provide(Layer.merge(stateAuthenticatorLayer, openAiClientLayer)),
  );
});

/** Run the packaged action program on Node; the bundled action entrypoint. */
export const main = (): void =>
  NodeRuntime.runMain(
    reviewActionProgram.pipe(
      Effect.tapError((error) =>
        Console.error(
          Schema.is(BudgetExceeded)(error)
            ? `Budget exceeded: ${error.limit} observed ${error.observedValue}, limit ${error.limitValue}.`
            : String(error),
        ),
      ),
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, compactReviewLoggingLayer),
      ),
    ),
    { disableErrorReporting: true },
  );

/** The packaged GitHub Actions surface. */
export const PrReviewAction = {
  inputs: resolveActionInputs,
  run: runReviewAction,
  program: reviewActionProgram,
  main,
} as const;
