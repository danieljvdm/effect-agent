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

import type { ChangedFile } from "./diff.ts";
import { computeChangesetFingerprint } from "./fingerprint.ts";
import { PublishedReview, ReviewPublisher } from "./github.ts";
import { planPublication, ReviewPublicationPlan } from "./render.ts";
import { clampMaxFindings, CodeReview, ReviewMission } from "./review-agent.ts";
import { rankAndDedupeFindings } from "./review-units.ts";
import { PullRequestSource, type PullRequestMetadata } from "./source.ts";

// ---------------------------------------------------------------------------
// One review run, end to end: read the pull request, run the bounded agent,
// validate the review against the real diff, then (optionally) publish.
// Publication happens strictly AFTER the agent loop so no model turn can
// observe or influence the mutation, and a failed run publishes nothing.
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
 * Run-level bounds for the fan-out coordinator. This budget observes only
 * the COORDINATOR'S own usage — delegated children are bounded separately by
 * the delegation's `SubagentPolicy` reservation and the child definition's
 * own `AgentPolicy`, never silently by the parent's budget. The duration
 * ceiling is wider because delegation Tool Calls hold the parent turn open
 * while bounded children run.
 */
export const fanOutReviewBudgetLimits = UsageBudgetLimits.make({
  maxInputTokens: 400_000,
  maxOutputTokens: 16_000,
  maxToolCalls: 24,
  maxCostMicrousd: 2_000_000,
  maxDurationMillis: 900_000,
});

/** Everything one review run produced, publication receipt included. */
export class ReviewRunOutcome extends Schema.Class<ReviewRunOutcome>(
  "@effect-agent/pr-review/ReviewRunOutcome",
)({
  review: CodeReview,
  plan: ReviewPublicationPlan,
  published: Schema.optionalKey(PublishedReview),
  turns: Schema.Int.check(Schema.isGreaterThan(0)),
  /**
   * The run budget's observed usage. For the fan-out reviewer this observes
   * the COORDINATOR only — delegated children are bounded and accounted
   * separately by their reservations.
   */
  usage: Schema.optionalKey(UsageTotals),
  /**
   * What `usage` observed: the whole run, or a fan-out coordinator only.
   * Absent when the caller declared no scope — consumers must not present
   * unscoped usage as whole-run totals.
   */
  usageScope: Schema.optionalKey(Schema.Literals(["run", "coordinator"])),
}) {}

export interface ExecuteReviewOptions {
  /** Post the review to GitHub; `false` stops after planning (dry run). */
  readonly post: boolean;
  /** Map the model's verdict onto APPROVE/REQUEST_CHANGES instead of COMMENT. */
  readonly applyVerdict: boolean;
  /** Run-level usage bounds; defaults to `reviewBudgetLimits`. */
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
  /**
   * What the run budget observes: the whole run, or a fan-out coordinator
   * only. Without a declared scope the footer omits usage entirely — this
   * generic path cannot know what a caller's binding shape observes, and an
   * unlabeled number would read as whole-run totals.
   */
  readonly usageScope?: "run" | "coordinator" | undefined;
}

/** Build the mission one review run frames from the source's snapshot. */
export const buildReviewMission = (
  metadata: PullRequestMetadata,
  files: ReadonlyArray<ChangedFile>,
): ReviewMission =>
  ReviewMission.make({
    repository: metadata.repository,
    number: metadata.number,
    title: metadata.title,
    body: metadata.body,
    baseRef: metadata.baseRef,
    headRef: metadata.headRef,
    changedFileCount: files.length,
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
      });

/**
 * Execute one review with any explicit Agent Binding whose contract is
 * `ReviewMission -> CodeReview` — the flat reviewer or the fan-out
 * coordinator; the toolkit stays generic because publication only depends on
 * the shared output contract. The binding stays a parameter (D-027): tests
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
    const mission = buildReviewMission(metadata, files);
    const fingerprint =
      options.signature === undefined
        ? undefined
        : yield* computeChangesetFingerprint(files, options.signature(mission));

    const budget = yield* makeUsageBudget(options.limits ?? reviewBudgetLimits);
    const result = yield* AgentRuntime.run(binding, mission, {
      budget: toRunBudgetHook(budget),
      estimateCostMicrousd: () => Effect.succeed(500),
    });

    // The engine validated the terminal JSON against the output schema; this
    // decode recovers the typed value on this side of the generic boundary.
    const decoded = yield* Schema.decodeUnknownEffect(CodeReview)(result.output);
    const review = enforceFindingsBound(decoded, clampMaxFindings(options.maxFindings));
    const usage = yield* budget.snapshot;
    const plan = planPublication(review, files, {
      applyVerdict: options.applyVerdict,
      headSha: metadata.headSha,
      totalChangedFiles: metadata.totalChangedFiles,
      baseRef: metadata.baseRef,
      headRef: metadata.headRef,
      modelLabel: options.modelLabel,
      runUrl: options.runUrl,
      usage,
      usageScope: options.usageScope,
      fingerprint,
    });

    const scope =
      options.usageScope === undefined ? {} : ({ usageScope: options.usageScope } as const);
    if (!options.post) {
      return ReviewRunOutcome.make({ review, plan, turns: result.turns, usage, ...scope });
    }
    const publisher = yield* ReviewPublisher;
    const published = yield* publisher.publish(plan);
    return ReviewRunOutcome.make({
      review,
      plan,
      published,
      turns: result.turns,
      usage,
      ...scope,
    });
  });
