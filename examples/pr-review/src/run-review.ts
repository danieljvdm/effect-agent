import { Effect, Schema } from "effect";
import {
  makeUsageBudget,
  toRunBudgetHook,
  UsageBudgetLimits,
  AgentRuntime,
  type RuntimeBinding,
} from "effect-agent";
import { type Toolkit } from "effect/unstable/ai";

import { PublishedReview, ReviewPublisher } from "./github.ts";
import { planPublication, ReviewPublicationPlan } from "./render.ts";
import { CodeReview, ReviewMission, type ReviewToolkit } from "./review-agent.ts";
import { PullRequestSource } from "./source.ts";

// ---------------------------------------------------------------------------
// One review run, end to end: read the pull request, run the bounded agent,
// validate the review against the real diff, then (optionally) publish.
// Publication happens strictly AFTER the agent loop so no model turn can
// observe or influence the mutation, and a failed run publishes nothing.
// ---------------------------------------------------------------------------

/**
 * Run-level usage bounds on top of the definition's AgentPolicy. Real diffs
 * are token-heavy, so the input budget is research-sized with cost as the
 * safety net (mirroring the live travel-chat profile).
 */
export const reviewBudgetLimits = UsageBudgetLimits.make({
  maxInputTokens: 400_000,
  maxOutputTokens: 16_000,
  maxToolCalls: 24,
  maxCostMicrousd: 2_000_000,
  maxDurationMillis: 480_000,
});

/** Everything one review run produced, publication receipt included. */
export class ReviewRunOutcome extends Schema.Class<ReviewRunOutcome>(
  "@effect-agent/example-pr-review/ReviewRunOutcome",
)({
  review: CodeReview,
  plan: ReviewPublicationPlan,
  published: Schema.optionalKey(PublishedReview),
  turns: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

export interface ExecuteReviewOptions {
  /** Post the review to GitHub; `false` stops after planning (dry run). */
  readonly post: boolean;
  /** Map the model's verdict onto APPROVE/REQUEST_CHANGES instead of COMMENT. */
  readonly applyVerdict: boolean;
}

/**
 * Execute one review with any explicit Agent Binding for the reviewer
 * definition. The binding stays a parameter (D-027): tests pass a scripted
 * model, the CLI passes a live OpenAI binding, and the model Layer's
 * requirements stay visible in this Effect's `R`.
 */
export const executeReview = <Instructions, Provider, ModelProvides, ModelRequires>(
  binding: RuntimeBinding<
    typeof ReviewMission,
    typeof CodeReview,
    Instructions,
    Toolkit.Tools<typeof ReviewToolkit>,
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
    const mission = ReviewMission.make({
      repository: metadata.repository,
      number: metadata.number,
      title: metadata.title,
      body: metadata.body,
      baseRef: metadata.baseRef,
      headRef: metadata.headRef,
      changedFileCount: files.length,
    });

    const budget = yield* makeUsageBudget(reviewBudgetLimits);
    const result = yield* AgentRuntime.run(binding, mission, {
      budget: toRunBudgetHook(budget),
      estimateCostMicrousd: () => Effect.succeed(500),
    });

    // The engine validated the terminal JSON against the output schema; this
    // decode recovers the typed value on this side of the generic boundary.
    const review = yield* Schema.decodeUnknownEffect(CodeReview)(result.output);
    const plan = planPublication(review, files, {
      applyVerdict: options.applyVerdict,
      headSha: metadata.headSha,
      totalChangedFiles: metadata.totalChangedFiles,
    });

    if (!options.post) {
      return ReviewRunOutcome.make({ review, plan, turns: result.turns });
    }
    const publisher = yield* ReviewPublisher;
    const published = yield* publisher.publish(plan);
    return ReviewRunOutcome.make({ review, plan, published, turns: result.turns });
  });
