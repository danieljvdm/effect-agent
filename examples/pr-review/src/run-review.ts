import { Effect, Schema } from "effect";
import {
  makeUsageBudget,
  toRunBudgetHook,
  UsageBudgetLimits,
  AgentRuntime,
  type RuntimeBinding,
} from "effect-agent";
import { type Tool } from "effect/unstable/ai";

import { PublishedReview, ReviewPublisher } from "./github.ts";
import { planPublication, ReviewPublicationPlan } from "./render.ts";
import { CodeReview, ReviewMission } from "./review-agent.ts";
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
  /** Run-level usage bounds; defaults to `reviewBudgetLimits`. */
  readonly limits?: UsageBudgetLimits | undefined;
}

/**
 * Execute one review with any explicit Agent Binding whose contract is
 * `ReviewMission -> CodeReview` — the flat reviewer or the fan-out
 * coordinator; the toolkit stays generic because publication only depends on
 * the shared output contract. The binding stays a parameter (D-027): tests
 * pass scripted models, the CLI passes live OpenAI bindings, and the model
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
    const mission = ReviewMission.make({
      repository: metadata.repository,
      number: metadata.number,
      title: metadata.title,
      body: metadata.body,
      baseRef: metadata.baseRef,
      headRef: metadata.headRef,
      changedFileCount: files.length,
    });

    const budget = yield* makeUsageBudget(options.limits ?? reviewBudgetLimits);
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
