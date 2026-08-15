import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Option, Schema } from "effect";
import { BudgetExceeded } from "effect-agent";

import { PrReview, type RunReviewOptions } from "./internal/factory.ts";
import { readGitHubEvent, resolveReviewTarget, gitHubReviewLayers } from "./internal/github-env.ts";
import { fingerprintUnchanged, PriorReviews } from "./internal/github.ts";
import {
  anthropicClientLayer,
  DEFAULT_PROVIDER,
  makeAnthropicReviewModel,
  makeOpenAiReviewModel,
  openAiClientLayer,
  type ReviewProvider,
} from "./internal/providers.ts";
import type { ReviewRunOutcome } from "./internal/run.ts";

// ---------------------------------------------------------------------------
// The GitHub Actions entrypoint (deployment class E: one bounded ephemeral
// run, no durability claim, no exactly-once posting). Inputs arrive as
// PR_REVIEW_* environment variables set by the action manifest; the target
// pull request comes from the standard Actions event environment. A non-PR
// or draft event is a typed skip — green job, nothing posted. Publication
// happens only after the run settles, so a failed or truncated run posts
// nothing.
// ---------------------------------------------------------------------------

/** When the action should turn the review verdict into a failing job. */
export const FailOnPolicy = Schema.Literals(["never", "request-changes"]);
export type FailOnPolicy = typeof FailOnPolicy.Type;

/** The review ran and the configured gate rejected the verdict. */
export class ReviewGateFailed extends Schema.TaggedError<ReviewGateFailed>()("ReviewGateFailed", {
  verdict: Schema.String,
  failOn: FailOnPolicy,
}) {
  override get message() {
    return `Review verdict '${this.verdict}' fails the configured '${this.failOn}' gate.`;
  }
}

/** Everything the packaged action reads from its environment. */
export interface ResolvedActionInputs {
  readonly provider: ReviewProvider;
  readonly model: string | undefined;
  readonly post: boolean;
  readonly applyVerdict: boolean;
  readonly fanOut: boolean;
  readonly guidance: string | undefined;
  readonly ignore: ReadonlyArray<string>;
  readonly maxFindings: number | undefined;
  readonly failOn: FailOnPolicy;
  readonly skipUnchanged: boolean;
}

/** Read the PR_REVIEW_* input surface (all optional, all defaulted). */
export const resolveActionInputs = Effect.fn("resolveActionInputs")(function* () {
  const provider = yield* Config.literals(["openai", "anthropic"], "PR_REVIEW_PROVIDER").pipe(
    Config.withDefault<ReviewProvider>(DEFAULT_PROVIDER),
  );
  const model = yield* Config.option(Config.nonEmptyString("PR_REVIEW_MODEL"));
  const post = yield* Config.boolean("PR_REVIEW_POST").pipe(Config.withDefault(true));
  const applyVerdict = yield* Config.boolean("PR_REVIEW_APPLY_VERDICT").pipe(
    Config.withDefault(false),
  );
  const fanOut = yield* Config.boolean("PR_REVIEW_FAN_OUT").pipe(Config.withDefault(false));
  const guidance = yield* Config.option(Config.nonEmptyString("PR_REVIEW_GUIDANCE"));
  const ignoreRaw = yield* Config.string("PR_REVIEW_IGNORE").pipe(Config.withDefault(""));
  const maxFindings = yield* Config.option(Config.int("PR_REVIEW_MAX_FINDINGS"));
  const failOn = yield* Config.literals(["never", "request-changes"], "PR_REVIEW_FAIL_ON").pipe(
    Config.withDefault<FailOnPolicy>("never"),
  );
  const skipUnchanged = yield* Config.boolean("PR_REVIEW_SKIP_UNCHANGED").pipe(
    Config.withDefault(true),
  );
  return {
    provider,
    model: Option.getOrUndefined(model),
    post,
    applyVerdict,
    fanOut,
    guidance: Option.getOrUndefined(guidance),
    ignore: ignoreRaw
      .split(",")
      .map((pattern) => pattern.trim())
      .filter((pattern) => pattern.length > 0),
    maxFindings: Option.getOrUndefined(maxFindings),
    failOn,
    skipUnchanged,
  } satisfies ResolvedActionInputs;
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

const outcomeOutputs = (outcome: ReviewRunOutcome): ReadonlyArray<readonly [string, string]> => [
  ["skipped", "false"],
  ["verdict", outcome.review.verdict],
  ["inline-comments", String(outcome.plan.comments.length)],
  ["demoted-findings", String(outcome.plan.demoted.length)],
  ["review-url", outcome.published?.url ?? ""],
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
    return { _tag: "Skipped", reason } satisfies ReviewActionResult;
  });

/** The reviewer surface the action harness drives: a run, and optionally the
 * changeset fingerprint enabling unchanged-changeset skips. `PrReview.make`
 * and `PrReview.makeFanOut` return exactly this shape. */
export interface HarnessedReviewer<E, R, FingerprintE, FingerprintR> {
  readonly run: (runOptions?: RunReviewOptions) => Effect.Effect<ReviewRunOutcome, E, R>;
  readonly fingerprint?: Effect.Effect<string, FingerprintE, FingerprintR> | undefined;
}

/**
 * Harness one already-built reviewer inside the Actions environment: resolve
 * the target from the event, provide the GitHub source/publisher/prior
 * reviews, skip when the changeset fingerprint matches the last posted
 * review, write step outputs, and apply the verdict gate. Skips (draft,
 * non-PR event, unchanged changeset) are values, not failures. The reviewer's
 * remaining requirements — its model client, any extra tool handlers — stay
 * visible in `R` for the caller.
 */
export const runReviewAction = <E, R, FingerprintE = never, FingerprintR = never>(
  reviewer: HarnessedReviewer<E, R, FingerprintE, FingerprintR>,
  options: {
    readonly post?: boolean | undefined;
    readonly failOn?: FailOnPolicy | undefined;
    /** Skip when the changeset fingerprint matches the last posted review;
     * defaults to true when the reviewer carries a fingerprint. */
    readonly skipUnchanged?: boolean | undefined;
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
    // An ambient PriorReviews (a test fixture, a custom store) wins over the
    // GitHub adapter the harness provides below.
    const ambientPriorReviews = yield* Effect.serviceOption(PriorReviews);
    // One layer build for the fingerprint check AND the run, so both observe
    // the same cached pull-request snapshot.
    return yield* Effect.gen(function* () {
      if (options.skipUnchanged !== false && reviewer.fingerprint !== undefined) {
        const current = yield* reviewer.fingerprint;
        const unchanged = yield* fingerprintUnchanged(current).pipe(
          Option.isSome(ambientPriorReviews)
            ? Effect.provideService(PriorReviews, ambientPriorReviews.value)
            : (effect) => effect,
        );
        if (unchanged) {
          yield* Console.log(
            `Skipping review of ${target.repository}#${target.number}: changeset unchanged since the last review.`,
          );
          yield* writeActionOutputs([
            ["skipped", "true"],
            ["skip-reason", "changeset unchanged since the last review"],
            ["fingerprint", current],
          ]);
          return {
            _tag: "Skipped",
            reason: "changeset unchanged since the last review",
          } satisfies ReviewActionResult;
        }
      }
      yield* Console.log(
        `Reviewing ${target.repository}#${target.number} (${options.post === false ? "dry run" : "posting"})...`,
      );
      const outcome = yield* reviewer.run({ post: options.post ?? true });
      yield* Console.log(
        `Review finished in ${outcome.turns} turn(s): verdict ${outcome.review.verdict}, ` +
          `${outcome.plan.comments.length} inline comment(s), ${outcome.plan.demoted.length} demoted finding(s).`,
      );
      if (outcome.published !== undefined) {
        yield* Console.log(`Posted ${outcome.published.event} review: ${outcome.published.url}`);
      }
      yield* writeActionOutputs(outcomeOutputs(outcome));
      if (options.failOn === "request-changes" && outcome.review.verdict === "request-changes") {
        return yield* ReviewGateFailed.make({
          verdict: outcome.review.verdict,
          failOn: "request-changes",
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
  const shared = {
    guidance: inputs.guidance,
    ignore: inputs.ignore,
    maxFindings: inputs.maxFindings,
    applyVerdict: inputs.applyVerdict,
  };
  const harness = {
    post: inputs.post,
    failOn: inputs.failOn,
    skipUnchanged: inputs.skipUnchanged,
  };
  if (inputs.provider === "anthropic") {
    const model = makeAnthropicReviewModel(inputs.model);
    const reviewer = inputs.fanOut
      ? PrReview.makeFanOut({ ...shared, model })
      : PrReview.make({ ...shared, model });
    return yield* runReviewAction(reviewer, harness).pipe(Effect.provide(anthropicClientLayer));
  }
  const model = makeOpenAiReviewModel(inputs.model);
  const reviewer = inputs.fanOut
    ? PrReview.makeFanOut({ ...shared, model })
    : PrReview.make({ ...shared, model });
  return yield* runReviewAction(reviewer, harness).pipe(Effect.provide(openAiClientLayer));
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
      Effect.provide(NodeServices.layer),
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
