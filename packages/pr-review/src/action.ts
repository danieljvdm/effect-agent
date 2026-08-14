import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Option, Schema } from "effect";
import { BudgetExceeded } from "effect-agent";

import { PrReview, type RunReviewOptions } from "./internal/factory.ts";
import { readGitHubEvent, resolveReviewTarget, gitHubReviewLayers } from "./internal/github-env.ts";
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

/**
 * Harness one already-built reviewer run inside the Actions environment:
 * resolve the target from the event, provide the GitHub source and publisher,
 * write step outputs, and apply the verdict gate. Skips (draft, non-PR event)
 * are values, not failures. The reviewer's remaining requirements — its model
 * client, any extra tool handlers — stay visible in `R` for the caller.
 */
export const runReviewAction = <E, R>(
  run: (runOptions?: RunReviewOptions) => Effect.Effect<ReviewRunOutcome, E, R>,
  options: {
    readonly post?: boolean | undefined;
    readonly failOn?: FailOnPolicy | undefined;
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
    yield* Console.log(
      `Reviewing ${target.repository}#${target.number} (${options.post === false ? "dry run" : "posting"})...`,
    );
    const outcome = yield* run({ post: options.post ?? true }).pipe(
      Effect.provide(gitHubReviewLayers(target)),
    );
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
  const harness = { post: inputs.post, failOn: inputs.failOn };
  if (inputs.provider === "anthropic") {
    const model = makeAnthropicReviewModel(inputs.model);
    const reviewer = inputs.fanOut
      ? PrReview.makeFanOut({ ...shared, model })
      : PrReview.make({ ...shared, model });
    return yield* runReviewAction(reviewer.run, harness).pipe(Effect.provide(anthropicClientLayer));
  }
  const model = makeOpenAiReviewModel(inputs.model);
  const reviewer = inputs.fanOut
    ? PrReview.makeFanOut({ ...shared, model })
    : PrReview.make({ ...shared, model });
  return yield* runReviewAction(reviewer.run, harness).pipe(Effect.provide(openAiClientLayer));
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
