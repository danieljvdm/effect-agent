import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Option, Schema } from "effect";
import { BudgetExceeded, UsageBudgetLimits } from "effect-agent";

import { InvalidEffortInput, parseEffortPosition, type EffortPosition } from "./internal/effort.ts";
import { PrReview, type RunReviewOptions } from "./internal/factory.ts";
import { readGitHubEvent, resolveReviewTarget, gitHubReviewLayers } from "./internal/github-env.ts";
import { fingerprintUnchanged, PriorReviews } from "./internal/github.ts";
import {
  anthropicClientLayer,
  DEFAULT_PROVIDER,
  describeReviewModel,
  makeAnthropicReviewModel,
  makeOpenAiReviewModel,
  openAiClientLayer,
  type ReviewProvider,
} from "./internal/providers.ts";
import { fanOutReviewBudgetLimits, reviewBudgetLimits } from "./internal/run.ts";
import type { ReviewRunOutcome } from "./internal/run.ts";
import { normalizeRepoRelativePath } from "./internal/source.ts";

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
  readonly failOn: FailOnPolicy;
  readonly skipUnchanged: boolean;
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
  const fanOut = yield* Config.boolean("PR_REVIEW_FAN_OUT").pipe(Config.withDefault(false));
  const guidance = yield* Config.option(Config.nonEmptyString("PR_REVIEW_GUIDANCE"));
  const guidanceFile = yield* Config.option(Config.nonEmptyString("PR_REVIEW_GUIDANCE_FILE"));
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
    failOn,
    skipUnchanged,
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

const outcomeOutputs = (outcome: ReviewRunOutcome): ReadonlyArray<readonly [string, string]> => [
  ["skipped", "false"],
  ["verdict", outcome.review.verdict],
  ["inline-comments", String(outcome.plan.comments.length)],
  ["demoted-findings", String(outcome.plan.demoted.length)],
  ["concerns", String(outcome.review.concerns?.length ?? 0)],
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
): ReadonlyArray<string> => [
  "### Pull-request review",
  `- Verdict: **${outcome.review.verdict}**`,
  `- Inline comments: ${outcome.plan.comments.length} · demoted findings: ${outcome.plan.demoted.length} · concerns: ${outcome.review.concerns?.length ?? 0}`,
  ...(modelLabel === undefined ? [] : [`- Model: \`${modelLabel}\``]),
  ...(outcome.usage === undefined
    ? []
    : [
        `- Tokens: ${outcome.usage.inputTokens} in / ${outcome.usage.outputTokens} out${
          outcome.usageScope === "coordinator" ? " (coordinator)" : ""
        }`,
      ]),
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
    /** Model binding descriptor for the Actions step summary. */
    readonly modelLabel?: string | undefined;
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
          yield* writeStepSummary([
            "### Pull-request review skipped",
            "- Reason: changeset unchanged since the last review",
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
      const runUrl = yield* resolveRunUrl();
      const outcome = yield* reviewer.run({ post: options.post ?? true, runUrl });
      yield* Console.log(
        `Review finished in ${outcome.turns} turn(s): verdict ${outcome.review.verdict}, ` +
          `${outcome.plan.comments.length} inline comment(s), ${outcome.plan.demoted.length} demoted finding(s).`,
      );
      if (outcome.published !== undefined) {
        yield* Console.log(`Posted ${outcome.published.event} review: ${outcome.published.url}`);
      }
      yield* writeActionOutputs(outcomeOutputs(outcome));
      yield* writeStepSummary(outcomeSummary(outcome, options.modelLabel));
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
    failOn: inputs.failOn,
    skipUnchanged: inputs.skipUnchanged,
    modelLabel,
  };
  if (inputs.provider === "anthropic") {
    const model = makeAnthropicReviewModel(inputs.model, inputs.effort);
    const reviewer = inputs.fanOut
      ? PrReview.makeFanOut({ ...shared, model })
      : PrReview.make({ ...shared, model });
    return yield* runReviewAction(reviewer, harness).pipe(Effect.provide(anthropicClientLayer));
  }
  const model = makeOpenAiReviewModel(inputs.model, inputs.effort);
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
