import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Layer, Option, Schema } from "effect";
import { BudgetExceeded } from "effect-agent";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";

import { InvalidEffortInput, parseEffortPosition, type EffortPosition } from "./internal/effort.ts";
import { PrReview, type RunReviewOptions } from "./internal/factory.ts";
import { gitHubReviewLayers, resolveReviewTarget } from "./internal/github-env.ts";
import { fingerprintUnchanged } from "./internal/github.ts";
import {
  anthropicClientLayer,
  DEFAULT_MODEL,
  DEFAULT_PROVIDER,
  describeReviewModel,
  makeAnthropicReviewModel,
  makeOpenAiReviewModel,
  openAiClientLayer,
  type ReviewProvider,
} from "./internal/providers.ts";
import { ReviewPublicationPlan } from "./internal/render.ts";
import type { ReviewRunOutcome } from "./internal/run.ts";

// ---------------------------------------------------------------------------
// The CLI entrypoint: resolve which pull request to review (flags first, then
// the GitHub Actions event environment), run one bounded ephemeral review,
// and either print the validated publication plan (default: dry run) or post
// it as a pull-request review (--post).
// ---------------------------------------------------------------------------

const repoFlag = Flag.string("repo").pipe(
  Flag.optional,
  Flag.withDescription("Repository as owner/name; defaults to GITHUB_REPOSITORY."),
);
const prFlag = Flag.integer("pr").pipe(
  Flag.optional,
  Flag.withDescription("Pull request number; defaults to the GITHUB_EVENT_PATH payload."),
);
const providerFlag = Flag.string("provider").pipe(
  Flag.withDefault<string>(DEFAULT_PROVIDER),
  Flag.withDescription('Model provider: "openai" (default) or "anthropic".'),
);
const modelFlag = Flag.string("model").pipe(
  Flag.optional,
  Flag.withDescription(
    `Model id (defaults: openai ${DEFAULT_MODEL.openai}, anthropic ${DEFAULT_MODEL.anthropic}).`,
  ),
);
const effortFlag = Flag.string("effort").pipe(
  Flag.optional,
  Flag.withDescription(
    'Reasoning effort: "low", "medium", "high", "xhigh", "max", or a number in [0, 1] resolved onto the provider\'s own ladder.',
  ),
);
const postFlag = Flag.boolean("post").pipe(
  Flag.withDescription("Post the review to GitHub; without it the plan prints to stdout."),
);
const applyVerdictFlag = Flag.boolean("apply-verdict").pipe(
  Flag.withDescription(
    "Map the model verdict onto APPROVE/REQUEST_CHANGES instead of always COMMENT.",
  ),
);
const fanOutFlag = Flag.boolean("fan-out").pipe(
  Flag.withDescription(
    "Fan the review out to bounded per-unit subagent reviewers (S1 attached delegation) instead of one flat reviewer.",
  ),
);
const ignoreFlag = Flag.string("ignore").pipe(
  Flag.withDefault(""),
  Flag.withDescription(
    'Comma-separated glob patterns removed from the review surface, e.g. "**/*.lock,dist/**".',
  ),
);
const maxFindingsFlag = Flag.integer("max-findings").pipe(
  Flag.optional,
  Flag.withDescription("Findings bound (1-20); the schema cap of 20 applies regardless."),
);
const skipUnchangedFlag = Flag.boolean("skip-unchanged").pipe(
  Flag.withDescription(
    "Skip when the changeset fingerprint matches the last posted review (explicit runs review unconditionally by default).",
  ),
);

/** An unknown --provider value; the two supported providers are spelled out. */
class UnknownProvider extends Schema.TaggedError<UnknownProvider>()("UnknownProvider", {
  provider: Schema.String,
}) {
  override get message() {
    return `Unknown provider '${this.provider}': expected "openai" or "anthropic".`;
  }
}

const decodeProvider = (raw: string): Effect.Effect<ReviewProvider, UnknownProvider> =>
  raw === "openai" || raw === "anthropic"
    ? Effect.succeed(raw)
    : Effect.fail(UnknownProvider.make({ provider: raw }));

const command = CliCommand.make(
  "pr-review",
  {
    repo: repoFlag,
    pr: prFlag,
    provider: providerFlag,
    model: modelFlag,
    effort: effortFlag,
    post: postFlag,
    applyVerdict: applyVerdictFlag,
    fanOut: fanOutFlag,
    ignore: ignoreFlag,
    maxFindings: maxFindingsFlag,
    skipUnchanged: skipUnchangedFlag,
  },
  (flags) =>
    Effect.gen(function* () {
      const provider = yield* decodeProvider(flags.provider);
      const target = yield* resolveReviewTarget({
        repository: Option.getOrUndefined(flags.repo),
        number: Option.getOrUndefined(flags.pr),
      });
      const model = Option.getOrUndefined(flags.model);
      const effortRaw = Option.getOrUndefined(flags.effort);
      let effort: EffortPosition | undefined;
      if (effortRaw !== undefined) {
        effort = parseEffortPosition(effortRaw);
        if (effort === undefined) {
          return yield* InvalidEffortInput.make({ input: effortRaw });
        }
      }
      const shared = {
        applyVerdict: flags.applyVerdict,
        ignore: flags.ignore
          .split(",")
          .map((pattern) => pattern.trim())
          .filter((pattern) => pattern.length > 0),
        maxFindings: Option.getOrUndefined(flags.maxFindings),
        modelLabel: describeReviewModel(provider, model, effort),
      };

      yield* Console.log(
        `Reviewing ${target.repository}#${target.number} with ${provider}:${model ?? DEFAULT_MODEL[provider]} (${flags.post ? "posting" : "dry run"}${flags.fanOut ? ", fan-out" : ""})...`,
      );

      const githubLayers = gitHubReviewLayers(target);
      // Fingerprint check and run under ONE provide, sharing the cached
      // pull-request snapshot; None means "unchanged, skipped".
      const runOrSkip = <E, R, FingerprintE, FingerprintR>(reviewer: {
        readonly run: (runOptions?: RunReviewOptions) => Effect.Effect<ReviewRunOutcome, E, R>;
        readonly fingerprint: Effect.Effect<string, FingerprintE, FingerprintR>;
      }) =>
        Effect.gen(function* () {
          if (flags.skipUnchanged) {
            const current = yield* reviewer.fingerprint;
            if (yield* fingerprintUnchanged(current)) {
              return Option.none<ReviewRunOutcome>();
            }
          }
          return Option.some(yield* reviewer.run({ post: flags.post }));
        });

      let result: Option.Option<ReviewRunOutcome>;
      if (provider === "anthropic") {
        const boundModel = makeAnthropicReviewModel(model, effort);
        const reviewer = flags.fanOut
          ? PrReview.makeFanOut({ ...shared, model: boundModel })
          : PrReview.make({ ...shared, model: boundModel });
        result = yield* runOrSkip(reviewer).pipe(
          Effect.provide(Layer.merge(githubLayers, anthropicClientLayer)),
        );
      } else {
        const boundModel = makeOpenAiReviewModel(model, effort);
        const reviewer = flags.fanOut
          ? PrReview.makeFanOut({ ...shared, model: boundModel })
          : PrReview.make({ ...shared, model: boundModel });
        result = yield* runOrSkip(reviewer).pipe(
          Effect.provide(Layer.merge(githubLayers, openAiClientLayer)),
        );
      }

      if (Option.isNone(result)) {
        yield* Console.log("Skipped: changeset unchanged since the last posted review.");
        return;
      }
      const outcome = result.value;

      yield* Console.log(
        `Review finished in ${outcome.turns} turn(s): verdict ${outcome.review.verdict}, ` +
          `${outcome.plan.comments.length} inline comment(s), ${outcome.plan.demoted.length} demoted finding(s).`,
      );
      if (outcome.published !== undefined) {
        yield* Console.log(`Posted ${outcome.published.event} review: ${outcome.published.url}`);
      } else {
        const encodedPlan = yield* Schema.encodeEffect(ReviewPublicationPlan)(outcome.plan);
        yield* Console.log(JSON.stringify(encodedPlan, null, 2));
      }
    }),
).pipe(
  CliCommand.withDescription(
    "Review one GitHub pull request with an @effect-agent/pr-review reviewer and post the validated result as a pull-request review.",
  ),
);

const program = CliCommand.run(command, { version: "0.0.0" }).pipe(
  Effect.tapError((error) =>
    Console.error(
      Schema.is(BudgetExceeded)(error)
        ? `Budget exceeded: ${error.limit} observed ${error.observedValue}, limit ${error.limitValue}.`
        : String(error),
    ),
  ),
  Effect.scoped,
  Effect.provide(Layer.merge(NodeServices.layer, FetchHttpClient.layer)),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
