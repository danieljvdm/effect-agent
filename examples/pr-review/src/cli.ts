import { BudgetExceeded } from "@effect-agent/capabilities";
import { IdGenerator } from "@effect-agent/core";
import { OpenAiClient } from "@effect/ai-openai";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Layer, Option, Schema } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";

import {
  GitHubReviewTarget,
  gitHubPullRequestSourceLayer,
  gitHubReviewPublisherLayer,
} from "./github.ts";
import { DEFAULT_REVIEW_MODEL, makeOpenAiReviewer } from "./profiles.ts";
import { ReviewPublicationPlan } from "./render.ts";
import { ReviewToolkitLayer } from "./review-agent.ts";
import { executeReview } from "./run-review.ts";

// ---------------------------------------------------------------------------
// The GitHub Action entrypoint: resolve which pull request to review (flags
// first, then the Actions event payload), run one bounded ephemeral review,
// and either print the validated publication plan (default: dry run) or post
// it as a pull-request review (--post).
// ---------------------------------------------------------------------------

/** The pull request could not be resolved from flags or the environment. */
class ReviewCliError extends Schema.TaggedError<ReviewCliError>()("ReviewCliError", {
  reason: Schema.String,
}) {
  override get message() {
    return this.reason;
  }
}

/** The slice of a GitHub Actions event payload this CLI understands. */
const GitHubEventWire = Schema.Struct({
  pull_request: Schema.optionalKey(Schema.Struct({ number: Schema.Int })),
  repository: Schema.optionalKey(Schema.Struct({ full_name: Schema.String })),
});
const decodeEvent = Schema.decodeUnknownEffect(Schema.fromJsonString(GitHubEventWire));

const repoFlag = Flag.string("repo").pipe(
  Flag.optional,
  Flag.withDescription("Repository as owner/name; defaults to GITHUB_REPOSITORY."),
);
const prFlag = Flag.integer("pr").pipe(
  Flag.optional,
  Flag.withDescription("Pull request number; defaults to the GITHUB_EVENT_PATH payload."),
);
const modelFlag = Flag.string("model").pipe(
  Flag.withDefault(DEFAULT_REVIEW_MODEL),
  Flag.withDescription(`OpenAI model id (default ${DEFAULT_REVIEW_MODEL}).`),
);
const postFlag = Flag.boolean("post").pipe(
  Flag.withDescription("Post the review to GitHub; without it the plan prints to stdout."),
);
const applyVerdictFlag = Flag.boolean("apply-verdict").pipe(
  Flag.withDescription(
    "Map the model verdict onto APPROVE/REQUEST_CHANGES instead of always COMMENT.",
  ),
);

const resolveTarget = Effect.fn("resolveTarget")(function* (flags: {
  readonly repo: Option.Option<string>;
  readonly pr: Option.Option<number>;
}) {
  let repository = Option.getOrElse(flags.repo, () => "");
  if (repository === "") {
    repository = yield* Config.string("GITHUB_REPOSITORY").pipe(Config.withDefault(""));
  }
  let number = Option.getOrUndefined(flags.pr);
  if (number === undefined || repository === "") {
    const eventPath = yield* Config.string("GITHUB_EVENT_PATH").pipe(Config.withDefault(""));
    if (eventPath !== "") {
      const fs = yield* FileSystem.FileSystem;
      const raw = yield* fs
        .readFileString(eventPath)
        .pipe(
          Effect.mapError((error) =>
            ReviewCliError.make({ reason: `Cannot read event payload: ${error.message}` }),
          ),
        );
      const event = yield* decodeEvent(raw).pipe(
        Effect.mapError((error) =>
          ReviewCliError.make({ reason: `Cannot decode event payload: ${error.message}` }),
        ),
      );
      number ??= event.pull_request?.number;
      if (repository === "") repository = event.repository?.full_name ?? "";
    }
  }
  if (repository === "" || number === undefined) {
    return yield* ReviewCliError.make({
      reason:
        "No pull request to review: pass --repo owner/name and --pr <number>, or run inside a GitHub Actions pull_request event.",
    });
  }
  return { repository, number };
});

const command = CliCommand.make(
  "pr-review",
  {
    repo: repoFlag,
    pr: prFlag,
    model: modelFlag,
    post: postFlag,
    applyVerdict: applyVerdictFlag,
  },
  (flags) =>
    Effect.gen(function* () {
      const { repository, number } = yield* resolveTarget(flags);
      const apiUrl = yield* Config.string("GITHUB_API_URL").pipe(
        Config.withDefault("https://api.github.com"),
      );
      const token = yield* Config.option(Config.redacted("GITHUB_TOKEN"));

      const targetLayer = GitHubReviewTarget.layer({ apiUrl, repository, number, token });
      const githubDeps = Layer.merge(targetLayer, FetchHttpClient.layer);
      const sourceLayer = gitHubPullRequestSourceLayer.pipe(Layer.provide(githubDeps));
      const publisherLayer = gitHubReviewPublisherLayer.pipe(Layer.provide(githubDeps));
      const openAiLayer = OpenAiClient.layerConfig({
        apiKey: Config.redacted("OPENAI_API_KEY"),
      }).pipe(Layer.provide(FetchHttpClient.layer));

      yield* Console.log(
        `Reviewing ${repository}#${number} with ${flags.model} (${flags.post ? "posting" : "dry run"})...`,
      );

      const outcome = yield* executeReview(makeOpenAiReviewer(flags.model), {
        post: flags.post,
        applyVerdict: flags.applyVerdict,
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            ReviewToolkitLayer.pipe(Layer.provideMerge(sourceLayer)),
            publisherLayer,
            openAiLayer,
            IdGenerator.layer,
          ),
        ),
        Effect.scoped,
      );

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
    "Review one GitHub pull request with an effect-agent reviewer and post the validated result as a pull-request review.",
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
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
