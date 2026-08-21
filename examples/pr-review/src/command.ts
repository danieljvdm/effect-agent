import {
  fullReviewExecutionContextLayer,
  gitHubReviewLayers,
  makeOpenAiReviewModel,
  openAiClientLayer,
  resolveReviewTarget,
  ReviewPublicationPlan,
} from "@effect-agent/pr-review";
import { Console, Effect, Layer, Option, Schema } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";

import { makeExampleReviewer, ReadReviewConventionsLayer } from "./reviewer.ts";

// ---------------------------------------------------------------------------
// The rung-2 consumer command: target resolution, GitHub adapters, and the
// OpenAI client come from the package; only the customization is local. The
// executable entrypoint supplies platform services and owns process startup.
// ---------------------------------------------------------------------------

const repoFlag = Flag.string("repo").pipe(
  Flag.optional,
  Flag.withDescription("Repository as owner/name; defaults to GITHUB_REPOSITORY."),
);
const prFlag = Flag.integer("pr").pipe(
  Flag.optional,
  Flag.withDescription("Pull request number; defaults to the GITHUB_EVENT_PATH payload."),
);
const modelFlag = Flag.string("model").pipe(
  Flag.optional,
  Flag.withDescription("OpenAI model id (defaults to the package default)."),
);
const postFlag = Flag.boolean("post").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Post the review to GitHub; without it the plan prints to stdout."),
);

export const command = CliCommand.make(
  "example-pr-review",
  { repo: repoFlag, pr: prFlag, model: modelFlag, post: postFlag },
  Effect.fn("examplePrReview.command")(function* (flags) {
    const target = yield* resolveReviewTarget({
      repository: Option.getOrUndefined(flags.repo),
      number: Option.getOrUndefined(flags.pr),
    });
    const reviewer = makeExampleReviewer(makeOpenAiReviewModel(Option.getOrUndefined(flags.model)));
    yield* Console.log(
      `Reviewing ${target.repository}#${target.number} with the example reviewer (${flags.post ? "posting" : "dry run"})...`,
    );
    const outcome = yield* reviewer
      .run({ post: flags.post })
      .pipe(
        Effect.provide(
          fullReviewExecutionContextLayer("explicit example CLI full review").pipe(
            Layer.provideMerge(
              Layer.mergeAll(
                gitHubReviewLayers(target),
                openAiClientLayer,
                ReadReviewConventionsLayer,
              ),
            ),
          ),
        ),
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
    "Review one GitHub pull request with this example's customized @effect-agent/pr-review reviewer.",
  ),
);
