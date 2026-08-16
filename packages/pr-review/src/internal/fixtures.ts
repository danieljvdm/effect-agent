import { DateTime, Effect, Layer, Option, Ref, Schema } from "effect";

import { ChangedFile } from "./diff.ts";
import {
  PriorReviewLookupFailure,
  PriorReviews,
  PublishedReview,
  ReviewPublisher,
} from "./github.ts";
import type { ReviewPublicationPlan } from "./render.ts";
import type { ReviewHeadComparison, ReviewState } from "./review-state.ts";
import {
  MAX_CHANGED_FILES,
  MAX_FILE_CHARS,
  normalizeRepoRelativePath,
  PullRequestMetadata,
  PullRequestSource,
  ReviewInputViolation,
} from "./source.ts";

// ---------------------------------------------------------------------------
// Deterministic in-memory adapters for both ports: a fixture pull request
// serving the PullRequestSource, and a collecting ReviewPublisher recording
// every plan. Tests, dry runs, and live smokes run against these with no
// network and no credentials.
// ---------------------------------------------------------------------------

/** One fixture file: its changeset entry plus optional head content. */
export class FixtureFile extends Schema.Class<FixtureFile>("@effect-agent/pr-review/FixtureFile")({
  file: ChangedFile,
  headContent: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(MAX_FILE_CHARS))),
}) {}

/** A complete in-memory pull request for tests, dry runs, and live smokes. */
export class FixturePullRequest extends Schema.Class<FixturePullRequest>(
  "@effect-agent/pr-review/FixturePullRequest",
)({
  metadata: PullRequestMetadata,
  files: Schema.Array(FixtureFile).check(Schema.isMaxLength(MAX_CHANGED_FILES)),
}) {}

const requireChanged = (
  fixture: FixturePullRequest,
  path: string,
): Effect.Effect<FixtureFile, ReviewInputViolation> => {
  const entry = fixture.files.find((candidate) => candidate.file.path === path);
  return entry === undefined
    ? Effect.fail(
        ReviewInputViolation.make({
          input: path,
          reason: "Path is not part of this pull request's changeset.",
        }),
      )
    : Effect.succeed(entry);
};

/** Deterministic `PullRequestSource` over one fixture pull request. */
export const fixturePullRequestSourceLayer = (
  fixture: FixturePullRequest,
): Layer.Layer<PullRequestSource> =>
  Layer.succeed(PullRequestSource)(
    PullRequestSource.of({
      metadata: Effect.succeed(fixture.metadata),
      changedFiles: Effect.succeed(fixture.files.map((entry) => entry.file)),
      anchorFiles: Effect.succeed(fixture.files.map((entry) => entry.file)),
      readFile: (path) =>
        Effect.gen(function* () {
          const relative = yield* normalizeRepoRelativePath(path);
          const entry = yield* requireChanged(fixture, relative);
          if (entry.headContent === undefined) {
            return yield* ReviewInputViolation.make({
              input: relative,
              reason: "No head content is available for this file.",
            });
          }
          return entry.headContent;
        }),
    }),
  );

/** In-memory publisher: records every plan and mints a deterministic receipt. */
export const collectingReviewPublisherLayer = (
  published: Ref.Ref<ReadonlyArray<ReviewPublicationPlan>>,
): Layer.Layer<ReviewPublisher> =>
  Layer.succeed(ReviewPublisher)(
    ReviewPublisher.of({
      publish: (plan) =>
        Ref.update(published, (plans) => [...plans, plan]).pipe(
          Effect.flatMap(() => Ref.get(published)),
          Effect.map((plans) =>
            PublishedReview.make({
              reviewId: plans.length,
              url: `memory://review/${plans.length}`,
              event: plan.event,
              inlineComments: plan.comments.length,
              authorNodeId: "BOT_memory-reviewer",
              submittedAt: DateTime.makeUnsafe(
                `2026-01-01T00:00:${String(plans.length).padStart(2, "0")}Z`,
              ),
            }),
          ),
        ),
    }),
  );

/** Static `PriorReviews` service for tests: fixed history and comparisons. */
export const staticPriorReviews = (
  fingerprint: Option.Option<string>,
  options: {
    readonly state?: Option.Option<ReviewState> | undefined;
    readonly comparison?: ReviewHeadComparison | undefined;
  } = {},
): PriorReviews["Service"] =>
  PriorReviews.of({
    latestFingerprint: Effect.succeed(fingerprint),
    latestState: Effect.succeed(options.state ?? Option.none()),
    compareHeads: () =>
      options.comparison === undefined
        ? Effect.fail(PriorReviewLookupFailure.make({ reason: "no fixture comparison" }))
        : Effect.succeed(options.comparison),
  });

/** Layer form for consumers whose Effect explicitly requires `PriorReviews`. */
export const staticPriorReviewsLayer = (
  fingerprint: Option.Option<string>,
  options: {
    readonly state?: Option.Option<ReviewState> | undefined;
    readonly comparison?: ReviewHeadComparison | undefined;
  } = {},
): Layer.Layer<PriorReviews> =>
  Layer.succeed(PriorReviews)(staticPriorReviews(fingerprint, options));
