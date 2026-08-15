import { Config, Effect, FileSystem, Layer, Option, Schema } from "effect";
import type { HttpClient } from "effect/unstable/http";

import type { PriorReviews, ReviewPublisher } from "./github.ts";
import {
  GitHubReviewTarget,
  gitHubPriorReviewsLayer,
  gitHubPullRequestSourceLayer,
  gitHubReviewPublisherLayer,
  gitHubReviewRetirementHostLayer,
} from "./github.ts";
import type { ReviewRetirementHost } from "./retirement.ts";
import type { PullRequestSource } from "./source.ts";

// ---------------------------------------------------------------------------
// GitHub Actions environment resolution: which pull request to review, from
// explicit values first and the standard Actions environment second
// (GITHUB_REPOSITORY, GITHUB_EVENT_PATH, GITHUB_API_URL, GITHUB_TOKEN).
// Platform-free: FileSystem and Config are Effect services supplied by the
// host entrypoint.
// ---------------------------------------------------------------------------

/** The pull request could not be resolved from options or the environment. */
export class ReviewTargetUnresolved extends Schema.TaggedError<ReviewTargetUnresolved>()(
  "ReviewTargetUnresolved",
  {
    reason: Schema.String,
  },
) {
  override get message() {
    return this.reason;
  }
}

/** The slice of a GitHub Actions event payload this package understands. */
export const GitHubEventWire = Schema.Struct({
  pull_request: Schema.optionalKey(
    Schema.Struct({
      number: Schema.Int,
      draft: Schema.optionalKey(Schema.Boolean),
    }),
  ),
  repository: Schema.optionalKey(Schema.Struct({ full_name: Schema.String })),
});
export type GitHubEventWire = typeof GitHubEventWire.Type;

const decodeEvent = Schema.decodeUnknownEffect(Schema.fromJsonString(GitHubEventWire));

/** Read and decode the GITHUB_EVENT_PATH payload, or none outside Actions. */
export const readGitHubEvent = Effect.fn("readGitHubEvent")(function* () {
  const eventPath = yield* Config.string("GITHUB_EVENT_PATH").pipe(Config.withDefault(""));
  if (eventPath === "") return Option.none<GitHubEventWire>();
  const fs = yield* FileSystem.FileSystem;
  const raw = yield* fs
    .readFileString(eventPath)
    .pipe(
      Effect.mapError((error) =>
        ReviewTargetUnresolved.make({ reason: `Cannot read event payload: ${error.message}` }),
      ),
    );
  const event = yield* decodeEvent(raw).pipe(
    Effect.mapError((error) =>
      ReviewTargetUnresolved.make({ reason: `Cannot decode event payload: ${error.message}` }),
    ),
  );
  return Option.some(event);
});

export interface ResolvedReviewTarget {
  readonly repository: string;
  readonly number: number;
}

/**
 * Resolve the review target: explicit values win, then GITHUB_REPOSITORY and
 * the pull_request event payload. Fails typed when no target can be named.
 */
export const resolveReviewTarget = Effect.fn("resolveReviewTarget")(function* (options: {
  readonly repository?: string | undefined;
  readonly number?: number | undefined;
}) {
  let repository = options.repository ?? "";
  if (repository === "") {
    repository = yield* Config.string("GITHUB_REPOSITORY").pipe(Config.withDefault(""));
  }
  let number = options.number;
  if (number === undefined || repository === "") {
    const event = yield* readGitHubEvent();
    if (Option.isSome(event)) {
      number ??= event.value.pull_request?.number;
      if (repository === "") repository = event.value.repository?.full_name ?? "";
    }
  }
  if (repository === "" || number === undefined) {
    return yield* ReviewTargetUnresolved.make({
      reason:
        "No pull request to review: pass an explicit repository and number, or run inside a GitHub Actions pull_request event.",
    });
  }
  return { repository, number } satisfies ResolvedReviewTarget;
});

/**
 * Build the GitHub source and publisher Layers for one resolved target,
 * reading GITHUB_API_URL and GITHUB_TOKEN from configuration. The returned
 * Layer is the complete GitHub side of a review run.
 */
export const gitHubReviewLayers = (
  target: ResolvedReviewTarget,
): Layer.Layer<
  PullRequestSource | ReviewPublisher | PriorReviews | ReviewRetirementHost,
  Config.ConfigError,
  HttpClient.HttpClient
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const apiUrl = yield* Config.string("GITHUB_API_URL").pipe(
        Config.withDefault("https://api.github.com"),
      );
      const graphqlUrl = yield* Config.string("GITHUB_GRAPHQL_URL").pipe(
        Config.withDefault(
          apiUrl === "https://api.github.com"
            ? "https://api.github.com/graphql"
            : apiUrl.replace(/\/api\/v3$/, "/api/graphql"),
        ),
      );
      const token = yield* Config.option(Config.redacted("GITHUB_TOKEN"));
      const targetLayer = GitHubReviewTarget.layer({
        apiUrl,
        graphqlUrl,
        repository: target.repository,
        number: target.number,
        token,
      });
      return Layer.mergeAll(
        gitHubPullRequestSourceLayer.pipe(Layer.provide(targetLayer)),
        gitHubReviewPublisherLayer.pipe(Layer.provide(targetLayer)),
        gitHubPriorReviewsLayer.pipe(Layer.provide(targetLayer)),
        gitHubReviewRetirementHostLayer.pipe(Layer.provide(targetLayer)),
      );
    }),
  );
