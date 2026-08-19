import { Context, Effect, Layer } from "effect";

import { GitHubApiFailure, type PullRequestView, type ReviewCommentView } from "./contracts.ts";

export class GitHubApi extends Context.Service<
  GitHubApi,
  {
    readonly getPullRequest: (
      repository: string,
      pullRequestNumber: number,
    ) => Effect.Effect<PullRequestView, GitHubApiFailure>;
    readonly getReviewComment: (
      repository: string,
      commentId: string,
    ) => Effect.Effect<ReviewCommentView, GitHubApiFailure>;
  }
>()("@effect-agent/example-pr-work-order-ingress/GitHubApi") {}

export interface FakeGitHubState {
  readonly repository: string;
  readonly pullRequest: PullRequestView;
  readonly comments: ReadonlyMap<string, ReviewCommentView>;
}

export const makeFakeGitHub = (
  state: FakeGitHubState,
): { readonly layer: Layer.Layer<GitHubApi> } => ({
  layer: Layer.succeed(
    GitHubApi,
    GitHubApi.of({
      getPullRequest: (repository, pullRequestNumber) =>
        repository === state.repository && pullRequestNumber === state.pullRequest.pullRequestNumber
          ? Effect.succeed(state.pullRequest)
          : GitHubApiFailure.make({
              operation: "get pull request",
              reason: "pull request is not in the fake GitHub state",
            }),
      getReviewComment: (repository, commentId) => {
        if (repository !== state.repository) {
          return GitHubApiFailure.make({
            operation: "get review comment",
            reason: "comment is not in the configured repository",
          });
        }
        const comment = state.comments.get(commentId);
        if (comment === undefined) {
          return GitHubApiFailure.make({
            operation: "get review comment",
            reason: `review comment ${commentId} was not found`,
          });
        }
        return Effect.succeed(comment);
      },
    }),
  ),
});
