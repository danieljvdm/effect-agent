import { GitCommitSha, StalePullRequestHead } from "@effect-agent/example-pr-work-orders";
import { Context, Effect, Layer, Ref, Schema } from "effect";

import {
  GitHubApiFailure,
  type PresentationFailure,
  type PullRequestView,
  type ReviewCommentView,
  type ThreadReply,
} from "./contracts.ts";

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
    readonly currentHead: (
      repository: string,
      pullRequestNumber: number,
    ) => Effect.Effect<GitCommitSha, GitHubApiFailure>;
    readonly updateHead: (input: {
      readonly repository: string;
      readonly pullRequestNumber: number;
      readonly expectedHeadSha: GitCommitSha;
      readonly patchDigest: string;
    }) => Effect.Effect<GitCommitSha, GitHubApiFailure | StalePullRequestHead>;
    readonly postThreadReply: (input: {
      readonly repository: string;
      readonly commentId: string;
      readonly reply: ThreadReply;
    }) => Effect.Effect<void, PresentationFailure>;
    readonly resolveThread: (input: {
      readonly repository: string;
      readonly commentId: string;
    }) => Effect.Effect<void, PresentationFailure>;
  }
>()("@effect-agent/example-pr-work-order-ingress/GitHubApi") {}

export interface FakeGitHubState {
  readonly repository: string;
  readonly pullRequest: PullRequestView;
  readonly comments: ReadonlyMap<string, ReviewCommentView>;
}

export interface FakeGitHub {
  readonly layer: Layer.Layer<GitHubApi>;
  readonly replies: () => ReadonlyArray<{
    readonly commentId: string;
    readonly reply: ThreadReply;
  }>;
  readonly resolveCount: () => number;
  readonly headSha: () => GitCommitSha;
}

const nextHead = (expected: GitCommitSha, patchDigest: string): GitCommitSha => {
  const hex = `${expected.slice(0, 8)}${patchDigest}`.replace(/[^0-9a-f]/g, "a").slice(0, 40);
  return Schema.decodeUnknownSync(GitCommitSha)(hex.padEnd(40, "a"));
};

export const makeFakeGitHub = (state: FakeGitHubState): FakeGitHub => {
  const pullRequest = { ...state.pullRequest };
  const replies: Array<{ readonly commentId: string; readonly reply: ThreadReply }> = [];
  let resolveCount = 0;
  const layer = Layer.effect(
    GitHubApi,
    Effect.gen(function* () {
      const head = yield* Ref.make(pullRequest.headSha);
      return GitHubApi.of({
        getPullRequest: (repository, pullRequestNumber) =>
          Effect.gen(function* () {
            if (
              repository !== state.repository ||
              pullRequestNumber !== pullRequest.pullRequestNumber
            ) {
              return yield* GitHubApiFailure.make({
                operation: "get pull request",
                reason: "pull request is not in the fake GitHub state",
              });
            }
            return {
              ...pullRequest,
              headSha: yield* Ref.get(head),
            };
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
        currentHead: (repository, pullRequestNumber) =>
          Effect.gen(function* () {
            if (
              repository !== state.repository ||
              pullRequestNumber !== pullRequest.pullRequestNumber
            ) {
              return yield* GitHubApiFailure.make({
                operation: "read pull-request head",
                reason: "pull request is not in the fake GitHub state",
              });
            }
            return yield* Ref.get(head);
          }),
        updateHead: (input) =>
          Effect.gen(function* () {
            if (
              input.repository !== state.repository ||
              input.pullRequestNumber !== pullRequest.pullRequestNumber
            ) {
              return yield* GitHubApiFailure.make({
                operation: "update pull-request head",
                reason: "pull request is not in the fake GitHub state",
              });
            }
            const actual = yield* Ref.get(head);
            if (actual !== input.expectedHeadSha) {
              return yield* StalePullRequestHead.make({
                expected: input.expectedHeadSha,
                actual,
              });
            }
            const published = nextHead(input.expectedHeadSha, input.patchDigest);
            yield* Ref.set(head, published);
            pullRequest.headSha = published;
            return published;
          }),
        postThreadReply: (input) =>
          Effect.sync(() => {
            replies.push({ commentId: input.commentId, reply: input.reply });
          }),
        resolveThread: () =>
          Effect.sync(() => {
            resolveCount += 1;
          }),
      });
    }),
  );
  return {
    layer,
    replies: () => replies,
    resolveCount: () => resolveCount,
    headSha: () => pullRequest.headSha,
  };
};
