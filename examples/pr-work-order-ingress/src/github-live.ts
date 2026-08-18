import { GitCommitSha } from "@effect-agent/example-pr-work-orders";
import { Effect, Layer, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import {
  GitHubApiFailure,
  PresentationFailure,
  PullRequestView,
  ReviewCommentView,
} from "./contracts.ts";
import { GitHubApi } from "./github.ts";

const GitHubRepoWire = Schema.Struct({
  full_name: Schema.NonEmptyString,
  fork: Schema.Boolean,
});

export const GitHubPullWire = Schema.Struct({
  number: Schema.Int,
  head: Schema.Struct({
    sha: GitCommitSha,
    ref: Schema.NonEmptyString,
    repo: Schema.NullOr(GitHubRepoWire),
  }),
  base: Schema.Struct({
    repo: GitHubRepoWire,
  }),
});

export const GitHubReviewCommentWire = Schema.Struct({
  id: Schema.Int,
  user: Schema.Struct({
    id: Schema.Int,
    login: Schema.NonEmptyString,
  }),
  commit_id: GitCommitSha,
  path: Schema.optionalKey(Schema.String),
  line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  start_line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  original_line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  body: Schema.String,
  pull_request_url: Schema.NonEmptyString,
});

const pullNumberFromUrl = (url: string): number | undefined => {
  const match = /\/pulls\/(\d+)$/.exec(url);
  if (match?.[1] === undefined) return undefined;
  const value = Number(match[1]);
  return Number.isInteger(value) && value > 0 ? value : undefined;
};

export const pullRequestFromWire = (
  repository: string,
  wire: typeof GitHubPullWire.Type,
): PullRequestView =>
  PullRequestView.make({
    repository,
    pullRequestNumber: wire.number,
    headSha: wire.head.sha,
    headRef: wire.head.ref,
    ...(wire.head.repo === null ? {} : { headRepository: wire.head.repo.full_name }),
    headIsFork: wire.head.repo?.fork === true,
    baseRepository: wire.base.repo.full_name,
  });

export const reviewCommentFromWire = (
  wire: typeof GitHubReviewCommentWire.Type,
): ReviewCommentView => {
  const endLine = wire.line ?? wire.original_line ?? undefined;
  const startLine = wire.start_line ?? endLine;
  return ReviewCommentView.make({
    commentId: String(wire.id),
    authorId: String(wire.user.id),
    authorLogin: wire.user.login,
    commitSha: wire.commit_id,
    ...(wire.path === undefined || wire.path.length === 0 ? {} : { path: wire.path }),
    ...(startLine === undefined || startLine === null ? {} : { startLine }),
    ...(endLine === undefined || endLine === null ? {} : { endLine }),
    body: wire.body.slice(0, 4_000),
  });
};

export const liveGitHubApiLayer = (options: {
  readonly token: string;
  readonly apiUrl?: string | undefined;
}): Layer.Layer<GitHubApi, never, HttpClient.HttpClient> =>
  Layer.effect(
    GitHubApi,
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const apiUrl = (options.apiUrl ?? "https://api.github.com").replace(/\/$/, "");
      const asFailure =
        (operation: string) =>
        (error: { readonly _tag: string; readonly message?: string }): GitHubApiFailure =>
          GitHubApiFailure.make({
            operation,
            reason: `${error._tag}: ${error.message ?? "request failed"}`.slice(0, 2_048),
          });
      const withHeaders = (request: HttpClientRequest.HttpClientRequest) =>
        request.pipe(
          HttpClientRequest.setHeaders({
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "effect-agent-pr-work-order-ingress",
          }),
          HttpClientRequest.acceptJson,
          HttpClientRequest.bearerToken(options.token),
        );
      const execute = (operation: string, request: HttpClientRequest.HttpClientRequest) =>
        HttpClient.execute(request).pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.mapError(asFailure(operation)),
          Effect.provideService(HttpClient.HttpClient, client),
        );
      const decodeJson =
        <S extends Schema.Top>(schema: S, operation: string) =>
        (response: HttpClientResponse.HttpClientResponse) =>
          response.json.pipe(
            Effect.mapError(asFailure(operation)),
            Effect.flatMap((payload) =>
              Schema.decodeUnknownEffect(schema)(payload).pipe(
                Effect.mapError(asFailure(operation)),
              ),
            ),
          );
      const loadPull = (repository: string, pullRequestNumber: number) =>
        Effect.gen(function* () {
          const response = yield* execute(
            "get pull request",
            withHeaders(
              HttpClientRequest.get(
                `${apiUrl}/repos/${repository}/pulls/${String(pullRequestNumber)}`,
              ),
            ),
          );
          return yield* decodeJson(GitHubPullWire, "get pull request")(response);
        });
      return GitHubApi.of({
        getPullRequest: (repository, pullRequestNumber) =>
          loadPull(repository, pullRequestNumber).pipe(
            Effect.map((wire) => pullRequestFromWire(repository, wire)),
          ),
        getReviewComment: (repository, commentId) =>
          Effect.gen(function* () {
            const response = yield* execute(
              "get review comment",
              withHeaders(
                HttpClientRequest.get(`${apiUrl}/repos/${repository}/pulls/comments/${commentId}`),
              ),
            );
            const wire = yield* decodeJson(GitHubReviewCommentWire, "get review comment")(response);
            return reviewCommentFromWire(wire);
          }),
        currentHead: (repository, pullRequestNumber) =>
          loadPull(repository, pullRequestNumber).pipe(Effect.map((wire) => wire.head.sha)),
        updateHead: () =>
          GitHubApiFailure.make({
            operation: "update pull-request head",
            reason: "this entrypoint admits and replies; it does not publish commits",
          }),
        postThreadReply: (input) =>
          Effect.gen(function* () {
            const toPresentation = (error: GitHubApiFailure) =>
              PresentationFailure.make({ reason: error.reason });
            const commentResponse = yield* execute(
              "read review comment for reply",
              withHeaders(
                HttpClientRequest.get(
                  `${apiUrl}/repos/${input.repository}/pulls/comments/${input.commentId}`,
                ),
              ),
            ).pipe(Effect.mapError(toPresentation));
            const comment = yield* decodeJson(
              GitHubReviewCommentWire,
              "read review comment for reply",
            )(commentResponse).pipe(Effect.mapError(toPresentation));
            const pullRequestNumber = pullNumberFromUrl(comment.pull_request_url);
            if (pullRequestNumber === undefined) {
              return yield* PresentationFailure.make({
                reason: "review comment does not name a pull request",
              });
            }
            yield* execute(
              "post thread reply",
              withHeaders(
                HttpClientRequest.post(
                  `${apiUrl}/repos/${input.repository}/pulls/${String(pullRequestNumber)}/comments`,
                ).pipe(
                  HttpClientRequest.bodyJsonUnsafe({
                    body: input.reply.body,
                    in_reply_to: Number(input.commentId),
                  }),
                ),
              ),
            ).pipe(Effect.mapError(toPresentation));
          }),
        resolveThread: () =>
          PresentationFailure.make({
            reason: "ingress never resolves the source thread",
          }),
      });
    }),
  );
