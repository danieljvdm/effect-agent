import { GitCommitSha, StalePullRequestHead } from "@effect-agent/example-pr-work-orders";
import { Context, Effect, Encoding, Layer, Result, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { type CheckedWorkOrder, JournalComment } from "./action-contracts.ts";
import type { PullRequestView } from "./contracts.ts";
import { GitHubApiFailure, PublicationUncertainty } from "./contracts.ts";
import { GitHubPullWire, pullRequestFromWire } from "./github-live.ts";

const ReviewCommentWire = Schema.Struct({
  id: Schema.Int,
  body: Schema.String,
  in_reply_to_id: Schema.optionalKey(Schema.Int),
  user: Schema.Struct({
    id: Schema.Int,
  }),
});

const GraphQlCommitResponse = Schema.Struct({
  data: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        createCommitOnBranch: Schema.NullOr(
          Schema.Struct({
            commit: Schema.Struct({ oid: GitCommitSha }),
          }),
        ),
      }),
    ),
  ),
  errors: Schema.optionalKey(
    Schema.Array(Schema.Struct({ message: Schema.String.check(Schema.isMaxLength(2_048)) })),
  ),
});

const GitHubFileWire = Schema.Struct({
  type: Schema.Literal("file"),
  encoding: Schema.Literal("base64"),
  content: Schema.String.check(Schema.isMaxLength(300_000)),
  size: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 200_000 })),
});

export class WorkOrderGitHub extends Context.Service<
  WorkOrderGitHub,
  {
    readonly getPullRequest: (
      repository: string,
      pullRequestNumber: number,
    ) => Effect.Effect<PullRequestView, GitHubApiFailure>;
    readonly listReviewComments: (
      repository: string,
      pullRequestNumber: number,
    ) => Effect.Effect<ReadonlyArray<JournalComment>, GitHubApiFailure>;
    readonly createReply: (input: {
      readonly repository: string;
      readonly pullRequestNumber: number;
      readonly commentId: string;
      readonly body: string;
    }) => Effect.Effect<JournalComment, GitHubApiFailure>;
    readonly updateComment: (input: {
      readonly repository: string;
      readonly commentId: string;
      readonly body: string;
    }) => Effect.Effect<JournalComment, GitHubApiFailure>;
    readonly getFileContent: (input: {
      readonly repository: string;
      readonly path: string;
      readonly ref: GitCommitSha;
    }) => Effect.Effect<string, GitHubApiFailure>;
    readonly publish: (input: {
      readonly checked: CheckedWorkOrder;
      readonly message: string;
    }) => Effect.Effect<
      GitCommitSha,
      GitHubApiFailure | StalePullRequestHead | PublicationUncertainty
    >;
  }
>()("@effect-agent/example-pr-work-order-ingress/WorkOrderGitHub") {}

const commentFromWire = (wire: typeof ReviewCommentWire.Type): JournalComment =>
  JournalComment.make({
    id: String(wire.id),
    authorId: String(wire.user.id),
    ...(wire.in_reply_to_id === undefined ? {} : { inReplyToId: String(wire.in_reply_to_id) }),
    body: wire.body,
  });

export const liveWorkOrderGitHubLayer = (options: {
  readonly token: string;
  readonly apiUrl?: string | undefined;
  readonly graphqlUrl?: string | undefined;
}): Layer.Layer<WorkOrderGitHub, never, HttpClient.HttpClient> =>
  Layer.effect(
    WorkOrderGitHub,
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const apiUrl = (options.apiUrl ?? "https://api.github.com").replace(/\/$/, "");
      const graphqlUrl = options.graphqlUrl ?? "https://api.github.com/graphql";
      const failure = (operation: string, cause: unknown) =>
        GitHubApiFailure.make({
          operation,
          reason: String(cause).slice(0, 4_096),
        });
      const request = (value: HttpClientRequest.HttpClientRequest) =>
        value.pipe(
          HttpClientRequest.setHeaders({
            Accept: "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "effect-agent-pr-work-order",
          }),
          HttpClientRequest.bearerToken(options.token),
        );
      const execute = (operation: string, value: HttpClientRequest.HttpClientRequest) =>
        HttpClient.execute(request(value)).pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.mapError((cause) => failure(operation, cause)),
          Effect.provideService(HttpClient.HttpClient, client),
        );
      const decode =
        <S extends Schema.Top>(schema: S, operation: string) =>
        (response: HttpClientResponse.HttpClientResponse) =>
          response.json.pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(schema)),
            Effect.mapError((cause) => failure(operation, cause)),
          );
      const getPullRequest = (repository: string, pullRequestNumber: number) =>
        execute(
          "get pull request",
          HttpClientRequest.get(`${apiUrl}/repos/${repository}/pulls/${String(pullRequestNumber)}`),
        ).pipe(
          Effect.flatMap(decode(GitHubPullWire, "get pull request")),
          Effect.map((wire) => pullRequestFromWire(repository, wire)),
        );
      const listReviewComments = (repository: string, pullRequestNumber: number) =>
        Effect.gen(function* () {
          const all: Array<JournalComment> = [];
          for (let page = 1; page <= 10; page += 1) {
            const response = yield* execute(
              "list review comments",
              HttpClientRequest.get(
                `${apiUrl}/repos/${repository}/pulls/${String(pullRequestNumber)}/comments?per_page=100&page=${String(page)}`,
              ),
            );
            const decoded = yield* decode(
              Schema.Array(ReviewCommentWire),
              "list review comments",
            )(response);
            all.push(...decoded.map(commentFromWire));
            if (decoded.length < 100) return all;
          }
          return yield* GitHubApiFailure.make({
            operation: "list review comments",
            reason: "review comment history exceeds the 1,000-comment admission bound",
          });
        });
      const createReply = (input: {
        readonly repository: string;
        readonly pullRequestNumber: number;
        readonly commentId: string;
        readonly body: string;
      }) =>
        execute(
          "create work-order journal reply",
          HttpClientRequest.post(
            `${apiUrl}/repos/${input.repository}/pulls/${String(input.pullRequestNumber)}/comments/${input.commentId}/replies`,
          ).pipe(HttpClientRequest.bodyJsonUnsafe({ body: input.body })),
        ).pipe(
          Effect.flatMap(decode(ReviewCommentWire, "create work-order journal reply")),
          Effect.map(commentFromWire),
        );
      const updateComment = (input: {
        readonly repository: string;
        readonly commentId: string;
        readonly body: string;
      }) =>
        execute(
          "update work-order journal reply",
          HttpClientRequest.patch(
            `${apiUrl}/repos/${input.repository}/pulls/comments/${input.commentId}`,
          ).pipe(HttpClientRequest.bodyJsonUnsafe({ body: input.body })),
        ).pipe(
          Effect.flatMap(decode(ReviewCommentWire, "update work-order journal reply")),
          Effect.map(commentFromWire),
        );
      const getFileContent: WorkOrderGitHub["Service"]["getFileContent"] = (input) => {
        const encodedPath = input.path.split("/").map(encodeURIComponent).join("/");
        return execute(
          `read expected-head file ${input.path}`,
          HttpClientRequest.get(
            `${apiUrl}/repos/${input.repository}/contents/${encodedPath}?ref=${input.ref}`,
          ),
        ).pipe(
          Effect.flatMap(decode(GitHubFileWire, `read expected-head file ${input.path}`)),
          Effect.flatMap((wire) => {
            const bytes = Result.getOrUndefined(
              Encoding.decodeBase64(wire.content.replaceAll("\n", "")),
            );
            const content =
              bytes === undefined
                ? undefined
                : Result.getOrUndefined(
                    Result.try({
                      try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
                      catch: (cause) => cause,
                    }),
                  );
            return content !== undefined && content.length <= 200_000
              ? Effect.succeed(content)
              : GitHubApiFailure.make({
                  operation: `read expected-head file ${input.path}`,
                  reason: "GitHub file content is not bounded UTF-8 text",
                });
          }),
        );
      };
      const publish: WorkOrderGitHub["Service"]["publish"] = ({ checked, message }) =>
        Effect.gen(function* () {
          const { order } = checked.admission;
          const pull = yield* getPullRequest(order.repository, order.pullRequestNumber);
          if (
            pull.repository !== order.repository ||
            pull.baseRepository !== order.repository ||
            pull.headRepository !== order.repository ||
            pull.headIsFork
          ) {
            return yield* GitHubApiFailure.make({
              operation: "publish work-order commit",
              reason: "pull request is no longer a trusted same-repository branch",
            });
          }
          if (pull.headSha !== order.headSha) {
            return yield* StalePullRequestHead.make({
              expected: order.headSha,
              actual: pull.headSha,
            });
          }
          const body = {
            query:
              "mutation($input: CreateCommitOnBranchInput!) { createCommitOnBranch(input: $input) { commit { oid } } }",
            variables: {
              input: {
                branch: {
                  repositoryNameWithOwner: order.repository,
                  branchName: pull.headRef,
                },
                expectedHeadOid: order.headSha,
                message: { headline: message.slice(0, 256) },
                fileChanges: {
                  additions: checked.files.map((file) => ({
                    path: file.path,
                    contents: Encoding.encodeBase64(file.content),
                  })),
                },
                clientMutationId: order.workOrderId,
              },
            },
          };
          const attempted = yield* execute(
            "publish work-order commit",
            HttpClientRequest.post(graphqlUrl).pipe(HttpClientRequest.bodyJsonUnsafe(body)),
          ).pipe(
            Effect.flatMap(decode(GraphQlCommitResponse, "publish work-order commit")),
            Effect.option,
          );
          let confirmedError: string | undefined;
          if (attempted._tag === "Some") {
            const published = attempted.value.data?.createCommitOnBranch?.commit.oid;
            if (published !== undefined && (attempted.value.errors?.length ?? 0) === 0) {
              return published;
            }
            if ((attempted.value.errors?.length ?? 0) > 0) {
              confirmedError = attempted.value.errors
                ?.map((error) => error.message)
                .join("; ")
                .slice(0, 4_096);
            }
          }
          const observed = yield* getPullRequest(order.repository, order.pullRequestNumber).pipe(
            Effect.map((current) => current.headSha),
            Effect.orElseSucceed(() => undefined),
          );
          if (observed !== undefined && observed !== order.headSha) {
            return yield* StalePullRequestHead.make({ expected: order.headSha, actual: observed });
          }
          if (confirmedError !== undefined && confirmedError.length > 0) {
            return yield* GitHubApiFailure.make({
              operation: "publish work-order commit",
              reason: confirmedError,
            });
          }
          return yield* PublicationUncertainty.make({
            reason: "GitHub did not return a confirmed createCommitOnBranch result",
            ...(observed === undefined ? {} : { observedHeadSha: observed }),
          });
        });
      return WorkOrderGitHub.of({
        getPullRequest,
        listReviewComments,
        createReply,
        updateComment,
        getFileContent,
        publish,
      });
    }),
  );
