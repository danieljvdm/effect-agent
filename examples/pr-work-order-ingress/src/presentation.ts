import type {
  PullRequestWorkOrder,
  WorkOrderHostResult,
} from "@effect-agent/example-pr-work-orders";
import { Effect } from "effect";

import { ThreadReply, type IngressError } from "./contracts.ts";
import { GitHubApi } from "./github.ts";

export const replyForSuccess = (result: WorkOrderHostResult): ThreadReply => {
  if (result._tag === "published") {
    return ThreadReply.make({
      kind: "published",
      body: `published ${result.publishedHeadSha} ${result.changedPaths.join(", ")}`,
    });
  }
  return ThreadReply.make({
    kind: "settled",
    body: `settled ${result.disposition}: ${result.summary}`,
  });
};

export const replyForFailure = (error: IngressError): ThreadReply =>
  ThreadReply.make({
    kind: "failed",
    body: `failed ${error._tag}`,
  });

export const presentSuccess = Effect.fn("presentSuccess")(function* (
  order: PullRequestWorkOrder,
  result: WorkOrderHostResult,
) {
  const github = yield* GitHubApi;
  yield* github.postThreadReply({
    repository: order.repository,
    commentId: order.source.commentId,
    reply: replyForSuccess(result),
  });
});

export const presentFailure = Effect.fn("presentFailure")(function* (
  order: PullRequestWorkOrder,
  error: IngressError,
) {
  const github = yield* GitHubApi;
  yield* github.postThreadReply({
    repository: order.repository,
    commentId: order.source.commentId,
    reply: replyForFailure(error),
  });
});
