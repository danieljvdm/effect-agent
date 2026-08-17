import {
  createWorkOrder,
  normalizeWorkspacePath,
  WorkOrderIdentity,
} from "@effect-agent/example-pr-work-orders";
import { Effect } from "effect";

import {
  DispatchTargetRejected,
  DispatchUnauthorized,
  StaleCommentAnchor,
  UntrustedPullRequest,
  type DispatchTarget,
  type IngressPolicy,
} from "./contracts.ts";
import { GitHubApi } from "./github.ts";

export const constructWorkOrder = Effect.fn("constructWorkOrder")(function* (
  target: DispatchTarget,
  policy: IngressPolicy["Service"],
  eventId: string,
) {
  if (!policy.authorizedActorIds.includes(target.actorId)) {
    return yield* DispatchUnauthorized.make({
      actorId: target.actorId,
      reason: "dispatch actor id is not a configured human principal",
    });
  }
  if (
    target.repository !== policy.repository ||
    target.pullRequestNumber !== policy.pullRequestNumber
  ) {
    return yield* UntrustedPullRequest.make({
      reason: "event does not target the configured repository and pull request",
    });
  }
  const github = yield* GitHubApi;
  const comment = yield* github.getReviewComment(target.repository, target.targetCommentId);
  const pull = yield* github.getPullRequest(target.repository, target.pullRequestNumber);
  if (
    pull.repository !== policy.repository ||
    pull.pullRequestNumber !== policy.pullRequestNumber ||
    pull.baseRepository !== policy.repository ||
    pull.headRepository !== policy.repository ||
    pull.headIsFork
  ) {
    return yield* UntrustedPullRequest.make({
      reason: "pull request is a fork or is not the configured same-repository target",
    });
  }
  if (comment.commitSha !== pull.headSha) {
    return yield* StaleCommentAnchor.make({
      sourceSha: comment.commitSha,
      headSha: pull.headSha,
    });
  }
  if (comment.path === undefined) {
    return yield* DispatchTargetRejected.make({
      reason: "inline comment does not name a repository-relative path",
    });
  }
  const path = yield* normalizeWorkspacePath(comment.path).pipe(
    Effect.mapError(() =>
      DispatchTargetRejected.make({
        reason: "inline comment path is not a normalized repository-relative path",
      }),
    ),
  );
  if (comment.body.length === 0) {
    return yield* DispatchTargetRejected.make({
      reason: "inline comment body is empty",
    });
  }
  const lineRange =
    comment.startLine !== undefined &&
    comment.endLine !== undefined &&
    comment.startLine <= comment.endLine
      ? { startLine: comment.startLine, endLine: comment.endLine }
      : comment.endLine !== undefined
        ? { startLine: comment.endLine, endLine: comment.endLine }
        : undefined;
  return yield* createWorkOrder(
    WorkOrderIdentity.make({
      version: 1,
      repository: pull.repository,
      pullRequestNumber: pull.pullRequestNumber,
      headSha: pull.headSha,
      source: {
        commentId: comment.commentId,
        ...(comment.threadId === undefined ? {} : { threadId: comment.threadId }),
        authorId: comment.authorId,
        authorLogin: comment.authorLogin,
        commitSha: comment.commitSha,
        path,
        ...(lineRange === undefined ? {} : { lineRange }),
        body: comment.body,
      },
      dispatch: {
        kind: target.kind,
        eventId,
        actorId: target.actorId,
        actorLogin: target.actorLogin,
      },
    }),
  );
});
