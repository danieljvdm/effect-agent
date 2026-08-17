import { Effect, Schema } from "effect";

import {
  DispatchTarget,
  DispatchTargetRejected,
  IngressPolicy,
  type PlatformDelivery,
} from "./contracts.ts";

const GitHubActor = Schema.Struct({
  id: Schema.Int,
  login: Schema.NonEmptyString,
});

const GitHubRepository = Schema.Struct({
  full_name: Schema.NonEmptyString,
  fork: Schema.Boolean,
});

const MentionEvent = Schema.Struct({
  action: Schema.Literal("created"),
  comment: Schema.Struct({
    id: Schema.Int,
    in_reply_to_id: Schema.optionalKey(Schema.Int),
    body: Schema.String,
    user: GitHubActor,
  }),
  pull_request: Schema.Struct({
    number: Schema.Int,
  }),
  repository: GitHubRepository,
  sender: GitHubActor,
});

const ReactionEvent = Schema.Struct({
  action: Schema.Literal("created"),
  reaction: Schema.Struct({
    content: Schema.String,
    user: GitHubActor,
  }),
  comment: Schema.Struct({
    id: Schema.Int,
    path: Schema.optionalKey(Schema.String),
    user: GitHubActor,
  }),
  pull_request: Schema.Struct({
    number: Schema.Int,
  }),
  repository: GitHubRepository,
  sender: GitHubActor,
});

const decodeJson = (rawBody: string) =>
  Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(rawBody).pipe(
    Effect.mapError(() =>
      DispatchTargetRejected.make({
        reason: "delivery body is not JSON",
      }),
    ),
  );

const actorId = (id: number): string => String(id);

export const parseDispatchTarget = Effect.fn("parseDispatchTarget")(function* (
  delivery: PlatformDelivery,
): Effect.fn.Return<DispatchTarget, DispatchTargetRejected, IngressPolicy> {
  const policy = yield* IngressPolicy;
  const payload = yield* decodeJson(delivery.rawBody);
  if (delivery.eventName === "issue_comment") {
    return yield* DispatchTargetRejected.make({
      reason: "a pull-request conversation comment is not an inline review comment",
    });
  }
  if (delivery.eventName === "pull_request_review") {
    return yield* DispatchTargetRejected.make({
      reason: "a review summary is not an inline review comment",
    });
  }
  if (delivery.eventName === "pull_request_review_comment") {
    const event = yield* Schema.decodeUnknownEffect(MentionEvent)(payload).pipe(
      Effect.mapError(() =>
        DispatchTargetRejected.make({
          reason: "mention delivery is not a created inline review comment",
        }),
      ),
    );
    if (event.comment.body !== policy.mentionCommand) {
      return yield* DispatchTargetRejected.make({
        reason: "mention command does not match configuration",
      });
    }
    if (event.comment.in_reply_to_id === undefined) {
      return yield* DispatchTargetRejected.make({
        reason: "mention is not linked to one inline comment",
      });
    }
    return DispatchTarget.make({
      kind: "mention",
      actorId: actorId(event.sender.id),
      actorLogin: event.sender.login,
      targetCommentId: String(event.comment.in_reply_to_id),
      repository: event.repository.full_name,
      pullRequestNumber: event.pull_request.number,
    });
  }
  if (delivery.eventName === "reaction") {
    const event = yield* Schema.decodeUnknownEffect(ReactionEvent)(payload).pipe(
      Effect.mapError(() =>
        DispatchTargetRejected.make({
          reason: "reaction delivery does not name one review comment",
        }),
      ),
    );
    if (event.reaction.content !== policy.reactionContent) {
      return yield* DispatchTargetRejected.make({
        reason: "reaction does not match configuration",
      });
    }
    return DispatchTarget.make({
      kind: "reaction",
      actorId: actorId(event.sender.id),
      actorLogin: event.sender.login,
      targetCommentId: String(event.comment.id),
      repository: event.repository.full_name,
      pullRequestNumber: event.pull_request.number,
    });
  }
  return yield* DispatchTargetRejected.make({
    reason: "event is not a mention reply or configured reaction",
  });
});
