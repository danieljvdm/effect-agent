import {
  GitCommitSha,
  PatchDigest,
  WorkOrderDigest,
  type WorkOrderHostError,
  type WorkspaceOperationFailure,
  WorkspacePath,
} from "@effect-agent/example-pr-work-orders";
import { Context, Layer, Schema } from "effect";

export const GITHUB_WRITE_TOKEN_ENV = "EFFECT_AGENT_GITHUB_WRITE_TOKEN";
export const MODEL_SECRET_ENV = "EFFECT_AGENT_MODEL_SECRET";

export const DEFAULT_MENTION_COMMAND = "@effect-agent fix this";
export const DEFAULT_REACTION_CONTENT = "+1";

export class ActionsIdentity extends Schema.Class<ActionsIdentity>(
  "@effect-agent/example-pr-work-order-ingress/ActionsIdentity",
)({
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  eventName: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
}) {}

export class PlatformDelivery extends Schema.Class<PlatformDelivery>(
  "@effect-agent/example-pr-work-order-ingress/PlatformDelivery",
)({
  deliveryId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  eventName: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  rawBody: Schema.String.check(Schema.isMaxLength(200_000)),
  signature: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(200))),
  actionsIdentity: Schema.optionalKey(ActionsIdentity),
}) {}

export class IngressPolicyConfig extends Schema.Class<IngressPolicyConfig>(
  "@effect-agent/example-pr-work-order-ingress/IngressPolicyConfig",
)({
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  authorizedActorIds: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(100))).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(100),
  ),
  mentionCommand: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  reactionContent: Schema.NonEmptyString.check(Schema.isMaxLength(40)),
  webhookSecret: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
}) {}

export class IngressPolicy extends Context.Service<IngressPolicy, IngressPolicyConfig>()(
  "@effect-agent/example-pr-work-order-ingress/IngressPolicy",
) {
  static readonly layer = (config: IngressPolicyConfig) =>
    Layer.succeed(IngressPolicy, IngressPolicy.of(config));
}

export class DeliveryUnauthentic extends Schema.TaggedError<DeliveryUnauthentic>()(
  "DeliveryUnauthentic",
  {
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class DispatchUnauthorized extends Schema.TaggedError<DispatchUnauthorized>()(
  "DispatchUnauthorized",
  {
    actorId: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class DispatchTargetRejected extends Schema.TaggedError<DispatchTargetRejected>()(
  "DispatchTargetRejected",
  {
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class UntrustedPullRequest extends Schema.TaggedError<UntrustedPullRequest>()(
  "UntrustedPullRequest",
  {
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class StaleCommentAnchor extends Schema.TaggedError<StaleCommentAnchor>()(
  "StaleCommentAnchor",
  {
    sourceSha: GitCommitSha,
    headSha: GitCommitSha,
  },
) {}

export class AttemptIncomplete extends Schema.TaggedError<AttemptIncomplete>()(
  "AttemptIncomplete",
  {
    eventId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
    workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  },
) {}

export class StoredDeliveryFailure extends Schema.TaggedError<StoredDeliveryFailure>()(
  "StoredDeliveryFailure",
  {
    errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
    detail: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class IngressStoreFailure extends Schema.TaggedError<IngressStoreFailure>()(
  "IngressStoreFailure",
  {
    operation: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
  },
) {}

export class GitHubApiFailure extends Schema.TaggedError<GitHubApiFailure>()("GitHubApiFailure", {
  operation: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  reason: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
}) {}

export const PublisherVerificationReason = Schema.Literals([
  "digest-mismatch",
  "path-not-allowed",
  "check-evidence",
  "identity-mismatch",
]);
export type PublisherVerificationReason = typeof PublisherVerificationReason.Type;

export class PublisherVerificationFailure extends Schema.TaggedError<PublisherVerificationFailure>()(
  "PublisherVerificationFailure",
  {
    reason: PublisherVerificationReason,
    detail: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class PublicationUncertainty extends Schema.TaggedError<PublicationUncertainty>()(
  "PublicationUncertainty",
  {
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
    observedHeadSha: Schema.optionalKey(GitCommitSha),
  },
) {}

export class PresentationFailure extends Schema.TaggedError<PresentationFailure>()(
  "PresentationFailure",
  {
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class IsolationViolation extends Schema.TaggedError<IsolationViolation>()(
  "IsolationViolation",
  {
    process: Schema.Literals(["check", "publish"]),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export type IngressError =
  | DeliveryUnauthentic
  | DispatchUnauthorized
  | DispatchTargetRejected
  | UntrustedPullRequest
  | StaleCommentAnchor
  | AttemptIncomplete
  | StoredDeliveryFailure
  | IngressStoreFailure
  | GitHubApiFailure
  | PublisherVerificationFailure
  | PublicationUncertainty
  | PresentationFailure
  | IsolationViolation
  | WorkspaceOperationFailure
  | WorkOrderHostError;

export class PublisherArtifacts extends Schema.Class<PublisherArtifacts>(
  "@effect-agent/example-pr-work-order-ingress/PublisherArtifacts",
)({
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderDigest: WorkOrderDigest,
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  expectedHeadSha: GitCommitSha,
  patch: Schema.String.check(Schema.isMaxLength(1_000_000)),
  patchDigest: PatchDigest,
  allowedPaths: Schema.Array(WorkspacePath).check(Schema.isMaxLength(100)),
  changedPaths: Schema.Array(WorkspacePath).check(Schema.isMaxLength(100)),
  requiredChecks: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
      status: Schema.Literals(["passed", "failed"]),
      summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
    }),
  ).check(Schema.isMaxLength(20)),
}) {}

export class ThreadReply extends Schema.Class<ThreadReply>(
  "@effect-agent/example-pr-work-order-ingress/ThreadReply",
)({
  kind: Schema.Literals(["published", "settled", "failed"]),
  body: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

export class ReviewCommentView extends Schema.Class<ReviewCommentView>(
  "@effect-agent/example-pr-work-order-ingress/ReviewCommentView",
)({
  commentId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  threadId: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(200))),
  authorId: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  authorLogin: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  commitSha: GitCommitSha,
  path: Schema.optionalKey(WorkspacePath),
  startLine: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  endLine: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  body: Schema.String.check(Schema.isMaxLength(4_000)),
}) {}

export class PullRequestView extends Schema.Class<PullRequestView>(
  "@effect-agent/example-pr-work-order-ingress/PullRequestView",
)({
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  headSha: GitCommitSha,
  headRepository: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(200))),
  headIsFork: Schema.Boolean,
  baseRepository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
}) {}

export class DispatchTarget extends Schema.Class<DispatchTarget>(
  "@effect-agent/example-pr-work-order-ingress/DispatchTarget",
)({
  kind: Schema.Literals(["mention", "reaction"]),
  actorId: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  actorLogin: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  targetCommentId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

export class IsolatedEnvironment extends Schema.Class<IsolatedEnvironment>(
  "@effect-agent/example-pr-work-order-ingress/IsolatedEnvironment",
)({
  process: Schema.Literals(["check", "publish"]),
  hasWriteToken: Schema.Boolean,
  hasModelSecret: Schema.Boolean,
}) {}
