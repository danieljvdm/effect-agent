import { Schema } from "effect";

import {
  GitCommitSha,
  PatchDigest,
  WorkOrderDigest,
  WorkspacePath,
} from "../../pr-work-orders/src/contracts.ts";

export class IsolatedCheckSpec extends Schema.Class<IsolatedCheckSpec>(
  "@effect-agent/example-pr-work-order-ingress/IsolatedCheckSpec",
)({
  name: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  command: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
  args: Schema.Array(Schema.String.check(Schema.isMaxLength(512))).check(Schema.isMaxLength(20)),
}) {}

export class IsolatedCheckRequest extends Schema.Class<IsolatedCheckRequest>(
  "@effect-agent/example-pr-work-order-ingress/IsolatedCheckRequest",
)({
  worktreeRoot: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
  checks: Schema.Array(IsolatedCheckSpec).check(Schema.isMaxLength(20)),
}) {}

export class PublisherTrust extends Schema.Class<PublisherTrust>(
  "@effect-agent/example-pr-work-order-ingress/PublisherTrust",
)({
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderDigest: WorkOrderDigest,
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  expectedHeadSha: GitCommitSha,
  allowedPaths: Schema.Array(WorkspacePath).check(Schema.isMaxLength(100)),
  patchDigest: PatchDigest,
  requiredChecks: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
      status: Schema.Literals(["passed", "failed"]),
      summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
    }),
  ).check(Schema.isMaxLength(20)),
}) {}

export class IsolatedPublishWorkerRequest extends Schema.Class<IsolatedPublishWorkerRequest>(
  "@effect-agent/example-pr-work-order-ingress/IsolatedPublishWorkerRequest",
)({
  patch: Schema.String.check(Schema.isMaxLength(1_000_000)),
  trust: PublisherTrust,
  expected: PublisherTrust,
  stateDir: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
}) {}

export const PublishFailpointLocation = Schema.Literals([
  "before-head-cas",
  "after-head-cas",
  "before-lock-release",
  "after-lock-release",
  "lock-release",
]);
export type PublishFailpointLocation = typeof PublishFailpointLocation.Type;

export const PUBLISH_FAILPOINT_ENV = "EFFECT_AGENT_PUBLISH_FAILPOINT";
