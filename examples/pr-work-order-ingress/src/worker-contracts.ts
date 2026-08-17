import { Schema } from "effect";

export const IsolatedCheckWorkerRequest = Schema.Struct({
  worktreeRoot: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
  checks: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
      command: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
      args: Schema.Array(Schema.String.check(Schema.isMaxLength(512))).check(
        Schema.isMaxLength(20),
      ),
    }),
  ).check(Schema.isMaxLength(20)),
});
export type IsolatedCheckWorkerRequest = typeof IsolatedCheckWorkerRequest.Type;

const PublisherTrustRecord = Schema.Struct({
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderDigest: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  expectedHeadSha: Schema.NonEmptyString.check(Schema.isMaxLength(40)),
  allowedPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(100),
  ),
  patchDigest: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  requiredChecks: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
      status: Schema.Literals(["passed", "failed"]),
      summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
    }),
  ).check(Schema.isMaxLength(20)),
});

export const IsolatedPublishWorkerRequest = Schema.Struct({
  patch: Schema.String.check(Schema.isMaxLength(1_000_000)),
  trust: PublisherTrustRecord,
  expected: PublisherTrustRecord,
  stateDir: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
});
export type IsolatedPublishWorkerRequest = typeof IsolatedPublishWorkerRequest.Type;
