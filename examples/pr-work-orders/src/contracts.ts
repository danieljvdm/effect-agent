import { Crypto, Effect, Encoding, Schema } from "effect";

export const GitCommitSha = Schema.NonEmptyString.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^[0-9a-f]{40,64}$/),
).pipe(Schema.brand("@effect-agent/example-pr-work-orders/GitCommitSha"));
export type GitCommitSha = typeof GitCommitSha.Type;

export const WorkspacePath = Schema.NonEmptyString.check(Schema.isMaxLength(512));
export type WorkspacePath = typeof WorkspacePath.Type;

export const HexDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));

export const PatchDigest = HexDigest.pipe(
  Schema.brand("@effect-agent/example-pr-work-orders/PatchDigest"),
);
export type PatchDigest = typeof PatchDigest.Type;

export const WorkOrderDigest = HexDigest.pipe(
  Schema.brand("@effect-agent/example-pr-work-orders/WorkOrderDigest"),
);
export type WorkOrderDigest = typeof WorkOrderDigest.Type;

export const WorkOrderDispatchKind = Schema.Literals(["mention", "reaction"]);
export type WorkOrderDispatchKind = typeof WorkOrderDispatchKind.Type;

export const WorkOrderDisposition = Schema.Literals(["fixed", "not-applicable", "needs-human"]);
export type WorkOrderDisposition = typeof WorkOrderDisposition.Type;

export class WorkOrderLineRange extends Schema.Class<WorkOrderLineRange>(
  "@effect-agent/example-pr-work-orders/WorkOrderLineRange",
)({
  startLine: Schema.Int.check(Schema.isGreaterThan(0)),
  endLine: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

export const DecodedWorkOrderLineRange = WorkOrderLineRange.pipe(
  Schema.refine((range): range is WorkOrderLineRange => range.startLine <= range.endLine, {
    expected: "startLine <= endLine",
  }),
);

export class WorkOrderSource extends Schema.Class<WorkOrderSource>(
  "@effect-agent/example-pr-work-orders/WorkOrderSource",
)({
  commentId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  threadId: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(200))),
  authorId: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  authorLogin: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  commitSha: GitCommitSha,
  path: WorkspacePath,
  lineRange: Schema.optionalKey(DecodedWorkOrderLineRange),
  body: Schema.NonEmptyString.check(Schema.isMaxLength(4_000)),
  suggestion: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(2_000))),
}) {}

export class WorkOrderDispatch extends Schema.Class<WorkOrderDispatch>(
  "@effect-agent/example-pr-work-orders/WorkOrderDispatch",
)({
  kind: WorkOrderDispatchKind,
  eventId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  actorId: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  actorLogin: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
}) {}

const workOrderIdentityFields = {
  version: Schema.Literal(1),
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  headSha: GitCommitSha,
  source: WorkOrderSource,
  dispatch: WorkOrderDispatch,
} as const;

export class WorkOrderIdentity extends Schema.Class<WorkOrderIdentity>(
  "@effect-agent/example-pr-work-orders/WorkOrderIdentity",
)(workOrderIdentityFields) {}

export class PullRequestWorkOrder extends Schema.Class<PullRequestWorkOrder>(
  "@effect-agent/example-pr-work-orders/PullRequestWorkOrder",
)({
  ...workOrderIdentityFields,
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
}) {}

export class WorkOrderRejected extends Schema.TaggedError<WorkOrderRejected>()(
  "WorkOrderRejected",
  {
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class WorkspaceViolation extends Schema.TaggedError<WorkspaceViolation>()(
  "WorkspaceViolation",
  {
    path: Schema.String.check(Schema.isMaxLength(1_024)),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class WorkspaceOperationFailure extends Schema.TaggedError<WorkspaceOperationFailure>()(
  "WorkspaceOperationFailure",
  {
    operation: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
  },
) {}

export const CheckStatus = Schema.Literals(["passed", "failed"]);
export type CheckStatus = typeof CheckStatus.Type;

export class WorkOrderCheckResult extends Schema.Class<WorkOrderCheckResult>(
  "@effect-agent/example-pr-work-orders/WorkOrderCheckResult",
)({
  name: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  status: CheckStatus,
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

export class PatchSnapshot extends Schema.Class<PatchSnapshot>(
  "@effect-agent/example-pr-work-orders/PatchSnapshot",
)({
  digest: PatchDigest,
  changedPaths: Schema.Array(WorkspacePath).check(Schema.isMaxLength(100)),
  preview: Schema.String.check(Schema.isMaxLength(20_000)),
  truncated: Schema.Boolean,
}) {}

export class WorkOrderReport extends Schema.Class<WorkOrderReport>(
  "@effect-agent/example-pr-work-orders/WorkOrderReport",
)({
  workOrderDigest: WorkOrderDigest,
  headSha: GitCommitSha,
  disposition: WorkOrderDisposition,
  changedPaths: Schema.Array(WorkspacePath).check(Schema.isMaxLength(100)),
  checks: Schema.Array(WorkOrderCheckResult).check(Schema.isMaxLength(20)),
  patchDigest: Schema.optionalKey(PatchDigest),
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

export const WorkOrderValidationReason = Schema.Literals([
  "work-order-mismatch",
  "changed-paths-mismatch",
  "path-not-allowed",
  "patch-digest-mismatch",
  "check-results-mismatch",
  "empty-patch",
  "unexpected-patch",
  "check-mutated-patch",
]);
export type WorkOrderValidationReason = typeof WorkOrderValidationReason.Type;

export class WorkOrderValidationFailure extends Schema.TaggedError<WorkOrderValidationFailure>()(
  "WorkOrderValidationFailure",
  {
    reason: WorkOrderValidationReason,
    detail: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class RequiredCheckFailed extends Schema.TaggedError<RequiredCheckFailed>()(
  "RequiredCheckFailed",
  {
    check: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
    summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
  },
) {}

export class StalePullRequestHead extends Schema.TaggedError<StalePullRequestHead>()(
  "StalePullRequestHead",
  {
    expected: GitCommitSha,
    actual: GitCommitSha,
  },
) {}

export class WorkOrderTimeout extends Schema.TaggedError<WorkOrderTimeout>()("WorkOrderTimeout", {
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  headSha: GitCommitSha,
}) {}

export class PublishedWorkOrder extends Schema.TaggedClass<PublishedWorkOrder>()("published", {
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderDigest: WorkOrderDigest,
  previousHeadSha: GitCommitSha,
  publishedHeadSha: GitCommitSha,
  patchDigest: PatchDigest,
  changedPaths: Schema.Array(WorkspacePath).check(Schema.isMaxLength(100)),
  checks: Schema.Array(WorkOrderCheckResult).check(Schema.isMaxLength(20)),
}) {}

export class SettledWorkOrder extends Schema.TaggedClass<SettledWorkOrder>()("settled", {
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderDigest: WorkOrderDigest,
  headSha: GitCommitSha,
  disposition: Schema.Literals(["not-applicable", "needs-human"]),
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

export type WorkOrderHostResult = PublishedWorkOrder | SettledWorkOrder;

export class WorkOrderReleaseFailure extends Schema.TaggedError<WorkOrderReleaseFailure>()(
  "WorkOrderReleaseFailure",
  {
    operation: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
    publication: Schema.optionalKey(PublishedWorkOrder),
    observedHeadSha: Schema.optionalKey(GitCommitSha),
  },
) {}

export class WorkOrderImplementationFailure extends Schema.TaggedError<WorkOrderImplementationFailure>()(
  "WorkOrderImplementationFailure",
  {
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
  },
) {}

export type WorkOrderHostError =
  | WorkOrderRejected
  | WorkOrderValidationFailure
  | RequiredCheckFailed
  | StalePullRequestHead
  | WorkspaceOperationFailure
  | WorkOrderReleaseFailure
  | WorkOrderTimeout
  | WorkOrderImplementationFailure;

const JsonArray = Schema.Array(Schema.Json);
const isJsonArray = Schema.is(JsonArray);

const canonicalJson = (value: Schema.Json): string => {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return JSON.stringify(value);
  }
  if (isJsonArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  const entries = Object.entries(value).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  );
  return `{${entries
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
    .join(",")}}`;
};

const digestCanonical = Effect.fn("digestCanonical")(function* (value: Schema.Json) {
  const crypto = yield* Crypto.Crypto;
  return yield* crypto.digest("SHA-256", new TextEncoder().encode(canonicalJson(value))).pipe(
    Effect.map(Encoding.encodeHex),
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "digest canonical value",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );
});

export const workOrderIdentityOf = (order: PullRequestWorkOrder): WorkOrderIdentity =>
  WorkOrderIdentity.make({
    version: order.version,
    repository: order.repository,
    pullRequestNumber: order.pullRequestNumber,
    headSha: order.headSha,
    source: order.source,
    dispatch: order.dispatch,
  });

export const workOrderIdFor = Effect.fn("workOrderIdFor")(function* (
  input: WorkOrderIdentity,
): Effect.fn.Return<string, WorkspaceOperationFailure, Crypto.Crypto> {
  const encoded = yield* Schema.encodeEffect(WorkOrderIdentity)(input).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "encode work-order identity",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );
  return yield* digestCanonical(encoded);
});

export const createWorkOrder = Effect.fn("createWorkOrder")(function* (
  input: WorkOrderIdentity,
): Effect.fn.Return<PullRequestWorkOrder, WorkspaceOperationFailure, Crypto.Crypto> {
  return PullRequestWorkOrder.make({
    ...input,
    workOrderId: yield* workOrderIdFor(input),
  });
});

export const workOrderDigest = Effect.fn("workOrderDigest")(function* (
  order: PullRequestWorkOrder,
): Effect.fn.Return<WorkOrderDigest, WorkspaceOperationFailure, Crypto.Crypto> {
  const encoded = yield* Schema.encodeEffect(PullRequestWorkOrder)(order).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "encode work order",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );
  const digest = yield* digestCanonical(encoded);
  return yield* Schema.decodeUnknownEffect(WorkOrderDigest)(digest).pipe(
    Effect.mapError(() =>
      WorkspaceOperationFailure.make({
        operation: "decode work-order digest",
        reason: "SHA-256 returned an invalid digest",
      }),
    ),
  );
});

export const normalizeWorkspacePath = (
  path: string,
): Effect.Effect<WorkspacePath, WorkspaceViolation> => {
  const fail = (reason: string) => WorkspaceViolation.make({ path, reason });
  if (path.length === 0 || path.length > 512) {
    return Effect.fail(fail("path length is out of bounds"));
  }
  const hasControlCharacter = [...path].some((character) => {
    const code = character.charCodeAt(0);
    return code <= 31 || code === 127;
  });
  if (hasControlCharacter || path.includes("\\")) {
    return Effect.fail(fail("path contains a forbidden character"));
  }
  if (path.startsWith("/") || /^[A-Za-z]:/.test(path)) {
    return Effect.fail(fail("path must be repository-relative"));
  }
  const segments = path.split("/");
  if (segments.some((segment) => segment === "" || segment === "." || segment === "..")) {
    return Effect.fail(fail("path segments must not be empty, '.' or '..'"));
  }
  return Schema.decodeUnknownEffect(WorkspacePath)(segments.join("/")).pipe(
    Effect.mapError(() => fail("path is outside the supported workspace path schema")),
  );
};
