import type { SettledWorkOrder } from "@effect-agent/example-pr-work-orders";
import {
  GitCommitSha,
  ProposedWorkOrder,
  PullRequestWorkOrder,
  WorkOrderCheckResult,
  WorkOrderDigest,
  WorkspacePath,
} from "@effect-agent/example-pr-work-orders";
import { Schema } from "effect";

export class WorkOrderActionFailure extends Schema.TaggedError<WorkOrderActionFailure>()(
  "WorkOrderActionFailure",
  {
    phase: Schema.Literals(["admit", "implement", "checks", "publish", "present"]),
    errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
    detail: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class WorkOrderAdmission extends Schema.Class<WorkOrderAdmission>(
  "@effect-agent/example-pr-work-order-ingress/WorkOrderAdmission",
)({
  version: Schema.Literal(1),
  order: PullRequestWorkOrder,
  workOrderDigest: WorkOrderDigest,
  journalCommentId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  runId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
}) {}

export class ActionCheckSpec extends Schema.Class<ActionCheckSpec>(
  "@effect-agent/example-pr-work-order-ingress/ActionCheckSpec",
)({
  name: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  command: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
  args: Schema.Array(Schema.String.check(Schema.isMaxLength(512))).check(Schema.isMaxLength(40)),
  timeoutSeconds: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3_600 })),
}) {}

export const ActionCheckSpecs = Schema.Array(ActionCheckSpec).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(20),
);

export class CheckedFile extends Schema.Class<CheckedFile>(
  "@effect-agent/example-pr-work-order-ingress/CheckedFile",
)({
  path: WorkspacePath,
  content: Schema.String.check(Schema.isMaxLength(200_000)),
}) {}

export class CheckedWorkOrder extends Schema.Class<CheckedWorkOrder>(
  "@effect-agent/example-pr-work-order-ingress/CheckedWorkOrder",
)({
  version: Schema.Literal(1),
  admission: WorkOrderAdmission,
  proposal: ProposedWorkOrder,
  checks: Schema.Array(WorkOrderCheckResult).check(Schema.isMinLength(1), Schema.isMaxLength(20)),
  files: Schema.Array(CheckedFile).check(Schema.isMinLength(1), Schema.isMaxLength(100)),
}) {}

export class PublishedTerminal extends Schema.TaggedClass<PublishedTerminal>()("published", {
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderDigest: WorkOrderDigest,
  previousHeadSha: GitCommitSha,
  publishedHeadSha: GitCommitSha,
  changedPaths: Schema.Array(WorkspacePath).check(Schema.isMinLength(1), Schema.isMaxLength(100)),
}) {}

export class SettledTerminal extends Schema.TaggedClass<SettledTerminal>()("settled", {
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderDigest: WorkOrderDigest,
  headSha: GitCommitSha,
  disposition: Schema.Literals(["not-applicable", "needs-human"]),
}) {}

export class FailedTerminal extends Schema.TaggedClass<FailedTerminal>()("failed", {
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderDigest: WorkOrderDigest,
  headSha: GitCommitSha,
  errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  detail: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
}) {}

export const WorkOrderTerminal = Schema.Union([PublishedTerminal, SettledTerminal, FailedTerminal]);
export type WorkOrderTerminal = typeof WorkOrderTerminal.Type;

const journalIdentity = {
  version: Schema.Literal(1),
  eventId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  sourceCommentId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  workOrderDigest: WorkOrderDigest,
  expectedHeadSha: GitCommitSha,
  runId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
} as const;

export class JournalClaimed extends Schema.TaggedClass<JournalClaimed>()("claimed", {
  ...journalIdentity,
}) {}

export class JournalCompleted extends Schema.TaggedClass<JournalCompleted>()("completed", {
  ...journalIdentity,
  terminal: WorkOrderTerminal,
}) {}

export const WorkOrderJournalState = Schema.Union([JournalClaimed, JournalCompleted]);
export type WorkOrderJournalState = typeof WorkOrderJournalState.Type;

export class JournalComment extends Schema.Class<JournalComment>(
  "@effect-agent/example-pr-work-order-ingress/JournalComment",
)({
  id: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  authorId: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  inReplyToId: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(200))),
  body: Schema.String.check(Schema.isMaxLength(60_000)),
}) {}

export const terminalFromSettlement = (settled: SettledWorkOrder): SettledTerminal =>
  SettledTerminal.make({
    workOrderId: settled.workOrderId,
    workOrderDigest: settled.workOrderDigest,
    headSha: settled.headSha,
    disposition: settled.disposition,
  });
