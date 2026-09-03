import { SettlementId } from "@effect-agent/core/Identifiers";
import { Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { DeploymentId } from "@effect-agent/thread/Records";
import { Context, Effect, Schema, type Scope } from "effect";
import { DurableDeferred } from "effect/unstable/workflow";

/** Only identities cross the Workflow boundary; the agent journal owns the outcome. */
export class WorkflowSubmission extends Schema.Class<WorkflowSubmission>(
  "@effect-agent/workflow/WorkflowSubmission",
)({
  version: Schema.Literal(1),
  deploymentId: DeploymentId,
  receipt: Receipt,
}) {}

export class WorkflowSettlementReference extends Schema.Class<WorkflowSettlementReference>(
  "@effect-agent/workflow/WorkflowSettlementReference",
)({
  version: Schema.Literal(1),
  submissionId: Receipt.fields.submissionId,
  threadId: Receipt.fields.threadId,
  settlementId: SettlementId,
}) {}

/** Retained until native success has been checked against the canonical Settlement. */
export class WorkflowDispatchIntent extends Schema.Class<WorkflowDispatchIntent>(
  "@effect-agent/workflow/WorkflowDispatchIntent",
)({
  ...WorkflowSubmission.fields,
  workflowName: Schema.NonEmptyString,
  executionId: Schema.NonEmptyString,
  /** One workflow step owns this submission. Repair must notify it before removing the intent. */
  completionToken: Schema.optionalKey(DurableDeferred.Token),
}) {}

export class WorkflowDispatchError extends Schema.TaggedError<WorkflowDispatchError>()(
  "WorkflowDispatchError",
  {
    operation: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

export class WorkflowDispatchScan extends Schema.Class<WorkflowDispatchScan>(
  "@effect-agent/workflow/WorkflowDispatchScan",
)({
  deploymentId: DeploymentId,
  workflowName: Schema.NonEmptyString,
  after: Schema.optionalKey(Schema.String),
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
}) {}

/**
 * Adapter-private outbox. `put` returns the stored intent, preserving an existing completion
 * token when omitted and allowing its one-time attachment. Different identities or tokens
 * conflict. `remove` must compare the entire intent, so a concurrent token attachment cannot
 * be lost. `scan` returns at most limit rows in executionId order, after
 * the exclusive cursor, filtered by deployment and workflow. All writes are durable
 * before returning. No transcript, outcome, or native engine storage belongs here.
 */
export class WorkflowDispatchStore extends Context.Service<
  WorkflowDispatchStore,
  {
    readonly put: (
      intent: WorkflowDispatchIntent,
    ) => Effect.Effect<WorkflowDispatchIntent, WorkflowDispatchError>;
    readonly scan: (
      request: WorkflowDispatchScan,
    ) => Effect.Effect<ReadonlyArray<WorkflowDispatchIntent>, WorkflowDispatchError>;
    readonly remove: (intent: WorkflowDispatchIntent) => Effect.Effect<void, WorkflowDispatchError>;
  }
>()("@effect-agent/workflow/WorkflowDispatchStore") {}

/**
 * Host-owned durable repair trigger. Register before admission opens. The host must
 * invoke repair at startup and repeatedly after lost hints or process loss. A Node
 * deployment may use a scoped poller; other hosts may use alarms or durable jobs.
 * Stop invoking the callback when this Scope closes. The shared driver starts no loop.
 */
export class WorkflowRepairTrigger extends Context.Service<
  WorkflowRepairTrigger,
  {
    readonly register: (repair: Effect.Effect<void>) => Effect.Effect<void, never, Scope.Scope>;
  }
>()("@effect-agent/workflow/WorkflowRepairTrigger") {}

export const WorkflowDispatchFailpointLocation = Schema.Literals([
  "intent:before-persist",
  "intent:after-persist",
  "launch:before",
  "launch:after",
  "completion:before-observe",
  "completion:after-observe",
  "completion:before-notify",
  "completion:after-notify",
  "cleanup:before",
  "cleanup:after",
]);

export type WorkflowDispatchFailpointLocation = typeof WorkflowDispatchFailpointLocation.Type;

export const WorkflowDispatchFailpoint = Context.Reference<{
  readonly hit: (
    location: WorkflowDispatchFailpointLocation,
    intent: WorkflowDispatchIntent,
  ) => Effect.Effect<void, WorkflowDispatchError>;
}>("@effect-agent/workflow/WorkflowDispatchFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

export class WorkflowRepairReport extends Schema.Class<WorkflowRepairReport>(
  "@effect-agent/workflow/WorkflowRepairReport",
)({
  discovered: Schema.Int,
  inspected: Schema.Int,
  completed: Schema.Int,
}) {}
