import { Schema } from "effect";

import { AgentPolicy } from "./AgentPolicy.ts";
import { Update } from "./AgentUpdates.ts";
import * as FailureDiagnostic from "./FailureDiagnostic.ts";
import { AgentId, DelegationId, RunId, SettlementId, ThreadId, ToolCallId } from "./Identifiers.ts";
import { MessageStatus } from "./internal/message-status.ts";
import { IdempotencyKey, Receipt } from "./Receipt.ts";
import { SubagentExecutionFailure, SubagentGrant } from "./SubagentContract.ts";

/** Assignment output selected through the Definition's runDisposition declaration. */
export const AssignmentDisposition = Schema.Literals(["completed", "waiting"]);
export type AssignmentDisposition = typeof AssignmentDisposition.Type;

/** A permanent destination outcome; an ordinary Run completion does not imply this. */
export const AssignmentTerminal = Schema.Literals(["completed", "failed", "cancelled"]);
export type AssignmentTerminal = typeof AssignmentTerminal.Type;

/** A reusable child Thread, correlated with its declaration. This value grants no authority. */
export const WorkerRef = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  delegationId: DelegationId,
  targetAgentId: AgentId,
  threadId: ThreadId,
});

export type WorkerRef = typeof WorkerRef.Type;

/** A continuing worker identity and retained first delivery, never an execution handle. */
export const WorkerStarted = Schema.Struct({
  worker: WorkerRef,
  delivery: MessageStatus,
}).check(
  Schema.makeFilter(
    ({ worker, delivery }) =>
      delivery.receipt === null || worker.threadId === delivery.receipt.threadId,
  ),
);

export type WorkerStarted = typeof WorkerStarted.Type;

/** Stable owner command. Reusing its key for a different worker conflicts. */
export const WorkerStop = Schema.Struct({ worker: WorkerRef, idempotencyKey: IdempotencyKey });
export type WorkerStop = typeof WorkerStop.Type;

/** The inbox is sealed and active execution has released ownership. External actions are not undone. */
export const WorkerStopped = WorkerStop;
export type WorkerStopped = typeof WorkerStopped.Type;

/** A host-authorized Run allowance is independent of the immutable delegation lineage. */
export const WorkerBudgetScope = Schema.Literals(["source-subtree", "worker-run"]);
export type WorkerBudgetScope = typeof WorkerBudgetScope.Type;

/** Host-bound caller metadata. Programmatic calls never fabricate Run or Tool Call identities. */
export const WorkerSource = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("tool"),
    agentId: AgentId,
    threadId: ThreadId,
    runId: RunId,
    toolCallId: ToolCallId,
  }),
  Schema.Struct({
    _tag: Schema.Literal("programmatic"),
    agentId: AgentId,
    threadId: ThreadId,
  }),
]);

export type WorkerSource = typeof WorkerSource.Type;

/** The caller's effective policy and root-relative depth, resolved by its host. */
export const WorkerContext = Schema.Struct({
  source: WorkerSource,
  policy: AgentPolicy,
  depth: Schema.Natural,
  grant: Schema.optionalKey(SubagentGrant),
});

export type WorkerContext = typeof WorkerContext.Type;

/** Admission identity is distinct from canonical input application. */
export const WorkerInput = Schema.Struct({ receipt: Receipt, messageId: IdempotencyKey });
export type WorkerInput = typeof WorkerInput.Type;

export const WorkerAppliedInput = Schema.Struct({
  ...WorkerInput.fields,
  runId: RunId,
  sequence: Schema.Natural,
});

/** A Run outcome is not assignment completion. Interpret only a Definition's disposition. */
export const WorkerRun = Schema.Struct({
  runId: RunId,
  hostReceipt: Receipt,
  outcome: Schema.NullOr(Schema.Literals(["completed", "failed", "aborted"])),
  disposition: Schema.NullOr(Schema.Json),
});

/** Bounded durable listing data; a latest Receipt never substitutes for a requested Receipt. */
export const WorkerSummary = Schema.Struct({
  worker: WorkerRef,
  latestReceipt: Schema.NullOr(Receipt),
  /** stopping/stopped identify an owner-issued worker stop, never an ordinary Receipt abort. */
  state: Schema.Literals([
    "starting",
    "active",
    "idle",
    "stopping",
    "stopped",
    "completed",
    "failed",
    "cancelled",
  ]),
  acceptedInput: Schema.NullOr(WorkerInput),
  appliedInput: Schema.NullOr(WorkerAppliedInput),
  run: Schema.NullOr(WorkerRun),
  /** One retained unadmitted input, if any; its state is independent of destination execution. */
  pendingDelivery: Schema.NullOr(MessageStatus),
  /** Consistent destination facts; pending delivery has its own source-owned version. */
  watermark: Schema.Struct({
    canonicalSequence: Schema.Natural,
    acceptedQueueSequence: Schema.Natural,
    pendingDeliveryVersion: Schema.NullOr(Schema.Natural),
  }),
});

export type WorkerSummary = typeof WorkerSummary.Type;

export const WorkerPage = Schema.Struct({
  items: Schema.Array(WorkerSummary).check(Schema.isMaxLength(100)),
  next: Schema.NullOr(ThreadId),
});

export type WorkerPage = typeof WorkerPage.Type;

/** Authorized canonical history, encoded independently of storage-adapter representations. */
export const WorkerHistoryEntry = Schema.Struct({
  sequence: Schema.Natural.check(Schema.isGreaterThan(0)),
  recordId: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
  record: Schema.Json,
});

export type WorkerHistoryEntry = typeof WorkerHistoryEntry.Type;

/**
 * Closed host-boundary failures. Original causes stay private and survive diagnostic transport.
 * Retained delivery states are successful MessageStatus values, including pending and refused.
 * After a storage failure, keep the same idempotency key and parameters when reconciling.
 */
export class WorkerError extends Schema.TaggedError<WorkerError>()("WorkerError", {
  cause: Schema.optionalKey(FailureDiagnostic.Value),
  stack: Schema.optionalKey(Schema.String),
  /** Concurrency pressure may clear without changing the request. */
  retryable: Schema.optionalKey(Schema.Literal(true)),
  operation: Schema.Literals([
    "context",
    "start",
    "followUp",
    "inspect",
    "observe",
    "await",
    "list",
    "cancel",
    "stop",
  ]),
  reason: Schema.Literals([
    "denied",
    "declaration-unavailable",
    "worker-mismatch",
    "receipt-mismatch",
    "message-mismatch",
    "idempotency-conflict",
    "capacity",
    "not-found",
    // Decode earlier failures without emitting them for retained deliveries.
    "delivery-pending",
    "storage",
    "corrupt",
    "unavailable",
  ]),
}) {}

export { WorkerOperationTool } from "./SubagentContract.ts";

/** One projected canonical Run outcome. Joined receipts share this identity. */
export const WorkerReport = <Success extends Schema.Top>(success: Success) => {
  const identity = {
    _tag: Schema.Literal("Settled"),
    worker: WorkerRef,
    receipt: Receipt,
    runId: RunId,
    settlementId: SettlementId,
  };

  return Schema.Union([
    // Union preserves the Success codec while making the result field required.
    Schema.Struct({
      ...identity,
      outcome: Schema.Literal("completed"),
      result: Schema.Union([success]),
    }),
    Schema.Struct({
      ...identity,
      outcome: Schema.Literals(["failed", "aborted"]),
      failure: SubagentExecutionFailure,
    }),
  ]);
};

export type WorkerReport<Success extends Schema.Top> = ReturnType<
  typeof WorkerReport<Success>
>["Type"];

/** Framework message stored separately from the parent's application input. */
export const WorkerCompletion = Schema.Struct({
  _tag: Schema.Literal("WorkerCompletion"),
  budgetExhausted: Schema.Boolean,
  schemaVersion: Schema.Literal(1),
  report: WorkerReport(Schema.Json),
}).check(Schema.makeFilter(({ report }) => report.worker.threadId === report.receipt.threadId));

export type WorkerCompletion = typeof WorkerCompletion.Type;

/** Source-worker intermediate output, kept separate from application input. */
export const WorkerUpdate = Schema.Struct({
  _tag: Schema.Literal("WorkerUpdate"),
  schemaVersion: Schema.Literal(1),
  worker: WorkerRef,
  update: Update,
}).check(
  Schema.makeFilter(
    ({ worker, update }) =>
      worker.threadId === update.threadId && worker.targetAgentId === update.agentId,
  ),
);

export type WorkerUpdate = typeof WorkerUpdate.Type;
export const FrameworkMessage = Schema.Union([WorkerCompletion, WorkerUpdate]);
export type FrameworkMessage = typeof FrameworkMessage.Type;
