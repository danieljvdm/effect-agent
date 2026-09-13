import { Schema } from "effect";

import { AgentPolicy } from "./AgentPolicy.ts";
import { Update } from "./AgentUpdates.ts";
import { AgentId, DelegationId, RunId, SettlementId, ThreadId, ToolCallId } from "./Identifiers.ts";
import { Receipt } from "./Receipt.ts";
import { SubagentExecutionFailure, SubagentGrant } from "./SubagentContract.ts";

/** A reusable child Thread, correlated with its declaration. This value grants no authority. */
export const WorkerRef = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  delegationId: DelegationId,
  targetAgentId: AgentId,
  threadId: ThreadId,
});

export type WorkerRef = typeof WorkerRef.Type;

/** A worker identity and the Receipt for one accepted input, never an execution handle. */
export const WorkerStarted = Schema.Struct({
  worker: WorkerRef,
  receipt: Receipt,
}).check(Schema.makeFilter((value) => value.worker.threadId === value.receipt.threadId));

export type WorkerStarted = typeof WorkerStarted.Type;

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

/** Bounded durable listing data; a latest Receipt never substitutes for a requested Receipt. */
export const WorkerSummary = Schema.Struct({
  worker: WorkerRef,
  latestReceipt: Schema.NullOr(Receipt),
  state: Schema.Literals(["starting", "active", "idle"]),
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
 * Closed host-boundary failures. Infrastructure diagnostics remain in host telemetry.
 * `delivery-pending` confirms retained input, not destination acceptance or execution.
 * Keep the same idempotency key and parameters when reconciling; never launch a replacement.
 */
export class WorkerError extends Schema.TaggedError<WorkerError>()("WorkerError", {
  /** Pending input or concurrency pressure may clear without changing the request. */
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
  ]),
  reason: Schema.Literals([
    "denied",
    "declaration-unavailable",
    "worker-mismatch",
    "receipt-mismatch",
    "idempotency-conflict",
    "capacity",
    "not-found",
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
