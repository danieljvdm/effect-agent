import { Schema } from "effect";

import { AgentPolicy } from "./AgentPolicy.ts";
import { AgentId, DelegationId, RunId, ThreadId, ToolCallId } from "./Identifiers.ts";
import { Receipt } from "./Receipt.ts";
import { SubagentGrant } from "./SubagentContract.ts";

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

/** Closed host-boundary failures. Infrastructure diagnostics remain in host telemetry. */
export class WorkerError extends Schema.TaggedError<WorkerError>()("WorkerError", {
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
    "storage",
    "corrupt",
    "unavailable",
  ]),
}) {}

export { WorkerOperationTool } from "./SubagentContract.ts";
