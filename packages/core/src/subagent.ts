import { Context, Schema } from "effect";

import { AgentId, ThreadId, DelegationId, RunId, ToolCallId } from "./identifiers.ts";

const Natural = Schema.Natural;

/** Finite delegable caps for one parent Run. Absent means not configured; present values are finite. */
export class SubagentDelegationCaps extends Schema.Class<SubagentDelegationCaps>(
  "@effect-agent/capabilities/SubagentDelegationCaps",
)({
  maxTotalChildInvocations: Schema.optionalKey(Natural),
  maxConcurrentChildren: Schema.optionalKey(Natural),
  maxTurns: Schema.optionalKey(Natural),
  maxToolCalls: Schema.optionalKey(Natural),
  maxDurationMillis: Schema.optionalKey(Natural),
  maxInputTokens: Schema.optionalKey(Natural),
  maxOutputTokens: Schema.optionalKey(Natural),
  maxCostMicrousd: Schema.optionalKey(Natural),
  maxResultBytes: Schema.optionalKey(Natural),
}) {}

const AmountFields = {
  turns: Natural,
  toolCalls: Natural,
  durationMillis: Natural,
  inputTokens: Natural,
  outputTokens: Natural,
  costMicrousd: Natural,
  resultBytes: Natural,
} as const;

/** Exact amounts across every reservable delegation dimension. */
export class SubagentReservationAmounts extends Schema.Class<SubagentReservationAmounts>(
  "@effect-agent/capabilities/SubagentReservationAmounts",
)(AmountFields) {}

/** One child reservation against the parent Run's shared delegation pool. */
export const SubagentBudgetReservation = Schema.Struct({
  caps: SubagentDelegationCaps,
  allocation: SubagentReservationAmounts,
});
export type SubagentBudgetReservation = typeof SubagentBudgetReservation.Type;

/** Root-relative delegation depth; the direct child of a top-level run has depth 1. */
export const DelegationDepth = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
export type DelegationDepth = typeof DelegationDepth.Type;

/** Model-visible naming convention; names never authorize recovery replay. */
export const delegationToolPrefix = "delegate_";

/** Checks the authoring convention only, never execution or recovery semantics. */
export const isDelegationToolName = (toolName: string): boolean =>
  toolName.startsWith(delegationToolPrefix);

/** Marks a Tool implementing the idempotent Subagent establishment protocol. */
export const DelegationTool = Context.Reference<boolean>("@effect-agent/core/DelegationTool", {
  defaultValue: () => false,
});

/** Persisted preparation classification, checked against the resolved Tool before replay. */
export const ToolExecutionKind = Schema.Literals(["ordinary", "delegation"]);
export type ToolExecutionKind = typeof ToolExecutionKind.Type;

/** Immutable lineage from a child Thread to the parent identity that established it. */
export class SubagentParentLink extends Schema.Class<SubagentParentLink>("SubagentParentLink")({
  delegationId: DelegationId,
  parentAgentId: AgentId,
  parentThreadId: ThreadId,
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
  depth: DelegationDepth,
}) {}
