import { Schema } from "effect";

import { AgentId, ConversationId, DelegationId, RunId, ToolCallId } from "./identifiers.ts";

/** Root-relative delegation depth; the direct child of a top-level run has depth 1. */
export const DelegationDepth = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
export type DelegationDepth = typeof DelegationDepth.Type;

/** Model-visible naming convention that marks every delegation Tool recognizably. */
export const delegationToolPrefix = "delegate_";

/**
 * Fail-closed delegation-Tool detection by naming convention (SUB-029, S2
 * recovery evidence): any Tool name carrying the delegation prefix is treated
 * as a delegation. The rule is core-owned so session recovery can classify a
 * prepared-without-outcome delegation call without importing capabilities;
 * `Subagent.define` (capabilities) enforces the same convention at authoring
 * time, so every declared delegation Tool satisfies it by construction.
 */
export const isDelegationToolName = (toolName: string): boolean =>
  toolName.startsWith(delegationToolPrefix);

/** Immutable lineage from a child Conversation to the parent identity that established it. */
export class SubagentParentLink extends Schema.Class<SubagentParentLink>("SubagentParentLink")({
  delegationId: DelegationId,
  parentAgentId: AgentId,
  parentConversationId: ConversationId,
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
  depth: DelegationDepth,
}) {}
