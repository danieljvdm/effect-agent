import { Schema } from "effect";

import { AgentId, ConversationId, DelegationId, RunId, ToolCallId } from "./identifiers.ts";

/** Root-relative delegation depth; the direct child of a top-level run has depth 1. */
export const DelegationDepth = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
export type DelegationDepth = typeof DelegationDepth.Type;

/** Immutable lineage from a child Conversation to the parent identity that established it. */
export class SubagentParentLink extends Schema.Class<SubagentParentLink>("SubagentParentLink")({
  delegationId: DelegationId,
  parentAgentId: AgentId,
  parentConversationId: ConversationId,
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
  depth: DelegationDepth,
}) {}
