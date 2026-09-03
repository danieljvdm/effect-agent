import { Schema } from "effect";

import { ToolCallId } from "./Identifiers.ts";

/** Run input failed to decode through the agent definition's input Schema. */
export class AgentInputError extends Schema.TaggedError<AgentInputError>()("AgentInputError", {
  message: Schema.String,
}) {}

/** Final model output failed to decode through the agent definition's output Schema. */
export class AgentOutputError extends Schema.TaggedError<AgentOutputError>()("AgentOutputError", {
  message: Schema.String,
}) {}

/** An application-selected run disposition failed its definition-owned Schema boundary. */
export class AgentRunDispositionError extends Schema.TaggedError<AgentRunDispositionError>()(
  "AgentRunDispositionError",
  {
    cause: Schema.Defect(),
    message: Schema.String,
  },
) {}

/** The finite policy dimension a run exhausted; shared with durable settlement metadata. */
export const PolicyLimit = Schema.Literals([
  "turns",
  "tool-calls",
  "duration",
  "usage",
  "tokens",
  "cost",
  "repeated-failures",
]);

export type PolicyLimit = typeof PolicyLimit.Type;

/** A run exhausted one of its finite policy limits. */
export class AgentPolicyError extends Schema.TaggedError<AgentPolicyError>()("AgentPolicyError", {
  limit: PolicyLimit,
  message: Schema.String,
}) {}

/** A native Effect AI Tool approval was explicitly denied before its Handler started. */
export class AgentApprovalDenied extends Schema.TaggedError<AgentApprovalDenied>()(
  "AgentApprovalDenied",
  {
    toolCallId: ToolCallId,
    toolName: Schema.NonEmptyString,
    message: Schema.String,
  },
) {
  /** Decode the native Effect AI Tool Call ID at the core error boundary. */
  static override make(input: {
    readonly toolCallId: string;
    readonly toolName: string;
    readonly message: string;
  }): AgentApprovalDenied {
    return super.make({
      ...input,
      toolCallId: Schema.decodeSync(ToolCallId)(input.toolCallId),
    });
  }
}

/** A host denied current execution authority for a Tool Call before its Handler started. */
export class AgentToolAuthorizationDenied extends Schema.TaggedError<AgentToolAuthorizationDenied>()(
  "AgentToolAuthorizationDenied",
  {
    toolCallId: ToolCallId,
    toolName: Schema.NonEmptyString,
    message: Schema.String,
  },
) {}

/** A native Effect AI Tool approval has no decision and the ephemeral Run cannot proceed. */
export class AgentApprovalPending extends Schema.TaggedError<AgentApprovalPending>()(
  "AgentApprovalPending",
  {
    approvalId: Schema.NonEmptyString,
    toolCallId: ToolCallId,
    toolName: Schema.NonEmptyString,
    message: Schema.String,
  },
) {
  /** Decode the native Effect AI Tool Call ID at the core error boundary. */
  static override make(input: {
    readonly approvalId: string;
    readonly toolCallId: string;
    readonly toolName: string;
    readonly message: string;
  }): AgentApprovalPending {
    return super.make({
      ...input,
      toolCallId: Schema.decodeSync(ToolCallId)(input.toolCallId),
    });
  }
}

/** A model response sequence violated an agent-loop protocol invariant. */
export class ModelProtocolError extends Schema.TaggedError<ModelProtocolError>()(
  "ModelProtocolError",
  {
    message: Schema.String,
  },
) {}

/** A run was interrupted without manufacturing a successful result. */
export class AgentInterrupted extends Schema.TaggedError<AgentInterrupted>()("AgentInterrupted", {
  message: Schema.String,
}) {}

/**
 * The provider rejected a prompt that exceeds the model context window and
 * compaction recovery is unavailable or was already attempted once. This is
 * an expected failure: the engine classifies the provider error rather than
 * surfacing an opaque transport failure.
 */
export class ContextOverflowError extends Schema.TaggedError<ContextOverflowError>()(
  "ContextOverflowError",
  {
    message: Schema.String,
    retried: Schema.Boolean,
  },
) {}

/** Local context preparation could not fit a legal prompt inside its target before provider I/O. */
export class ContextBudgetError extends Schema.TaggedError<ContextBudgetError>()(
  "ContextBudgetError",
  {
    message: Schema.String,
    estimatedTokens: Schema.Natural,
    targetTokens: Schema.Natural,
    completionReserveTokens: Schema.Natural,
  },
) {}

/** Schema for framework-owned agent errors; application and Effect AI failures remain separate. */
export const AgentError = Schema.Union([
  AgentInputError,
  AgentOutputError,
  AgentRunDispositionError,
  AgentPolicyError,
  AgentApprovalDenied,
  AgentToolAuthorizationDenied,
  AgentApprovalPending,
  ModelProtocolError,
  AgentInterrupted,
  ContextOverflowError,
  ContextBudgetError,
]);

export type AgentError = typeof AgentError.Type;
