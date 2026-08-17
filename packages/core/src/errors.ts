import { Schema } from "effect";

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
    toolCallId: Schema.NonEmptyString,
    toolName: Schema.NonEmptyString,
    message: Schema.String,
  },
) {}

/** A native Effect AI Tool approval has no decision and the ephemeral Run cannot proceed. */
export class AgentApprovalPending extends Schema.TaggedError<AgentApprovalPending>()(
  "AgentApprovalPending",
  {
    approvalId: Schema.NonEmptyString,
    toolCallId: Schema.NonEmptyString,
    toolName: Schema.NonEmptyString,
    message: Schema.String,
  },
) {}

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

/** Schema for framework-owned agent errors; application and Effect AI failures remain separate. */
export const AgentError = Schema.Union([
  AgentInputError,
  AgentOutputError,
  AgentRunDispositionError,
  AgentPolicyError,
  AgentApprovalDenied,
  AgentApprovalPending,
  ModelProtocolError,
  AgentInterrupted,
  ContextOverflowError,
]);
export type AgentError = typeof AgentError.Type;
