import { Schema } from "effect";

/** Run input failed to decode through the agent definition's input Schema. */
export class AgentInputError extends Schema.TaggedErrorClass<AgentInputError>()("AgentInputError", {
  message: Schema.String,
}) {}

/** Final model output failed to decode through the agent definition's output Schema. */
export class AgentOutputError extends Schema.TaggedErrorClass<AgentOutputError>()(
  "AgentOutputError",
  {
    message: Schema.String,
  },
) {}

/** A run exhausted one of its finite policy limits. */
export class AgentPolicyError extends Schema.TaggedErrorClass<AgentPolicyError>()(
  "AgentPolicyError",
  {
    limit: Schema.Literals(["turns", "tool-calls", "duration"]),
    message: Schema.String,
  },
) {}

/** A model response sequence violated an agent-loop protocol invariant. */
export class ModelProtocolError extends Schema.TaggedErrorClass<ModelProtocolError>()(
  "ModelProtocolError",
  {
    message: Schema.String,
  },
) {}

/** A run was interrupted without manufacturing a successful result. */
export class AgentInterrupted extends Schema.TaggedErrorClass<AgentInterrupted>()(
  "AgentInterrupted",
  {
    message: Schema.String,
  },
) {}

/** Schema for framework-owned agent errors; application and Effect AI failures remain separate. */
export const AgentError = Schema.Union([
  AgentInputError,
  AgentOutputError,
  AgentPolicyError,
  ModelProtocolError,
  AgentInterrupted,
]);
export type AgentError = typeof AgentError.Type;
