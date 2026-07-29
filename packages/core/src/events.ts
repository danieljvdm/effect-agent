import { Schema } from "effect";

import { AgentId, ConversationId, RunId, ToolCallId, TurnId } from "./identifiers.ts";

const RunEventBase = Schema.Struct({
  eventVersion: Schema.Literal(1),
  runId: RunId,
  conversationId: ConversationId,
  agentId: AgentId,
  sequence: Schema.Natural,
  timestamp: Schema.DateTimeUtcFromString,
  turnId: Schema.optional(TurnId),
  toolCallId: Schema.optional(ToolCallId),
});

/** Signals that the runtime has created a run and assigned its identities. */
export const RunStarted = Schema.TaggedStruct("RunStarted", {
  ...RunEventBase.fields,
});

/** Signals the start of a positive-numbered model turn. */
export const TurnStarted = Schema.TaggedStruct("TurnStarted", {
  ...RunEventBase.fields,
  turn: Schema.Int.check(Schema.isGreaterThan(0)),
});

/** Signals that the model request for a turn is starting. */
export const ModelStarted = Schema.TaggedStruct("ModelStarted", {
  ...RunEventBase.fields,
  turn: Schema.Int.check(Schema.isGreaterThan(0)),
});

/** Carries one live assistant text delta. */
export const TextDelta = Schema.TaggedStruct("TextDelta", {
  ...RunEventBase.fields,
  text: Schema.String,
});

/** Carries reasoning content explicitly exposed by the provider. */
export const ReasoningDelta = Schema.TaggedStruct("ReasoningDelta", {
  ...RunEventBase.fields,
  text: Schema.String,
});

/** Reports a model-declared Tool Call before its handler starts. */
export const ToolCallDeclared = Schema.TaggedStruct("ToolCallDeclared", {
  ...RunEventBase.fields,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
});

/** Signals that a validated Tool Call handler has started. */
export const ToolCallStarted = Schema.TaggedStruct("ToolCallStarted", {
  ...RunEventBase.fields,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
});

/** Carries a preliminary Tool result that is not the call's terminal outcome. */
export const ToolProgress = Schema.TaggedStruct("ToolProgress", {
  ...RunEventBase.fields,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
  result: Schema.Json,
});

/** Records the successful terminal result of a Tool Call. */
export const ToolCallSucceeded = Schema.TaggedStruct("ToolCallSucceeded", {
  ...RunEventBase.fields,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
  result: Schema.Json,
});

/** Records a terminal Tool Call failure using safe, serializable diagnostics. */
export const ToolCallFailed = Schema.TaggedStruct("ToolCallFailed", {
  ...RunEventBase.fields,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
  errorTag: Schema.NonEmptyString,
  message: Schema.String,
});

/** Signals that a decoded Tool Call requires approval before execution. */
export const ApprovalRequested = Schema.TaggedStruct("ApprovalRequested", {
  ...RunEventBase.fields,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
});

/** Records the normalized finish reason for a completed assistant turn. */
export const TurnCompleted = Schema.TaggedStruct("TurnCompleted", {
  ...RunEventBase.fields,
  turn: Schema.Int.check(Schema.isGreaterThan(0)),
  finishReason: Schema.Literals([
    "stop",
    "length",
    "content-filter",
    "tool-calls",
    "error",
    "pause",
    "other",
    "unknown",
  ]),
});

/** Successful terminal event carrying Schema-compatible output and the completed turn count. */
export const RunCompleted = Schema.TaggedStruct("RunCompleted", {
  ...RunEventBase.fields,
  output: Schema.Json,
  turns: Schema.Int.check(Schema.isGreaterThan(0)),
  finishReason: Schema.Literals(["completed", "model-stop"]),
});

/** Terminal event for a run that failed with an expected error. */
export const RunFailed = Schema.TaggedStruct("RunFailed", {
  ...RunEventBase.fields,
  errorTag: Schema.NonEmptyString,
  message: Schema.String,
});

/** Terminal event for a run stopped by interruption. */
export const RunInterrupted = Schema.TaggedStruct("RunInterrupted", {
  ...RunEventBase.fields,
  message: Schema.String,
});

/** Terminal event for a run awaiting resumable external input. */
export const RunSuspended = Schema.TaggedStruct("RunSuspended", {
  ...RunEventBase.fields,
  reason: Schema.String,
});

/** Versioned union of stable semantic run events, excluding raw provider chunks. */
export const RunEvent = Schema.Union([
  RunStarted,
  TurnStarted,
  ModelStarted,
  TextDelta,
  ReasoningDelta,
  ToolCallDeclared,
  ToolCallStarted,
  ToolProgress,
  ToolCallSucceeded,
  ToolCallFailed,
  ApprovalRequested,
  TurnCompleted,
  RunCompleted,
  RunFailed,
  RunInterrupted,
  RunSuspended,
]);
export type RunEvent = typeof RunEvent.Type;
