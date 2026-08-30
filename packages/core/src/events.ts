import { Schema } from "effect";

import { AgentId, ConversationId, DelegationId, RunId, ToolCallId, TurnId } from "./identifiers.ts";
import { DelegationDepth } from "./subagent.ts";

const RunEventBase = {
  eventVersion: Schema.Literal(1),
  runId: RunId,
  conversationId: ConversationId,
  agentId: AgentId,
  sequence: Schema.Natural,
  timestamp: Schema.DateTimeUtcFromString,
  turnId: Schema.optionalKey(TurnId),
  toolCallId: Schema.optionalKey(ToolCallId),
};

/** Signals that the runtime has created a run and assigned its identities. */
export class RunStarted extends Schema.TaggedClass<RunStarted>()("RunStarted", RunEventBase) {}

/** Signals the start of a positive-numbered model turn. */
export class TurnStarted extends Schema.TaggedClass<TurnStarted>()("TurnStarted", {
  ...RunEventBase,
  turn: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

/** Signals that the model request for a turn is starting. */
export class ModelStarted extends Schema.TaggedClass<ModelStarted>()("ModelStarted", {
  ...RunEventBase,
  turn: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

/** Carries one live assistant text delta. */
export class TextDelta extends Schema.TaggedClass<TextDelta>()("TextDelta", {
  ...RunEventBase,
  text: Schema.String,
}) {}

/** Carries reasoning content explicitly exposed by the provider. */
export class ReasoningDelta extends Schema.TaggedClass<ReasoningDelta>()("ReasoningDelta", {
  ...RunEventBase,
  text: Schema.String,
}) {}

/** Reports a complete model-declared Tool Call and its execution boundary. */
export class ToolCallDeclared extends Schema.TaggedClass<ToolCallDeclared>()("ToolCallDeclared", {
  ...RunEventBase,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
  parameters: Schema.Json,
  providerExecuted: Schema.Boolean,
}) {}

/** Signals that a validated Tool Call handler has started. */
export class ToolCallStarted extends Schema.TaggedClass<ToolCallStarted>()("ToolCallStarted", {
  ...RunEventBase,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
}) {}

/** Carries a preliminary Tool result that is not the call's terminal outcome. */
export class ToolProgress extends Schema.TaggedClass<ToolProgress>()("ToolProgress", {
  ...RunEventBase,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
  result: Schema.Json,
  providerExecuted: Schema.Boolean,
}) {}

/** Records the successful terminal result of a Tool Call. */
export class ToolCallSucceeded extends Schema.TaggedClass<ToolCallSucceeded>()(
  "ToolCallSucceeded",
  {
    ...RunEventBase,
    toolCallId: ToolCallId,
    toolName: Schema.NonEmptyString,
    result: Schema.Json,
    providerExecuted: Schema.Boolean,
  },
) {}

/** Records a terminal Tool Call failure using safe, serializable diagnostics. */
export class ToolCallFailed extends Schema.TaggedClass<ToolCallFailed>()("ToolCallFailed", {
  ...RunEventBase,
  toolCallId: ToolCallId,
  toolName: Schema.NonEmptyString,
  errorTag: Schema.NonEmptyString,
  message: Schema.String,
  providerExecuted: Schema.Boolean,
  /** Synthetic policy rejection; no handler ran and the failure streak is unchanged. */
  budgetRejected: Schema.optionalKey(Schema.Literal(true)),
}) {}

/** Signals that a decoded Tool Call requires approval before execution. */
export class ApprovalRequested extends Schema.TaggedClass<ApprovalRequested>()(
  "ApprovalRequested",
  {
    ...RunEventBase,
    toolCallId: ToolCallId,
    toolName: Schema.NonEmptyString,
  },
) {}

/** Records the normalized finish reason for a completed assistant turn. */
export class TurnCompleted extends Schema.TaggedClass<TurnCompleted>()("TurnCompleted", {
  ...RunEventBase,
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
}) {}

/** Advisory that one policy limit crossed its warning fraction; emitted once per limit per run. */
export class BudgetWarning extends Schema.TaggedClass<BudgetWarning>()("BudgetWarning", {
  ...RunEventBase,
  limit: Schema.Literals(["tokens", "tool-calls", "turns", "duration", "context"]),
  consumed: Schema.Natural,
  limitValue: Schema.Natural,
}) {}

/** Records that the engine reduced model context at a Turn seam without erasing source history. */
export class CompactionPerformed extends Schema.TaggedClass<CompactionPerformed>()(
  "CompactionPerformed",
  {
    ...RunEventBase,
    turn: Schema.Int.check(Schema.isGreaterThan(0)),
    kind: Schema.Literals(["clear-tool-results", "summarize"]),
    tokensBeforeEstimate: Schema.Natural,
    tokensAfterEstimate: Schema.Natural,
  },
) {}

/**
 * Dimension that bound when a Run settled through the final-answer resolution
 * (RUN-018; the token dimension per RUN-025). This is the exhausted marker
 * the re-delegation grant flow (RUN-021) consumes and the durable
 * `SubmissionSettled` record persists (RUN-011).
 */
export const ExhaustedLimit = Schema.Literals(["tokens", "tool-calls", "turns"]);
export type ExhaustedLimit = typeof ExhaustedLimit.Type;

const CompletionFinishReason = Schema.Literals(["completed", "model-stop", "budget-exhausted"]);

type CompletionMetadata = Readonly<{
  finishReason: typeof CompletionFinishReason.Type;
  exhausted?: ExhaustedLimit | undefined;
}>;

const validateCompletionMetadata = ({
  exhausted,
  finishReason,
}: CompletionMetadata): string | undefined => {
  const isBudgetExhausted = finishReason === "budget-exhausted";
  return isBudgetExhausted === (exhausted !== undefined)
    ? undefined
    : '`exhausted` must be present exactly when `finishReason` is "budget-exhausted"';
};

const RunCompletedFields = Schema.Struct({
  ...RunEventBase,
  output: Schema.Json,
  /** Schema-encoded application disposition, present only for an ordinary completed Run. */
  runDisposition: Schema.optionalKey(Schema.Json),
  turns: Schema.Int.check(Schema.isGreaterThan(0)),
  finishReason: CompletionFinishReason,
  exhausted: Schema.optionalKey(ExhaustedLimit),
}).check(
  Schema.makeFilter((event) => validateCompletionMetadata(event)),
  Schema.makeFilter((event) =>
    event.finishReason === "budget-exhausted" && event.runDisposition !== undefined
      ? {
          path: ["runDisposition"],
          issue: '`runDisposition` is not allowed when `finishReason` is "budget-exhausted"',
        }
      : undefined,
  ),
);

/**
 * Successful terminal event carrying Schema-compatible output and the completed turn count.
 * `"budget-exhausted"` marks a Run that settled through the policy's final-answer resolution
 * after Turn, Tool Call, or token exhaustion — never a plain `"model-stop"` (RUN-011); its
 * `turns` count may exceed `maxTurns` by the single grace Turn, and `exhausted` names the
 * dimension that bound.
 *
 * Invariant: `exhausted` is present exactly when `finishReason` is
 * `"budget-exhausted"`, and a budget-exhausted completion has no application
 * run disposition. The checked Struct enforces both rules during construction,
 * decoding, and encoding without replacing the class in `RunEvent`.
 */
export class RunCompleted extends Schema.TaggedClass<RunCompleted>()(
  "RunCompleted",
  RunCompletedFields,
) {}

/** Terminal event for a run that failed with an expected error. */
export class RunFailed extends Schema.TaggedClass<RunFailed>()("RunFailed", {
  ...RunEventBase,
  errorTag: Schema.NonEmptyString,
  message: Schema.String,
}) {}

/** Terminal event for a run stopped by interruption. */
export class RunInterrupted extends Schema.TaggedClass<RunInterrupted>()("RunInterrupted", {
  ...RunEventBase,
  message: Schema.String,
}) {}

/** Terminal event for a run awaiting resumable external input. */
export class RunSuspended extends Schema.TaggedClass<RunSuspended>()("RunSuspended", {
  ...RunEventBase,
  reason: Schema.String,
}) {}

const SubagentText = Schema.String.check(Schema.isMaxLength(4 * 1024));

const SubagentEventBase = {
  ...RunEventBase,
  turnId: TurnId,
  toolCallId: ToolCallId,
  delegationId: DelegationId,
  childConversationId: ConversationId,
  childRunId: RunId,
  targetAgentId: AgentId,
  depth: DelegationDepth,
};

/** Signals that a delegation Tool Call passed preflight with preallocated child identity. */
export class SubagentRequested extends Schema.TaggedClass<SubagentRequested>()(
  "SubagentRequested",
  SubagentEventBase,
) {}

/** Signals that the child run has started inside its parent-owned scoped region. */
export class SubagentStarted extends Schema.TaggedClass<SubagentStarted>()(
  "SubagentStarted",
  SubagentEventBase,
) {}

/** Carries a bounded child progress summary without duplicating the child event stream. */
export class SubagentProgress extends Schema.TaggedClass<SubagentProgress>()("SubagentProgress", {
  ...SubagentEventBase,
  summary: SubagentText,
}) {}

/**
 * Records the child run's successful terminal outcome before the parent join.
 * Same invariant as `RunCompleted`: `exhausted` is present exactly when
 * `finishReason` is `"budget-exhausted"` (the pair travels verbatim from the
 * child's terminal event); consumers treat a divergent pair fail-safe.
 */
export class SubagentCompleted extends Schema.TaggedClass<SubagentCompleted>()(
  "SubagentCompleted",
  Schema.Struct({
    ...SubagentEventBase,
    turns: Schema.Int.check(Schema.isGreaterThan(0)),
    finishReason: CompletionFinishReason,
    exhausted: Schema.optionalKey(ExhaustedLimit),
  }).check(Schema.makeFilter((event) => validateCompletionMetadata(event))),
) {}

/** Records the child run's expected terminal failure using safe, serializable diagnostics. */
export class SubagentFailed extends Schema.TaggedClass<SubagentFailed>()("SubagentFailed", {
  ...SubagentEventBase,
  errorTag: Schema.NonEmptyString,
  message: SubagentText,
}) {}

/** Records that the child run was stopped by interruption before a terminal outcome. */
export class SubagentInterrupted extends Schema.TaggedClass<SubagentInterrupted>()(
  "SubagentInterrupted",
  {
    ...SubagentEventBase,
    reason: SubagentText,
  },
) {}

/** Records that the child's terminal outcome was joined into its parent delegation Tool Call. */
export class SubagentJoined extends Schema.TaggedClass<SubagentJoined>()(
  "SubagentJoined",
  SubagentEventBase,
) {}

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
  BudgetWarning,
  CompactionPerformed,
  RunCompleted,
  RunFailed,
  RunInterrupted,
  RunSuspended,
  SubagentRequested,
  SubagentStarted,
  SubagentProgress,
  SubagentCompleted,
  SubagentFailed,
  SubagentInterrupted,
  SubagentJoined,
]);
export type RunEvent = typeof RunEvent.Type;
