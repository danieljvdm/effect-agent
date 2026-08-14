import { Schema } from "effect";

import { ApprovalRequest, UsageBudgetLimits, UsageTotals } from "@effect-agent/capabilities";
import { ConversationId, RunEvent, RunId, ToolCallId } from "@effect-agent/core";
import { SandboxEvent } from "@effect-agent/sandbox";
import { ChatInput } from "./general-chat";

export const DemoRunHandle = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/example-demo/DemoRunHandle"),
);
export type DemoRunHandle = typeof DemoRunHandle.Type;

export const DemoScenario = Schema.Literals([
  "guided",
  "hold",
  "budget-tokens",
  "budget-tools",
  "budget-cost",
  "budget-duration",
  "tool-defect",
]);
export type DemoScenario = typeof DemoScenario.Type;

export const DemoOpenAiModel = Schema.Literals(["gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol"]);
export type DemoOpenAiModel = typeof DemoOpenAiModel.Type;

export const DemoReasoningEffort = Schema.Literals([
  "none",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
]);
export type DemoReasoningEffort = typeof DemoReasoningEffort.Type;

/** Browser-selected model settings for the live research travel agent. */
export class DemoModelSettings extends Schema.Class<DemoModelSettings>("DemoModelSettings")({
  model: DemoOpenAiModel,
  reasoningEffort: DemoReasoningEffort,
  /** OpenAI priority processing ("fast mode"). */
  fast: Schema.Boolean,
}) {}

export class StartOperationalRunRequest extends Schema.Class<StartOperationalRunRequest>(
  "StartOperationalRunRequest",
)({
  scenario: DemoScenario,
}) {}

/** One live OpenAI-coordinated travel Run over deterministic demo inventory. */
export class StartLiveTravelChatRequest extends Schema.Class<StartLiveTravelChatRequest>(
  "StartLiveTravelChatRequest",
)({
  message: ChatInput.fields.message,
  scenario: DemoScenario,
  settings: Schema.optionalKey(DemoModelSettings),
}) {}

export const DemoCommandKind = Schema.Literals(["steering", "follow-up"]);
export type DemoCommandKind = typeof DemoCommandKind.Type;

export class QueueRunCommandRequest extends Schema.Class<QueueRunCommandRequest>(
  "QueueRunCommandRequest",
)({
  handle: DemoRunHandle,
  kind: DemoCommandKind,
  content: Schema.NonEmptyString.check(Schema.isMaxLength(8_000)),
}) {}

export const DemoApprovalChoice = Schema.Literals(["approve", "deny"]);
export type DemoApprovalChoice = typeof DemoApprovalChoice.Type;

export class ResolveRunApprovalRequest extends Schema.Class<ResolveRunApprovalRequest>(
  "ResolveRunApprovalRequest",
)({
  handle: DemoRunHandle,
  requestId: Schema.NonEmptyString,
  choice: DemoApprovalChoice,
}) {}

export class DemoControlAccepted extends Schema.Class<DemoControlAccepted>("DemoControlAccepted")({
  accepted: Schema.Literal(true),
}) {}

export class DemoControlFailure extends Schema.TaggedErrorClass<DemoControlFailure>()(
  "DemoControlFailure",
  {
    reason: Schema.Literals([
      "run-not-found",
      "run-closed",
      "approval-not-found",
      "approval-already-decided",
      "invalid-command",
    ]),
    message: Schema.String,
  },
) {}

export class DemoRunFailure extends Schema.TaggedErrorClass<DemoRunFailure>()("DemoRunFailure", {
  errorTag: Schema.NonEmptyString,
  message: Schema.String,
}) {}

const DemoEventBase = {
  handle: DemoRunHandle,
  emittedAt: Schema.DateTimeUtcFromString,
} as const;

export class DemoRunOpened extends Schema.TaggedClass<DemoRunOpened>()("DemoRunOpened", {
  ...DemoEventBase,
  runId: RunId,
  conversationId: ConversationId,
  scenario: DemoScenario,
  executionClass: Schema.Literal("ephemeral"),
  schedulerConcurrency: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

export class DemoCommandStateChanged extends Schema.TaggedClass<DemoCommandStateChanged>()(
  "DemoCommandStateChanged",
  {
    ...DemoEventBase,
    commandId: Schema.NonEmptyString,
    kind: DemoCommandKind,
    content: Schema.String,
    status: Schema.Literals(["queued", "claimed", "delivered"]),
    deliverySeam: Schema.Literals(["after-tool-batch", "otherwise-stop"]),
  },
) {}

export class DemoToolBatchCommitted extends Schema.TaggedClass<DemoToolBatchCommitted>()(
  "DemoToolBatchCommitted",
  {
    ...DemoEventBase,
    declaredOrder: Schema.Array(ToolCallId),
    completionOrder: Schema.Array(ToolCallId),
  },
) {}

export class DemoBudgetChanged extends Schema.TaggedClass<DemoBudgetChanged>()(
  "DemoBudgetChanged",
  {
    ...DemoEventBase,
    scopeLevel: Schema.Literals(["global", "tenant", "agent", "conversation", "run"]),
    scopeId: Schema.NonEmptyString,
    limits: UsageBudgetLimits,
    totals: UsageTotals,
  },
) {}

export class DemoBudgetRejected extends Schema.TaggedClass<DemoBudgetRejected>()(
  "DemoBudgetRejected",
  {
    ...DemoEventBase,
    scopeLevel: Schema.Literals(["global", "tenant", "agent", "conversation", "run"]),
    scopeId: Schema.NonEmptyString,
    limit: Schema.Literals(["input-tokens", "output-tokens", "tool-calls", "cost", "duration"]),
    limitValue: Schema.Natural,
    observedValue: Schema.Natural,
  },
) {}

export class DemoContextPrepared extends Schema.TaggedClass<DemoContextPrepared>()(
  "DemoContextPrepared",
  {
    ...DemoEventBase,
    turn: Schema.Int.check(Schema.isGreaterThan(0)),
    officialMessageCount: Schema.Natural,
    modelMessageCount: Schema.Natural,
    compacted: Schema.Boolean,
    summary: Schema.String.check(Schema.isMaxLength(8_000)),
  },
) {}

export class DemoApprovalPending extends Schema.TaggedClass<DemoApprovalPending>()(
  "DemoApprovalPending",
  {
    ...DemoEventBase,
    request: ApprovalRequest,
  },
) {}

export class DemoApprovalSettled extends Schema.TaggedClass<DemoApprovalSettled>()(
  "DemoApprovalSettled",
  {
    ...DemoEventBase,
    requestId: Schema.NonEmptyString,
    choice: DemoApprovalChoice,
  },
) {}

export class DemoHoldHandlerState extends Schema.TaggedClass<DemoHoldHandlerState>()(
  "DemoHoldHandlerState",
  {
    ...DemoEventBase,
    starts: Schema.Natural,
  },
) {}

export class DemoMcpConnected extends Schema.TaggedClass<DemoMcpConnected>()("DemoMcpConnected", {
  ...DemoEventBase,
  serverId: Schema.NonEmptyString,
  implementationName: Schema.NonEmptyString,
  implementationVersion: Schema.NonEmptyString,
  toolCount: Schema.Natural,
  encodedBytes: Schema.Natural,
  toolkitSchemaDigest: Schema.NonEmptyString,
  maxToolCount: Schema.Int.check(Schema.isGreaterThan(0)),
  maxDiscoveryBytes: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

export class DemoSandboxObserved extends Schema.TaggedClass<DemoSandboxObserved>()(
  "DemoSandboxObserved",
  {
    ...DemoEventBase,
    event: SandboxEvent,
  },
) {}

export const DemoOperationalEvent = Schema.Union([
  RunEvent,
  DemoRunOpened,
  DemoCommandStateChanged,
  DemoToolBatchCommitted,
  DemoBudgetChanged,
  DemoBudgetRejected,
  DemoContextPrepared,
  DemoApprovalPending,
  DemoApprovalSettled,
  DemoHoldHandlerState,
  DemoMcpConnected,
  DemoSandboxObserved,
]);
export type DemoOperationalEvent = typeof DemoOperationalEvent.Type;
