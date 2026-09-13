import { Context, Schema } from "effect";

import { AgentId, ThreadId, DelegationId, RunId, ToolCallId, SubmissionId } from "./Identifiers.ts";

const Natural = Schema.Natural;

/** Tree authority ceiling. Each child exposes only its own Toolkit's permitted names. */
export class SubagentGrant extends Schema.Class<SubagentGrant>(
  "@effect-agent/capabilities/SubagentGrant",
)({
  allowedToolNames: Schema.Array(Schema.NonEmptyString).check(Schema.isMaxLength(128)),
  maxDepth: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 16 })),
  /** Omitted permits either lifetime within the depth and reserved subtree budgets. */
  childLifetimes: Schema.optionalKey(
    Schema.Array(Schema.Literals(["attached", "background"])).check(Schema.isMaxLength(2)),
  ),
}) {}

/** Intersect inherited authority; another delegation can never restore a removed capability. */
export const narrowSubagentGrant = (
  declared: SubagentGrant,
  inherited?: SubagentGrant,
): SubagentGrant =>
  inherited === undefined
    ? declared
    : SubagentGrant.make({
        allowedToolNames: declared.allowedToolNames.filter((name) =>
          inherited.allowedToolNames.includes(name),
        ),
        maxDepth: Math.min(declared.maxDepth, inherited.maxDepth),
        childLifetimes: (declared.childLifetimes ?? ["attached", "background"]).filter((lifetime) =>
          (inherited.childLifetimes ?? ["attached", "background"]).includes(lifetime),
        ),
      });

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
  /** Slots reserved for every descendant input beneath this input; absent reserves none. */
  descendantInvocations: Schema.optionalKey(Natural),
});

export type SubagentBudgetReservation = typeof SubagentBudgetReservation.Type;

/** Root-relative delegation depth; the direct child of a top-level run has depth 1. */
export const DelegationDepth = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
export type DelegationDepth = typeof DelegationDepth.Type;

/** @deprecated Names are application-owned; use DelegationTool metadata for classification. */
export const delegationToolPrefix = "delegate_";

/** @deprecated Checks a legacy spelling only, never execution, nesting or recovery semantics. */
export const isDelegationToolName = (toolName: string): boolean =>
  toolName.startsWith(delegationToolPrefix);

/** Marks a Tool implementing the idempotent Subagent establishment protocol. */
export const DelegationTool = Context.Reference<boolean>("@effect-agent/core/DelegationTool", {
  defaultValue: () => false,
});

/** Marks a host-managed idempotent worker operation, independently of its Tool name. */
export const WorkerOperationTool = Context.Reference<boolean>(
  "@effect-agent/core/WorkerOperationTool",
  {
    defaultValue: () => false,
  },
);

/** Identifies the background launch operation within a generated management Toolkit. */
export const BackgroundSpawnTool = Context.Reference<boolean>(
  "@effect-agent/core/BackgroundSpawnTool",
  {
    defaultValue: () => false,
  },
);

/** Declaration metadata and inherited authority determine the effective visible Tool set. */
export const isSubagentToolAllowed = (
  grant: SubagentGrant | undefined,
  depth: number,
  name: string,
  annotations: Context.Context<never>,
): boolean =>
  grant === undefined ||
  (grant.allowedToolNames.includes(name) &&
    (Context.get(annotations, BackgroundSpawnTool)
      ? depth < grant.maxDepth &&
        (grant.childLifetimes ?? ["attached", "background"]).includes("background")
      : Context.get(annotations, DelegationTool)
        ? depth < grant.maxDepth &&
          (grant.childLifetimes ?? ["attached", "background"]).includes("attached")
        : true));

/** Persisted preparation classification, checked against the resolved Tool before replay. */
export const ToolExecutionKind = Schema.Literals(["ordinary", "delegation", "orchestration"]);
export type ToolExecutionKind = typeof ToolExecutionKind.Type;

/** Resolve only trusted declaration metadata; names never authorize automatic replay. */
export const getToolExecutionKind = (annotations: Context.Context<never>): ToolExecutionKind =>
  Context.get(annotations, WorkerOperationTool)
    ? "orchestration"
    : Context.get(annotations, DelegationTool)
      ? "delegation"
      : "ordinary";

/** Immutable lineage from a child Thread to the parent identity that established it. */
export class SubagentParentLink extends Schema.Class<SubagentParentLink>("SubagentParentLink")({
  delegationId: DelegationId,
  parentAgentId: AgentId,
  parentThreadId: ThreadId,
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
  depth: DelegationDepth,
}) {}

/**
 * Classification of one bounded durable delegation failure.
 * `"child-failed"` and `"child-aborted"` project the
 * child's canonical failed/aborted Settlement; `"child-compatibility"`
 * projects the framework's `ChildCompatibilityFailure` child Settlement (the
 * stored child Binding digest was unavailable — recovery never substituted
 * current code); `"establishment-denied"` is a fail-closed coordinator
 * refusal (lineage/digest verification, divergent replay); and
 * `"declaration-unavailable"` is retained for decoding failures recorded by
 * versions that required an explicit durable digest declaration.
 */
export const SubagentExecutionFailureClassification = Schema.Literals([
  "child-failed",
  "child-aborted",
  "child-compatibility",
  "establishment-denied",
  "declaration-unavailable",
]);

export type SubagentExecutionFailureClassification =
  typeof SubagentExecutionFailureClassification.Type;

export const maxErrorTagLength = 256;
const BoundedErrorTag = Schema.NonEmptyString.check(Schema.isMaxLength(maxErrorTagLength));

/**
 * Bounded framework projection of a durable child failure, shared by attached joins and reports.
 * A failed or aborted durable child
 * joins its parent Tool Call as exactly this typed failure: a classification,
 * the child references, and the coordinator's bounded `{errorTag, message}`
 * projection — never a raw Cause, stack, provider response, secret, or child
 * payload. The typed child failure union does not survive a durable
 * Settlement, so `mapChildFailure` remains the ephemeral-path contract;
 * Schema-declared durable domain-failure mapping is a recorded later
 * extension.
 */
export class SubagentExecutionFailure extends Schema.TaggedError<SubagentExecutionFailure>()(
  "SubagentExecutionFailure",
  {
    delegationId: DelegationId,
    targetAgentId: AgentId,
    classification: SubagentExecutionFailureClassification,
    /** Child references, present once establishment reached a child identity. */
    childThreadId: Schema.optionalKey(ThreadId),
    childSubmissionId: Schema.optionalKey(SubmissionId),
    childRunId: Schema.optionalKey(RunId),
    errorTag: BoundedErrorTag,
    message: Schema.String.check(Schema.isMaxLength(4 * 1024)),
  },
) {}
