import {
  AgentId,
  ThreadId,
  DelegationId,
  RunId,
  SubmissionId,
} from "@effect-agent/core/Identifiers";
import {
  SubagentDelegationCaps,
  SubagentReservationAmounts,
} from "@effect-agent/core/SubagentContract";
import { Duration, Schema } from "effect";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const Natural = Schema.Natural;

const FinitePositiveDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && Duration.isPositive(duration),
    { expected: "a finite positive duration" },
  ),
);

const SubagentPolicyFields = Schema.Struct({
  /** Total invocation slots, including reserved descendants, available through this delegation budget. */
  maxChildren: PositiveInt,
  /** Concurrently executing children per parent Run. */
  maxConcurrency: PositiveInt,
  /** Invocation slots held for the entire descendant subtree; omitted reserves zero. */
  descendantInvocations: Schema.optionalKey(Natural),
  /** Model turns reserved for each child invocation. */
  maxTurns: PositiveInt,
  /** Tool Calls reserved for each child invocation. */
  maxToolCalls: PositiveInt,
  /** Wall-clock duration reserved for each child invocation. */
  maxDuration: FinitePositiveDuration,
  maxInputTokens: Schema.optionalKey(PositiveInt),
  maxOutputTokens: Schema.optionalKey(PositiveInt),
  maxCostMicrousd: Schema.optionalKey(Natural),
  maxResultBytes: Schema.optionalKey(PositiveInt),
});

type SubagentPolicyFields = typeof SubagentPolicyFields.Type;

/** Inputs normalized and validated by `SubagentPolicy.make`. */
export type SubagentPolicyInput = Readonly<
  Omit<SubagentPolicyFields, "maxDuration"> & {
    /** Finite, positive wall-clock duration accepted in any Effect Duration input form. */
    readonly maxDuration: Duration.Input;
  }
>;

/**
 * Finite delegation bounds declared by one Subagent capability.
 * Structural limits are hard limits; token
 * and cost caps are optional and enforced only as honestly as provider
 * reporting allows.
 */
export class SubagentPolicy extends Schema.Class<SubagentPolicy>(
  "@effect-agent/capabilities/SubagentPolicy",
)(SubagentPolicyFields) {
  /** Normalize and validate finite delegation bounds, throwing on invalid input. */
  static override make(input: SubagentPolicyInput): SubagentPolicy {
    return super.make({
      ...input,
      maxDuration: Duration.fromInputUnsafe(input.maxDuration),
    });
  }
}

const BoundedFailureText = Schema.String.check(Schema.isMaxLength(4 * 1024));

/**
 * Delegation preflight denied before any child started.
 * No reservation, identity, or event exists for the
 * denied invocation; retry requires a new authorized parent Tool Call.
 */
export class SubagentPrestartDenied extends Schema.TaggedError<SubagentPrestartDenied>()(
  "SubagentPrestartDenied",
  {
    delegationId: DelegationId,
    targetAgentId: AgentId,
    reason: Schema.Literals(["nested-delegation", "grant-violation", "budget-conflict"]),
    message: BoundedFailureText,
  },
) {}

/**
 * Input or result projection failed its Schema or bounds.
 * Fail closed: the message is a fixed description and
 * never carries the raw child value.
 */
export class SubagentProjectionFailure extends Schema.TaggedError<SubagentProjectionFailure>()(
  "SubagentProjectionFailure",
  {
    delegationId: DelegationId,
    stage: Schema.Literals(["input", "result"]),
    message: BoundedFailureText,
  },
) {}

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
 * Bounded framework projection of a durable attached-child failure.
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
    message: BoundedFailureText,
  },
) {}

const millisOfMaxDuration = (policy: SubagentPolicy): number =>
  Math.max(1, Math.ceil(Duration.toMillis(policy.maxDuration)));

/**
 * Derive the parent-Run delegation caps registered with
 * `SubagentReservations` from one delegation policy: the per-invocation
 * bounds scaled by `maxChildren` plus the invocation and concurrency limits.
 */
export const delegationCapsFromPolicy = (policy: SubagentPolicy): SubagentDelegationCaps =>
  SubagentDelegationCaps.make({
    maxTotalChildInvocations: policy.maxChildren,
    maxConcurrentChildren: policy.maxConcurrency,
    maxTurns: policy.maxChildren * policy.maxTurns,
    maxToolCalls: policy.maxChildren * policy.maxToolCalls,
    maxDurationMillis: policy.maxChildren * millisOfMaxDuration(policy),
    ...(policy.maxInputTokens === undefined
      ? {}
      : { maxInputTokens: policy.maxChildren * policy.maxInputTokens }),
    ...(policy.maxOutputTokens === undefined
      ? {}
      : { maxOutputTokens: policy.maxChildren * policy.maxOutputTokens }),
    ...(policy.maxCostMicrousd === undefined
      ? {}
      : { maxCostMicrousd: policy.maxChildren * policy.maxCostMicrousd }),
    ...(policy.maxResultBytes === undefined
      ? {}
      : { maxResultBytes: policy.maxChildren * policy.maxResultBytes }),
  });

/** Derive the all-or-nothing per-invocation reservation from one delegation policy. */
export const delegationAllocationFromPolicy = (
  policy: SubagentPolicy,
): SubagentReservationAmounts =>
  SubagentReservationAmounts.make({
    turns: policy.maxTurns,
    toolCalls: policy.maxToolCalls,
    durationMillis: millisOfMaxDuration(policy),
    inputTokens: policy.maxInputTokens ?? 0,
    outputTokens: policy.maxOutputTokens ?? 0,
    costMicrousd: policy.maxCostMicrousd ?? 0,
    resultBytes: policy.maxResultBytes ?? 0,
  });
