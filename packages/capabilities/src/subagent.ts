import {
  AgentId,
  ConversationId,
  type Definition,
  DelegationId,
  delegationToolPrefix,
  IdGenerator,
  isDelegationToolName,
  RunId,
  SubmissionId,
  ToolCallId,
} from "@effect-agent/core";
import {
  type AgentRuntimeFailure,
  type AgentRuntimeRequirements,
  AgentSpawner,
  type AgentSpawnerParent,
  type RunBudgetHook,
  RunEventSink,
  type RunSubagentChildIdentity,
  type RunSubagentDigests,
  type RuntimeBinding,
  type RunUsageDelta,
  type SpawnRunOptions,
  SubagentDurability,
  type SubagentDurabilityDurable,
  SubagentDurabilityError,
  type SubagentEventBasePayload,
  type SubagentEventPayload,
  ToolCallWaiting,
} from "@effect-agent/engine";
import { Clock, Duration, Effect, Layer, Option, Ref, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import {
  type BudgetReservationId,
  makeBudgetReservationId,
  SubagentBudgetExhausted,
  SubagentDelegationCaps,
  SubagentObservedUsage,
  SubagentReservationAmounts,
  SubagentReservationRequest,
  SubagentReservations,
} from "./subagent-reservation.ts";

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
  /** Total child invocations one parent Run may establish through this delegation budget. */
  maxChildren: PositiveInt,
  /** Concurrently executing children per parent Run. */
  maxConcurrency: PositiveInt,
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
 * Finite delegation bounds declared by one Delegation Definition.
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

/**
 * S1 fail-closed authority ceiling skeleton. The full
 * grant model (MCP methods, sandbox rights, secret handles, model classes,
 * per-action reauthorization inputs) is deferred to a later slice; S1 checks
 * allowed child Tool names and the delegation-depth ceiling at preflight.
 */
export class SubagentGrant extends Schema.Class<SubagentGrant>(
  "@effect-agent/capabilities/SubagentGrant",
)({
  allowedToolNames: Schema.Array(Schema.NonEmptyString).check(Schema.isMaxLength(128)),
  /** S1 and S2 fix the delegation-depth ceiling to one: no further delegation. */
  maxDepth: Schema.Literal(1),
}) {}

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
 * `"declaration-unavailable"` marks a durable coordinator driving a
 * delegation Layer that was constructed without its durable declaration.
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

const maxErrorTagLength = 256;
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
    childConversationId: Schema.optionalKey(ConversationId),
    childSubmissionId: Schema.optionalKey(SubmissionId),
    childRunId: Schema.optionalKey(RunId),
    errorTag: BoundedErrorTag,
    message: BoundedFailureText,
  },
) {}

/**
 * The delegation naming rule (`delegationToolPrefix`/`isDelegationToolName`)
 * is core-owned (plan §4.1) so session recovery can classify delegation calls
 * without importing capabilities; re-exported here unchanged for existing
 * consumers.
 */
export { delegationToolPrefix, isDelegationToolName } from "@effect-agent/core";

const DelegationToolName = Schema.String.pipe(
  Schema.refine(
    (name): name is string =>
      name.startsWith(delegationToolPrefix) && name.length > delegationToolPrefix.length,
    { expected: `a delegation Tool name of the form "${delegationToolPrefix}<target>"` },
  ),
);
const decodeDelegationToolName = Schema.decodeSync(DelegationToolName);
const decodeDelegationId = Schema.decodeSync(DelegationId);

/**
 * Bounded parent metadata visible to `prepareInput`.
 * It never contains the parent transcript, prompt, or a root runtime Context.
 */
export interface SubagentPrepareContext {
  readonly delegationId: DelegationId;
  readonly toolCallId: ToolCallId;
  readonly parent: AgentSpawnerParent;
}

/**
 * Bounded framework context handed to `projectResult` (SUB-034).
 * `budgetExhausted` is true exactly when the child settled through the
 * final-answer exhaustion resolution (RUN-018) — on the ephemeral path from
 * the child result's `finishReason`, on the durable path from the child
 * Settlement's honest marker — so the projection can surface a
 * budget-truncated partial to the orchestrator.
 */
export interface SubagentResultContext {
  readonly budgetExhausted: boolean;
}

/**
 * The delegation Tool failure Schema: the author's declared failure plus the
 * framework's preflight/budget/projection/durable-execution failure family.
 * With the default `failureMode: "error"`, exactly
 * this union (plus Effect AI's own error) can enter the parent handler `E`.
 *
 * The engine-owned `ToolCallWaiting` suspension signal and the typed
 * `SubagentDurabilityError` are members so the durable branch's waiting
 * signal and coordinator-seam failures travel typed through
 * `failureMode: "error"` (S2 plan §2); neither ever reaches an ephemeral
 * caller, and a waiting signal is not a Tool failure — the batch executor
 * keeps the raising call open and suspends the Run.
 */
export type SubagentToolFailure<Failure extends Schema.Top> = Schema.Union<
  readonly [
    Failure,
    typeof SubagentPrestartDenied,
    typeof SubagentBudgetExhausted,
    typeof SubagentProjectionFailure,
    typeof SubagentExecutionFailure,
    typeof ToolCallWaiting,
    typeof SubagentDurabilityError,
  ]
>;

/**
 * Resolution for expected delegation failures (SUB-033, ADR-0019 S2).
 * `"error"` (the default) keeps today's semantics: every expected failure
 * travels the Effect error channel and fails the parent Tool batch (D-008).
 * `"return"` contains them: the declared child failure and the framework
 * failure family become model-visible result data instead of parent-fatal
 * errors, so one dead child cannot detonate the whole parent Run.
 *
 * Effect AI's native `failureMode: "return"` is deliberately NOT used: its
 * `Stream.catch` converts every handler failure into a result, which would
 * encode the engine-owned `ToolCallWaiting` suspension signal as data and
 * silently orphan a durable child. Containment therefore lives in the
 * delegation handler — the underlying Tool keeps Effect AI
 * `failureMode: "error"`, the Tool success Schema widens to a union of the
 * declared success and the contained failure family, and exactly
 * `ToolCallWaiting` and `SubagentDurabilityError` stay in the error channel,
 * preserving durable suspension semantics by construction.
 */
export type SubagentFailureMode = "error" | "return";

/**
 * The failure family that becomes model-visible result data under
 * `failureMode: "return"`: the author-declared failure plus every expected
 * framework delegation failure. The engine-signal members are excluded by
 * construction.
 */
export type SubagentContainedFailure<Failure extends Schema.Top> = Schema.Union<
  readonly [
    Failure,
    typeof SubagentPrestartDenied,
    typeof SubagentBudgetExhausted,
    typeof SubagentProjectionFailure,
    typeof SubagentExecutionFailure,
  ]
>;

/**
 * The only failures a `"return"`-mode delegation Tool can raise: the
 * engine-owned waiting signal (consumed by the batch executor, never a Tool
 * failure) and the durable coordinator-seam error. Neither ever reaches an
 * ephemeral caller.
 */
export type SubagentReturnModeFailure = Schema.Union<
  readonly [typeof ToolCallWaiting, typeof SubagentDurabilityError]
>;

/**
 * The native Effect AI Tool created by `Subagent.define` (SUB-001, SUB-003).
 * Its per-call dependencies are exactly the engine-provided `AgentSpawner`,
 * `RunEventSink`, and `SubagentDurability` plus `IdGenerator`; every child
 * requirement is a construction requirement of `SubagentRuntime.layer`
 * instead. The engine excludes its own per-batch services from the runtime's
 * public requirements, so the visible per-call surface stays `IdGenerator`.
 */
export type SubagentTool<
  Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Mode extends SubagentFailureMode = "error",
> = Mode extends "return"
  ? Tool.Tool<
      Name,
      {
        readonly parameters: Parameters;
        readonly success: Schema.Union<readonly [Success, SubagentContainedFailure<Failure>]>;
        readonly failure: SubagentReturnModeFailure;
        readonly failureMode: "error";
      },
      AgentSpawner | RunEventSink | SubagentDurability | IdGenerator
    >
  : Tool.Tool<
      Name,
      {
        readonly parameters: Parameters;
        readonly success: Success;
        readonly failure: SubagentToolFailure<Failure>;
        readonly failureMode: "error";
      },
      AgentSpawner | RunEventSink | SubagentDurability | IdGenerator
    >;

/** Singleton Tool record provided by one `SubagentRuntime.layer`. */
export type SubagentTools<
  Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Mode extends SubagentFailureMode = "error",
> = {
  readonly [Key in Name]: SubagentTool<Name, Parameters, Success, Failure, Mode>;
};

/** Options accepted by `Subagent.define`. */
export interface SubagentDefineOptions<
  TargetInput extends Schema.Top,
  TargetOutput extends Schema.Top,
  TargetInstructions,
  TargetTools extends Record<string, Tool.Any>,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  PrepareRequirements = never,
  ProjectRequirements = never,
  Mode extends SubagentFailureMode = "error",
> {
  /**
   * Expected-failure resolution (SUB-033): `"error"` (default) fails the
   * parent Tool batch; `"return"` contains the declared failure and the
   * framework failure family as model-visible result data while
   * `ToolCallWaiting`/`SubagentDurabilityError` stay in the error channel.
   */
  readonly failureMode?: Mode;
  /** Model-visible description of the delegated capability. */
  readonly description: string;
  /** The model-agnostic child Agent Definition this delegation targets (SUB-002). */
  readonly target: Definition<
    TargetInput,
    TargetOutput,
    TargetInstructions,
    Toolkit.Toolkit<TargetTools>
  >;
  /** Schema for the model-decoded delegation parameters. */
  readonly parameters: Parameters;
  /** Schema for the bounded parent Tool result. */
  readonly success: Success;
  /** Schema for the author-declared delegation failure data. */
  readonly failure: Failure;
  /**
   * Effectful, typed projection from decoded parameters and bounded parent
   * metadata to the child Agent input. It never
   * receives the parent transcript or a root Context; its expected failures
   * are already the declared Tool failure.
   */
  readonly prepareInput: (
    parameters: Parameters["Type"],
    context: SubagentPrepareContext,
  ) => Effect.Effect<TargetInput["Type"], Failure["Type"], PrepareRequirements>;
  /**
   * Bounded result projection from the Schema-decoded child output, result
   * context, and original Schema-decoded Tool parameters to the declared Tool
   * success value. This is the explicit
   * declassification boundary for child output: the parameters let it bind
   * echoed identity and scope to the exact request without prompt parsing.
   * The context carries the
   * framework's honest exhaustion marker (SUB-034): when `budgetExhausted` is
   * true the child settled through the final-answer resolution (RUN-018) and
   * its output is a budget-truncated partial — surface it in the declared
   * success Schema so the orchestrator can decide to re-delegate with a
   * raised `toolCallAllowance`.
   */
  readonly projectResult: (
    output: TargetOutput["Type"],
    context: SubagentResultContext,
    parameters: Parameters["Type"],
  ) => Effect.Effect<Success["Type"], Failure["Type"], ProjectRequirements>;
  /**
   * Per-invocation child Tool Call allowance (SUB-034): a tightening-only
   * bound below the child Definition's own `maxToolCalls`, additionally
   * clamped to this delegation's `SubagentPolicy.maxToolCalls` (the
   * per-invocation reservation slice). `fromParameters` lets the orchestrator
   * model grant a larger allowance through an author-owned parameter field —
   * the budget-extension flow is a fresh re-delegation with a raised
   * allowance, never a mid-flight top-up. Ephemeral delegation only: a
   * durable child keeps running at its Definition policy (its lane owns no
   * per-run options channel; the reservation stays accounting-only per §7).
   */
  readonly toolCallAllowance?:
    | {
        /** Applied when `fromParameters` is absent or yields nothing. */
        readonly default: number;
        /** Extract the model-granted allowance from the decoded parameters. */
        readonly fromParameters?: (parameters: Parameters["Type"]) => number | undefined;
      }
    | undefined;
  /** Finite delegation bounds reserved for every invocation (SUB-009). */
  readonly policy: SubagentPolicy;
  /**
   * Authority ceiling for the child. Defaults to
   * exactly the target's declared Tool names at depth ceiling one; a narrower
   * grant fails preflight closed because S1 cannot shrink the child Toolkit.
   */
  readonly grant?: SubagentGrant | undefined;
  /**
   * Native Effect AI approval metadata for establishment only (SUB-026):
   * parent approval never authorizes child actions, siblings, or retries.
   */
  readonly needsApproval?: Tool.NeedsApproval<Parameters> | undefined;
}

/**
 * An immutable Delegation Definition: one target Agent Definition exposed to
 * a parent as one Effect AI Tool with explicit projections, policy, and
 * authority ceiling. It owns no acquired resources and
 * is not executable until `SubagentRuntime.layer` pairs it with an explicit
 * child Binding.
 */
export interface SubagentDelegation<
  Name extends string,
  TargetInput extends Schema.Top,
  TargetOutput extends Schema.Top,
  TargetInstructions,
  TargetTools extends Record<string, Tool.Any>,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  PrepareRequirements = never,
  ProjectRequirements = never,
  Mode extends SubagentFailureMode = "error",
> extends SubagentDefineOptions<
  TargetInput,
  TargetOutput,
  TargetInstructions,
  TargetTools,
  Parameters,
  Success,
  Failure,
  PrepareRequirements,
  ProjectRequirements,
  Mode
> {
  readonly name: Name;
  /** Stable delegation identity; S1 derives it from the unique Tool name. */
  readonly delegationId: DelegationId;
  readonly grant: SubagentGrant;
  /** The resolved expected-failure resolution (SUB-033); never absent after `define`. */
  readonly failureMode: Mode;
  /**
   * The canonical contained-failure family for this delegation (SUB-033):
   * the declared failure plus the framework members. Consumers that classify
   * returned failure data (for example a coverage gate) MUST decode through
   * this value rather than reconstructing the union, so a framework change
   * to the contained family can never diverge from their decoder.
   */
  readonly containedFailure: SubagentContainedFailure<Failure>;
  /** The real Effect AI Tool to include in the parent Toolkit (SUB-001). */
  readonly tool: SubagentTool<Name, Parameters, Success, Failure, Mode>;
}

/**
 * Declare one attached delegation as a pure value.
 *
 * The returned `.tool` is a native Effect AI Tool whose handler dependencies
 * are exactly the engine-owned `AgentSpawner` and `RunEventSink` plus
 * `IdGenerator` (SUB-003); the concrete child Binding arrives only through
 * `SubagentRuntime.layer`. Throws on an invalid delegation name or a target
 * whose Toolkit already contains a delegation Tool (SUB-029).
 */
interface SubagentDefine {
  /**
   * Contained-failure mode (SUB-033): `failureMode: "return"` must be present
   * as a VALUE — the resolved Tool channels follow what `define` actually
   * constructs, so return mode cannot be claimed through a type argument
   * while the runtime builds the error-mode Tool.
   */
  <
    const Name extends string,
    TargetInput extends Schema.Top,
    TargetOutput extends Schema.Top,
    TargetInstructions,
    TargetTools extends Record<string, Tool.Any>,
    Parameters extends Schema.Top,
    Success extends Schema.Top,
    Failure extends Schema.Top,
    PrepareRequirements = never,
    ProjectRequirements = never,
  >(
    name: Name,
    options: SubagentDefineOptions<
      TargetInput,
      TargetOutput,
      TargetInstructions,
      TargetTools,
      Parameters,
      Success,
      Failure,
      PrepareRequirements,
      ProjectRequirements,
      "return"
    > & { readonly failureMode: "return" },
  ): SubagentDelegation<
    Name,
    TargetInput,
    TargetOutput,
    TargetInstructions,
    TargetTools,
    Parameters,
    Success,
    Failure,
    PrepareRequirements,
    ProjectRequirements,
    "return"
  >;
  /** Default error mode: `failureMode` may be omitted or explicitly `"error"`. */
  <
    const Name extends string,
    TargetInput extends Schema.Top,
    TargetOutput extends Schema.Top,
    TargetInstructions,
    TargetTools extends Record<string, Tool.Any>,
    Parameters extends Schema.Top,
    Success extends Schema.Top,
    Failure extends Schema.Top,
    PrepareRequirements = never,
    ProjectRequirements = never,
  >(
    name: Name,
    options: SubagentDefineOptions<
      TargetInput,
      TargetOutput,
      TargetInstructions,
      TargetTools,
      Parameters,
      Success,
      Failure,
      PrepareRequirements,
      ProjectRequirements,
      "error"
    >,
  ): SubagentDelegation<
    Name,
    TargetInput,
    TargetOutput,
    TargetInstructions,
    TargetTools,
    Parameters,
    Success,
    Failure,
    PrepareRequirements,
    ProjectRequirements,
    "error"
  >;
}

const define: SubagentDefine = <
  const Name extends string,
  TargetInput extends Schema.Top,
  TargetOutput extends Schema.Top,
  TargetInstructions,
  TargetTools extends Record<string, Tool.Any>,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  PrepareRequirements = never,
  ProjectRequirements = never,
  Mode extends SubagentFailureMode = "error",
>(
  name: Name,
  options: SubagentDefineOptions<
    TargetInput,
    TargetOutput,
    TargetInstructions,
    TargetTools,
    Parameters,
    Success,
    Failure,
    PrepareRequirements,
    ProjectRequirements,
    Mode
  >,
): SubagentDelegation<
  Name,
  TargetInput,
  TargetOutput,
  TargetInstructions,
  TargetTools,
  Parameters,
  Success,
  Failure,
  PrepareRequirements,
  ProjectRequirements,
  Mode
> => {
  decodeDelegationToolName(name);
  for (const childToolName of Object.keys(options.target.toolkit.tools)) {
    if (isDelegationToolName(childToolName)) {
      throw new Error(
        `Subagent.define(${JSON.stringify(name)}): target Agent ${options.target.id} exposes delegation Tool ${childToolName}; S1 rejects every nested delegation (SUB-029)`,
      );
    }
  }
  const delegationId = decodeDelegationId(name);
  const grant =
    options.grant ??
    SubagentGrant.make({
      allowedToolNames: Object.keys(options.target.toolkit.tools),
      maxDepth: 1,
    });
  // `Mode` defaults to `"error"` exactly when `failureMode` is absent, so the
  // resolved literal always inhabits `Mode`; the assertion bridges only that
  // inference gap and crosses no schema boundary.
  const failureMode = (options.failureMode ?? "error") as Mode;
  const containedFailure = Schema.Union([
    options.failure,
    SubagentPrestartDenied,
    SubagentBudgetExhausted,
    SubagentProjectionFailure,
    SubagentExecutionFailure,
  ]);
  const returnModeTool = Tool.make(name, {
    description: options.description,
    parameters: options.parameters,
    // Containment (SUB-033): the contained failure family is model-visible
    // RESULT data, so it lives in the success union; only the engine-signal
    // members remain raisable. The underlying Effect AI failureMode stays
    // "error" so the waiting signal is never encoded as a result.
    success: Schema.Union([options.success, containedFailure]),
    failure: Schema.Union([ToolCallWaiting, SubagentDurabilityError]),
    needsApproval: options.needsApproval,
  });
  const errorModeTool = Tool.make(name, {
    description: options.description,
    parameters: options.parameters,
    success: options.success,
    failure: Schema.Union([
      options.failure,
      SubagentPrestartDenied,
      SubagentBudgetExhausted,
      SubagentProjectionFailure,
      SubagentExecutionFailure,
      ToolCallWaiting,
      SubagentDurabilityError,
    ]),
    needsApproval: options.needsApproval,
  });
  // Each branch is exactly `SubagentTool<..., Mode>` at its concrete `Mode`;
  // TypeScript cannot relate a runtime branch to the conditional generic, so
  // this assertion bridges only that limitation and crosses no schema
  // boundary (the schemas above are constructed per mode, never reinterpreted).
  const tool = (failureMode === "return" ? returnModeTool : errorModeTool)
    .addDependency(AgentSpawner)
    .addDependency(RunEventSink)
    .addDependency(SubagentDurability)
    .addDependency(IdGenerator) as unknown as SubagentTool<
    Name,
    Parameters,
    Success,
    Failure,
    Mode
  >;
  return Object.freeze({
    ...options,
    name,
    delegationId,
    grant,
    failureMode,
    containedFailure,
    tool,
  });
};

/** Pure Subagent authoring surface. */
export const Subagent = { define } as const;

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

type InstructionResultOf<Instructions, Input> = Instructions extends (input: Input) => infer Result
  ? Result
  : Instructions;

type InstructionErrorOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer Error,
    infer _Requirements
  >
    ? Error
    : never;

type InstructionRequirementsOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer _Error,
    infer Requirements
  >
    ? Requirements
    : never;

/**
 * Every expected child Run failure the delegation's total mapping must cover:
 * the child's inferred Agent failures plus
 * the interpreter's policy, protocol, and approval failures. Interruption and
 * defects are not members; they retain their distinct semantics.
 */
export type SubagentChildRunFailure<
  TargetInput extends Schema.Top,
  TargetOutput extends Schema.Top,
  TargetInstructions,
  TargetTools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  InstructionError = InstructionErrorOf<TargetInstructions, TargetInput["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<TargetInstructions, TargetInput["Type"]>,
> = AgentRuntimeFailure<
  RuntimeBinding<
    TargetInput,
    TargetOutput,
    TargetInstructions,
    TargetTools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements
  >,
  never,
  InstructionError
>;

/**
 * Construction requirements of one `SubagentRuntime.layer`: the child
 * Binding's full runtime requirements (Model Layer requirements, child Tool
 * handlers and their services, Schema services), both projection
 * requirements, and the parent-owned reservation service. Nothing here leaks
 * into the per-call Tool handler requirements.
 */
export type SubagentLayerRequirements<
  TargetInput extends Schema.Top,
  TargetOutput extends Schema.Top,
  TargetInstructions,
  TargetTools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  PrepareRequirements,
  ProjectRequirements,
  HookRequirements = never,
  InstructionError = InstructionErrorOf<TargetInstructions, TargetInput["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<TargetInstructions, TargetInput["Type"]>,
> =
  | AgentRuntimeRequirements<
      RuntimeBinding<
        TargetInput,
        TargetOutput,
        TargetInstructions,
        TargetTools,
        Provider,
        ModelProvides,
        ModelRequires,
        InstructionError,
        InstructionRequirements
      >,
      HookRequirements,
      InstructionRequirements
    >
  | TargetInput["EncodingServices"]
  | PrepareRequirements
  | ProjectRequirements
  | SubagentReservations;

/**
 * Construction-fixed durable delegation declaration.
 * Supplying it makes the delegation Layer establishable under a
 * durable coordinator; a durable-mode invocation of a Layer constructed
 * without it fails closed with `SubagentExecutionFailure`
 * (`"declaration-unavailable"`) instead of inventing digests or degrading to
 * an in-process spawn.
 */
export interface SubagentDurableOptions {
  /**
   * Application-computed digests of the exact child Agent Binding this Layer
   * pairs with the delegation — the same digest authority the host uses for
   * durable admission (`DurableSubmitOptions.definitions`) and for
   * registering the child Binding with its resolver. Fixed once at
   * handler-Layer construction; the coordinator stores them in
   * `SubagentRequested` and recovery resolves the child Binding by stable
   * identity and exact stored digest, fail-closed (SUB-023): a changed or
   * missing Binding produces a typed compatibility failure, never
   * silently-substituted current code.
   */
  readonly targetDigests: RunSubagentDigests;
}

/**
 * The conservative final accounting summary the durable delegation handler
 * attaches to every settlement join. The handler has no live child usage stream at durable join
 * time, so every dimension conservatively consumes its full reservation and
 * releases nothing — unreported usage never creates budget. The coordinator
 * owns the canonical accounting decision on `SubagentJoined` and may replace
 * structural dimensions from canonical child evidence; this value is opaque
 * encoded policy math to the engine and to storage adapters (D8).
 */
export class SubagentDurableAccounting extends Schema.Class<SubagentDurableAccounting>(
  "@effect-agent/capabilities/SubagentDurableAccounting",
)({
  /** The per-invocation allocation reserved at establishment. */
  allocation: SubagentReservationAmounts,
  /** Final consumed decision per dimension. */
  consumed: SubagentReservationAmounts,
  /** Amounts returned to the parent: `allocation - consumed` per dimension. */
  released: SubagentReservationAmounts,
  /** How the decision was computed; the handler only ever reports conservatively. */
  basis: Schema.Literals(["reserved-conservative"]),
}) {}

/** Options accepted by `SubagentRuntime.layer`. */
export interface SubagentRuntimeOptions<
  Failure extends Schema.Top,
  ChildFailure,
  HookRequirements = never,
> {
  /**
   * Durable delegation declaration (S2). Absent, the Layer remains fully
   * functional for ephemeral runs; under a durable coordinator every
   * invocation then fails closed instead of establishing.
   */
  readonly durable?: SubagentDurableOptions | undefined;
  /**
   * Total mapping from every expected child Run failure to the declared Tool
   * failure (SUB-028). The parameter type is the child Binding's complete
   * expected failure union, so a mapping that covers only part of it is a
   * compile error. Interruption stays interruption and defects stay defects;
   * neither reaches this mapping.
   */
  readonly mapChildFailure: (failure: ChildFailure) => Failure["Type"];
  /**
   * Override the parent-Run delegation caps registered with
   * `SubagentReservations`. Defaults to `delegationCapsFromPolicy(policy)`.
   * Supply shared caps explicitly when one parent Run uses several delegation
   * Tools, because re-registering different caps denies start fail-closed.
   */
  readonly parentCaps?: SubagentDelegationCaps | undefined;
  /**
   * Seed child Run options (approval, context, input hooks) so child actions
   * follow their own ordinary approval and policy (SUB-026). Hook failures
   * must be handled inside the hook (`never` in `E`); a supplied budget hook
   * is composed after the reservation's own usage observation.
   */
  readonly child?: SpawnRunOptions<never, HookRequirements> | undefined;
}

const maxEventTextLength = 4 * 1024;
const boundedEventText = (text: string): string =>
  text.length <= maxEventTextLength ? text : `${text.slice(0, maxEventTextLength - 1)}…`;

const ErrorMessage = Schema.Struct({ message: Schema.String });
const ErrorTag = Schema.Struct({ _tag: Schema.NonEmptyString });

const errorMessageOf = (error: unknown): string =>
  Option.match(Schema.decodeUnknownOption(ErrorMessage)(error), {
    onNone: () => String(error),
    onSome: ({ message }) => message,
  });

const errorTagOf = (error: unknown): string =>
  Option.match(Schema.decodeUnknownOption(ErrorTag)(error), {
    onNone: () => "UnknownError",
    onSome: ({ _tag }) => _tag,
  });

const boundedErrorTag = (tag: string): string => {
  const nonEmpty = tag.length === 0 ? "UnknownError" : tag;
  return nonEmpty.length <= maxErrorTagLength ? nonEmpty : nonEmpty.slice(0, maxErrorTagLength);
};

/**
 * The coordinator's bounded projection of a failed/aborted child Settlement
 * result (S2 plan §1.6, D5): `{errorTag, message}`, never a raw Cause. The
 * fallback tolerates any other bounded shape without ever surfacing the raw
 * value beyond a tag and message.
 */
const ChildFailureProjection = Schema.Struct({
  errorTag: Schema.NonEmptyString,
  message: Schema.String,
});

const childFailureProjectionOf = (
  encodedResult: unknown,
): { readonly errorTag: string; readonly message: string } =>
  Option.match(Schema.decodeUnknownOption(ChildFailureProjection)(encodedResult), {
    onNone: () => ({
      errorTag: errorTagOf(encodedResult),
      message: errorMessageOf(encodedResult),
    }),
    onSome: (projection) => projection,
  });

const zeroReservationAmounts = SubagentReservationAmounts.make({
  turns: 0,
  toolCalls: 0,
  durationMillis: 0,
  inputTokens: 0,
  outputTokens: 0,
  costMicrousd: 0,
  resultBytes: 0,
});

const encodeDurableAccounting = Schema.encodeEffect(SubagentDurableAccounting);
const encodeProjectionFailure = Schema.encodeEffect(SubagentProjectionFailure);
const encodeBudgetFailure = Schema.encodeEffect(SubagentBudgetExhausted);
const encodeExecutionFailure = Schema.encodeEffect(SubagentExecutionFailure);
const encodeGrant = Schema.encodeEffect(SubagentGrant);
const encodeAllocationAmounts = Schema.encodeEffect(SubagentReservationAmounts);

const utf8ByteLength = (value: string): number => {
  let total = 0;
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    total += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }
  return total;
};

const toNatural = (value: number): number =>
  Number.isFinite(value) && value > 0 ? Math.floor(value) : 0;

const observedUsageFromDelta = (delta: RunUsageDelta): SubagentObservedUsage =>
  SubagentObservedUsage.make({
    turns: delta.modelCalls,
    toolCalls: toNatural(delta.toolCalls),
    inputTokens: toNatural(delta.inputTokens),
    outputTokens: toNatural(delta.outputTokens),
    costMicrousd: toNatural(delta.costMicrousd),
  });

/** A child that never started consumed nothing; observing zeros releases everything. */
const neverStartedUsage = SubagentObservedUsage.make({
  turns: 0,
  toolCalls: 0,
  durationMillis: 0,
  inputTokens: 0,
  outputTokens: 0,
  costMicrousd: 0,
  resultBytes: 0,
});

/**
 * Finalizer-driven settlement: observe honest final usage, then release the
 * unused allocation exactly once. Runs on every handler exit path — success,
 * declared failure, interruption, and defect — through the reservation's
 * `Effect.acquireRelease`; a missing reservation after a successful reserve
 * is a ledger invariant violation and therefore a defect.
 */
const settleReservation = (
  reservations: SubagentReservations["Service"],
  reservationId: BudgetReservationId,
  startedAt: Ref.Ref<number | undefined>,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const started = yield* Ref.get(startedAt);
    if (started === undefined) {
      yield* reservations.observe(reservationId, neverStartedUsage);
    } else {
      const now = yield* Clock.currentTimeMillis;
      yield* reservations.observe(
        reservationId,
        SubagentObservedUsage.make({ durationMillis: Math.max(0, Math.floor(now - started)) }),
      );
    }
    yield* reservations.release(reservationId);
  }).pipe(Effect.orDie);

/**
 * Module-private provenance wrapper (SUB-033 hardening): ONLY the operations
 * that can genuinely produce the engine signals wrap them here, so
 * containment classification never trusts the runtime identity of classes an
 * author could declare in their own failure Schema — a spoofed
 * `ToolCallWaiting` in author data is contained as data, never rethrown as a
 * suspension signal.
 */
class GenuineEngineSignal {
  readonly _tag = "GenuineEngineSignal";
  constructor(readonly signal: ToolCallWaiting | SubagentDurabilityError) {}
}

const wrapEngineSignal = (signal: ToolCallWaiting | SubagentDurabilityError) =>
  new GenuineEngineSignal(signal);

type SubagentHandler<
  Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Mode extends SubagentFailureMode = "error",
> = (
  parameters: Parameters["Type"],
  context: Toolkit.HandlerContext<SubagentTool<Name, Parameters, Success, Failure, Mode>>,
) => Effect.Effect<
  Mode extends "return"
    ? Success["Type"] | SubagentContainedFailure<Failure>["Type"]
    : Success["Type"],
  Mode extends "return" ? SubagentReturnModeFailure["Type"] : SubagentToolFailure<Failure>["Type"],
  Tool.HandlerServices<SubagentTool<Name, Parameters, Success, Failure, Mode>>
>;

/**
 * Build the Toolkit handler Layer for one delegation Tool from an explicit
 * child Agent Binding.
 *
 * Construction requirements carry the child Binding's full runtime needs and
 * both projections; they are captured once via `Effect.context` so the
 * per-call handler requirements stay exactly the Tool's declared engine
 * dependencies. The handler dispatches on the engine-provided per-batch
 * `SubagentDurability` service mode:
 *
 * - **ephemeral** (the explicit engine default when no durable coordinator
 *   supplied `RunOptions.subagent`): the S1 path unchanged — preflight, an
 *   in-process scoped child Run (SUB-011/012), stable lifecycle events,
 *   total-mapped expected child failures, and Scope-finalizer reservation
 *   settlement on every exit path.
 * - **durable** (S2): the same fail-closed preflight and input projection,
 *   then idempotent establishment through the coordinator (spec §12 steps
 *   2-9) with construction-fixed child Binding digests, encoded grant, and
 *   encoded allocation; the engine-owned waiting signal while the attached
 *   child is nonterminal; and, on re-entry with a settled child, output
 *   decoding, `projectResult`, and ONE atomic settlement join carrying the
 *   conservative accounting summary. Failed children join as the bounded
 *   `SubagentExecutionFailure`; no in-process child fiber ever starts.
 */
const layer = <
  Name extends string,
  TargetInput extends Schema.Top,
  TargetOutput extends Schema.Top,
  TargetInstructions,
  TargetTools extends Record<string, Tool.Any>,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  PrepareRequirements,
  ProjectRequirements,
  Provider,
  ModelProvides,
  ModelRequires,
  HookRequirements = never,
  Mode extends SubagentFailureMode = "error",
  InstructionError = InstructionErrorOf<TargetInstructions, TargetInput["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<TargetInstructions, TargetInput["Type"]>,
>(
  delegation: SubagentDelegation<
    Name,
    TargetInput,
    TargetOutput,
    TargetInstructions,
    TargetTools,
    Parameters,
    Success,
    Failure,
    PrepareRequirements,
    ProjectRequirements,
    Mode
  >,
  childBinding: RuntimeBinding<
    TargetInput,
    TargetOutput,
    TargetInstructions,
    TargetTools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements
  >,
  options: SubagentRuntimeOptions<
    Failure,
    SubagentChildRunFailure<
      TargetInput,
      TargetOutput,
      TargetInstructions,
      TargetTools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements
    >,
    HookRequirements
  >,
): Layer.Layer<
  Tool.HandlersFor<SubagentTools<Name, Parameters, Success, Failure, Mode>>,
  never,
  SubagentLayerRequirements<
    TargetInput,
    TargetOutput,
    TargetInstructions,
    TargetTools,
    Provider,
    ModelProvides,
    ModelRequires,
    PrepareRequirements,
    ProjectRequirements,
    HookRequirements,
    InstructionError,
    InstructionRequirements
  >
> => {
  const caps = options.parentCaps ?? delegationCapsFromPolicy(delegation.policy);
  const allocation = delegationAllocationFromPolicy(delegation.policy);
  // `Toolkit.ToolsByName` cannot reduce its mapped-as key while `Name` is
  // generic (it degrades to a string index signature); at every concrete
  // `Name` the two records are identical, so this assertion bridges only that
  // compiler limitation and crosses no schema boundary.
  const toolkit = Toolkit.make(delegation.tool as Tool.Any) as unknown as Toolkit.Toolkit<
    SubagentTools<Name, Parameters, Success, Failure, Mode>
  >;
  // Containment (SUB-033): under `failureMode: "return"` every expected
  // delegation failure becomes the handler's SUCCESS value (the Tool success
  // Schema is the union of the declared success and the contained family);
  // exactly the engine signals stay raisable, so the durable waiting
  // suspension and coordinator-seam errors keep their semantics.
  const contained = delegation.failureMode === "return";
  const encodeChildInput = Schema.encodeEffect(delegation.target.input);
  const encodeSuccess = Schema.encodeEffect(delegation.success);
  const decodeChildOutput = Schema.decodeUnknownEffect(delegation.target.output);
  const encodeDeclaredFailure = Schema.encodeEffect(delegation.failure);
  const childToolNames = Object.keys(delegation.target.toolkit.tools);
  // Construction-fixed durable declaration (S2): the exact digest strings the
  // establishment request carries on every Attempt, including batch resume.
  const durableDeclaration: SubagentDurableOptions | undefined =
    options.durable === undefined
      ? undefined
      : {
          targetDigests: {
            agent: options.durable.targetDigests.agent,
            model: options.durable.targetDigests.model,
            tools: options.durable.targetDigests.tools,
          },
        };

  const executionFailure = (
    classification: SubagentExecutionFailureClassification,
    errorTag: string,
    message: string,
    child?: RunSubagentChildIdentity,
  ): SubagentExecutionFailure =>
    SubagentExecutionFailure.make({
      delegationId: delegation.delegationId,
      targetAgentId: delegation.target.id,
      classification,
      errorTag: boundedErrorTag(errorTag),
      message: boundedEventText(message),
      ...(child === undefined
        ? {}
        : {
            childConversationId: child.childConversationId,
            childSubmissionId: child.childSubmissionId,
            childRunId: child.childRunId,
          }),
    });

  const prestartDenied = (
    reason: SubagentPrestartDenied["reason"],
    message: string,
  ): SubagentPrestartDenied =>
    SubagentPrestartDenied.make({
      delegationId: delegation.delegationId,
      targetAgentId: delegation.target.id,
      reason,
      message: boundedEventText(message),
    });

  const build = Effect.gen(function* () {
    const captured =
      yield* Effect.context<
        SubagentLayerRequirements<
          TargetInput,
          TargetOutput,
          TargetInstructions,
          TargetTools,
          Provider,
          ModelProvides,
          ModelRequires,
          PrepareRequirements,
          ProjectRequirements,
          HookRequirements,
          InstructionError,
          InstructionRequirements
        >
      >();

    // Durable establishment statics, computed once at Layer construction
    // (S2 plan §5 WP5): the encoded grant and per-invocation allocation the
    // coordinator digests into `SubagentRequested`, and the conservative
    // accounting summary attached to every settlement join. These schemas
    // carry plain fields, so an encoding failure is a defect, not an
    // expected delegation failure.
    const encodedGrant = yield* encodeGrant(delegation.grant).pipe(Effect.orDie);
    const encodedAllocation = yield* encodeAllocationAmounts(allocation).pipe(Effect.orDie);
    const conservativeAccounting = yield* encodeDurableAccounting(
      SubagentDurableAccounting.make({
        allocation,
        consumed: allocation,
        released: zeroReservationAmounts,
        basis: "reserved-conservative",
      }),
    ).pipe(Effect.orDie);

    const invoke = Effect.fn(`SubagentRuntime.${delegation.name}`)(function* (
      parameters: Parameters["Type"],
      handlerContext: Toolkit.HandlerContext<
        SubagentTool<Name, Parameters, Success, Failure, Mode>
      >,
    ) {
      const spawner = yield* AgentSpawner;
      const sink = yield* RunEventSink;
      const reservations = yield* SubagentReservations;
      // The interpreter supplies the parent Tool Call identity for every
      // executed Tool Call; its absence is an engine defect, not an expected
      // failure of the delegation.
      const toolCallId = yield* Schema.decodeUnknownEffect(ToolCallId)(
        handlerContext.toolCallId,
      ).pipe(Effect.orDie);

      // Preflight: depth ceiling,
      // nested delegation in the child Toolkit, and the grant ceiling all
      // fail closed before any reservation or child identity exists.
      if (spawner.depth + 1 > delegation.grant.maxDepth) {
        return yield* prestartDenied(
          "nested-delegation",
          `Delegation ${delegation.name} was requested at depth ${spawner.depth}; S1 rejects every nested delegation`,
        );
      }
      for (const childToolName of childToolNames) {
        if (isDelegationToolName(childToolName)) {
          return yield* prestartDenied(
            "nested-delegation",
            `Child Toolkit exposes delegation Tool ${childToolName}; S1 rejects every nested delegation`,
          );
        }
        if (!delegation.grant.allowedToolNames.includes(childToolName)) {
          return yield* prestartDenied(
            "grant-violation",
            `Child Tool ${childToolName} is outside the delegation grant ceiling`,
          );
        }
      }

      const prepared = yield* delegation.prepareInput(parameters, {
        delegationId: delegation.delegationId,
        toolCallId,
        parent: spawner.parent,
      });
      const encodedInput = yield* encodeChildInput(prepared).pipe(
        Effect.mapError(() =>
          SubagentProjectionFailure.make({
            delegationId: delegation.delegationId,
            stage: "input",
            message: "Prepared child input did not satisfy the target Agent input Schema",
          }),
        ),
      );

      const parentRunId = spawner.parent.runId;
      yield* reservations
        .registerParent(parentRunId, caps)
        .pipe(
          Effect.catchTag("SubagentParentBudgetConflict", () =>
            Effect.fail(
              prestartDenied(
                "budget-conflict",
                "The parent Run is already registered with different delegation caps; supply shared parentCaps explicitly",
              ),
            ),
          ),
        );

      const reservationId = makeBudgetReservationId(parentRunId, toolCallId);
      const startedAt = yield* Ref.make<number | undefined>(undefined);
      // Reservation settlement is finalizer-driven from this point on: every
      // exit path — success, declared failure, interruption, defect — settles
      // accounting exactly once when the handler scope closes.
      yield* Effect.acquireRelease(
        reservations
          .reserve(
            SubagentReservationRequest.make({
              parentRunId,
              parentToolCallId: toolCallId,
              allocation,
            }),
          )
          .pipe(
            // A same-key conflict or unregistered parent after a successful
            // registerParent is a ledger invariant violation, not an expected
            // delegation failure.
            Effect.catchTags({
              SubagentReservationConflict: (conflict) => Effect.die(conflict),
              SubagentParentBudgetUnknown: (unknown) => Effect.die(unknown),
            }),
          ),
        () => settleReservation(reservations, reservationId, startedAt),
      );
      // Scope-owned concurrency permit: interruption while queued frees the
      // slot, and the settlement finalizer above releases the reservation.
      yield* reservations
        .acquireChildSlot(parentRunId)
        .pipe(Effect.catchTag("SubagentParentBudgetUnknown", (unknown) => Effect.die(unknown)));

      const seededChild = options.child;
      const seededBudget = seededChild?.budget;
      const budget: RunBudgetHook<never, HookRequirements> = {
        guard:
          seededBudget === undefined ? (effect) => effect : (effect) => seededBudget.guard(effect),
        consume: (delta) =>
          reservations
            .observe(reservationId, observedUsageFromDelta(delta))
            .pipe(
              Effect.orDie,
              Effect.andThen(
                seededBudget === undefined ? Effect.void : seededBudget.consume(delta),
              ),
            ),
      };
      // Per-invocation allowance (SUB-034): tightening-only, clamped to the
      // delegation's own per-invocation reservation slice; the child's
      // Definition policy stays the engine-side ceiling (RUN-021).
      const allowanceOption = delegation.toolCallAllowance;
      // Fail closed on non-finite values (SUB-034): a model-derived NaN falls
      // back to the author default, and a non-finite default falls back to
      // the delegation's own reservation slice — a CONFIGURED allowance never
      // silently widens to the child Definition policy.
      const extracted = allowanceOption?.fromParameters?.(parameters);
      const requestedAllowance =
        allowanceOption === undefined
          ? undefined
          : extracted !== undefined && Number.isFinite(extracted)
            ? extracted
            : Number.isFinite(allowanceOption.default)
              ? allowanceOption.default
              : delegation.policy.maxToolCalls;
      const toolCallAllowance =
        requestedAllowance === undefined
          ? undefined
          : Math.min(Math.max(1, Math.floor(requestedAllowance)), delegation.policy.maxToolCalls);
      const childOptions: SpawnRunOptions<never, HookRequirements> = {
        ...seededChild,
        budget,
        ...(toolCallAllowance === undefined ? {} : { toolCallAllowance }),
      };

      yield* Ref.set(startedAt, yield* Clock.currentTimeMillis);
      const child = yield* spawner.spawn<
        TargetInput,
        TargetOutput,
        TargetInstructions,
        TargetTools,
        Provider,
        ModelProvides,
        ModelRequires,
        never,
        HookRequirements,
        InstructionError,
        InstructionRequirements
      >(
        childBinding,
        encodedInput,
        { delegationId: delegation.delegationId, parentToolCallId: toolCallId },
        childOptions,
      );

      const payload: SubagentEventBasePayload = {
        toolCallId,
        delegationId: delegation.delegationId,
        childConversationId: child.conversationId,
        childRunId: child.runId,
        targetAgentId: delegation.target.id,
        depth: child.parentLink.depth,
      };
      // The sink cannot be closed while this Tool batch is live; a closed
      // sink here is an engine defect.
      const emit = (event: SubagentEventPayload): Effect.Effect<void> =>
        sink.emit(event).pipe(Effect.orDie);

      yield* emit({ _tag: "SubagentRequested", ...payload });
      yield* emit({ _tag: "SubagentStarted", ...payload });

      const joined = Effect.gen(function* () {
        const result = yield* child.await.pipe(
          Effect.catch((childFailure) =>
            emit({
              _tag: "SubagentFailed",
              ...payload,
              errorTag: errorTagOf(childFailure),
              message: boundedEventText(errorMessageOf(childFailure)),
            }).pipe(Effect.andThen(Effect.fail(options.mapChildFailure(childFailure)))),
          ),
          Effect.timeoutOrElse({
            duration: Duration.millis(allocation.durationMillis),
            orElse: () =>
              emit({
                _tag: "SubagentFailed",
                ...payload,
                errorTag: "SubagentBudgetExhausted",
                message: `Attached child exceeded its ${allocation.durationMillis}ms delegation duration budget`,
              }).pipe(
                Effect.andThen(
                  Effect.fail(
                    SubagentBudgetExhausted.make({
                      parentRunId,
                      dimension: "duration",
                      limitValue: allocation.durationMillis,
                      observedValue: allocation.durationMillis,
                    }),
                  ),
                ),
              ),
          }),
        );
        yield* emit({
          _tag: "SubagentCompleted",
          ...payload,
          turns: result.turns,
          finishReason: result.finishReason,
          // A child that settled through graceful budget exhaustion (RUN-025)
          // stays a success; the marker keeps the degradation observable to
          // the parent without leaking any child transcript.
          ...(result.exhausted !== undefined ? { exhausted: result.exhausted } : {}),
        });
        const projected = yield* delegation.projectResult(
          result.output,
          {
            budgetExhausted: result.finishReason === "budget-exhausted",
          },
          parameters,
        );
        const encodedResult = yield* encodeSuccess(projected).pipe(
          Effect.mapError(() =>
            SubagentProjectionFailure.make({
              delegationId: delegation.delegationId,
              stage: "result",
              message: "Projected child result did not satisfy the delegation success Schema",
            }),
          ),
        );
        const resultBytes = utf8ByteLength(JSON.stringify(encodedResult) ?? "");
        yield* reservations
          .observe(reservationId, SubagentObservedUsage.make({ resultBytes }))
          .pipe(Effect.orDie);
        if (
          delegation.policy.maxResultBytes !== undefined &&
          resultBytes > delegation.policy.maxResultBytes
        ) {
          yield* emit({
            _tag: "SubagentFailed",
            ...payload,
            errorTag: "SubagentBudgetExhausted",
            message: `Projected child result of ${resultBytes} bytes exceeds the ${delegation.policy.maxResultBytes}-byte delegation budget`,
          });
          return yield* SubagentBudgetExhausted.make({
            parentRunId,
            dimension: "result-bytes",
            limitValue: delegation.policy.maxResultBytes,
            observedValue: resultBytes,
          });
        }
        yield* emit({ _tag: "SubagentJoined", ...payload });
        return projected;
      });

      // Interruption stays interruption: record the
      // bounded lifecycle event best-effort, then let the closing handler
      // scope interrupt and join the child and settle the reservation.
      return yield* joined.pipe(
        Effect.onInterrupt(() =>
          sink
            .emit({
              _tag: "SubagentInterrupted",
              ...payload,
              reason: "Parent Run interrupted the attached child before it settled",
            })
            .pipe(Effect.ignore),
        ),
      );
    });

    /**
     * Durable branch: establishment through the
     * coordinator's idempotent protocol instead of an in-process spawn. No
     * child fiber ever starts here, and the S1 in-memory reservation service
     * is deliberately not consulted — its Scope-finalizer settlement
     * contradicts a handler that exits with the waiting signal while the
     * child keeps running; durable budget lives in the coordinator's fenced
     * ledger reservation built from `encodedAllocation`.
     */
    const invokeDurable = Effect.fn(`SubagentRuntime.${delegation.name}.durable`)(function* (
      parameters: Parameters["Type"],
      handlerContext: Toolkit.HandlerContext<
        SubagentTool<Name, Parameters, Success, Failure, Mode>
      >,
      durability: SubagentDurabilityDurable,
    ) {
      const spawner = yield* AgentSpawner;
      const sink = yield* RunEventSink;
      const toolCallId = yield* Schema.decodeUnknownEffect(ToolCallId)(
        handlerContext.toolCallId,
      ).pipe(Effect.orDie);
      const emit = (event: SubagentEventPayload): Effect.Effect<void> =>
        sink.emit(event).pipe(Effect.orDie);

      // A durable coordinator driving a Layer constructed without its durable
      // declaration can never establish: fail closed with the framework
      // failure. Never invent digests, load the latest declaration, or
      // degrade to an in-process spawn.
      if (durableDeclaration === undefined) {
        return yield* executionFailure(
          "declaration-unavailable",
          "SubagentDeclarationUnavailable",
          `Delegation ${delegation.name} runs under a durable coordinator but its handler Layer was constructed without SubagentRuntimeOptions.durable`,
        );
      }

      // Preflight re-runs on every Attempt, including batch resume (SUB-026
      // per-action reauthorization): a narrowed or revoked grant denies the
      // next action typed before any establishment replay.
      const depth = spawner.depth + 1;
      if (depth > delegation.grant.maxDepth) {
        return yield* prestartDenied(
          "nested-delegation",
          `Delegation ${delegation.name} was requested at depth ${spawner.depth}; S2 rejects every nested delegation`,
        );
      }
      for (const childToolName of childToolNames) {
        if (isDelegationToolName(childToolName)) {
          return yield* prestartDenied(
            "nested-delegation",
            `Child Toolkit exposes delegation Tool ${childToolName}; S2 rejects every nested delegation`,
          );
        }
        if (!delegation.grant.allowedToolNames.includes(childToolName)) {
          return yield* prestartDenied(
            "grant-violation",
            `Child Tool ${childToolName} is outside the delegation grant ceiling`,
          );
        }
      }

      const prepared = yield* delegation.prepareInput(parameters, {
        delegationId: delegation.delegationId,
        toolCallId,
        parent: spawner.parent,
      });
      const encodedInput = yield* encodeChildInput(prepared).pipe(
        Effect.mapError(() =>
          SubagentProjectionFailure.make({
            delegationId: delegation.delegationId,
            stage: "input",
            message: "Prepared child input did not satisfy the target Agent input Schema",
          }),
        ),
      );

      // Establishment is idempotent by construction (SUB-016): the identical
      // request replays spec §12 steps 2-9 under the parent fence and
      // converges on the one existing child; a divergent replay (changed
      // digests, grant, allocation, or input) is denied fail-closed by the
      // coordinator and surfaces below as `"establishment-denied"`.
      const status = yield* Effect.mapError(wrapEngineSignal)(
        durability.establish({
          toolCallId,
          delegationId: delegation.delegationId,
          targetAgentId: delegation.target.id,
          depth,
          targetDigests: durableDeclaration.targetDigests,
          encodedChildInput: encodedInput,
          encodedGrant,
          encodedAllocation,
        }),
      );

      switch (status._tag) {
        case "denied": {
          return yield* executionFailure("establishment-denied", status.errorTag, status.message);
        }
        case "waiting": {
          const payload: SubagentEventBasePayload = {
            toolCallId,
            delegationId: delegation.delegationId,
            childConversationId: status.childConversationId,
            childRunId: status.childRunId,
            targetAgentId: delegation.target.id,
            depth,
          };
          yield* emit({ _tag: "SubagentRequested", ...payload });
          yield* emit({ _tag: "SubagentStarted", ...payload });
          // The handler ends here and never returns: the engine keeps the
          // call open (no Tool failure, no batch failure policy), siblings
          // finish, and the Run suspends waitingForChild — never polling,
          // never respawning (SUB-018, SUB-030).
          return yield* Effect.mapError(wrapEngineSignal)(durability.waiting(toolCallId, status));
        }
        case "settled": {
          const payload: SubagentEventBasePayload = {
            toolCallId,
            delegationId: delegation.delegationId,
            childConversationId: status.childConversationId,
            childRunId: status.childRunId,
            targetAgentId: delegation.target.id,
            depth,
          };
          // Every terminal projection of a settled child — success or typed
          // failure — goes through ONE atomic join (SUB-019): the coordinator
          // appends `SubagentJoined` + the parent `ToolCallSettled` in one
          // canonical batch and applies the accounting decision. The handler
          // then fails/returns the same value so the live batch continues
          // with exactly what canonical history recorded.
          const settleFailure = <F>(
            failure: F,
            encodedFailure: unknown,
          ): Effect.Effect<never, F | GenuineEngineSignal> =>
            emit({
              _tag: "SubagentFailed",
              ...payload,
              errorTag: errorTagOf(failure),
              message: boundedEventText(errorMessageOf(failure)),
            }).pipe(
              Effect.andThen(
                Effect.mapError(wrapEngineSignal)(
                  durability.join({
                    toolCallId,
                    encodedResult: encodedFailure,
                    // Canonical history must record exactly what the live
                    // batch continues with (SUB-019): under containment the
                    // failure is the call's model-visible RESULT (the dispatch
                    // wrapper converts the fail below into a success), so the
                    // joined settlement records it as a non-failure result;
                    // the child's own failed Settlement and the
                    // `SubagentFailed` event stay the honest failure record.
                    isFailure: !contained,
                    encodedAccounting: conservativeAccounting,
                  }),
                ),
              ),
              Effect.andThen(Effect.fail(failure)),
            );

          if (status.outcome !== "completed") {
            const projection = childFailureProjectionOf(status.encodedResult);
            const failure = executionFailure(
              status.outcome === "aborted"
                ? "child-aborted"
                : projection.errorTag === "ChildCompatibilityFailure"
                  ? "child-compatibility"
                  : "child-failed",
              projection.errorTag,
              projection.message,
              status,
            );
            const encodedFailure = yield* encodeExecutionFailure(failure).pipe(Effect.orDie);
            return yield* settleFailure(failure, encodedFailure);
          }

          // The coordinator already verified lineage, target, digests, and
          // settlement identity fail-closed (spec §12 join steps 1-2); the
          // handler still refuses settled output that escapes the target
          // output Schema — hostile child output cannot cross the
          // declassification boundary undecoded, and the fixed message never
          // carries the raw value.
          const decoded = yield* decodeChildOutput(status.encodedResult).pipe(
            Effect.catch(() => {
              const failure = SubagentProjectionFailure.make({
                delegationId: delegation.delegationId,
                stage: "result",
                message: "Settled child output did not satisfy the target Agent output Schema",
              });
              return encodeProjectionFailure(failure).pipe(
                Effect.orDie,
                Effect.flatMap((encodedFailure) => settleFailure(failure, encodedFailure)),
              );
            }),
          );
          const projected = yield* delegation
            .projectResult(
              decoded,
              {
                budgetExhausted: status.finishReason === "budget-exhausted",
              },
              parameters,
            )
            .pipe(
              Effect.catch((declared) =>
                encodeDeclaredFailure(declared).pipe(
                  Effect.orDie,
                  Effect.flatMap((encodedFailure) => settleFailure(declared, encodedFailure)),
                ),
              ),
            );
          const encodedResult = yield* encodeSuccess(projected).pipe(
            Effect.catch(() => {
              const failure = SubagentProjectionFailure.make({
                delegationId: delegation.delegationId,
                stage: "result",
                message: "Projected child result did not satisfy the delegation success Schema",
              });
              return encodeProjectionFailure(failure).pipe(
                Effect.orDie,
                Effect.flatMap((encodedFailure) => settleFailure(failure, encodedFailure)),
              );
            }),
          );
          const resultBytes = utf8ByteLength(JSON.stringify(encodedResult) ?? "");
          if (
            delegation.policy.maxResultBytes !== undefined &&
            resultBytes > delegation.policy.maxResultBytes
          ) {
            const failure = SubagentBudgetExhausted.make({
              parentRunId: spawner.parent.runId,
              dimension: "result-bytes",
              limitValue: delegation.policy.maxResultBytes,
              observedValue: resultBytes,
            });
            const encodedFailure = yield* encodeBudgetFailure(failure).pipe(Effect.orDie);
            return yield* settleFailure(failure, encodedFailure);
          }
          yield* Effect.mapError(wrapEngineSignal)(
            durability.join({
              toolCallId,
              encodedResult,
              isFailure: false,
              encodedAccounting: conservativeAccounting,
            }),
          );
          yield* emit({ _tag: "SubagentJoined", ...payload });
          return projected;
        }
      }
    });

    // Containment boundary (SUB-033): under `"return"`, every expected
    // delegation failure becomes the handler's success value; exactly the
    // GENUINE engine signals re-fail unwrapped. Provenance comes from the
    // module-private `GenuineEngineSignal` wrapper applied at the only
    // operations that can produce those signals — an author-declared failure
    // that happens to use the exported signal classes is contained as data,
    // never rethrown as a suspension signal.
    const containSignals = (
      failure: SubagentToolFailure<Failure>["Type"] | GenuineEngineSignal,
    ): Effect.Effect<
      SubagentContainedFailure<Failure>["Type"],
      ToolCallWaiting | SubagentDurabilityError
    > =>
      failure instanceof GenuineEngineSignal
        ? Effect.fail(failure.signal)
        : Effect.succeed(failure);

    // Error mode still unwraps genuine signals so the engine sees the raw
    // `ToolCallWaiting`/`SubagentDurabilityError` it owns.
    const unwrapSignals = (
      failure: SubagentToolFailure<Failure>["Type"] | GenuineEngineSignal,
    ): SubagentToolFailure<Failure>["Type"] =>
      failure instanceof GenuineEngineSignal ? failure.signal : failure;

    const handlerImpl = (
      parameters: Parameters["Type"],
      handlerContext: Toolkit.HandlerContext<
        SubagentTool<Name, Parameters, Success, Failure, Mode>
      >,
    ) =>
      Effect.gen(function* () {
        // Resolve the engine-provided per-call services before providing the
        // captured construction context and re-provide them innermost: if the
        // Layer was constructed inside another Run's Tool batch, the captured
        // context would otherwise shadow this Run's `AgentSpawner` (and its
        // delegation depth), this batch's live `RunEventSink`, and this
        // batch's live `SubagentDurability` mode.
        const spawner = yield* AgentSpawner;
        const sink = yield* RunEventSink;
        const durability = yield* SubagentDurability;
        // Service-mode dispatch (S2 plan §2): the engine states ephemeral
        // mode explicitly when no durable coordinator supplied the hook, so
        // absence keeps the S1 in-process spawn semantics honestly.
        if (durability.mode === "durable") {
          const durable = invokeDurable(parameters, handlerContext, durability).pipe(
            Effect.scoped,
            Effect.provideService(AgentSpawner, spawner),
            Effect.provideService(RunEventSink, sink),
            Effect.provideService(SubagentDurability, durability),
            Effect.provide(captured),
          );
          return yield* contained
            ? durable.pipe(Effect.catch(containSignals))
            : durable.pipe(Effect.mapError(unwrapSignals));
        }
        const ephemeral = invoke(parameters, handlerContext).pipe(
          Effect.scoped,
          Effect.provideService(AgentSpawner, spawner),
          Effect.provideService(RunEventSink, sink),
          Effect.provideService(SubagentDurability, durability),
          Effect.provide(captured),
        );
        return yield* contained
          ? ephemeral.pipe(Effect.catch(containSignals))
          : ephemeral.pipe(Effect.mapError(unwrapSignals));
      });

    // `handlerImpl`'s channels are the UNION of both modes because
    // `contained` is a runtime branch TypeScript cannot relate to the
    // conditional generic `Mode`; at every concrete `Mode` the branch taken
    // matches `SubagentHandler`'s channels exactly (proven by the mode type
    // tests), so this assertion bridges only that limitation and crosses no
    // schema boundary.
    const handler = handlerImpl as SubagentHandler<Name, Parameters, Success, Failure, Mode>;

    // TypeScript cannot relate a computed single-key object literal to the
    // generic mapped key `Name`; `handler` is fully checked against the
    // Tool's declared handler signature above, so this assertion bridges only
    // that compiler limitation and crosses no schema boundary.
    return { [delegation.name]: handler } as Toolkit.HandlersFrom<
      SubagentTools<Name, Parameters, Success, Failure, Mode>
    >;
  });

  return toolkit.toLayer(build);
};

/**
 * Runtime wiring for declared attached delegation:
 * `layer` pairs one immutable Delegation Definition with one explicit
 * child Agent Binding and produces the Toolkit handler Layer for the
 * delegation Tool.
 */
export const SubagentRuntime = { layer } as const;
