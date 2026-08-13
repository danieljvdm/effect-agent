import { Clock, Duration, Effect, Layer, Option, Ref, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import {
  AgentId,
  type Definition,
  DelegationId,
  IdGenerator,
  ToolCallId,
} from "@effect-agent/core";
import {
  type AgentRuntimeFailure,
  type AgentRuntimeRequirements,
  AgentSpawner,
  type AgentSpawnerParent,
  type RunBudgetHook,
  RunEventSink,
  type RuntimeBinding,
  type RunUsageDelta,
  type SpawnRunOptions,
  type SubagentEventBasePayload,
  type SubagentEventPayload,
} from "@effect-agent/engine";

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
 * Finite delegation bounds declared by one Delegation Definition
 * (spec/subagents.md §7, SUB-009). Structural limits are hard limits; token
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
 * S1 fail-closed authority ceiling skeleton (spec/subagents.md §6). The full
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
 * Delegation preflight denied before any child started
 * (spec/subagents.md §15). No reservation, identity, or event exists for the
 * denied invocation; retry requires a new authorized parent Tool Call.
 */
export class SubagentPrestartDenied extends Schema.TaggedErrorClass<SubagentPrestartDenied>()(
  "SubagentPrestartDenied",
  {
    delegationId: DelegationId,
    targetAgentId: AgentId,
    reason: Schema.Literals(["nested-delegation", "grant-violation", "budget-conflict"]),
    message: BoundedFailureText,
  },
) {}

/**
 * Input or result projection failed its Schema or bounds
 * (spec/subagents.md §15). Fail closed: the message is a fixed description and
 * never carries the raw child value.
 */
export class SubagentProjectionFailure extends Schema.TaggedErrorClass<SubagentProjectionFailure>()(
  "SubagentProjectionFailure",
  {
    delegationId: DelegationId,
    stage: Schema.Literals(["input", "result"]),
    message: BoundedFailureText,
  },
) {}

/** Model-visible naming convention that marks every delegation Tool recognizably. */
export const delegationToolPrefix = "delegate_";

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
 * Names of every Delegation Definition created by `Subagent.define` in this
 * process. This is detection metadata for nested-delegation preflight, not an
 * execution registry: Bindings still arrive only through explicit Layers.
 */
const delegationToolNames = new Map<string, DelegationId>();

/**
 * Fail-closed delegation-Tool detection used by nested-delegation preflight
 * (SUB-029): a Tool is treated as a delegation when it was registered by
 * `Subagent.define` or merely follows the delegation naming convention.
 */
export const isDelegationToolName = (toolName: string): boolean =>
  delegationToolNames.has(toolName) || toolName.startsWith(delegationToolPrefix);

/**
 * Bounded parent metadata visible to `prepareInput` (spec/subagents.md §4.1).
 * It never contains the parent transcript, prompt, or a root runtime Context.
 */
export interface SubagentPrepareContext {
  readonly delegationId: DelegationId;
  readonly toolCallId: ToolCallId;
  readonly parent: AgentSpawnerParent;
}

/**
 * The delegation Tool failure Schema: the author's declared failure plus the
 * framework's S1 preflight/budget/projection failure family
 * (spec/subagents.md §15). With the default `failureMode: "error"`, exactly
 * this union (plus Effect AI's own error) can enter the parent handler `E`.
 */
export type SubagentToolFailure<Failure extends Schema.Top> = Schema.Union<
  readonly [
    Failure,
    typeof SubagentPrestartDenied,
    typeof SubagentBudgetExhausted,
    typeof SubagentProjectionFailure,
  ]
>;

/**
 * The native Effect AI Tool created by `Subagent.define` (SUB-001, SUB-003).
 * Its per-call dependencies are exactly the engine-provided `AgentSpawner`
 * and `RunEventSink` plus `IdGenerator`; every child requirement is a
 * construction requirement of `SubagentRuntime.layer` instead.
 */
export type SubagentTool<
  Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
> = Tool.Tool<
  Name,
  {
    readonly parameters: Parameters;
    readonly success: Success;
    readonly failure: SubagentToolFailure<Failure>;
    readonly failureMode: "error";
  },
  AgentSpawner | RunEventSink | IdGenerator
>;

/** Singleton Tool record provided by one `SubagentRuntime.layer`. */
export type SubagentTools<
  Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
> = {
  readonly [Key in Name]: SubagentTool<Name, Parameters, Success, Failure>;
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
> {
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
   * metadata to the child Agent input (spec/subagents.md §4.1). It never
   * receives the parent transcript or a root Context; its expected failures
   * are already the declared Tool failure.
   */
  readonly prepareInput: (
    parameters: Parameters["Type"],
    context: SubagentPrepareContext,
  ) => Effect.Effect<TargetInput["Type"], Failure["Type"], PrepareRequirements>;
  /**
   * Bounded result projection from the Schema-decoded child output to the
   * declared Tool success value (spec/subagents.md §4.2). This is the explicit
   * declassification boundary for child output.
   */
  readonly projectResult: (
    output: TargetOutput["Type"],
  ) => Effect.Effect<Success["Type"], Failure["Type"], ProjectRequirements>;
  /** Finite delegation bounds reserved for every invocation (SUB-009). */
  readonly policy: SubagentPolicy;
  /**
   * Authority ceiling for the child (spec/subagents.md §6). Defaults to
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
 * authority ceiling (spec/subagents.md §3). It owns no acquired resources and
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
> extends SubagentDefineOptions<
  TargetInput,
  TargetOutput,
  TargetInstructions,
  TargetTools,
  Parameters,
  Success,
  Failure,
  PrepareRequirements,
  ProjectRequirements
> {
  readonly name: Name;
  /** Stable delegation identity; S1 derives it from the unique Tool name. */
  readonly delegationId: DelegationId;
  readonly grant: SubagentGrant;
  /** The real Effect AI Tool to include in the parent Toolkit (SUB-001). */
  readonly tool: SubagentTool<Name, Parameters, Success, Failure>;
}

/**
 * Declare one attached delegation as a pure value (spec/subagents.md §4).
 *
 * The returned `.tool` is a native Effect AI Tool whose handler dependencies
 * are exactly the engine-owned `AgentSpawner` and `RunEventSink` plus
 * `IdGenerator` (SUB-003); the concrete child Binding arrives only through
 * `SubagentRuntime.layer`. Throws on an invalid delegation name or a target
 * whose Toolkit already contains a delegation Tool (SUB-029).
 */
const define = <
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
    ProjectRequirements
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
  ProjectRequirements
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
  const tool = Tool.make(name, {
    description: options.description,
    parameters: options.parameters,
    success: options.success,
    failure: Schema.Union([
      options.failure,
      SubagentPrestartDenied,
      SubagentBudgetExhausted,
      SubagentProjectionFailure,
    ]),
    needsApproval: options.needsApproval,
  })
    .addDependency(AgentSpawner)
    .addDependency(RunEventSink)
    .addDependency(IdGenerator);
  delegationToolNames.set(name, delegationId);
  return Object.freeze({
    ...options,
    name,
    delegationId,
    grant,
    tool,
  });
};

/** Pure Subagent authoring surface (spec/subagents.md §4, S1). */
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
 * Every expected child Run failure the delegation's total mapping must cover
 * (spec/subagents.md §4.2, SUB-028): the child's inferred Agent failures plus
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

/** Options accepted by `SubagentRuntime.layer`. */
export interface SubagentRuntimeOptions<
  Failure extends Schema.Top,
  ChildFailure,
  HookRequirements = never,
> {
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

type SubagentHandler<
  Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
> = (
  parameters: Parameters["Type"],
  context: Toolkit.HandlerContext<SubagentTool<Name, Parameters, Success, Failure>>,
) => Effect.Effect<
  Success["Type"],
  SubagentToolFailure<Failure>["Type"],
  Tool.HandlerServices<SubagentTool<Name, Parameters, Success, Failure>>
>;

/**
 * Build the Toolkit handler Layer for one delegation Tool from an explicit
 * child Agent Binding (spec/subagents.md §4, §8, §9, §15).
 *
 * Construction requirements carry the child Binding's full runtime needs and
 * both projections; they are captured once via `Effect.context` so the
 * per-call handler requirements stay exactly the Tool's declared engine
 * dependencies. The handler preflights depth, nested delegation, grant, and
 * budget; runs the child in a handler-owned scoped region (SUB-011/012);
 * emits the stable Subagent lifecycle events; total-maps expected child
 * failures; and settles the budget reservation through Scope finalizers on
 * every exit path.
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
    ProjectRequirements
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
  Tool.HandlersFor<SubagentTools<Name, Parameters, Success, Failure>>,
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
  const toolkit = Toolkit.make(delegation.tool) as unknown as Toolkit.Toolkit<
    SubagentTools<Name, Parameters, Success, Failure>
  >;
  const encodeChildInput = Schema.encodeEffect(delegation.target.input);
  const encodeSuccess = Schema.encodeEffect(delegation.success);
  const childToolNames = Object.keys(delegation.target.toolkit.tools);

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

    const invoke = Effect.fn(`SubagentRuntime.${delegation.name}`)(function* (
      parameters: Parameters["Type"],
      handlerContext: Toolkit.HandlerContext<SubagentTool<Name, Parameters, Success, Failure>>,
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

      // Preflight (spec/subagents.md §8 step 2, SUB-029): depth ceiling,
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
      const childOptions: SpawnRunOptions<never, HookRequirements> = {
        ...seededChild,
        budget,
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
        });
        const projected = yield* delegation.projectResult(result.output);
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

      // Interruption stays interruption (spec/subagents.md §15): record the
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

    const handler: SubagentHandler<Name, Parameters, Success, Failure> = (
      parameters,
      handlerContext,
    ) =>
      Effect.gen(function* () {
        // Resolve the engine-provided per-call services before providing the
        // captured construction context and re-provide them innermost: if the
        // Layer was constructed inside another Run's Tool batch, the captured
        // context would otherwise shadow this Run's `AgentSpawner` (and its
        // delegation depth) and this batch's live `RunEventSink`.
        const spawner = yield* AgentSpawner;
        const sink = yield* RunEventSink;
        return yield* invoke(parameters, handlerContext).pipe(
          Effect.scoped,
          Effect.provideService(AgentSpawner, spawner),
          Effect.provideService(RunEventSink, sink),
          Effect.provide(captured),
        );
      });

    // TypeScript cannot relate a computed single-key object literal to the
    // generic mapped key `Name`; `handler` is fully checked against the
    // Tool's declared handler signature above, so this assertion bridges only
    // that compiler limitation and crosses no schema boundary.
    return { [delegation.name]: handler } as Toolkit.HandlersFrom<
      SubagentTools<Name, Parameters, Success, Failure>
    >;
  });

  return toolkit.toLayer(build);
};

/**
 * Runtime wiring for declared attached delegation (spec/subagents.md §17,
 * S1): `layer` pairs one immutable Delegation Definition with one explicit
 * child Agent Binding and produces the Toolkit handler Layer for the
 * delegation Tool.
 */
export const SubagentRuntime = { layer } as const;
