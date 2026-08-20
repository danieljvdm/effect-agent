import {
  type Agent,
  AgentApprovalDenied,
  AgentApprovalPending,
  AgentInputError,
  AgentOutputError,
  AgentRunDispositionError,
  AgentToolAuthorizationDenied,
  type AgentPolicy,
  AgentPolicyError,
  type AgentId,
  ApprovalRequested,
  applyToolResultBounds,
  BudgetWarning,
  unserializableToolResult,
  CompactionPerformed,
  ContextOverflowError,
  ConversationId,
  type Definition,
  DelegationDepth,
  type DelegationId,
  IdGenerator,
  type InstructionSource,
  isDelegationToolName,
  type RunDispositionDeclaration,
  ModelStarted,
  ModelProtocolError,
  ReasoningDelta,
  ReceiptId,
  RunCompleted,
  RunFailed,
  RunStarted,
  RunSuspended,
  SubmissionId,
  SubagentCompleted,
  SubagentFailed,
  SubagentInterrupted,
  SubagentJoined,
  SubagentParentLink,
  SubagentProgress,
  SubagentRequested,
  SubagentStarted,
  TextDelta,
  ToolCallDeclared,
  ToolCallFailed,
  type RunEvent,
  RunId,
  ToolCallStarted,
  ToolCallSucceeded,
  ToolProgress,
  ToolCallId,
  type ToolResultBounds,
  TurnCompleted,
  TurnStarted,
  type TurnId,
} from "@effect-agent/core";
import type { Take } from "effect";
import {
  Cause,
  Clock,
  Context,
  DateTime,
  Duration,
  Effect,
  ErrorReporter,
  Exit,
  Fiber,
  Metric,
  Option,
  PubSub,
  Queue,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import {
  type Tool,
  AiError,
  LanguageModel,
  type Model,
  Prompt,
  Response,
  type Toolkit,
} from "effect/unstable/ai";

import { boundedValueFootprint, utf8ByteLength } from "./bounded-value-internal.ts";
import { insertOutputContract, outputSchemaContract } from "./output-contract-internal.ts";
import {
  boundedCanonicalJsonSnapshot,
  boundedJsonSnapshot,
  type BoundedJsonSnapshot,
} from "./provider-result-staging-internal.ts";
import {
  annotateToolSpanTerminalOutcome,
  emitThenAfter,
  isolateToolTerminalTelemetry,
  restoreToolSpanFailureCause,
  stripToolSpanFailures,
  ToolSpanFailure,
  ToolSpanTelemetry,
  type ToolSpanTelemetryService,
} from "./tool-telemetry-internal.ts";

/**
 * Structural shape of an Agent Binding accepted by the interpreter and by
 * `AgentSpawner.spawn`: a model-agnostic Definition paired with an explicit
 * Effect AI Model whose requirements stay visible.
 */
export type RuntimeBinding<
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
> = {
  readonly definition: Definition<
    InputSchema,
    OutputSchema,
    Instructions,
    Toolkit.Toolkit<Tools>,
    RunDispositionValue
  > & {
    readonly instructions: InstructionSource<
      InputSchema["Type"],
      NoInfer<InstructionError>,
      NoInfer<InstructionRequirements>
    >;
  };
  readonly model: Model.Model<Provider, LanguageModel.LanguageModel | ModelProvides, ModelRequires>;
};

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

import {
  buildCompactedView,
  chooseSummarizeCut,
  choosePruneBound,
  collectCoveredMessages,
  COMPACTION_INSTRUCTION,
  estimatePromptTokens,
  initialCompactionState,
  isContextOverflowMessage,
  renderForSummary,
  type ContextCompactionState,
} from "./compaction.ts";
import {
  DurableStep,
  DurableStepError,
  getToolExecutionClass,
  type DurableStepService,
  type RunStepHook,
  type RunStepKey,
} from "./durable-step.ts";
import {
  type ChildEstablishStatus,
  type CommandDrainPolicy,
  type RunApprovalDecision,
  type RunBufferLimits,
  RunResumeUsageSchema,
  type RunOptions,
  type RunSchedulingHook,
  type RunSubagentChildIdentity,
  type RunSubagentEstablishRequest,
  type RunSubagentHook,
  type RunSubagentJoinRequest,
  type RunCompactionCommit,
  type RunToolCallDescriptor,
  RunTurnResumeSettledCallSchema,
  type RunTurnResume,
  type RunUsageDelta,
} from "./run-options.ts";

export * from "./durable-step.ts";
export * from "./run-events.ts";
export * from "./run-options.ts";
export * from "./tool-broker.ts";
export {
  CLEARED_TOOL_RESULT,
  COMPACTION_INSTRUCTION,
  COMPACTION_SUMMARY_PREFIX,
  estimateMessageTokens,
  estimatePromptTokens,
  isContextOverflowMessage,
} from "./compaction.ts";

import {
  RunEventSink,
  RunEventSinkClosedError,
  type RunEventSinkService,
  type SubagentEventPayload,
} from "./run-events.ts";
import {
  ToolBroker,
  ToolBrokerConfigurationError,
  ToolBrokerUnavailableError,
  type ProgrammaticCallOutcome,
  type ProgrammaticToolInput,
  type ToolBrokerPass,
  type ToolBrokerService,
} from "./tool-broker.ts";

/** Schema factory for the terminal value produced by reducing a completed agent event stream. */
export const AgentResultSchema = <Output extends Schema.Top>(output: Output) =>
  Schema.Struct({
    output,
    conversationId: ConversationId,
    runId: RunId,
    turns: Schema.Int.check(Schema.isGreaterThan(0)),
    finishReason: Schema.Literals(["completed", "model-stop", "budget-exhausted"]),
    /** Dimension that bound when the Run settled budget-exhausted (RUN-025; the grant-flow marker). */
    exhausted: Schema.optionalKey(Schema.Literals(["tokens", "tool-calls", "turns"])),
    /** Schema-encoded application disposition declared for an ordinary completed Run. */
    runDisposition: Schema.optionalKey(Schema.Json),
  }).check(
    Schema.makeFilter(
      (result) =>
        !("runDisposition" in result) ||
        result.runDisposition === undefined ||
        !("finishReason" in result) ||
        result.finishReason !== "budget-exhausted",
      {
        expected: "runDisposition only when finishReason is not budget-exhausted",
      },
    ),
  );

/** Decoded terminal value produced by reducing a completed agent event stream. */
export type AgentResult<Output> = ReturnType<
  typeof AgentResultSchema<Schema.Schema<Output>>
>["Type"];

/** Expected agent, policy, and model-protocol failures exposed by the runtime. */
export type AgentRuntimeFailure<
  AgentValue extends Agent.Any,
  HookError = never,
  InstructionError = never,
> =
  | Agent.Failure<AgentValue>
  | AgentPolicyError
  | ContextOverflowError
  | ModelProtocolError
  | AgentApprovalDenied
  | AgentToolAuthorizationDenied
  | AgentApprovalPending
  | AgentChildPending
  | HookError
  | InstructionError;

/**
 * Services the engine itself provides locally to every Run: `AgentSpawner`
 * bound to the Run's immutable identity and delegation depth, `RunEventSink`
 * and `SubagentDurability` bound to the active Tool batch, `DurableStep`
 * bound to the active Tool Call, and `ToolSpanTelemetry` bound at the Run
 * composition edge. They are excluded from the runtime's public
 * requirements and MUST NOT be satisfied from an application Layer.
 */
export type EngineProvidedToolServices =
  | AgentSpawner
  | RunEventSink
  | DurableStep
  | SubagentDurability
  | ToolBroker
  | ToolSpanTelemetry;

/**
 * Inferred agent services plus the runtime's identity-generation authority.
 * Engine-provided Tool handler services are excluded because the interpreter
 * supplies them itself, bound to the current Run's identity. Output decoding
 * services stay listed unexcluded: `run`/`start` re-decode the terminal
 * output outside the Run stream's provision boundary.
 */
export type AgentRuntimeRequirements<
  AgentValue extends Agent.Any,
  HookRequirements = never,
  InstructionRequirements = never,
> =
  | Exclude<Agent.Requirements<AgentValue>, EngineProvidedToolServices>
  | AgentValue["definition"]["output"]["DecodingServices"]
  | IdGenerator
  | HookRequirements
  | InstructionRequirements;

/**
 * Interpreter-internal requirements before the Run boundary in `stream`
 * provides the engine-owned Tool services and the bound Model.
 */
type InterpreterRequirements<
  AgentValue extends Agent.Any,
  HookRequirements = never,
  InstructionRequirements = never,
> = Agent.Requirements<AgentValue> | IdGenerator | HookRequirements | InstructionRequirements;

interface RunContext {
  readonly agentId: Agent.AnyDefinition["id"];
  readonly conversationId: ConversationId;
  readonly runId: RunId;
  /** Agent-Schema encoded input identifying this logical Run's originating authority/wake. */
  input: unknown;
  readonly pendingFollowUps: Array<Prompt.RawInput>;
  /** Wall-clock Run start, the base of the run-status elapsed rendering (RUN-024). */
  readonly startedAtMillis: number;
  /** Absolute `maxDuration` rail shared by every durable Attempt (RUN-030). */
  readonly durationDeadlineMillis: number;
  history: Prompt.Prompt;
  modelCalls: number;
  consecutiveToolFailures: number;
  inputTokens: number;
  outputTokens: number;
  /** The most recent model call's provider-reported tokens — the live-context estimate (RUN-023). */
  lastInputTokens: number;
  lastOutputTokens: number;
  costMicrousd: number;
  /** The most recent model call's estimated spend, staged for the Turn's canonical record. */
  lastCostMicrousd: number;
  /** Budget dimensions whose one-shot `BudgetWarning` already fired (RUN-025). */
  readonly warnedLimits: Set<"tokens" | "tool-calls" | "turns">;
  /** Held during the compaction summarizer's accounting so it cannot breach or recurse (RUN-026). */
  finalizing: boolean;
  /** One-shot token-breach flag: joins the final-answer derivation, never re-breaches (RUN-025). */
  tokenExhausted: boolean;
  /** First dimension that bound — the exhausted marker on budget-exhausted settlement (RUN-025). */
  exhaustedDimension: "tokens" | "tool-calls" | "turns" | undefined;
  /** Model-visible view state for engine-native compaction (RUN-026). */
  readonly compaction: ContextCompactionState;
  /** Finite engine-owned memory ceilings, optionally tightened per Run. */
  readonly bufferLimits: EffectiveRunBufferLimits;
  sequence: number;
  /**
   * Run-wide count of programmatic (broker) Tool invocations whose handler
   * started (RUN-017). The broker consumes it mid-pass; the Turn-seam
   * `maxToolCalls` checks add it to the declared-call count.
   */
  programmaticToolCalls: number;
}

const runCounter = Metric.counter("effect_agent_runs_total", {
  description: "Agent runs started; no content or high-cardinality identifiers are recorded.",
});
const modelCounter = Metric.counter("effect_agent_model_calls_total", {
  description:
    "Agent model calls started; no content or high-cardinality identifiers are recorded.",
});
const toolCounter = Metric.counter("effect_agent_tool_calls_total", {
  description:
    "Agent tool handlers started; no content or high-cardinality identifiers are recorded.",
});

/**
 * Tool Call parameters live in two intentionally divergent forms on the
 * trace: `parts` carries the prompt/canonical form (parameters encoded back
 * through the owning Schema — the wire form official history and canonical
 * records persist), while `applicationToolCalls` keeps the handler form
 * (Schema-decoded parameters, because the pinned `Toolkit.handle` expects the
 * value `prepareToolCall` re-encodes for it).
 */
interface TurnTrace {
  /** Number of decoded provider parts retained or inspected during this model call. */
  responsePartCount: number;
  /** Conservative retained-memory estimate across decoded provider parts. */
  responsePartBytes: number;
  readonly parts: Array<Response.AnyPart>;
  readonly text: Array<string>;
  readonly textParts: Map<string, PartLifecycle>;
  readonly reasoningParts: Map<string, PartLifecycle>;
  readonly toolParameterParts: Map<
    string,
    {
      readonly name: string;
      readonly providerExecuted: boolean;
      state: PartLifecycle;
    }
  >;
  readonly toolCalls: Map<
    string,
    {
      readonly name: string;
      readonly providerExecuted: boolean;
    }
  >;
  readonly finalToolResultIds: Set<string>;
  /** Provider result payloads held until the complete model response validates. */
  readonly providerResultPayloads: Array<ProviderResultEventPayload>;
  /** Exact retained JSON bytes across provider Tool results and staged provider events. */
  providerStagedPayloadBytes: number;
  /** Turn completion held with provider results so malformed trailing parts append neither. */
  turnCompletion: { readonly finishReason: Response.FinishReason } | undefined;
  readonly applicationToolCalls: Array<Response.ToolCallPart<string, unknown>>;
  /** Durable-hook view of the application calls, in declaration order (encoded parameters). */
  readonly applicationCallDescriptors: Array<RunToolCallDescriptor>;
  readonly applicationToolResults: Array<{
    readonly id: string;
    readonly name: string;
    readonly encodedResult: unknown;
    readonly isFailure: boolean;
    /** Settled synthetically by budget rejection: no handler ran, exempt from repeated-failure folding. */
    readonly budgetRejected?: boolean;
  }>;
  finished: boolean;
  finishReason: Response.FinishReason | undefined;
  usage: Response.Usage | undefined;
}

type ProviderResultEventPayload =
  | {
      readonly _tag: "ToolProgress";
      readonly toolCallId: ToolCallId;
      readonly toolName: string;
      readonly result: Schema.Json;
      readonly providerExecuted: true;
    }
  | {
      readonly _tag: "ToolCallSucceeded";
      readonly toolCallId: ToolCallId;
      readonly toolName: string;
      readonly result: Schema.Json;
      readonly providerExecuted: true;
    }
  | {
      readonly _tag: "ToolCallFailed";
      readonly toolCallId: ToolCallId;
      readonly toolName: string;
      readonly errorTag: string;
      readonly message: string;
      readonly providerExecuted: true;
    };

// Provider output is untrusted. Holding terminal facts until whole-response validation is
// fail-closed only while the additional staging allocation itself is deterministic and bounded.
const MAX_STAGED_PROVIDER_EVENTS = 256;
const MAX_STAGED_PROVIDER_BYTES = 1024 * 1024;

const DEFAULT_RUN_BUFFER_LIMITS = {
  maxModelResponseParts: 16_384,
  maxModelResponseBytes: 8 * 1024 * 1024,
  maxRunEvents: 65_536,
  maxSubagentEventsPerBatch: 1_024,
} as const;

interface EffectiveRunBufferLimits {
  readonly maxModelResponseParts: number;
  readonly maxModelResponseBytes: number;
  readonly maxRunEvents: number;
  readonly maxSubagentEventsPerBatch: number;
}

const tighteningBufferLimit = (
  configured: number | undefined,
  ceiling: number,
  minimum: number,
): number =>
  configured === undefined || !Number.isFinite(configured)
    ? ceiling
    : Math.min(ceiling, Math.max(minimum, Math.floor(configured)));

const effectiveRunBufferLimits = (
  configured: RunBufferLimits | undefined,
): EffectiveRunBufferLimits => ({
  maxModelResponseParts: tighteningBufferLimit(
    configured?.maxModelResponseParts,
    DEFAULT_RUN_BUFFER_LIMITS.maxModelResponseParts,
    1,
  ),
  maxModelResponseBytes: tighteningBufferLimit(
    configured?.maxModelResponseBytes,
    DEFAULT_RUN_BUFFER_LIMITS.maxModelResponseBytes,
    1,
  ),
  // A Run always needs one ordinary event and one reserved typed terminal event.
  maxRunEvents: tighteningBufferLimit(
    configured?.maxRunEvents,
    DEFAULT_RUN_BUFFER_LIMITS.maxRunEvents,
    2,
  ),
  maxSubagentEventsPerBatch: tighteningBufferLimit(
    configured?.maxSubagentEventsPerBatch,
    DEFAULT_RUN_BUFFER_LIMITS.maxSubagentEventsPerBatch,
    1,
  ),
});

interface ModelResponseBufferUsage {
  responsePartCount: number;
  responsePartBytes: number;
}

const consumeModelResponsePart = (
  usage: ModelResponseBufferUsage,
  part: Response.AnyPart,
  limits: EffectiveRunBufferLimits,
): Effect.Effect<void, ModelProtocolError> =>
  Effect.suspend(() => {
    if (usage.responsePartCount >= limits.maxModelResponseParts) {
      return Effect.fail(
        ModelProtocolError.make({
          message: `Model response exceeded the ${limits.maxModelResponseParts}-part response limit`,
        }),
      );
    }
    const bytes = boundedValueFootprint(
      part,
      limits.maxModelResponseBytes - usage.responsePartBytes,
    );
    if (bytes === undefined) {
      return Effect.fail(
        ModelProtocolError.make({
          message: `Model response exceeded the ${limits.maxModelResponseBytes}-byte retained response limit`,
        }),
      );
    }
    usage.responsePartCount += 1;
    usage.responsePartBytes += bytes;
    return Effect.void;
  });

type PartLifecycle = "open" | "closed";

type ToolUnion<Tools extends Record<string, Tool.Any>> = Tools[keyof Tools];

interface PreparedToolCall<Tools extends Record<string, Tool.Any>> {
  readonly call: Response.ToolCallPart<string, unknown>;
  readonly name: keyof Tools & string;
  readonly toolCallId: ToolCallId;
  readonly decodedParams: Tool.Parameters<ToolUnion<Tools>>;
  readonly nativeHandlerParams: Tool.Parameters<ToolUnion<Tools>>;
  readonly tool: ToolUnion<Tools>;
  readonly declarationIndex: number;
}

const withSemaphorePermit = <A, E, R>(
  semaphore: Semaphore.Semaphore,
  stream: Stream.Stream<A, E, R>,
): Stream.Stream<A, E, R> =>
  Stream.scoped(
    Stream.fromEffect(
      Effect.acquireRelease(semaphore.take(1), (permits) =>
        semaphore.release(permits).pipe(Effect.asVoid),
      ),
    ).pipe(Stream.flatMap(() => stream)),
  );

const hasTool = <Tools extends Record<string, Tool.Any>>(
  tools: Tools,
  name: string,
): name is keyof Tools & string => Object.hasOwn(tools, name);

const startPart = (
  parts: Map<string, PartLifecycle>,
  id: string,
  description: string,
): Effect.Effect<void, ModelProtocolError> => {
  if (parts.has(id)) {
    return Effect.fail(
      ModelProtocolError.make({
        message: `Model response repeated ${description} start for ${id}`,
      }),
    );
  }
  parts.set(id, "open");
  return Effect.void;
};

const continuePart = (
  parts: Map<string, PartLifecycle>,
  id: string,
  description: string,
): Effect.Effect<void, ModelProtocolError> =>
  parts.get(id) === "open"
    ? Effect.void
    : Effect.fail(
        ModelProtocolError.make({
          message: `Model response emitted ${description} for inactive part ${id}`,
        }),
      );

const endPart = (
  parts: Map<string, PartLifecycle>,
  id: string,
  description: string,
): Effect.Effect<void, ModelProtocolError> =>
  continuePart(parts, id, `${description} end`).pipe(
    Effect.tap(() => Effect.sync(() => parts.set(id, "closed"))),
  );

const firstOpenPart = (trace: TurnTrace): string | undefined => {
  for (const [id, state] of trace.textParts) {
    if (state === "open") {
      return `text part ${id}`;
    }
  }
  for (const [id, state] of trace.reasoningParts) {
    if (state === "open") {
      return `reasoning part ${id}`;
    }
  }
  for (const [id, part] of trace.toolParameterParts) {
    if (part.state === "open") {
      return `Tool parameter part ${id}`;
    }
  }
  return undefined;
};

/**
 * `LanguageModel.streamText` has already decoded a complete Tool Call against
 * the owning parameter Schema, so transformed values must be encoded back to
 * the native runtime boundary before they can cross it again. These private
 * function-type assertions restore correlation lost by dynamic record lookup
 * only around a successful Schema encode; they never bypass validation.
 */
const encodeToolCallParameters = <Tools extends Record<string, Tool.Any>>(
  tool: ToolUnion<Tools>,
  toolName: string,
  decodedParams: Tool.Parameters<ToolUnion<Tools>>,
): Effect.Effect<unknown, ModelProtocolError, Tool.HandlerServices<ToolUnion<Tools>>> => {
  const encodeParameters = Schema.encodeUnknownEffect(tool.parametersSchema) as (
    input: Tool.Parameters<ToolUnion<Tools>>,
  ) => Effect.Effect<unknown, Schema.SchemaError, Tool.HandlerServices<ToolUnion<Tools>>>;
  return encodeParameters(decodedParams).pipe(
    Effect.mapError((cause) =>
      ModelProtocolError.make({
        message: `Invalid parameters for Tool ${toolName}: ${cause.message}`,
      }),
    ),
  );
};

/**
 * The pinned `Toolkit.handle` implementation decodes once more internally,
 * despite its decoded-parameter signature, so handler parameters are the
 * encoded form produced by `encodeToolCallParameters`.
 */
const prepareToolCall = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<Tools>,
  call: Response.ToolCallPart<string, unknown>,
  declarationIndex: number,
): Effect.Effect<
  PreparedToolCall<Tools>,
  ModelProtocolError,
  Tool.HandlerServices<ToolUnion<Tools>>
> => {
  const name = call.name;
  if (!hasTool(toolkit.tools, name)) {
    return Effect.fail(
      ModelProtocolError.make({ message: `Model requested unknown Tool ${call.name}` }),
    );
  }
  const tool = toolkit.tools[name] as ToolUnion<Tools>;
  const decodedParams = call.params as Tool.Parameters<ToolUnion<Tools>>;
  return decodeToolCallId(call.id).pipe(
    Effect.flatMap((toolCallId) =>
      encodeToolCallParameters<Tools>(tool, call.name, decodedParams).pipe(
        Effect.map((encodedParams) => ({
          call,
          name,
          toolCallId,
          decodedParams,
          nativeHandlerParams: encodedParams as Tool.Parameters<ToolUnion<Tools>>,
          tool,
          declarationIndex,
        })),
      ),
    ),
  );
};

/**
 * Re-validate one canonically recorded Tool Call's encoded parameters through
 * the owning parameter Schema before a resumed batch may execute anything.
 * Declarations were validated when they entered the log (RUN-004), but a
 * resumed Attempt re-validates on read (STORE-006); a decode failure is a
 * strict no-start boundary. The same private assertion contract as
 * `encodeToolCallParameters` applies: correlation lost by dynamic record
 * lookup is restored only around a successful Schema decode.
 */
/**
 * Effective Run bounds (RUN-021): per-Run allowances tighten the Agent
 * Policy's Turn and Tool Call ceilings but can never widen them — the
 * effective limit is `min(policy bound, max(1, floor(allowance)))`. The
 * `onExhaustion` resolution (RUN-018/RUN-019) keys off these effective
 * limits, which is how an orchestrator grants a delegated child a budget
 * extension by re-invoking with a larger allowance below the Definition's
 * ceiling.
 */
const boundedAllowance = (policyBound: number, allowance: number | undefined): number =>
  // Fail closed on non-finite allowances (RUN-021): `NaN` propagates through
  // floor/max/min and every later `>` comparison answers false, which would
  // silently erase the bound — an invalid allowance keeps the policy bound.
  allowance === undefined || !Number.isFinite(allowance)
    ? policyBound
    : Math.min(policyBound, Math.max(1, Math.floor(allowance)));

const effectiveRunBounds = (
  policy: AgentPolicy,
  options: {
    readonly toolCallAllowance?: number | undefined;
    readonly turnAllowance?: number | undefined;
  },
): { readonly maxTurns: number; readonly maxToolCalls: number } => ({
  maxTurns: boundedAllowance(policy.maxTurns, options.turnAllowance),
  maxToolCalls: boundedAllowance(policy.maxToolCalls, options.toolCallAllowance),
});

const decodeResumedToolCallParameters = <Tools extends Record<string, Tool.Any>>(
  tool: ToolUnion<Tools>,
  toolName: string,
  encodedParams: unknown,
): Effect.Effect<
  Tool.Parameters<ToolUnion<Tools>>,
  ModelProtocolError,
  Tool.HandlerServices<ToolUnion<Tools>>
> => {
  const decodeParameters = Schema.decodeUnknownEffect(tool.parametersSchema) as (
    input: unknown,
  ) => Effect.Effect<
    Tool.Parameters<ToolUnion<Tools>>,
    Schema.SchemaError,
    Tool.HandlerServices<ToolUnion<Tools>>
  >;
  return decodeParameters(encodedParams).pipe(
    Effect.mapError((cause) =>
      ModelProtocolError.make({
        message: `Recorded parameters for Tool ${toolName} failed validation on resume: ${cause.message}`,
      }),
    ),
  );
};

const decodeResumedSettledCall = Effect.fn("AgentRuntime.decodeResumedSettledCall")(
  (input: unknown, maxResultBytes: number) =>
    Effect.gen(function* () {
      const raw = yield* Effect.try({
        try: () => {
          if (input === null || typeof input !== "object") {
            throw new TypeError("settled Tool Call must be an object");
          }
          return {
            id: Reflect.get(input, "id"),
            result: Reflect.get(input, "result"),
            isFailure: Reflect.get(input, "isFailure"),
          };
        },
        catch: () =>
          ModelProtocolError.make({
            message: "Turn resume contains an invalid settled Tool Call",
          }),
      });
      const result = boundedCanonicalJsonSnapshot(raw.result, maxResultBytes);
      if (result === undefined) {
        return yield* ModelProtocolError.make({
          message: "Turn resume settled Tool result is not bounded canonical JSON",
        });
      }
      return yield* Schema.decodeUnknownEffect(RunTurnResumeSettledCallSchema)({
        ...raw,
        result: result.value,
      }).pipe(
        Effect.mapError(() =>
          ModelProtocolError.make({
            message: "Turn resume contains an invalid settled Tool Call",
          }),
        ),
      );
    }),
);

const decodeResumeUsage = Effect.fn("AgentRuntime.decodeResumeUsage")((input: unknown) =>
  Schema.decodeUnknownEffect(RunResumeUsageSchema)(input).pipe(
    Effect.mapError(() =>
      ModelProtocolError.make({
        message:
          "Run resume usage requires non-negative safe-integer totals and last-call tokens no greater than their cumulative totals",
      }),
    ),
  ),
);

const makeToolFailedEvent = Effect.fn("AgentRuntime.makeToolFailedEvent")(function* (
  context: RunContext,
  turnId: TurnId,
  call: Response.ToolCallPart<string, unknown>,
  error: unknown,
): Effect.fn.Return<RunEvent, ModelProtocolError> {
  const toolCallId = yield* decodeToolCallId(call.id);
  return ToolCallFailed.make({
    ...(yield* eventBase(context)),
    turnId,
    toolCallId,
    toolName: call.name,
    errorTag: errorTag(error),
    message: errorMessage(error),
    providerExecuted: false,
  });
});

/**
 * Settle an over-budget declared Tool batch without starting any handler
 * (RUN-018): every open application call is marked settled in the trace with
 * the encoded policy failure as its model-visible result, flagged
 * `budgetRejected` so it is exempt from repeated-failure folding, and one
 * `ToolCallFailed` is emitted per call with no `ToolCallStarted` (the
 * approval-denied precedent). The caller hands the trace to
 * `toolBatchContinuation`, so the synthetic results advance history through
 * the ordinary tool message and — under a durable coordinator that never saw
 * a response commit for this Turn — settle canonically via the single-batch
 * Turn commit.
 */
const settleRejectedBatch = Effect.fn("AgentRuntime.settleRejectedBatch")(function* (
  context: RunContext,
  turnId: TurnId,
  trace: TurnTrace,
  policyError: AgentPolicyError,
  alreadySettled?: ReadonlySet<string>,
): Effect.fn.Return<ReadonlyArray<RunEvent>, ModelProtocolError> {
  const encodedResult = {
    _tag: policyError._tag,
    limit: policyError.limit,
    message: policyError.message,
  };
  const events: Array<RunEvent> = [];
  for (const [index, call] of trace.applicationToolCalls.entries()) {
    if (alreadySettled?.has(call.id) === true) {
      continue;
    }
    trace.finalToolResultIds.add(call.id);
    trace.applicationToolResults[index] = {
      id: call.id,
      name: call.name,
      encodedResult,
      isFailure: true,
      budgetRejected: true,
    };
    events.push(yield* makeToolFailedEvent(context, turnId, call, policyError));
  }
  return events;
});

/**
 * Stamp one pre-base Subagent payload into a first-class Run event through
 * the same `eventBase` path as every other event, so the Run's sequence stays
 * monotonic across handler-emitted and engine-emitted events. The engine is
 * authoritative for the base identity and the emitting batch's `turnId`; a
 * handler cannot forge either.
 */
const stampSubagentEvent = Effect.fn("AgentRuntime.stampSubagentEvent")(function* (
  context: RunContext,
  turnId: TurnId,
  payload: SubagentEventPayload,
): Effect.fn.Return<RunEvent, ModelProtocolError> {
  const shared = {
    ...(yield* eventBase(context)),
    turnId,
    toolCallId: payload.toolCallId,
    delegationId: payload.delegationId,
    childConversationId: payload.childConversationId,
    childRunId: payload.childRunId,
    targetAgentId: payload.targetAgentId,
    depth: payload.depth,
  };
  switch (payload._tag) {
    case "SubagentRequested": {
      return SubagentRequested.make(shared);
    }
    case "SubagentStarted": {
      return SubagentStarted.make(shared);
    }
    case "SubagentProgress": {
      return SubagentProgress.make({ ...shared, summary: payload.summary });
    }
    case "SubagentCompleted": {
      return SubagentCompleted.make({
        ...shared,
        turns: payload.turns,
        finishReason: payload.finishReason,
        ...(payload.exhausted !== undefined ? { exhausted: payload.exhausted } : {}),
      });
    }
    case "SubagentFailed": {
      return SubagentFailed.make({
        ...shared,
        errorTag: payload.errorTag,
        message: payload.message,
      });
    }
    case "SubagentInterrupted": {
      return SubagentInterrupted.make({ ...shared, reason: payload.reason });
    }
    case "SubagentJoined": {
      return SubagentJoined.make(shared);
    }
  }
});

const approvalDecision = <Tools extends Record<string, Tool.Any>, Error, Requirements>(
  context: RunContext,
  turnId: TurnId,
  prepared: PreparedToolCall<Tools>,
  options: RunOptions<Error, Requirements>,
): Effect.Effect<
  | {
      readonly required: false;
    }
  | {
      readonly required: true;
      readonly request: Response.ToolApprovalRequestPart;
      readonly decision: RunApprovalDecision;
    },
  Error | ModelProtocolError,
  Requirements
> =>
  Effect.gen(function* () {
    const approval = prepared.tool.needsApproval;
    if (approval === undefined || approval === false) {
      return { required: false as const };
    }
    const required =
      typeof approval === "function"
        ? yield* Effect.suspend(() => {
            const result = approval(prepared.decodedParams, {
              toolCallId: prepared.call.id,
              messages: context.history.content,
            });
            return Effect.isEffect(result) ? result : Effect.succeed(result);
          })
        : approval;
    if (!required) {
      return { required: false as const };
    }

    const request = Response.makePart("tool-approval-request", {
      approvalId: `${context.runId}:${prepared.call.id}`,
      toolCallId: prepared.call.id,
    });
    if (options.approval === undefined) {
      return {
        required: true as const,
        request,
        decision: {
          _tag: "unresolved" as const,
          reason: "No approval decision hook is available",
        },
      };
    }
    const toolCallId = yield* decodeToolCallId(prepared.call.id);
    const decision = yield* options.approval.request({
      request,
      conversationId: context.conversationId,
      runId: context.runId,
      turnId,
      toolCallId,
      toolName: prepared.call.name,
      parameters: prepared.decodedParams,
    });
    return {
      required: true as const,
      request,
      decision,
    };
  });

/**
 * Resolve every native Effect AI approval before the handler scheduler starts.
 * Concatenating this preflight stream ahead of handler streams makes denied or
 * unresolved batches a strict no-start boundary.
 */
const preflightApproval = <Tools extends Record<string, Tool.Any>, HookError, HookRequirements>(
  context: RunContext,
  turnId: TurnId,
  prepared: PreparedToolCall<Tools>,
  options: RunOptions<HookError, HookRequirements>,
): Stream.Stream<
  RunEvent,
  HookError | ModelProtocolError | AgentApprovalDenied | AgentApprovalPending,
  HookRequirements
> =>
  Stream.unwrap(
    approvalDecision(context, turnId, prepared, options).pipe(
      Effect.flatMap(
        (
          approval,
        ): Effect.Effect<
          Stream.Stream<RunEvent, AgentApprovalDenied | AgentApprovalPending>,
          ModelProtocolError
        > => {
          if (!approval.required) {
            return Effect.succeed(Stream.empty);
          }
          return Effect.gen(function* () {
            const toolCallId = yield* decodeToolCallId(prepared.call.id);
            const requested = ApprovalRequested.make({
              ...(yield* eventBase(context)),
              turnId,
              toolCallId,
              toolName: prepared.call.name,
            });
            switch (approval.decision._tag) {
              case "approved": {
                return Stream.succeed<RunEvent>(requested);
              }
              case "denied": {
                const denied = AgentApprovalDenied.make({
                  toolCallId: prepared.call.id,
                  toolName: prepared.call.name,
                  message: approval.decision.reason ?? "Tool approval was denied",
                });
                const failed = ToolCallFailed.make({
                  ...(yield* eventBase(context)),
                  turnId,
                  toolCallId,
                  toolName: prepared.call.name,
                  errorTag: denied._tag,
                  message: denied.message,
                  providerExecuted: false,
                });
                return Stream.fromIterable<RunEvent>([requested, failed]).pipe(
                  Stream.concat(Stream.fail(denied)),
                );
              }
              case "unresolved": {
                const pending = AgentApprovalPending.make({
                  approvalId: approval.request.approvalId,
                  toolCallId: prepared.call.id,
                  toolName: prepared.call.name,
                  message: approval.decision.reason ?? "Tool approval remains unresolved",
                });
                return Stream.succeed<RunEvent>(requested).pipe(
                  Stream.concat(Stream.fail(pending)),
                );
              }
            }
          });
        },
      ),
    ),
  );

/**
 * Recheck host-owned Tool authority after approval and before durable preparation or scheduling.
 * Every call in the executable batch is authorized in declaration order before ANY Handler may
 * start, so a later denial leaves this Attempt's complete batch at zero application side effects.
 */
const preflightToolAuthorization = <HookError, HookRequirements>(
  context: RunContext,
  turnId: TurnId,
  turn: number,
  call: RunToolCallDescriptor,
  options: RunOptions<HookError, HookRequirements>,
): Stream.Stream<
  RunEvent,
  HookError | ModelProtocolError | AgentToolAuthorizationDenied,
  HookRequirements
> => {
  const authorization = options.toolAuthorization;
  if (authorization === undefined) return Stream.empty;
  return Stream.unwrap(
    authorization
      .authorize({
        conversationId: context.conversationId,
        runId: context.runId,
        turnId,
        turn,
        input: context.input,
        call,
      })
      .pipe(
        Effect.map((decision) => {
          if (decision._tag === "allowed") return Stream.empty;
          const denied = AgentToolAuthorizationDenied.make({
            toolCallId: call.toolCallId,
            toolName: call.toolName,
            message: decision.reason,
          });
          return Stream.fromEffect(
            Effect.map(eventBase(context), (base) =>
              ToolCallFailed.make({
                ...base,
                turnId,
                toolCallId: call.toolCallId,
                toolName: call.toolName,
                errorTag: denied._tag,
                message: denied.message,
                providerExecuted: false,
              }),
            ),
          ).pipe(Stream.concat(Stream.fail(denied)));
        }),
      ),
  );
};

const ProviderResponsePartId = Schema.String.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/),
);
const ProviderToolCallId = ToolCallId.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/),
);
const isTelemetryToolCallId = Schema.is(ProviderToolCallId);

type ToolTelemetryOutcome = "success" | "failure";

interface ToolTelemetryDescriptor {
  readonly context: RunContext;
  readonly turnId: TurnId;
  readonly toolCallId: string | undefined;
  readonly toolName: string;
  readonly executionClass: ReturnType<typeof getToolExecutionClass>;
  readonly invocationKind: "model" | "programmatic";
  readonly parentToolCallId?: ToolCallId | undefined;
  readonly sequenceIndex?: number | undefined;
}

/** One bounded identity surface shared by canonical Tool spans and terminal logs. */
const toolTelemetryAttributes = (descriptor: ToolTelemetryDescriptor) => ({
  "gen_ai.operation.name": "execute_tool",
  "gen_ai.tool.name": descriptor.toolName,
  "gen_ai.tool.type": "function",
  ...(descriptor.toolCallId === undefined
    ? {}
    : {
        "gen_ai.tool.call.id": descriptor.toolCallId,
        toolCallId: descriptor.toolCallId,
      }),
  "gen_ai.agent.name": descriptor.context.agentId,
  "gen_ai.conversation.id": descriptor.context.conversationId,
  "effect_agent.tool.execution_class": descriptor.executionClass,
  "effect_agent.tool.invocation_kind": descriptor.invocationKind,
  ...(descriptor.parentToolCallId === undefined
    ? {}
    : {
        "effect_agent.tool.parent_call.id": descriptor.parentToolCallId,
        parentToolCallId: descriptor.parentToolCallId,
      }),
  ...(descriptor.sequenceIndex === undefined
    ? {}
    : {
        "effect_agent.tool.sequence_index": descriptor.sequenceIndex,
        sequenceIndex: descriptor.sequenceIndex,
      }),
  agentId: descriptor.context.agentId,
  conversationId: descriptor.context.conversationId,
  runId: descriptor.context.runId,
  turnId: descriptor.turnId,
  toolName: descriptor.toolName,
});

/** Canonical content-free terminal signal for any application Tool handler attempt. */
const terminalToolTelemetry = (
  descriptor: ToolTelemetryDescriptor,
  outcome: ToolTelemetryOutcome,
  failureMarker?: ToolSpanFailure,
): Effect.Effect<void> =>
  annotateToolSpanTerminalOutcome(outcome, failureMarker).pipe(
    Effect.andThen(
      (outcome === "success"
        ? Effect.logInfo("agent tool execution completed")
        : Effect.logWarning("agent tool execution failed")
      ).pipe(
        Effect.annotateLogs({
          ...toolTelemetryAttributes(descriptor),
          "effect_agent.tool.outcome": outcome,
          toolExecutionClass: descriptor.executionClass,
          toolOutcome: outcome,
        }),
      ),
    ),
  );

const executePreparedToolCall = <Tools extends Record<string, Tool.Any>>(
  context: RunContext,
  turnId: TurnId,
  toolkit: Toolkit.WithHandler<Tools>,
  prepared: PreparedToolCall<Tools>,
  trace: TurnTrace,
  resultBounds: ToolResultBounds,
  /**
   * Batch-level collector for the durable-Subagent waiting signal. A
   * `ToolCallWaiting` failure is not a Tool failure: the call stays open (no
   * terminal result, no `ToolCallFailed`, no batch failure policy) and the
   * batch reports it through `AgentChildPending` after every non-waiting
   * sibling settled.
   */
  onWaiting: (waiting: ToolCallWaiting) => void,
): Stream.Stream<
  RunEvent,
  ModelProtocolError | AiError.AiError | Tool.HandlerError<ToolUnion<Tools>>,
  ToolSpanTelemetry | Tool.HandlerServices<ToolUnion<Tools>>
> => {
  type ToolExecutionError =
    | ModelProtocolError
    | AiError.AiError
    | Tool.HandlerError<ToolUnion<Tools>>;

  const call = prepared.call;
  const telemetryToolCallId = isTelemetryToolCallId(call.id) ? call.id : undefined;
  const executionClass = getToolExecutionClass(prepared.tool);
  const telemetryDescriptor: ToolTelemetryDescriptor = {
    context,
    turnId,
    toolCallId: telemetryToolCallId,
    toolName: call.name,
    executionClass,
    invocationKind: "model",
  };
  const toolSpanFailure = ToolSpanFailure.marker();
  let terminal = false;
  let terminalResultCommitted = false;
  let terminalOutcome: "success" | "failure" | undefined;
  let terminalResult:
    | {
        readonly encodedResult: unknown;
        readonly isFailure: boolean;
        readonly result: unknown;
      }
    | undefined;
  let propagatedFailure: Cause.Cause<ToolExecutionError> | undefined;

  const terminalTelemetry = (outcome: ToolTelemetryOutcome) =>
    terminalToolTelemetry(
      telemetryDescriptor,
      outcome,
      outcome === "failure" ? toolSpanFailure : undefined,
    );

  const isolatedTerminalTelemetry = (outcome: "success" | "failure") =>
    // Measurement is derivative: a broken Logger/Tracer must never change the Tool event or make
    // an already-completed external side effect eligible for recovery. Non-interrupt Causes reach
    // Effect's owned reporter boundary; external interruption remains interruption.
    isolateToolTerminalTelemetry(terminalTelemetry(outcome));

  /**
   * Preserve an actual emission boundary between canonical state and derivative telemetry. The
   * singleton event chunk reaches the downstream Run stream before telemetry can start. The next
   * pull normally owns telemetry; structured finalization owns it when downstream closes after the
   * event. Their shared phase gate permits one attempt, while a returned Tool failure still fails a
   * normal second pull with the private span marker.
   */
  const terminalEventThenTelemetry = <EventError>(
    event: Effect.Effect<RunEvent, EventError>,
    outcome: "success" | "failure",
    failSpan: boolean,
  ): Stream.Stream<RunEvent, EventError | ToolSpanFailure> =>
    emitThenAfter(
      event,
      isolatedTerminalTelemetry(outcome).pipe(
        Effect.andThen(failSpan ? Effect.fail(toolSpanFailure) : Effect.void),
      ),
    );

  const started = Stream.fromEffect(
    Effect.gen(function* () {
      const toolCallId = yield* decodeToolCallId(call.id);
      yield* Effect.logDebug("agent tool handler started").pipe(
        Effect.annotateLogs({
          agentId: context.agentId,
          runId: context.runId,
          turnId,
          ...(telemetryToolCallId === undefined ? {} : { toolCallId: telemetryToolCallId }),
          toolName: call.name,
        }),
      );
      yield* Metric.update(toolCounter, 1);
      return ToolCallStarted.make({
        ...(yield* eventBase(context)),
        turnId,
        toolCallId,
        toolName: call.name,
      });
    }).pipe(Effect.withLogSpan("AgentRuntime.tool")),
  );

  const results: Stream.Stream<
    RunEvent,
    ToolExecutionError,
    ToolSpanTelemetry | Tool.HandlerServices<ToolUnion<Tools>>
  > = Stream.unwrap(
    Effect.flatMap(ToolSpanTelemetry, ({ isolateToolkitHandle }) =>
      isolateToolkitHandle(toolkit.handle(prepared.name, prepared.nativeHandlerParams, call.id)),
    ),
  ).pipe(
    Stream.mapEffect(
      (result): Effect.Effect<RunEvent | undefined, ModelProtocolError> =>
        Effect.gen(function* () {
          if (terminal || trace.finalToolResultIds.has(call.id)) {
            return yield* ModelProtocolError.make({
              message: `Tool Call ${call.id} produced more than one terminal result`,
            });
          }
          const toolCallId = yield* decodeToolCallId(call.id);
          if (result.preliminary) {
            const event: RunEvent = ToolProgress.make({
              ...(yield* eventBase(context)),
              turnId,
              toolCallId,
              toolName: call.name,
              result: yield* decodeEventJson(result.encodedResult, "Tool result"),
              providerExecuted: false,
            });
            return event;
          }

          terminal = true;
          terminalOutcome = result.isFailure ? "failure" : "success";
          terminalResult = {
            encodedResult: result.encodedResult,
            isFailure: result.isFailure,
            result: result.result,
          };
          // A terminal Toolkit value is provisional until its handler stream closes. Emitting the
          // append-only event or committing the Turn trace here would leave contradictory Run state
          // if that stream later fails or produces another terminal value.
          return undefined;
        }),
    ),
    Stream.filter((event): event is RunEvent => event !== undefined),
  );

  const commitTerminalResult: Effect.Effect<RunEvent, ModelProtocolError> = Effect.suspend(
    (): Effect.Effect<RunEvent, ModelProtocolError> => {
      if (!terminal || terminalOutcome === undefined || terminalResult === undefined) {
        return Effect.fail(
          ModelProtocolError.make({
            message: `Tool Call ${call.id} completed without a terminal result`,
          }),
        );
      }
      const result = terminalResult;
      return Effect.gen(function* () {
        const toolCallId = yield* decodeToolCallId(call.id);
        // RUN-022: the policy bound applies exactly once, as the result
        // becomes terminal — the settled trace entry, the durable record,
        // and the live success event all carry the same bounded value.
        // Provider-executed results and the final-output path never pass
        // through this seam.
        const encodedResult = boundEncodedToolResult(result.encodedResult, resultBounds);
        const event: RunEvent = result.isFailure
          ? ToolCallFailed.make({
              ...(yield* eventBase(context)),
              turnId,
              toolCallId,
              toolName: call.name,
              errorTag: errorTag(result.result),
              message: errorMessage(result.result),
              providerExecuted: false,
            })
          : ToolCallSucceeded.make({
              ...(yield* eventBase(context)),
              turnId,
              toolCallId,
              toolName: call.name,
              result: yield* decodeEventJson(encodedResult, "Tool result"),
              providerExecuted: false,
            });
        trace.finalToolResultIds.add(call.id);
        trace.applicationToolResults[prepared.declarationIndex] = {
          id: call.id,
          name: call.name,
          encodedResult,
          isFailure: result.isFailure,
        };
        terminalResultCommitted = true;
        return event;
      });
    },
  );

  const finalizeTerminalResult: Stream.Stream<RunEvent, ModelProtocolError | ToolSpanFailure> =
    Stream.unwrap(
      Effect.sync(() =>
        terminalEventThenTelemetry(
          commitTerminalResult,
          terminalOutcome ?? "failure",
          terminalOutcome === "failure",
        ),
      ),
    );

  const failTerminalResult = (
    cause: Cause.Cause<ToolExecutionError>,
  ): Stream.Stream<RunEvent, ModelProtocolError | ToolSpanFailure> => {
    terminal = true;
    terminalOutcome = "failure";
    terminalResult = undefined;
    propagatedFailure = cause;
    return terminalEventThenTelemetry(
      makeToolFailedEvent(context, turnId, call, Cause.squash(cause)).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            trace.finalToolResultIds.add(call.id);
            terminalResultCommitted = true;
          }),
        ),
      ),
      "failure",
      true,
    );
  };

  const measured = started.pipe(
    Stream.concat(results),
    Stream.concat(finalizeTerminalResult),
    Stream.catchCause((cause) => {
      // A value-level failure already has its terminal event and bounded span-facing
      // signal. Let only the outer recovery consume it.
      const { found: hasToolSpanFailure, residual: toolCause } = stripToolSpanFailures(
        cause,
        toolSpanFailure,
      );
      if (hasToolSpanFailure) {
        return Stream.failCause(cause);
      }
      if (terminalResultCommitted) {
        return Stream.failCause(toolCause);
      }
      if (toolCause.reasons.length > 0 && toolCause.reasons.every(Cause.isInterruptReason)) {
        // Interruption terminates only this in-memory handler attempt. It emits no append-only Tool
        // terminal event and no success/failure terminal log; the canonical span observes and
        // preserves the exact interruption Cause from its enclosing stream Exit.
        return Stream.failCause(toolCause);
      }
      const waiting = waitingFromCause(toolCause);
      if (waiting !== undefined) {
        if (terminal || trace.finalToolResultIds.has(call.id)) {
          // A handler that produced a terminal result cannot also wait: the
          // anomaly fails loud and typed instead of suspending a settled call.
          return failTerminalResult(
            Cause.fail(
              ModelProtocolError.make({
                message: `Tool Call ${call.id} raised the waiting signal after its terminal result`,
              }),
            ),
          );
        }
        // The waiting call stays open: no terminal result is recorded, no
        // failure event is emitted, and the batch failure policy never
        // applies — sibling handlers keep running and the batch terminates
        // with `AgentChildPending` once they all settled.
        onWaiting(waiting);
        return Stream.empty;
      }
      if (terminal) {
        // The provisional value never became append-only state. Replace it with one failed
        // terminal event, then preserve the handler Cause outside the span-facing marker.
        return failTerminalResult(toolCause);
      }
      return failTerminalResult(toolCause);
    }),
    Stream.withSpan(`execute_tool ${call.name}`, {
      kind: "internal",
      attributes: toolTelemetryAttributes(telemetryDescriptor),
    }),
  );

  return Stream.unwrap(
    Effect.map(ToolSpanTelemetry, ({ isolateSpanLifecycle }) => isolateSpanLifecycle(measured)),
  ).pipe(
    Stream.catchCause((cause) => {
      const { found, restored } = restoreToolSpanFailureCause(
        cause,
        toolSpanFailure,
        propagatedFailure,
      );
      if (!found) return Stream.failCause(restored);
      return restored.reasons.length === 0 ? Stream.empty : Stream.failCause(restored);
    }),
  );
};

/**
 * Execute a wholly preflighted native Toolkit batch under one finite engine
 * semaphore.
 *
 * The engine provides this batch's `RunEventSink` locally to the handler
 * streams, bound to the batch's Turn. Sink emissions are drained into the
 * batch stream and stamped through `eventBase`; the batch settles (including
 * by failure) only after every already-emitted Subagent event has surfaced.
 * The public exclusion of engine-provided services happens once at the Run
 * boundary in `stream`, so this signature keeps the naked handler services.
 */
const executeToolBatch = <Tools extends Record<string, Tool.Any>, HookError, HookRequirements>(
  context: RunContext,
  turnId: TurnId,
  turn: number,
  toolkit: Toolkit.WithHandler<Tools>,
  calls: ReadonlyArray<Response.ToolCallPart<string, unknown>>,
  trace: TurnTrace,
  concurrency: number,
  options: RunOptions<HookError, HookRequirements>,
  /**
   * Tool-call accounting for the per-call `ToolBroker` (RUN-017):
   * `declaredToolCalls` is the model-declared total committed through this
   * batch, and `maxToolCalls` the Agent policy bound the broker consumes
   * against mid-pass.
   */
  brokerAccounting: { readonly maxToolCalls: number; readonly declaredToolCalls: number },
  /** Policy byte bound applied to each settling result (RUN-022). */
  resultBounds: ToolResultBounds,
  /**
   * Call IDs already settled canonically (batch-resume seam). Their recorded
   * results were injected into the trace by the caller; approval preflight
   * and durable preparation still cover them, but no handler starts.
   */
  settledCallIds?: ReadonlySet<string>,
): Stream.Stream<
  RunEvent,
  | HookError
  | ModelProtocolError
  | AgentApprovalDenied
  | AgentApprovalPending
  | AgentToolAuthorizationDenied
  | AgentChildPending
  | AiError.AiError
  | Tool.HandlerError<ToolUnion<Tools>>,
  HookRequirements | ToolSpanTelemetry | Tool.HandlerServices<ToolUnion<Tools>>
> =>
  Stream.unwrap(
    Effect.gen(function* () {
      // Resolve every name and decode every parameter object before any call stream is constructed.
      const prepared = yield* Effect.forEach(calls, (call, declarationIndex) =>
        prepareToolCall(toolkit, call, declarationIndex),
      );
      const semaphore = yield* Semaphore.make(concurrency);
      const approvalPreflight = prepared.reduce<
        Stream.Stream<
          RunEvent,
          HookError | ModelProtocolError | AgentApprovalDenied | AgentApprovalPending,
          HookRequirements
        >
      >(
        (stream, call) =>
          stream.pipe(Stream.concat(preflightApproval(context, turnId, call, options))),
        Stream.empty,
      );

      // Reuse the canonical wire-form descriptors already committed with the response. Handler
      // parameters were decoded/re-encoded separately above, so the host policy never receives
      // the handler's live parameter object.
      const descriptors = trace.applicationCallDescriptors;

      const durability = options.durability;
      // The hook's requirements are captured here (they are already part of
      // this stream's requirements) so the per-call `DurableStep` service can
      // run hook effects without leaking `HookRequirements` into handler
      // signatures.
      const hookServices = yield* Effect.context<HookRequirements>();

      const executable =
        settledCallIds === undefined
          ? prepared
          : prepared.filter((call) => !settledCallIds.has(call.call.id));
      const executableDescriptors =
        settledCallIds === undefined
          ? descriptors
          : descriptors.filter((call) => !settledCallIds.has(call.toolCallId));
      const authorizationPreflight = executableDescriptors.reduce<
        Stream.Stream<
          RunEvent,
          HookError | ModelProtocolError | AgentToolAuthorizationDenied,
          HookRequirements
        >
      >(
        (stream, call) =>
          stream.pipe(
            Stream.concat(preflightToolAuthorization(context, turnId, turn, call, options)),
          ),
        Stream.empty,
      );

      // Durable preparation runs strictly after every approval resolved
      // approved and before any handler acquires a permit. `readonly` calls
      // need no uncertainty protocol; a batch whose calls are all `readonly`
      // skips the hook entirely. A resumed batch replays the identical full
      // descriptor list so the prepared batch identity stays stable.
      const preparation: Stream.Stream<never, HookError, HookRequirements> =
        durability === undefined
          ? Stream.empty
          : Stream.fromEffect(
              Effect.suspend(() => {
                const preparedDescriptors = descriptors.filter(
                  (call) => call.executionClass !== "readonly",
                );
                return preparedDescriptors.length === 0
                  ? Effect.void
                  : durability.prepareToolCalls(preparedDescriptors);
              }),
            ).pipe(Stream.drain);

      // Each executable call gets its own locally provided `DurableStep`,
      // bound to that call's identity: durable over the coordinator's step
      // hook, honest pass-through otherwise.
      const stepServiceFor = (call: PreparedToolCall<Tools>): DurableStepService =>
        durability === undefined
          ? passthroughDurableStep()
          : makeDurableStepService(call.toolCallId, durability.step, hookServices);

      // One live broker per executable outer call: construction resolves its
      // telemetry requirement from Context at this engine-owned operation
      // edge, and provision plus the settling closer share one lifecycle.
      const liveBrokers = new Map<string, LiveToolBroker>();
      for (const call of executable) {
        const broker = yield* makeToolBrokerService({
          context,
          turnId,
          outerToolCallId: call.toolCallId,
          maxToolCalls: brokerAccounting.maxToolCalls,
          declaredToolCalls: brokerAccounting.declaredToolCalls,
          budget: options.budget,
          hookServices,
        });
        liveBrokers.set(call.call.id, broker);
      }
      const brokerFor = (call: PreparedToolCall<Tools>): LiveToolBroker => {
        const broker = liveBrokers.get(call.call.id);
        if (broker === undefined) {
          throw new Error(`Missing live Tool broker for executable call ${call.call.id}`);
        }
        return broker;
      };

      // This batch's live `SubagentDurability` service: durable over the
      // coordinator's subagent hook, the explicit ephemeral-mode default
      // otherwise. Absence of the hook means the Run is not under a durable
      // coordinator (the coordinator always supplies it), so no durable claim
      // is being made and the S1 in-process spawn semantics apply honestly.
      const subagentHook = options.subagent;
      const batchSubagentDurability: SubagentDurabilityService =
        subagentHook === undefined
          ? ephemeralSubagentDurability
          : makeSubagentDurabilityService(subagentHook, hookServices);

      // Delegation calls that raised the waiting signal, keyed by declaration
      // index so `AgentChildPending` lists its children deterministically
      // regardless of parallel handler completion order.
      const waitingByDeclaration = new Map<number, ToolCallWaiting>();

      const groups: Array<ReadonlyArray<PreparedToolCall<Tools>>> = [];
      let parallel: Array<PreparedToolCall<Tools>> = [];
      for (const call of executable) {
        if (options.scheduling?.toolRequiresSequential?.(call.name) === true) {
          if (parallel.length > 0) {
            groups.push(parallel);
            parallel = [];
          }
          groups.push([call]);
        } else {
          parallel.push(call);
        }
      }
      if (parallel.length > 0) {
        groups.push(parallel);
      }

      const handlers = groups.reduce<
        Stream.Stream<
          RunEvent,
          ModelProtocolError | AiError.AiError | Tool.HandlerError<ToolUnion<Tools>>,
          ToolSpanTelemetry | Tool.HandlerServices<ToolUnion<Tools>>
        >
      >((stream, group) => {
        const next = Stream.mergeAll(
          group.map((call) =>
            withSemaphorePermit(
              semaphore,
              executePreparedToolCall(
                context,
                turnId,
                toolkit,
                call,
                trace,
                resultBounds,
                (waiting) => {
                  waitingByDeclaration.set(call.declarationIndex, waiting);
                },
              ).pipe(
                Stream.provideService(DurableStep, stepServiceFor(call)),
                // The live broker executes under this call's already-held
                // permit: invocations run inside the handler's own fiber and
                // never touch the batch semaphore (RUN-016). Its lifecycle
                // closes with this call's stream so no retained pass can
                // outlive the batch's scheduling authority.
                Stream.provideService(ToolBroker, brokerFor(call).service),
                Stream.ensuring(Effect.sync(() => brokerFor(call).close())),
              ),
            ),
          ),
          { concurrency: "unbounded" },
        );
        return stream.pipe(Stream.concat(next));
      }, Stream.empty);

      // This batch's live sink uses bounded structured backpressure. The Run's own stream drains
      // it concurrently with handlers, so a burst may suspend its emitting handler but a detached
      // external observer can never backpressure the batch. Emitting after settlement still fails
      // closed with `RunEventSinkClosedError`.
      const sinkQueue = yield* Queue.bounded<SubagentEventPayload, Cause.Done>(
        context.bufferLimits.maxSubagentEventsPerBatch,
      );
      const batchSink: RunEventSinkService = {
        emit: (payload) =>
          Queue.offer(sinkQueue, payload).pipe(
            Effect.flatMap((accepted) =>
              accepted
                ? Effect.void
                : Effect.fail(
                    RunEventSinkClosedError.make({
                      message: `Subagent event ${payload._tag} was emitted after its Tool batch settled`,
                    }),
                  ),
            ),
          ),
      };

      // The batch fails only after already-emitted Subagent events surface:
      // a handler failure cause is held back, the sink queue is ended and
      // fully drained through the merge, and the cause is rethrown once both
      // sides finish. External interruption bypasses the capture and tears
      // the whole batch down through ordinary Scope closure. The widened
      // annotation reabsorbs the local `RunEventSink` exclusion because the
      // Run boundary in `stream` owns the public exclusion.
      let settledCause:
        | Cause.Cause<ModelProtocolError | AiError.AiError | Tool.HandlerError<ToolUnion<Tools>>>
        | undefined;
      const settling: Stream.Stream<
        RunEvent,
        never,
        ToolSpanTelemetry | Tool.HandlerServices<ToolUnion<Tools>>
      > = handlers.pipe(
        Stream.provideService(RunEventSink, batchSink),
        Stream.provideService(SubagentDurability, batchSubagentDurability),
        Stream.catchCause((cause) => {
          settledCause = cause;
          return Stream.empty;
        }),
        Stream.ensuring(Queue.end(sinkQueue)),
      );
      const sinkEvents = Stream.fromQueue(sinkQueue).pipe(
        Stream.mapEffect((payload) => stampSubagentEvent(context, turnId, payload)),
      );
      const settled = Stream.merge(settling, sinkEvents).pipe(
        Stream.concat(
          Stream.fromEffect(
            Effect.suspend(
              (): Effect.Effect<
                void,
                | AgentChildPending
                | ModelProtocolError
                | AiError.AiError
                | Tool.HandlerError<ToolUnion<Tools>>
              > => {
                if (settledCause !== undefined) {
                  // A real sibling failure keeps the existing batch failure
                  // policy even when another call is waiting: the Run fails
                  // with the sibling's cause and the coordinator's durable
                  // ledger state (not this stream) carries the attached child.
                  return Effect.failCause(settledCause);
                }
                const waiting = [...waitingByDeclaration.entries()]
                  .sort(([left], [right]) => left - right)
                  .map(([, signal]) => signal);
                const [first, ...rest] = waiting;
                if (first === undefined) {
                  return Effect.void;
                }
                // Every non-waiting sibling has settled; the Run now suspends
                // `waitingForChild` (spec/subagents.md §12 step 10, SUB-030)
                // via the AgentApprovalPending-mirroring typed error below.
                const child = (signal: ToolCallWaiting) => ({
                  toolCallId: signal.toolCallId,
                  childConversationId: signal.childConversationId,
                  childSubmissionId: signal.childSubmissionId,
                  childRunId: signal.childRunId,
                });
                return Effect.fail(
                  AgentChildPending.make({
                    children: [child(first), ...rest.map(child)],
                    message: `${waiting.length} durable delegation ${
                      waiting.length === 1 ? "call is" : "calls are"
                    } waiting on attached children; the Run suspended without settling`,
                  }),
                );
              },
            ),
          ).pipe(Stream.drain),
        ),
      );
      return approvalPreflight.pipe(
        Stream.concat(authorizationPreflight),
        Stream.concat(preparation),
        Stream.concat(settled),
      );
    }),
  );

const ErrorMessage = Schema.Struct({ message: Schema.String });
const ErrorTag = Schema.Struct({ _tag: Schema.NonEmptyString });
const DIAGNOSTIC_MESSAGE_LIMIT = 4_096;
const DIAGNOSTIC_TAG_LIMIT = 128;

const boundedDiagnostic = (value: string, maxCharacters: number): string =>
  value.length <= maxCharacters ? value : value.slice(0, maxCharacters);

/** Total, bounded projection for opaque failures. It never invokes arbitrary coercion hooks. */
const errorMessage = (error: unknown): string => {
  try {
    const decoded = Schema.decodeUnknownOption(ErrorMessage)(error);
    if (Option.isSome(decoded)) {
      return boundedDiagnostic(decoded.value.message, DIAGNOSTIC_MESSAGE_LIMIT);
    }
  } catch {
    // Hostile getters and proxy traps are opaque diagnostic values.
  }
  return typeof error === "string"
    ? boundedDiagnostic(error, DIAGNOSTIC_MESSAGE_LIMIT)
    : "Unknown error";
};

const errorTag = (error: unknown): string => {
  try {
    const decoded = Schema.decodeUnknownOption(ErrorTag)(error);
    if (Option.isSome(decoded)) {
      return boundedDiagnostic(decoded.value._tag, DIAGNOSTIC_TAG_LIMIT);
    }
  } catch {
    // Hostile getters and proxy traps are opaque diagnostic values.
  }
  return "UnknownError";
};

const schedulingConcurrency = (
  configured: number,
  scheduling: RunSchedulingHook | undefined,
): Effect.Effect<number, AgentPolicyError> => {
  const override = scheduling?.runOverride;
  if (override === undefined) {
    return Effect.succeed(configured);
  }
  if (override.mode === "sequential") {
    return Effect.succeed(1);
  }
  if (!Number.isInteger(override.concurrency) || override.concurrency <= 0) {
    return Effect.fail(
      AgentPolicyError.make({
        limit: "usage",
        message: "Run Tool concurrency override must be a positive integer",
      }),
    );
  }
  return Effect.succeed(Math.min(configured, override.concurrency));
};

/**
 * One terminal outcome per declared Tool Call of the completed Turn, in
 * declaration order. Provider-executed results are read from the response
 * parts; application results from the settled batch.
 */
const turnToolFailures = (trace: TurnTrace): ReadonlyArray<boolean> => {
  const outcomes: Array<boolean> = [];
  for (const [id, call] of trace.toolCalls) {
    if (call.providerExecuted) {
      for (const part of trace.parts) {
        if (part.type === "tool-result" && part.id === id && part.preliminary !== true) {
          outcomes.push(part.isFailure);
          break;
        }
      }
      continue;
    }
    const result = trace.applicationToolResults.find((candidate) => candidate?.id === id);
    // Budget-rejected calls never ran a handler; they neither advance nor
    // reset the consecutive-failure counter.
    if (result !== undefined && result.budgetRejected !== true) {
      outcomes.push(result.isFailure);
    }
  }
  return outcomes;
};

/**
 * Fold the completed Turn's terminal Tool outcomes into the Run's
 * consecutive-failure counter in declaration order, then enforce the
 * repeated-failure Stop Policy before the next model request. A terminal
 * success resets the counter; a `repeatedFailureLimit` of `0` disables the
 * bound.
 */
const applyRepeatedFailurePolicy = (
  context: RunContext,
  trace: TurnTrace,
  repeatedFailureLimit: number,
): Effect.Effect<void, AgentPolicyError> =>
  Effect.suspend(() => {
    for (const isFailure of turnToolFailures(trace)) {
      context.consecutiveToolFailures = isFailure ? context.consecutiveToolFailures + 1 : 0;
    }
    if (repeatedFailureLimit > 0 && context.consecutiveToolFailures >= repeatedFailureLimit) {
      return Effect.fail(
        AgentPolicyError.make({
          limit: "repeated-failures",
          message: `Agent reached its ${repeatedFailureLimit} consecutive Tool Call failure limit`,
        }),
      );
    }
    return Effect.void;
  });

const inputsToPrompt = (
  inputs: ReadonlyArray<Prompt.RawInput>,
): Effect.Effect<Prompt.Prompt, AgentInputError> =>
  Effect.try({
    try: () => Prompt.fromMessages(inputs.flatMap((input) => Prompt.make(input).content)),
    catch: (cause) =>
      AgentInputError.make({
        message: `Unable to materialize queued Run input: ${errorMessage(cause)}`,
      }),
  });

const advanceHistory = <HookError, HookRequirements>(
  context: RunContext,
  history: Prompt.Prompt,
  options: RunOptions<HookError, HookRequirements>,
): Effect.Effect<void, HookError, HookRequirements> =>
  Effect.gen(function* () {
    context.history = history;
    if (options.onHistory !== undefined) {
      yield* options.onHistory(history);
    }
  });

const drainInputs = <HookError, HookRequirements>(
  context: RunContext,
  options: RunOptions<HookError, HookRequirements>,
): Effect.Effect<ReadonlyArray<Prompt.RawInput>, HookError, HookRequirements> =>
  Effect.gen(function* () {
    if (options.input === undefined) {
      return [];
    }
    const commands = yield* options.input.drain(options.commandDrainPolicy ?? "one");
    const steering: Array<Prompt.RawInput> = [];
    for (const command of commands) {
      if (command.kind === "steering") {
        steering.push(command.input);
      } else {
        context.pendingFollowUps.push(command.input);
      }
    }
    return steering;
  });

const takeFollowUps = (
  context: RunContext,
  policy: CommandDrainPolicy,
): ReadonlyArray<Prompt.RawInput> => {
  if (context.pendingFollowUps.length === 0) {
    return [];
  }
  if (policy === "one") {
    const input = context.pendingFollowUps.shift();
    return input === undefined ? [] : [input];
  }
  return context.pendingFollowUps.splice(0, context.pendingFollowUps.length);
};

const appendInputs = <HookError, HookRequirements>(
  context: RunContext,
  source: Prompt.Prompt,
  inputs: ReadonlyArray<Prompt.RawInput>,
  options: RunOptions<HookError, HookRequirements>,
): Effect.Effect<Prompt.Prompt, HookError | AgentInputError, HookRequirements> =>
  Effect.gen(function* () {
    if (inputs.length === 0) {
      return source;
    }
    const additions = yield* inputsToPrompt(inputs);
    const history = Prompt.fromMessages([...source.content, ...additions.content]);
    yield* advanceHistory(context, history, options);
    return history;
  });

/**
 * RUN-022: bound one application Tool result at the settle seam. The bounded
 * value is what official history, the settled record, and the success event
 * all carry, so every downstream surface agrees; a within-bounds result
 * passes through with its identity preserved.
 */
const boundEncodedToolResult = (encodedResult: unknown, bounds: ToolResultBounds): unknown => {
  let text: string | undefined;
  try {
    text = JSON.stringify(encodedResult);
  } catch (cause) {
    // Fail closed (runtime spec §9): an unserializable result is unbounded by
    // construction, so the sentinel replaces it before history or records.
    return unserializableToolResult(cause);
  }
  if (text === undefined) {
    return unserializableToolResult("the encoded result is not a JSON value");
  }
  const bounded = applyToolResultBounds(text, bounds);
  // The measured JSON representation is the ONLY value retained, in both the
  // truncated and unmodified cases: returning the original object would let a
  // stateful `toJSON` pass the byte check small and expand or throw on later
  // canonical serialization, carrying unchecked state past the boundary.
  try {
    return Schema.decodeSync(Schema.fromJsonString(Schema.Unknown))(bounded);
  } catch (cause) {
    // Defensive only: `applyToolResultBounds` always emits valid JSON.
    return unserializableToolResult(cause);
  }
};

/** Deterministic inputs of one run-status message (RUN-024). */
export interface RunStatusView {
  readonly turn: number;
  readonly maxTurns: number;
  readonly toolCallsUsed: number;
  readonly maxToolCalls: number;
  readonly tokensConsumed: number;
  readonly tokenBudget: number | undefined;
  readonly lastInputTokens: number;
  readonly elapsedSeconds: number;
  readonly maxDurationSeconds: number;
}

const RUN_STATUS_WARNING =
  " · WARNING: approaching limits — converge and deliver your final result now.";

/** Fraction checks use `consumed * 5 >= limit * 4` so the 80% threshold stays integer-exact. */
const nearingLimit = (consumed: number, limit: number): boolean => consumed * 5 >= limit * 4;

/**
 * RUN-024: render the derived run-status message appended to each outgoing
 * model request. The format is pinned here — tests target this function.
 */
export const formatRunStatus = (view: RunStatusView): string => {
  const warn =
    nearingLimit(view.turn, view.maxTurns) ||
    nearingLimit(view.toolCallsUsed, view.maxToolCalls) ||
    (view.tokenBudget !== undefined && nearingLimit(view.tokensConsumed, view.tokenBudget)) ||
    nearingLimit(view.elapsedSeconds, view.maxDurationSeconds);
  return `<run-status>turn ${view.turn}/${view.maxTurns} · tool-calls ${view.toolCallsUsed}/${view.maxToolCalls} · tokens ${view.tokensConsumed}/${view.tokenBudget ?? "unbounded"} · last-context ${view.lastInputTokens} · elapsed ${view.elapsedSeconds}s/${view.maxDurationSeconds}s${warn ? RUN_STATUS_WARNING : ""}</run-status>`;
};

/**
 * The run-status message is derived per request and appended only to the
 * OUTGOING prompt: official history and durable commits never carry it, so it
 * can never accumulate or replay.
 */
const outgoingModelPrompt = (
  policy: AgentPolicy,
  context: RunContext,
  prepared: Prompt.Prompt,
  turn: number,
  declaredToolCalls: number,
): Effect.Effect<Prompt.Prompt> =>
  Effect.gen(function* () {
    if (policy.runStatus !== "appended") {
      return prepared;
    }
    const now = yield* Clock.currentTimeMillis;
    const status = formatRunStatus({
      turn,
      maxTurns: policy.maxTurns,
      toolCallsUsed: declaredToolCalls + context.programmaticToolCalls,
      maxToolCalls: policy.maxToolCalls,
      tokensConsumed: context.inputTokens + context.outputTokens,
      tokenBudget: policy.tokenBudget,
      lastInputTokens: context.lastInputTokens,
      elapsedSeconds: Math.max(0, Math.floor((now - context.startedAtMillis) / 1000)),
      maxDurationSeconds: Math.floor(Duration.toMillis(policy.maxDuration) / 1000),
    });
    return Prompt.fromMessages([
      ...prepared.content,
      Prompt.makeMessage("user", {
        content: [Prompt.makePart("text", { text: status })],
      }),
    ]);
  });

/** Usage accounting outcome of one completed model response (RUN-025). */
interface ConsumedUsage {
  /** A finalize-eligible budget breach; fail-fast breaches never reach the caller. */
  readonly breach: AgentPolicyError | undefined;
  readonly warnings: ReadonlyArray<RunEvent>;
}

const consumeUsage = <AgentValue extends Agent.Any, HookError, HookRequirements>(
  agent: AgentValue,
  context: RunContext,
  usage: Response.Usage | undefined,
  toolCallCount: number,
  options: RunOptions<HookError, HookRequirements>,
): Effect.Effect<
  ConsumedUsage,
  AgentPolicyError | ModelProtocolError | HookError,
  HookRequirements
> =>
  Effect.gen(function* () {
    if (usage === undefined) {
      return yield* AgentPolicyError.make({
        limit: "usage",
        message: "A completed model response did not report usage",
      });
    }
    const inputTokens = Math.max(
      0,
      usage.inputTokens.total ??
        (usage.inputTokens.uncached ?? 0) +
          (usage.inputTokens.cacheRead ?? 0) +
          (usage.inputTokens.cacheWrite ?? 0),
    );
    const outputTokens = Math.max(
      0,
      usage.outputTokens.total ??
        (usage.outputTokens.text ?? 0) + (usage.outputTokens.reasoning ?? 0),
    );
    const totalTokens = inputTokens + outputTokens;
    const costMicrousd =
      options.estimateCostMicrousd === undefined ? 0 : yield* options.estimateCostMicrousd(usage);
    if (!Number.isInteger(costMicrousd) || costMicrousd < 0) {
      return yield* AgentPolicyError.make({
        limit: "cost",
        message: "Model cost estimation must produce a non-negative integer number of microdollars",
      });
    }
    context.modelCalls += 1;
    context.inputTokens += inputTokens;
    context.outputTokens += outputTokens;
    context.lastInputTokens = inputTokens;
    context.lastOutputTokens = outputTokens;
    context.costMicrousd += costMicrousd;
    context.lastCostMicrousd = costMicrousd;

    const policy = agent.definition.policy;
    const consumedTokens = context.inputTokens + context.outputTokens;
    const tokenBudget = policy.tokenBudget;
    let breach: AgentPolicyError | undefined;
    if (
      !context.finalizing &&
      !context.tokenExhausted &&
      tokenBudget !== undefined &&
      consumedTokens > tokenBudget
    ) {
      breach = AgentPolicyError.make({
        limit: "tokens",
        message: `Agent exceeded its ${tokenBudget} token budget`,
      });
      // Fail-fast token breaches keep the pre-soft-landing contract exactly: no
      // budget-hook charge and no warning event on the failing response.
      if (policy.onExhaustion === "fail") {
        return yield* breach;
      }
      // One-shot (RUN-025): the flag joins the final-answer derivation so the
      // grace Turn's own usage accumulates and charges without re-breaching.
      context.tokenExhausted = true;
      context.exhaustedDimension ??= "tokens";
    }
    // Cost is an unconditional hard rail (runtime spec §3): it is enforced on
    // every response, including one that just soft-breached the token budget —
    // a simultaneous breach fails typed instead of soft-landing on overspend.
    const costBudget = policy.costBudgetMicrousd;
    if (costBudget !== undefined) {
      if (options.estimateCostMicrousd === undefined) {
        return yield* AgentPolicyError.make({
          limit: "cost",
          message: "Agent cost budget requires a model cost estimator",
        });
      }
      if (context.costMicrousd > costBudget) {
        // Cost is spend, not context: it never earns a grace Turn.
        return yield* AgentPolicyError.make({
          limit: "cost",
          message: `Agent exceeded its ${costBudget} microdollar cost budget`,
        });
      }
    }
    if (options.budget !== undefined) {
      const delta: RunUsageDelta = {
        modelCalls: 1,
        inputTokens,
        outputTokens,
        totalTokens,
        toolCalls: toolCallCount,
        costMicrousd,
        usage,
      };
      yield* options.budget.consume(delta);
    }
    const warnings: Array<RunEvent> = [];
    if (
      tokenBudget !== undefined &&
      !context.warnedLimits.has("tokens") &&
      nearingLimit(consumedTokens, tokenBudget)
    ) {
      context.warnedLimits.add("tokens");
      warnings.push(
        BudgetWarning.make({
          ...(yield* eventBase(context)),
          limit: "tokens",
          consumed: consumedTokens,
          limitValue: tokenBudget,
        }),
      );
    }
    return { breach, warnings };
  });

// `Effect.fnUntraced`: this helper runs for every streamed Response Part, so a
// named span here would emit one span per TextDelta/ReasoningDelta. Spans stay
// on per-Run, per-Turn, and per-Tool operations.
const eventBaseFor = Effect.fnUntraced(function* (context: RunContext, terminal: boolean) {
  const ceiling = terminal
    ? context.bufferLimits.maxRunEvents
    : context.bufferLimits.maxRunEvents - 1;
  if (context.sequence >= ceiling) {
    return yield* ModelProtocolError.make({
      message: `Run exceeded the ${context.bufferLimits.maxRunEvents}-event buffer limit`,
    });
  }
  const timestamp = DateTime.makeUnsafe(yield* Clock.currentTimeMillis);
  const sequence = context.sequence;
  context.sequence += 1;
  return {
    eventVersion: 1 as const,
    runId: context.runId,
    conversationId: context.conversationId,
    agentId: context.agentId,
    sequence,
    timestamp,
  };
});

const eventBase = (context: RunContext) => eventBaseFor(context, false);

/** The ordinary event budget always reserves one slot for this typed terminal projection. */
const terminalEventBase = (context: RunContext) => eventBaseFor(context, true);

const snapshotStagedProviderEvent = (
  trace: TurnTrace,
  payload: unknown,
): Effect.Effect<BoundedJsonSnapshot, ModelProtocolError> =>
  Effect.suspend(() => {
    const stagedEventCount =
      trace.providerResultPayloads.length + (trace.turnCompletion === undefined ? 0 : 1);
    if (stagedEventCount >= MAX_STAGED_PROVIDER_EVENTS) {
      return Effect.fail(
        ModelProtocolError.make({
          message: `Model response exceeded the ${MAX_STAGED_PROVIDER_EVENTS}-event staged provider event limit`,
        }),
      );
    }
    const snapshot = boundedJsonSnapshot(
      payload,
      MAX_STAGED_PROVIDER_BYTES - trace.providerStagedPayloadBytes,
    );
    if (snapshot === undefined) {
      return Effect.fail(
        ModelProtocolError.make({
          message: `Model response exceeded the ${MAX_STAGED_PROVIDER_BYTES}-byte staged provider event limit`,
        }),
      );
    }
    return Effect.succeed(snapshot);
  });

const stageProviderResultPayload = (
  trace: TurnTrace,
  payload: ProviderResultEventPayload,
): Effect.Effect<void, ModelProtocolError> =>
  Effect.gen(function* () {
    const snapshot = yield* snapshotStagedProviderEvent(trace, payload);
    const snapshotObject = snapshot.value;
    if (
      snapshotObject === null ||
      typeof snapshotObject !== "object" ||
      Array.isArray(snapshotObject) ||
      Object.getPrototypeOf(snapshotObject) !== null
    ) {
      return yield* ModelProtocolError.make({
        message: "Provider Tool result could not be normalized as JSON",
      });
    }
    const resultDescriptor = Object.getOwnPropertyDescriptor(snapshotObject, "result");
    const normalizedResult =
      resultDescriptor !== undefined && "value" in resultDescriptor
        ? Schema.decodeUnknownOption(Schema.Json)(resultDescriptor.value)
        : Option.none<Schema.Json>();
    if (payload._tag !== "ToolCallFailed" && Option.isNone(normalizedResult)) {
      return yield* ModelProtocolError.make({
        message: "Provider Tool result could not be normalized as JSON",
      });
    }
    const normalized: ProviderResultEventPayload =
      payload._tag === "ToolCallFailed"
        ? Object.freeze({ ...payload })
        : Object.freeze({ ...payload, result: Option.getOrThrow(normalizedResult) });
    trace.providerResultPayloads.push(normalized);
    trace.providerStagedPayloadBytes += snapshot.bytes;
  });

const ProviderToolResultSnapshot = Schema.Struct({
  result: Schema.Json,
  metadata: Schema.Record(Schema.String, Schema.Json),
});

const snapshotProviderToolResultPart = Effect.fnUntraced(function* (
  trace: TurnTrace,
  part: Response.ToolResultPart<string, unknown, unknown>,
) {
  const snapshot = boundedJsonSnapshot(
    { result: part.encodedResult, metadata: part.metadata },
    MAX_STAGED_PROVIDER_BYTES - trace.providerStagedPayloadBytes,
  );
  if (snapshot === undefined) {
    return yield* ModelProtocolError.make({
      message: `Model response exceeded the ${MAX_STAGED_PROVIDER_BYTES}-byte staged provider event limit`,
    });
  }
  const normalized = yield* Schema.decodeUnknownEffect(ProviderToolResultSnapshot)(
    snapshot.value,
  ).pipe(
    Effect.mapError(() =>
      ModelProtocolError.make({
        message: "Provider Tool result could not be normalized as JSON",
      }),
    ),
  );
  trace.providerStagedPayloadBytes += snapshot.bytes;
  return normalized;
});

const stampProviderResultEvent = (
  context: RunContext,
  turnId: TurnId,
  payload: ProviderResultEventPayload,
): Effect.Effect<RunEvent, ModelProtocolError> =>
  Effect.map(eventBase(context), (base) => {
    switch (payload._tag) {
      case "ToolProgress":
        return ToolProgress.make({ ...base, turnId, ...payload });
      case "ToolCallSucceeded":
        return ToolCallSucceeded.make({ ...base, turnId, ...payload });
      case "ToolCallFailed":
        return ToolCallFailed.make({ ...base, turnId, ...payload });
    }
  });

/**
 * RUN-026: deterministic estimate of the NEXT model call's live context.
 * Anchored on the last provider-reported call when the view has only grown
 * since then; a fresh Run or a just-compacted view falls back to the full
 * chars/4 estimate.
 */
const nextContextEstimate = (context: RunContext, view: ReadonlyArray<Prompt.Message>): number => {
  const state = context.compaction;
  if (
    context.lastInputTokens > 0 &&
    state.lastViewLength >= 0 &&
    state.lastViewLength <= view.length
  ) {
    return (
      context.lastInputTokens +
      context.lastOutputTokens +
      estimatePromptTokens(view.slice(state.lastViewLength))
    );
  }
  return estimatePromptTokens(view);
};

/** Text a provider overflow classification matches against (message + reason). */
const overflowText = (error: AiError.AiError): string => `${error.message} ${error.reason.message}`;

/** Outcome of one compaction pass: the advisory events to splice into the Run stream. */
interface CompactionOutcome {
  readonly events: ReadonlyArray<RunEvent>;
}

/**
 * RUN-026: compact the model-visible view of `source` in place on
 * `context.compaction`. Stage 1 clears old tool results; stage 2 folds the
 * covered span into a summary through one metered model call on the Run's
 * bound model. Official history is never mutated — the durable commit
 * machinery slices it by length, so only the outgoing view may change. The
 * summarizer call's usage is consumed like any other model call, with
 * `context.finalizing` held during its accounting so a budget breach
 * surfaces at the next response's check instead of recursing into finalize
 * mid-compaction; its usage is deliberately not staged for resume re-seed
 * (only canonical response records carry usage).
 */
const compactContext = <AgentValue extends Agent.Any, HookError, HookRequirements>(
  agent: AgentValue,
  context: RunContext,
  source: Prompt.Prompt,
  turn: number,
  options: RunOptions<HookError, HookRequirements>,
  forceSummarize: boolean,
): Effect.Effect<
  CompactionOutcome,
  AgentPolicyError | ModelProtocolError | AiError.AiError | HookError,
  HookRequirements | LanguageModel.LanguageModel
> =>
  Effect.gen(function* () {
    const policy = agent.definition.policy;
    const state = context.compaction;
    const events: Array<RunEvent> = [];
    const messages = source.content;
    const before = estimatePromptTokens(buildCompactedView(messages, state));
    const limit = policy.contextTokenLimit;
    const mode = policy.compaction.mode;

    const commitDurable = (commit: RunCompactionCommit) =>
      options.durability === undefined ? Effect.void : options.durability.commitCompaction(commit);

    if (!forceSummarize && mode !== "summarize") {
      const bound = choosePruneBound(messages, state, policy.compaction.keepRecentTokens);
      if (bound > state.clearedThrough) {
        state.clearedThrough = bound;
        state.lastViewLength = -1;
        const after = estimatePromptTokens(buildCompactedView(messages, state));
        events.push(
          CompactionPerformed.make({
            ...(yield* eventBase(context)),
            turn,
            kind: "clear-tool-results",
            tokensBeforeEstimate: before,
            tokensAfterEstimate: after,
          }),
        );
        yield* commitDurable({
          turn,
          kind: "clear-tool-results",
          tokensBeforeEstimate: before,
          tokensAfterEstimate: after,
        });
        if (limit !== undefined && after <= limit) {
          return { events };
        }
      }
    }
    if (mode === "prune" && !forceSummarize) {
      // Pruning may legitimately reclaim nothing (a single protected result):
      // the Turn proceeds anyway — the provider may still accept, and the
      // RUN-027 overflow path is the typed backstop when it does not.
      return { events };
    }

    const cut = chooseSummarizeCut(messages, state, policy.compaction.keepRecentTokens);
    const covered = collectCoveredMessages(messages, state, cut);
    if (covered.length === 0) {
      return { events };
    }
    const transcript = renderForSummary(covered, state.summary);
    const summarizerPrompt = Prompt.fromMessages([
      Prompt.makeMessage("user", {
        content: [
          Prompt.makePart("text", {
            text: `${COMPACTION_INSTRUCTION}\n\n<transcript>\n${transcript}\n</transcript>`,
          }),
        ],
      }),
    ]);
    const pieces: Array<string> = [];
    const responseUsage: ModelResponseBufferUsage = {
      responsePartCount: 0,
      responsePartBytes: 0,
    };
    let summaryUsage: Response.Usage | undefined;
    yield* guardBudgetStream(
      LanguageModel.streamText({ prompt: summarizerPrompt }),
      options.budget,
    ).pipe(
      Stream.runForEach((part) =>
        Effect.gen(function* () {
          yield* consumeModelResponsePart(responseUsage, part, context.bufferLimits);
          if (part.type === "text-delta") {
            pieces.push(part.delta);
          } else if (part.type === "finish") {
            summaryUsage = part.usage;
          }
        }),
      ),
    );
    const wasFinalizing = context.finalizing;
    context.finalizing = true;
    const consumed = yield* consumeUsage(agent, context, summaryUsage, 0, options).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          context.finalizing = wasFinalizing;
        }),
      ),
    );
    if (options.durability !== undefined) {
      // The summarizer's spend must survive ownership changes: it stages into
      // the same canonical Turn as the Turn's own response, and the session
      // accumulates both (RUN-023).
      yield* options.durability.noteTurnUsage({
        turn,
        inputTokens: context.lastInputTokens,
        outputTokens: context.lastOutputTokens,
        costMicrousd: context.lastCostMicrousd,
      });
    }
    events.push(...consumed.warnings);
    const summaryText = pieces.join("").trim();
    const summary = summaryText.length === 0 ? "(no summary produced)" : summaryText;
    state.summary = summary;
    state.summarizedThrough = cut;
    state.lastViewLength = -1;
    const after = estimatePromptTokens(buildCompactedView(messages, state));
    events.push(
      CompactionPerformed.make({
        ...(yield* eventBase(context)),
        turn,
        kind: "summarize",
        tokensBeforeEstimate: before,
        tokensAfterEstimate: after,
      }),
    );
    yield* commitDurable({
      turn,
      kind: "summarize",
      summary,
      tokensBeforeEstimate: before,
      tokensAfterEstimate: after,
    });
    return { events };
  });

const decodeInput = Effect.fn("AgentRuntime.decodeInput")(
  <AgentValue extends Agent.Any>(
    agent: AgentValue,
    input: unknown,
  ): Effect.Effect<
    Agent.Input<AgentValue>,
    AgentInputError,
    AgentValue["definition"]["input"]["DecodingServices"]
  > =>
    Schema.decodeUnknownEffect(agent.definition.input)(input).pipe(
      Effect.mapError((cause) =>
        AgentInputError.make({
          message: cause.message,
        }),
      ),
    ),
);

const evaluateInstructions = Effect.fn("AgentRuntime.evaluateInstructions")(
  <Input, Error, Services>(
    instructions: InstructionSource<Input, Error, Services>,
    input: Input,
  ): Effect.Effect<Prompt.RawInput, Error, Services> =>
    Effect.suspend(() => {
      const result = typeof instructions === "function" ? instructions(input) : instructions;
      return Effect.isEffect(result) ? result : Effect.succeed(result);
    }),
);

const encodeInput = Effect.fn("AgentRuntime.encodeInput")(
  <AgentValue extends Agent.Any>(
    agent: AgentValue,
    input: Agent.Input<AgentValue>,
  ): Effect.Effect<
    AgentValue["definition"]["input"]["Encoded"],
    AgentInputError,
    AgentValue["definition"]["input"]["EncodingServices"]
  > =>
    Schema.encodeEffect(agent.definition.input)(input).pipe(
      Effect.mapError((cause) =>
        AgentInputError.make({
          message: `Unable to encode Agent input: ${cause.message}`,
        }),
      ),
    ),
);

const makeInitialPrompt = Effect.fn("AgentRuntime.makeInitialPrompt")(
  (
    instructions: Prompt.RawInput,
    input: unknown,
    history: Prompt.Prompt,
  ): Effect.Effect<Prompt.Prompt, AgentInputError> =>
    Effect.try({
      try: () => {
        const encodedInput = JSON.stringify(input);
        if (encodedInput === undefined) {
          throw new Error("Agent input cannot be represented as JSON");
        }
        const instructionPrompt =
          typeof instructions === "string"
            ? Prompt.fromMessages([
                Prompt.makeMessage("system", {
                  content: instructions,
                }),
              ])
            : Prompt.make(instructions);
        return Prompt.fromMessages([
          ...history.content,
          ...instructionPrompt.content,
          Prompt.makeMessage("user", {
            content: [Prompt.makePart("text", { text: encodedInput })],
          }),
        ]);
      },
      catch: (cause) =>
        AgentInputError.make({
          message: `Unable to materialize Agent input: ${errorMessage(cause)}`,
        }),
    }),
);

const decodeToolCallId = Effect.fn("AgentRuntime.decodeToolCallId")((id: string) =>
  Schema.decodeEffect(ToolCallId)(id).pipe(
    Effect.mapError((cause) =>
      ModelProtocolError.make({
        message: `Invalid Tool Call ID: ${cause.message}`,
      }),
    ),
  ),
);

const decodeProviderToolCallId = Effect.fn("AgentRuntime.decodeProviderToolCallId")((id: string) =>
  Schema.decodeEffect(ProviderToolCallId)(id).pipe(
    Effect.mapError(() =>
      ModelProtocolError.make({
        message:
          "Model supplied an invalid Tool Call ID; expected 1-128 ASCII letters, digits, dots, underscores, colons, or hyphens",
      }),
    ),
  ),
);

const decodeProviderResponsePartId = Effect.fn("AgentRuntime.decodeProviderResponsePartId")(
  (id: string) =>
    Schema.decodeEffect(ProviderResponsePartId)(id).pipe(
      Effect.mapError(() =>
        ModelProtocolError.make({
          message:
            "Model supplied an invalid response part ID; expected 1-128 ASCII letters, digits, dots, underscores, colons, or hyphens",
        }),
      ),
    ),
);

const validateProviderPartIdentifiers = Effect.fnUntraced(function* (part: Response.AnyPart) {
  switch (part.type) {
    case "text-start":
    case "text-delta":
    case "text-end":
    case "reasoning-start":
    case "reasoning-delta":
    case "reasoning-end":
    case "source":
      yield* decodeProviderResponsePartId(part.id);
      return;
    case "response-metadata":
      if (part.id !== undefined) yield* decodeProviderResponsePartId(part.id);
      return;
    case "tool-params-start":
    case "tool-params-delta":
    case "tool-params-end":
    case "tool-call":
    case "tool-result":
      yield* decodeProviderToolCallId(part.id);
      return;
    case "tool-approval-request":
      yield* decodeProviderResponsePartId(part.approvalId);
      yield* decodeProviderToolCallId(part.toolCallId);
      return;
    case "error":
    case "file":
    case "finish":
    case "reasoning":
    case "text":
      return;
  }
});

const decodeEventJson = Effect.fn("AgentRuntime.decodeEventJson")(
  (value: unknown, label: string): Effect.Effect<Schema.Json, ModelProtocolError> =>
    Schema.decodeUnknownEffect(Schema.Json)(value).pipe(
      Effect.mapError((cause) =>
        ModelProtocolError.make({
          message: `${label} is not JSON: ${cause.message}`,
        }),
      ),
    ),
);

/**
 * Preserves provider-executed results as assistant content while application
 * handler results remain Tool messages for the next model request.
 */
const promptFromTurnParts = (trace: TurnTrace): Prompt.Prompt => {
  const responsePrompt = Prompt.fromResponseParts(
    trace.parts.filter((part) => !(part.type === "tool-result" && part.providerExecuted)),
  );
  const providerResults = trace.parts.flatMap((part) =>
    part.type === "tool-result" && part.providerExecuted && !part.preliminary
      ? [
          Prompt.makePart("tool-result", {
            id: part.id,
            name: part.name,
            isFailure: part.isFailure,
            result: part.encodedResult,
            providerExecuted: true,
          }),
        ]
      : [],
  );
  if (providerResults.length === 0) {
    return responsePrompt;
  }

  const messages: Array<Prompt.Message> = [];
  let attachedProviderResults = false;
  for (const message of responsePrompt.content) {
    if (message.role === "assistant") {
      messages.push(
        Prompt.makeMessage("assistant", {
          content: [...message.content, ...providerResults],
          options: message.options,
        }),
      );
      attachedProviderResults = true;
      continue;
    }
    if (message.role === "tool") {
      // Application results are added by the engine after scheduling. Provider
      // results are retained as assistant content above, never as a tool turn.
      continue;
    }
    messages.push(message);
  }
  if (!attachedProviderResults) {
    messages.push(Prompt.makeMessage("assistant", { content: providerResults }));
  }
  return Prompt.fromMessages(messages);
};

// `Effect.fnUntraced`: this dispatcher runs for every streamed Response Part,
// so a named span here would emit one span per TextDelta/ReasoningDelta. The
// enclosing model request keeps its `AgentRuntime.model` stream span.
const eventsForPart = Effect.fnUntraced(function* <Tools extends Record<string, Tool.Any>>(
  context: RunContext,
  turnId: TurnId,
  turn: number,
  tools: Tools,
  trace: TurnTrace,
  part: Response.AnyPart,
): Effect.fn.Return<
  ReadonlyArray<RunEvent>,
  ModelProtocolError,
  Tool.HandlerServices<ToolUnion<Tools>>
> {
  yield* consumeModelResponsePart(trace, part, context.bufferLimits);
  if (trace.finished) {
    return yield* ModelProtocolError.make({
      message: "Model response emitted content after its finish part",
    });
  }
  // Provider/model identifiers are untrusted correlation keys. Reject them before they enter
  // the Turn trace, lifecycle maps, canonical event stream, diagnostics, or Tool scheduler.
  yield* validateProviderPartIdentifiers(part);
  if (part.type !== "tool-call" && part.type !== "tool-result") trace.parts.push(part);
  switch (part.type) {
    case "text-start": {
      yield* startPart(trace.textParts, part.id, "text");
      return [];
    }
    case "text-delta": {
      yield* continuePart(trace.textParts, part.id, "text delta");
      trace.text.push(part.delta);
      return [
        TextDelta.make({
          ...(yield* eventBase(context)),
          turnId,
          text: part.delta,
        }),
      ];
    }
    case "text-end": {
      yield* endPart(trace.textParts, part.id, "text");
      return [];
    }
    case "reasoning-start": {
      yield* startPart(trace.reasoningParts, part.id, "reasoning");
      return [];
    }
    case "reasoning-delta": {
      yield* continuePart(trace.reasoningParts, part.id, "reasoning delta");
      return [
        ReasoningDelta.make({
          ...(yield* eventBase(context)),
          turnId,
          text: part.delta,
        }),
      ];
    }
    case "reasoning-end": {
      yield* endPart(trace.reasoningParts, part.id, "reasoning");
      return [];
    }
    case "tool-params-start": {
      if (trace.toolParameterParts.has(part.id)) {
        return yield* ModelProtocolError.make({
          message: `Model response repeated Tool parameter start for ${part.id}`,
        });
      }
      trace.toolParameterParts.set(part.id, {
        name: part.name,
        providerExecuted: part.providerExecuted,
        state: "open",
      });
      return [];
    }
    case "tool-params-delta": {
      const parameterPart = trace.toolParameterParts.get(part.id);
      if (parameterPart?.state !== "open") {
        return yield* ModelProtocolError.make({
          message: `Model response emitted Tool parameter delta for inactive part ${part.id}`,
        });
      }
      return [];
    }
    case "tool-params-end": {
      const parameterPart = trace.toolParameterParts.get(part.id);
      if (parameterPart?.state !== "open") {
        return yield* ModelProtocolError.make({
          message: `Model response emitted Tool parameter end for inactive part ${part.id}`,
        });
      }
      parameterPart.state = "closed";
      return [];
    }
    case "tool-call": {
      if (!hasTool(tools, part.name)) {
        return yield* ModelProtocolError.make({
          message: `Model requested unknown Tool ${part.name}`,
        });
      }
      if (trace.toolCalls.has(part.id)) {
        return yield* ModelProtocolError.make({
          message: `Model response repeated Tool Call ID ${part.id}`,
        });
      }
      const parameterPart = trace.toolParameterParts.get(part.id);
      if (parameterPart?.state === "open") {
        return yield* ModelProtocolError.make({
          message: `Model response declared Tool Call ${part.id} before its parameters completed`,
        });
      }
      if (
        parameterPart !== undefined &&
        (parameterPart.name !== part.name ||
          parameterPart.providerExecuted !== part.providerExecuted)
      ) {
        return yield* ModelProtocolError.make({
          message: `Completed Tool parameters did not match Tool Call ${part.id}`,
        });
      }
      const toolCallId = yield* decodeToolCallId(part.id);
      const tool = tools[part.name] as ToolUnion<Tools>;
      const encodedParameters = yield* encodeToolCallParameters<Tools>(
        tool,
        part.name,
        part.params as Tool.Parameters<ToolUnion<Tools>>,
      );
      const parameters = yield* decodeEventJson(encodedParameters, "Tool parameters");
      // Official history carries the wire form: rebuild the trace
      // part with the Schema-encoded parameters (the provider re-serializes
      // them on the next request regardless), while `applicationToolCalls`
      // below keeps the decoded part for the handler path — see TurnTrace.
      trace.parts.push(
        Response.makePart("tool-call", {
          id: part.id,
          name: part.name,
          params: parameters,
          providerExecuted: part.providerExecuted,
          metadata: part.metadata,
        }),
      );
      trace.toolCalls.set(part.id, {
        name: part.name,
        providerExecuted: part.providerExecuted,
      });
      if (!part.providerExecuted) {
        trace.applicationToolCalls.push(part);
        trace.applicationCallDescriptors.push({
          toolCallId,
          toolName: part.name,
          parameters,
          executionClass: getToolExecutionClass(tool),
        });
      }
      const declared = ToolCallDeclared.make({
        ...(yield* eventBase(context)),
        turnId,
        toolCallId,
        toolName: part.name,
        parameters,
        providerExecuted: part.providerExecuted,
      });
      return [declared];
    }
    case "tool-result": {
      const declaredCall = trace.toolCalls.get(part.id);
      if (declaredCall === undefined) {
        return yield* ModelProtocolError.make({
          message: `Model response returned an unrequested Tool result ${part.id}`,
        });
      }
      if (declaredCall.name !== part.name) {
        return yield* ModelProtocolError.make({
          message: `Tool result name did not match Tool Call ${part.id}: expected ${declaredCall.name}, received ${part.name}`,
        });
      }
      if (declaredCall.providerExecuted !== part.providerExecuted) {
        return yield* ModelProtocolError.make({
          message: `Tool result execution boundary did not match Tool Call ${part.id}`,
        });
      }
      if (!part.providerExecuted) {
        return yield* ModelProtocolError.make({
          message: `Model response included an application Tool result before engine execution for ${part.id}`,
        });
      }
      const toolCallId = yield* decodeToolCallId(part.id);
      const normalized = yield* snapshotProviderToolResultPart(trace, part);
      const result = normalized.result;
      if (part.preliminary === true) {
        yield* stageProviderResultPayload(trace, {
          _tag: "ToolProgress",
          toolCallId,
          toolName: part.name,
          result,
          providerExecuted: true,
        });
        trace.parts.push(
          Response.toolResultPart({
            id: part.id,
            name: part.name,
            isFailure: part.isFailure,
            result,
            encodedResult: result,
            providerExecuted: true,
            preliminary: true,
            metadata: normalized.metadata,
          }),
        );
        return [];
      }
      if (trace.finalToolResultIds.has(part.id)) {
        return yield* ModelProtocolError.make({
          message: `Tool Call ${part.id} produced more than one terminal result`,
        });
      }
      if (part.isFailure) {
        yield* stageProviderResultPayload(trace, {
          _tag: "ToolCallFailed",
          toolCallId,
          toolName: part.name,
          errorTag: errorTag(result),
          message: errorMessage(result),
          providerExecuted: true,
        });
      } else {
        yield* stageProviderResultPayload(trace, {
          _tag: "ToolCallSucceeded",
          toolCallId,
          toolName: part.name,
          result,
          providerExecuted: true,
        });
      }
      trace.finalToolResultIds.add(part.id);
      trace.parts.push(
        Response.toolResultPart({
          id: part.id,
          name: part.name,
          isFailure: part.isFailure,
          result,
          encodedResult: result,
          providerExecuted: true,
          preliminary: false,
          metadata: normalized.metadata,
        }),
      );
      return [];
    }
    case "finish": {
      const openPart = firstOpenPart(trace);
      if (openPart !== undefined) {
        return yield* ModelProtocolError.make({
          message: `Model response finished before completing ${openPart}`,
        });
      }
      trace.finished = true;
      trace.finishReason = part.reason;
      trace.usage = part.usage;
      if (Array.from(trace.toolCalls.values()).some(({ providerExecuted }) => providerExecuted)) {
        const turnCompletion = { finishReason: part.reason };
        const snapshot = yield* snapshotStagedProviderEvent(trace, {
          _tag: "TurnCompleted",
          ...turnCompletion,
        });
        trace.turnCompletion = turnCompletion;
        trace.providerStagedPayloadBytes += snapshot.bytes;
        return [];
      }
      return [
        TurnCompleted.make({
          ...(yield* eventBase(context)),
          turnId,
          turn,
          finishReason: part.reason,
        }),
      ];
    }
    case "error": {
      return yield* ModelProtocolError.make({
        message: `Model response failed: ${errorMessage(part.error)}`,
      });
    }
    case "file":
    case "reasoning":
    case "response-metadata":
    case "source":
    case "text":
    case "tool-approval-request": {
      return [];
    }
  }
});

const decodeFinalOutput = Effect.fn("AgentRuntime.decodeFinalOutput")(function* <
  AgentValue extends Agent.Any,
>(
  agent: AgentValue,
  text: string,
): Effect.fn.Return<
  { readonly encoded: Schema.Json; readonly decoded: Agent.Output<AgentValue> },
  AgentOutputError,
  AgentValue["definition"]["output"]["DecodingServices"]
> {
  const eventJson = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json))(
    text,
  ).pipe(
    Effect.mapError((cause) =>
      AgentOutputError.make({
        message: `Agent output is not valid JSON: ${cause.message}`,
      }),
    ),
  );
  const candidateOutput: unknown = eventJson;
  const decoded = yield* Schema.decodeUnknownEffect(agent.definition.output)(candidateOutput).pipe(
    Effect.mapError((cause) =>
      AgentOutputError.make({
        message: cause.message,
      }),
    ),
  );
  return { encoded: eventJson, decoded };
});

const encodeRunDispositionCandidate = Effect.fn("AgentRuntime.encodeRunDisposition")(function* <
  Output,
  DispositionSchema extends Schema.Top,
>(
  declaration: RunDispositionDeclaration<Output, DispositionSchema>,
  output: Output,
): Effect.fn.Return<
  Schema.Json | undefined,
  AgentRunDispositionError,
  DispositionSchema["EncodingServices"]
> {
  const selected = yield* Effect.try({
    try: () => declaration.fromOutput(output),
    catch: (cause) =>
      AgentRunDispositionError.make({
        cause,
        message: "Run disposition selector failed",
      }),
  });
  if (selected === undefined) return undefined;
  const encoded = yield* Schema.encodeUnknownEffect(declaration.schema)(selected).pipe(
    Effect.mapError((cause) =>
      AgentRunDispositionError.make({
        cause,
        message: "Run disposition failed Schema encoding",
      }),
    ),
  );
  return yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
    Effect.mapError((cause) =>
      AgentRunDispositionError.make({
        cause,
        message: "Run disposition did not encode as durable JSON",
      }),
    ),
  );
});

function encodeRunDisposition<AgentValue extends Agent.Any>(
  agent: AgentValue,
  output: Agent.Output<AgentValue>,
): Effect.Effect<
  Schema.Json | undefined,
  Agent.RunDispositionFailure<AgentValue>,
  Agent.RunDispositionSchema<AgentValue>["EncodingServices"]
>;
function encodeRunDisposition<Output, DispositionSchema extends Schema.Top>(
  agent: {
    readonly definition: {
      readonly runDisposition?: RunDispositionDeclaration<Output, DispositionSchema> | undefined;
    };
  },
  output: Output,
): Effect.Effect<
  Schema.Json | void,
  AgentRunDispositionError,
  DispositionSchema["EncodingServices"]
> {
  const declaration = agent.definition.runDisposition;
  return declaration === undefined
    ? Effect.void
    : encodeRunDispositionCandidate(declaration, output);
}

const decodeRunDispositionCandidate = Effect.fn("AgentRuntime.decodeRunDisposition")(function* <
  Output,
  DispositionSchema extends Schema.Top,
>(
  declaration: RunDispositionDeclaration<Output, DispositionSchema>,
  encoded: Schema.Json,
): Effect.fn.Return<
  DispositionSchema["Type"],
  AgentRunDispositionError,
  DispositionSchema["DecodingServices"]
> {
  return yield* Schema.decodeUnknownEffect(declaration.schema)(encoded).pipe(
    Effect.mapError((cause) =>
      AgentRunDispositionError.make({
        cause,
        message: cause.message,
      }),
    ),
  );
});

function decodeRunDisposition<AgentValue extends Agent.Any>(
  agent: AgentValue,
  encoded: Schema.Json,
): Effect.Effect<
  Agent.RunDisposition<AgentValue>,
  Agent.RunDispositionFailure<AgentValue> | ModelProtocolError,
  Agent.RunDispositionSchema<AgentValue>["DecodingServices"]
>;
function decodeRunDisposition<DispositionSchema extends Schema.Top>(
  agent: {
    readonly definition: {
      readonly runDisposition?: RunDispositionDeclaration<never, DispositionSchema> | undefined;
    };
  },
  encoded: Schema.Json,
): Effect.Effect<
  DispositionSchema["Type"],
  AgentRunDispositionError | ModelProtocolError,
  DispositionSchema["DecodingServices"]
> {
  const declaration = agent.definition.runDisposition;
  return declaration === undefined
    ? Effect.fail(
        ModelProtocolError.make({
          message: "RunCompleted declared a run disposition without a definition-owned Schema",
        }),
      )
    : decodeRunDispositionCandidate(declaration, encoded);
}

const makeTurn = <
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  HookError,
  HookRequirements,
  InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
>(
  agent: RuntimeBinding<
    InputSchema,
    OutputSchema,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements,
    RunDispositionValue
  >,
  context: RunContext,
  prompt: Prompt.Prompt,
  turn: number,
  priorToolCalls: number,
  options: RunOptions<HookError, HookRequirements>,
): Stream.Stream<
  RunEvent,
  AgentRuntimeFailure<typeof agent, HookError, InstructionError>,
  InterpreterRequirements<typeof agent, HookRequirements, InstructionRequirements>
> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      if (now >= context.durationDeadlineMillis) {
        return failRunEventStream(durationLimitError(agent.definition.policy));
      }
      const ids = yield* IdGenerator;
      const turnId = yield* ids.nextTurnId;
      // Model-visible final-output contract (RUN-028):
      // derived before context preparation so a limit-targeting adapter can
      // reserve the contract's overhead in its window calculation, applied to
      // the request after preparation so compaction cannot drop it, and never
      // entered into official history, so canonical records are unchanged.
      // An unrenderable output Schema falls back to the prior behavior with
      // one Turn-1 diagnostic.
      const outputContract = outputSchemaContract(agent.definition);
      const outputContractMessage =
        outputContract._tag === "rendered" ? outputContract.message : undefined;
      const modelContext =
        options.context === undefined
          ? { prompt }
          : yield* options.context.prepare({
              conversationId: context.conversationId,
              runId: context.runId,
              turnId,
              turn,
              source: prompt,
              // Omit the key entirely when no contract renders so the
              // fallback request is byte-identical to the prior behavior.
              ...(outputContractMessage === undefined
                ? {}
                : { outputContract: outputContractMessage }),
            });
      const trace: TurnTrace = {
        responsePartCount: 0,
        responsePartBytes: 0,
        parts: [],
        text: [],
        textParts: new Map(),
        reasoningParts: new Map(),
        toolParameterParts: new Map(),
        toolCalls: new Map(),
        finalToolResultIds: new Set(),
        providerResultPayloads: [],
        providerStagedPayloadBytes: 0,
        turnCompletion: undefined,
        applicationToolCalls: [],
        applicationCallDescriptors: [],
        applicationToolResults: [],
        finished: false,
        finishReason: undefined,
        usage: undefined,
      };

      const started = Stream.fromEffect(
        Effect.gen(function* () {
          yield* Metric.update(modelCounter, 1);
          yield* Effect.logDebug("agent model call started").pipe(
            Effect.annotateLogs({
              agentId: context.agentId,
              runId: context.runId,
              turnId,
            }),
          );
          return [
            TurnStarted.make({
              ...(yield* eventBase(context)),
              turnId,
              turn,
            }),
            ModelStarted.make({
              ...(yield* eventBase(context)),
              turnId,
              turn,
            }),
          ] satisfies ReadonlyArray<RunEvent>;
        }).pipe(Effect.withLogSpan("AgentRuntime.model")),
      ).pipe(Stream.flatMap(Stream.fromIterable));

      // Final-answer mode (RUN-018/RUN-019, extended to the token dimension
      // by RUN-025 to the token dimension): once the Turn, Tool Call, or token budget is
      // exhausted, the model keeps its toolkit declaration but may not call
      // it. Turn and Tool Call conditions are pure derivations of committed
      // state (`turn`, `priorToolCalls`, `programmaticToolCalls`); the token
      // condition is the one-shot `tokenExhausted` flag consumeUsage stamps
      // when the cumulative budget breaches under `"final-answer"` (durable
      // resume re-seeds the counters, so the derivation survives ownership
      // changes). Strict `>` keeps an exact-cap Run on today's unconstrained
      // path byte-for-byte.
      const policy = agent.definition.policy;
      const bounds = effectiveRunBounds(policy, options);
      const finalAnswerOnly =
        policy.onExhaustion !== "fail" &&
        (turn > bounds.maxTurns ||
          priorToolCalls + context.programmaticToolCalls > bounds.maxToolCalls ||
          context.tokenExhausted);
      if (finalAnswerOnly && context.exhaustedDimension === undefined) {
        // First-cause dimension marker (the RUN-021 grant-flow marker).
        context.exhaustedDimension =
          turn > bounds.maxTurns
            ? "turns"
            : priorToolCalls + context.programmaticToolCalls > bounds.maxToolCalls
              ? "tool-calls"
              : "tokens";
      }
      if (outputContract._tag === "unrenderable" && turn === 1) {
        yield* Effect.logWarning(
          "Agent output schema cannot render to JSON Schema; the model-visible final output contract is omitted",
        ).pipe(
          Effect.annotateLogs({
            agentId: context.agentId,
            runId: context.runId,
            reason: outputContract.reason,
          }),
        );
      }
      // The output contract (RUN-028) rides every outgoing request after
      // compaction, so the window calculation reserves its estimated size the
      // same way Tool-schema overhead is provider-side reality: the view must
      // fit the limit WITH the contract the engine will append.
      const outputContractTokens =
        outputContractMessage === undefined
          ? 0
          : estimatePromptTokens([
              Prompt.makeMessage("system", { content: outputContractMessage }),
            ]);
      let preEvents: ReadonlyArray<RunEvent> = [];
      if (policy.contextTokenLimit !== undefined && !context.finalizing) {
        const view = buildCompactedView(modelContext.prompt.content, context.compaction);
        const estimate = nextContextEstimate(context, view);
        if (
          estimate + outputContractTokens > policy.contextTokenLimit &&
          context.compaction.lastCompactionTurn !== turn
        ) {
          // Loop guard: at most one threshold compaction per Turn. If the
          // post-compaction estimate still exceeds the limit the Turn
          // proceeds anyway — the provider may accept, and RUN-027 overflow
          // recovery is the typed backstop when it does not.
          context.compaction.lastCompactionTurn = turn;
          const outcome = yield* compactContext(
            agent,
            context,
            modelContext.prompt,
            turn,
            options,
            false,
          );
          preEvents = outcome.events;
        }
      }
      /** The model-visible view of the Turn basis under current compaction state. */
      const compactedOutgoing = (): Prompt.Prompt => {
        const view = buildCompactedView(modelContext.prompt.content, context.compaction);
        context.compaction.lastViewLength = view.length;
        return Prompt.fromMessages([...view]);
      };
      const attempt = (basis: Prompt.Prompt) =>
        Stream.unwrap(
          outgoingModelPrompt(policy, context, basis, turn, priorToolCalls).pipe(
            Effect.map((outgoing) =>
              guardBudgetStream(
                LanguageModel.streamText({
                  // The contract joins the final outgoing prompt (after
                  // compaction and the run-status append), so every attempt —
                  // including the overflow retry — carries it at the last
                  // system block.
                  prompt:
                    outputContractMessage === undefined
                      ? outgoing
                      : insertOutputContract(outgoing, outputContractMessage),
                  toolkit: agent.definition.toolkit,
                  disableToolCallResolution: true,
                  ...(finalAnswerOnly ? { toolChoice: "none" as const } : {}),
                }),
                options.budget,
              ).pipe(
                Stream.mapEffect((part) =>
                  eventsForPart(context, turnId, turn, agent.definition.toolkit.tools, trace, part),
                ),
                Stream.flatMap(Stream.fromIterable),
              ),
            ),
          ),
        );
      // RUN-027: one summarize-and-retry for a classified provider context
      // overflow when compaction is configured; every other provider error
      // propagates unchanged. A response that already streamed parts mutated
      // the trace, so it is never retried.
      const response = attempt(compactedOutgoing()).pipe(
        Stream.catch(
          (
            error,
          ): Stream.Stream<
            RunEvent,
            AgentRuntimeFailure<typeof agent, HookError, InstructionError>,
            InterpreterRequirements<typeof agent, HookRequirements, InstructionRequirements>
          > => {
            if (
              !(error instanceof AiError.AiError) ||
              !isContextOverflowMessage(overflowText(error))
            ) {
              return Stream.fail(error);
            }
            const message = overflowText(error);
            if (trace.parts.length > 0 || policy.contextTokenLimit === undefined) {
              return Stream.fail(ContextOverflowError.make({ message, retried: false }));
            }
            if (context.compaction.overflowRetryTurn === turn) {
              return Stream.fail(ContextOverflowError.make({ message, retried: true }));
            }
            context.compaction.overflowRetryTurn = turn;
            context.compaction.lastCompactionTurn = turn;
            type TurnStream = Stream.Stream<
              RunEvent,
              AgentRuntimeFailure<typeof agent, HookError, InstructionError>,
              InterpreterRequirements<typeof agent, HookRequirements, InstructionRequirements>
            >;
            return Stream.unwrap(
              Effect.gen(function* () {
                const outcome = yield* compactContext(
                  agent,
                  context,
                  modelContext.prompt,
                  turn,
                  options,
                  true,
                ).pipe(
                  Effect.mapError(
                    (inner): AgentRuntimeFailure<typeof agent, HookError, InstructionError> =>
                      inner instanceof AiError.AiError &&
                      isContextOverflowMessage(overflowText(inner))
                        ? ContextOverflowError.make({
                            message: overflowText(inner),
                            retried: true,
                          })
                        : inner,
                  ),
                );
                // The retried call is outside the outer catch: a second
                // classified overflow converts here, typed, no retry.
                const retried: TurnStream = attempt(compactedOutgoing()).pipe(
                  Stream.catch(
                    (again): TurnStream =>
                      again instanceof AiError.AiError &&
                      isContextOverflowMessage(overflowText(again))
                        ? Stream.fail(
                            ContextOverflowError.make({
                              message: overflowText(again),
                              retried: true,
                            }),
                          )
                        : Stream.fail(again),
                  ),
                );
                const events: TurnStream = Stream.fromIterable(outcome.events);
                return events.pipe(Stream.concat(retried));
              }),
            );
          },
        ),
        Stream.withSpan("AgentRuntime.model", {
          attributes: {
            agentId: context.agentId,
            runId: context.runId,
            turnId,
          },
        }),
      );

      const continuation = Stream.unwrap(
        Effect.sync(() => {
          if (!trace.finished) {
            return failRunEventStream(
              ModelProtocolError.make({
                message: "Model response ended without a finish part",
              }),
            );
          }
          const hasProviderCalls = Array.from(trace.toolCalls.values()).some(
            ({ providerExecuted }) => providerExecuted,
          );
          const turnCompletion = trace.turnCompletion;
          if (hasProviderCalls && turnCompletion === undefined) {
            return failRunEventStream(
              ModelProtocolError.make({ message: "Model response omitted staged Turn completion" }),
            );
          }
          // Fail-closed (RUN-020): a final-answer Turn requested with
          // `toolChoice: "none"`, so any declared call — application or
          // provider-executed — is a protocol violation, never another
          // rejection round.
          if (finalAnswerOnly && trace.toolCalls.size > 0) {
            return failRunEventStream(
              ModelProtocolError.make({
                message: `Model declared ${trace.toolCalls.size} Tool Call(s) under toolChoice "none" after budget exhaustion`,
              }),
            );
          }
          const providerOnly =
            trace.toolCalls.size > 0 &&
            Array.from(trace.toolCalls.values()).every(({ providerExecuted }) => providerExecuted);
          const missingProviderResult = Array.from(trace.toolCalls.entries()).find(
            ([id, call]) => call.providerExecuted && !trace.finalToolResultIds.has(id),
          );
          if (missingProviderResult !== undefined) {
            return failRunEventStream(
              ModelProtocolError.make({
                message: `Provider-executed Tool Call ${missingProviderResult[0]} completed without a terminal result`,
              }),
            );
          }
          const toolCalls = priorToolCalls + trace.toolCalls.size;
          const overToolBudget = toolCalls + context.programmaticToolCalls > bounds.maxToolCalls;
          if (overToolBudget && policy.onExhaustion === "fail") {
            return failRunEventStream(
              AgentPolicyError.make({
                limit: "tool-calls",
                message: `Agent exceeded its ${bounds.maxToolCalls} Tool Call limit`,
              }),
            );
          }

          const stagedResponse = Stream.fromIterable(trace.providerResultPayloads).pipe(
            Stream.mapEffect((payload) => stampProviderResultEvent(context, turnId, payload)),
            Stream.concat(
              turnCompletion === undefined
                ? Stream.empty
                : Stream.fromEffect(
                    Effect.map(eventBase(context), (base) =>
                      TurnCompleted.make({
                        ...base,
                        turnId,
                        turn,
                        finishReason: turnCompletion.finishReason,
                      }),
                    ),
                  ),
            ),
          );

          /** Official history advanced through this Turn's response, plus any Tool message. */
          const historyWithResponse = (...additions: ReadonlyArray<Prompt.Message>) =>
            Prompt.fromMessages([
              ...prompt.content,
              ...promptFromTurnParts(trace).content,
              ...additions,
            ]);

          /**
           * Post-validation seam: charge the response's usage (RUN-023), stage
           * it for the Turn's canonical commit, emit one-shot `BudgetWarning`
           * advisories (RUN-025), and resolve a token-budget breach before
           * `next` continues the Run. Only the token dimension surfaces as a
           * `consumed.breach` — fail-mode and cost breaches already failed
           * inside `consumeUsage`. A breaching stop response that decodes as
           * the final output completes directly (the answer exists, so no
           * grace call is spent); a breaching Tool-declaring response settles
           * its batch synthetically through the RUN-018 path and the next
           * Turn is final-answer constrained via `tokenExhausted`.
           */
          type TurnEvents = Stream.Stream<
            RunEvent,
            AgentRuntimeFailure<typeof agent, HookError, InstructionError>,
            InterpreterRequirements<typeof agent, HookRequirements, InstructionRequirements>
          >;
          const afterValidatedResponse = (
            next: Effect.Effect<
              TurnEvents,
              AgentRuntimeFailure<typeof agent, HookError, InstructionError>,
              InterpreterRequirements<typeof agent, HookRequirements, InstructionRequirements>
            >,
          ): TurnEvents =>
            stagedResponse.pipe(
              Stream.concat(
                Stream.unwrap(
                  Effect.gen(function* () {
                    const consumed = yield* consumeUsage(
                      agent,
                      context,
                      trace.usage,
                      trace.toolCalls.size,
                      options,
                    );
                    if (options.durability !== undefined) {
                      yield* options.durability.noteTurnUsage({
                        turn,
                        inputTokens: context.lastInputTokens,
                        outputTokens: context.lastOutputTokens,
                        costMicrousd: context.lastCostMicrousd,
                      });
                    }
                    const pre: Array<RunEvent> = [...consumed.warnings];
                    if (
                      !context.warnedLimits.has("tool-calls") &&
                      nearingLimit(toolCalls + context.programmaticToolCalls, bounds.maxToolCalls)
                    ) {
                      context.warnedLimits.add("tool-calls");
                      pre.push(
                        BudgetWarning.make({
                          ...(yield* eventBase(context)),
                          limit: "tool-calls",
                          consumed: toolCalls + context.programmaticToolCalls,
                          limitValue: bounds.maxToolCalls,
                        }),
                      );
                    }
                    if (!context.warnedLimits.has("turns") && nearingLimit(turn, bounds.maxTurns)) {
                      context.warnedLimits.add("turns");
                      pre.push(
                        BudgetWarning.make({
                          ...(yield* eventBase(context)),
                          limit: "turns",
                          consumed: turn,
                          limitValue: bounds.maxTurns,
                        }),
                      );
                    }
                    const emitThen = <NextError, NextRequirements>(
                      nextStream: Stream.Stream<RunEvent, NextError, NextRequirements>,
                    ): Stream.Stream<RunEvent, NextError, NextRequirements> =>
                      pre.length === 0
                        ? nextStream
                        : Stream.fromIterable(pre).pipe(Stream.concat(nextStream));
                    if (consumed.breach !== undefined) {
                      // Provider-executed calls already ran provider-side: a
                      // stop response with no APPLICATION calls is final and
                      // settles the breach directly (RUN-025).
                      if (
                        trace.applicationToolCalls.length === 0 &&
                        trace.finishReason === "stop"
                      ) {
                        const output = yield* decodeFinalOutput(agent, trace.text.join("")).pipe(
                          Effect.map(Option.some),
                          Effect.catch(() => Effect.succeed(Option.none())),
                        );
                        if (Option.isSome(output)) {
                          yield* advanceHistory(context, historyWithResponse(), options);
                          return emitThen(
                            Stream.fromEffect(
                              Effect.map(eventBase(context), (base) =>
                                RunCompleted.make({
                                  ...base,
                                  output: output.value.encoded,
                                  turns: turn,
                                  finishReason: "budget-exhausted",
                                  exhausted: "tokens",
                                }),
                              ),
                            ),
                          );
                        }
                      }
                      if (trace.applicationToolCalls.length > 0) {
                        // RUN-025 joins the RUN-018 synthetic-settlement path:
                        // the token-breaching batch never executes a handler,
                        // and `tokenExhausted` (stamped by `consumeUsage`)
                        // constrains every subsequent request.
                        const rejection = yield* settleRejectedBatch(
                          context,
                          turnId,
                          trace,
                          AgentPolicyError.make({
                            limit: "tokens",
                            message: `Token budget exhausted: this Run's ${policy.tokenBudget ?? 0} token budget was reached, so this call was rejected without executing. Do not request more tools; produce your final answer now from the information you already have.`,
                          }),
                        );
                        return emitThen(
                          Stream.fromIterable(rejection).pipe(
                            Stream.concat(
                              toolBatchContinuation(
                                agent,
                                context,
                                trace,
                                prompt,
                                turn,
                                toolCalls,
                                options,
                              ),
                            ),
                          ),
                        );
                      }
                      // A breaching stop response without decodable output
                      // falls through: the ordinary settle decodes (and fails
                      // typed) exactly as it would without the breach.
                    }
                    return emitThen(yield* next);
                  }),
                ),
              ),
            );

          /**
           * Advance official history for a Turn that always continues, drain
           * steering at the safe seam, and start the next Turn. The `maxTurns`
           * guard already ran for these Tool Call sites.
           */
          const continueTurn = (history: Prompt.Prompt) =>
            Effect.gen(function* () {
              yield* advanceHistory(context, history, options);
              const steering = yield* drainInputs(context, options);
              const nextPrompt = yield* appendInputs(context, history, steering, options);
              return makeTurn(agent, context, nextPrompt, turn + 1, toolCalls, options);
            });

          /**
           * The otherwise-stop seam: advance official history, drain steering,
           * fall back to buffered follow-ups, then either start the next Turn
           * under the `maxTurns` and repeated-failure Stop Policy bounds or
           * complete the Run with the Turn's decoded final output.
           */
          const settleOrFollowUp = (history: Prompt.Prompt) =>
            Effect.gen(function* () {
              yield* advanceHistory(context, history, options);
              const steering = yield* drainInputs(context, options);
              const queued =
                steering.length > 0
                  ? steering
                  : takeFollowUps(context, options.commandDrainPolicy ?? "one");
              if (queued.length > 0) {
                // Final-answer mode admits exactly one grace Turn past
                // `maxTurns` (RUN-019): `turn > maxTurns` can only be
                // `maxTurns + 1`, so a second grace is structurally
                // impossible.
                const turnsBlocked =
                  policy.onExhaustion === "fail" ? turn >= bounds.maxTurns : turn > bounds.maxTurns;
                if (turnsBlocked) {
                  return failRunEventStream(
                    AgentPolicyError.make({
                      limit: "turns",
                      message: `Agent exceeded its ${bounds.maxTurns} Turn limit`,
                    }),
                  );
                }
                yield* applyRepeatedFailurePolicy(
                  context,
                  trace,
                  agent.definition.policy.repeatedFailureLimit,
                );
                const nextPrompt = yield* appendInputs(context, history, queued, options);
                return makeTurn(agent, context, nextPrompt, turn + 1, toolCalls, options);
              }
              const output = yield* decodeFinalOutput(agent, trace.text.join(""));
              const declaration = agent.definition.runDisposition;
              const runDisposition =
                finalAnswerOnly || declaration === undefined
                  ? undefined
                  : yield* encodeRunDisposition(agent, output.decoded);
              return Stream.fromEffect(
                Effect.map(eventBase(context), (base) =>
                  RunCompleted.make({
                    ...base,
                    output: output.encoded,
                    ...(runDisposition === undefined ? {} : { runDisposition }),
                    turns: turn,
                    // A Run settled under the final-answer constraint reports
                    // the exhaustion honestly (RUN-011), never a plain model
                    // stop, and carries the dimension that bound.
                    finishReason: finalAnswerOnly ? "budget-exhausted" : "model-stop",
                    ...(finalAnswerOnly && context.exhaustedDimension !== undefined
                      ? { exhausted: context.exhaustedDimension }
                      : {}),
                  }),
                ),
              );
            });

          if (providerOnly && trace.finishReason === "stop") {
            return afterValidatedResponse(settleOrFollowUp(historyWithResponse()));
          }

          if (trace.toolCalls.size > 0) {
            if (trace.finishReason !== "tool-calls") {
              return failRunEventStream(
                ModelProtocolError.make({
                  message: `Model declared Tool Calls with incompatible finish reason ${trace.finishReason}`,
                }),
              );
            }
            const turnsBlocked =
              policy.onExhaustion === "fail" ? turn >= bounds.maxTurns : turn > bounds.maxTurns;
            if (turnsBlocked) {
              return failRunEventStream(
                AgentPolicyError.make({
                  limit: "turns",
                  message: `Agent exceeded its ${bounds.maxTurns} Turn limit`,
                }),
              );
            }
            // RUN-018: the over-budget batch never executes a handler and is
            // never durably declared — it settles synthetically through the
            // ordinary batch continuation, so the model sees one failed
            // result per rejected call and the next Turn is final-answer
            // constrained. `commitResponse` is deliberately skipped: with no
            // response commit the Turn stays on the single-batch canonical
            // commit shape and recovery replays it like any no-tool Turn. The
            // rejected Turn's usage is still charged via
            // `afterValidatedResponse` because the Run continues.
            if (overToolBudget && trace.applicationToolCalls.length > 0) {
              return afterValidatedResponse(
                Effect.gen(function* () {
                  const rejection = yield* settleRejectedBatch(
                    context,
                    turnId,
                    trace,
                    AgentPolicyError.make({
                      limit: "tool-calls",
                      message: `Tool Call budget exhausted: this Run's ${bounds.maxToolCalls} Tool Call limit was reached, so this call was rejected without executing. Do not request more tools; produce your final answer now from the information you already have.`,
                    }),
                  );
                  return Stream.fromIterable(rejection).pipe(
                    Stream.concat(
                      toolBatchContinuation(
                        agent,
                        context,
                        trace,
                        prompt,
                        turn,
                        toolCalls,
                        options,
                      ),
                    ),
                  );
                }),
              );
            }
            if (trace.applicationToolCalls.length === 0) {
              return afterValidatedResponse(
                Effect.gen(function* () {
                  yield* applyRepeatedFailurePolicy(
                    context,
                    trace,
                    agent.definition.policy.repeatedFailureLimit,
                  );
                  return yield* continueTurn(historyWithResponse());
                }),
              );
            }
            return afterValidatedResponse(
              Effect.gen(function* () {
                const toolkit = yield* agent.definition.toolkit;
                const concurrency = yield* schedulingConcurrency(
                  agent.definition.policy.toolConcurrency,
                  options.scheduling,
                );
                if (options.durability !== undefined) {
                  // Turn-response commit seam: staged canonical response events are emitted before
                  // this persistence mutation, while approval preflight and preparation still run
                  // afterward. This retains durability §15's provably-safe resume window without
                  // allowing eager continuation work to overtake the append-only event stream.
                  yield* options.durability.commitResponse({
                    turn,
                    turnId,
                    responseMessages: promptFromTurnParts(trace),
                    calls: trace.applicationCallDescriptors,
                  });
                }
                const toolResults = guardBudgetStream(
                  executeToolBatch(
                    context,
                    turnId,
                    turn,
                    toolkit,
                    trace.applicationToolCalls,
                    trace,
                    concurrency,
                    options,
                    {
                      maxToolCalls: bounds.maxToolCalls,
                      declaredToolCalls: toolCalls,
                    },
                    agent.definition.policy.toolResultBounds,
                  ),
                  options.budget,
                );
                return toolResults.pipe(
                  Stream.concat(
                    toolBatchContinuation(agent, context, trace, prompt, turn, toolCalls, options),
                  ),
                );
              }),
            );
          }

          if (trace.finishReason !== "stop") {
            return failRunEventStream(
              ModelProtocolError.make({
                message: `Model stopped without a final answer (${trace.finishReason})`,
              }),
            );
          }
          return afterValidatedResponse(settleOrFollowUp(historyWithResponse()));
        }),
      );

      return Stream.fromIterable(preEvents).pipe(
        Stream.concat(started),
        Stream.concat(response),
        Stream.concat(continuation),
      );
    }),
  );

/**
 * Settle a completed application Tool batch and continue the Run: verify one
 * final result per declared call, fold outcomes into the repeated-failure
 * Stop Policy, advance official history with the Tool message in declaration
 * order, drain steering at the safe seam, and start the next Turn. Shared by
 * the ordinary model-declared path (`makeTurn`) and the durable batch-resume
 * path (`makeResumeTurn`).
 */
const toolBatchContinuation = <
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  HookError,
  HookRequirements,
  InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
>(
  agent: RuntimeBinding<
    InputSchema,
    OutputSchema,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements,
    RunDispositionValue
  >,
  context: RunContext,
  trace: TurnTrace,
  prompt: Prompt.Prompt,
  turn: number,
  toolCalls: number,
  options: RunOptions<HookError, HookRequirements>,
): Stream.Stream<
  RunEvent,
  AgentRuntimeFailure<typeof agent, HookError, InstructionError>,
  InterpreterRequirements<typeof agent, HookRequirements, InstructionRequirements>
> =>
  Stream.unwrap(
    Effect.gen(function* () {
      if (trace.finalToolResultIds.size !== trace.toolCalls.size) {
        return failRunEventStream(
          ModelProtocolError.make({
            message: "A Tool Call turn completed without one final result per Tool Call",
          }),
        );
      }
      const orderedResults: Array<(typeof trace.applicationToolResults)[number]> = [];
      for (const call of trace.applicationToolCalls) {
        const result = trace.applicationToolResults.find((candidate) => candidate?.id === call.id);
        if (result === undefined) {
          return failRunEventStream(
            ModelProtocolError.make({ message: "Tool batch did not settle completely" }),
          );
        }
        orderedResults.push(result);
      }
      yield* applyRepeatedFailurePolicy(
        context,
        trace,
        agent.definition.policy.repeatedFailureLimit,
      );
      const toolMessage = Prompt.makeMessage("tool", {
        content: orderedResults.map((result) =>
          Prompt.makePart("tool-result", {
            id: result.id,
            name: result.name,
            result: result.encodedResult,
            isFailure: result.isFailure,
            providerExecuted: false,
          }),
        ),
      });
      const history = Prompt.fromMessages([
        ...prompt.content,
        ...promptFromTurnParts(trace).content,
        toolMessage,
      ]);
      yield* advanceHistory(context, history, options);
      const steering = yield* drainInputs(context, options);
      const nextPrompt = yield* appendInputs(context, history, steering, options);
      return makeTurn(agent, context, nextPrompt, turn + 1, toolCalls, options);
    }),
  );

/**
 * Resume one canonically declared Tool batch without re-invoking the model
 * (the durable batch-resume seam consumed via `RunOptions.resume`).
 *
 * The declared calls are re-validated through their Tool parameter Schemas
 * before anything executes; a decode failure is a strict no-start boundary
 * (RUN-004/STORE-006). The resumed Turn's response messages are rebuilt in
 * canonical encoded form so official history matches the recorded response.
 * Calls listed in `settled` are injected as final results without starting
 * their handlers — recorded Tool outcomes never rerun — while approval
 * preflight and durable preparation still cover the complete declared batch.
 * No model request is made and no usage is consumed for the resumed Turn.
 */
const makeResumeTurn = <
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  HookError,
  HookRequirements,
  InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
>(
  agent: RuntimeBinding<
    InputSchema,
    OutputSchema,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements,
    RunDispositionValue
  >,
  context: RunContext,
  prompt: Prompt.Prompt,
  resume: RunTurnResume,
  options: RunOptions<HookError, HookRequirements>,
): Stream.Stream<
  RunEvent,
  AgentRuntimeFailure<typeof agent, HookError, InstructionError>,
  InterpreterRequirements<typeof agent, HookRequirements, InstructionRequirements>
> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const tools = agent.definition.toolkit.tools;
      const turn = resume.turn;
      const turnId = resume.turnId;
      if (!Number.isInteger(turn) || turn <= 0) {
        return failRunEventStream(
          ModelProtocolError.make({
            message: "Turn resume requires a positive integer turn number",
          }),
        );
      }
      if (resume.calls.length === 0) {
        return failRunEventStream(
          ModelProtocolError.make({
            message: "Turn resume requires at least one declared Tool Call",
          }),
        );
      }
      const trace: TurnTrace = {
        responsePartCount: 0,
        responsePartBytes: 0,
        parts: [],
        text: [],
        textParts: new Map(),
        reasoningParts: new Map(),
        toolParameterParts: new Map(),
        toolCalls: new Map(),
        finalToolResultIds: new Set(),
        providerResultPayloads: [],
        providerStagedPayloadBytes: 0,
        turnCompletion: undefined,
        applicationToolCalls: [],
        applicationCallDescriptors: [],
        applicationToolResults: [],
        finished: true,
        finishReason: "tool-calls",
        usage: undefined,
      };
      const declarationByCallId = new Map<
        string,
        { readonly index: number; readonly name: string }
      >();
      for (const call of resume.calls) {
        if (!hasTool(tools, call.name)) {
          return failRunEventStream(
            ModelProtocolError.make({ message: `Turn resume declared unknown Tool ${call.name}` }),
          );
        }
        if (trace.toolCalls.has(call.id)) {
          return failRunEventStream(
            ModelProtocolError.make({ message: `Turn resume repeated Tool Call ID ${call.id}` }),
          );
        }
        const tool = tools[call.name] as ToolUnion<Tools>;
        const toolCallId = yield* decodeToolCallId(call.id);
        const decodedParams = yield* decodeResumedToolCallParameters<Tools>(
          tool,
          call.name,
          call.params,
        );
        const parameters = yield* decodeEventJson(call.params, "Tool parameters");
        declarationByCallId.set(call.id, {
          index: trace.applicationToolCalls.length,
          name: call.name,
        });
        trace.parts.push(
          Response.makePart("tool-call", {
            id: call.id,
            name: call.name,
            params: parameters,
            providerExecuted: false,
          }),
        );
        trace.toolCalls.set(call.id, { name: call.name, providerExecuted: false });
        trace.applicationToolCalls.push(
          Response.makePart("tool-call", {
            id: call.id,
            name: call.name,
            params: decodedParams,
            providerExecuted: false,
          }),
        );
        trace.applicationCallDescriptors.push({
          toolCallId,
          toolName: call.name,
          parameters,
          executionClass: getToolExecutionClass(tool),
        });
      }
      const settledIds = new Set<string>();
      for (const settledInput of resume.settled) {
        const settledCall = yield* decodeResumedSettledCall(
          settledInput,
          agent.definition.policy.toolResultBounds.maxBytes,
        );
        const declared = declarationByCallId.get(settledCall.id);
        if (declared === undefined) {
          return failRunEventStream(
            ModelProtocolError.make({
              message: `Turn resume settled an undeclared Tool Call ${settledCall.id}`,
            }),
          );
        }
        if (settledIds.has(settledCall.id)) {
          return failRunEventStream(
            ModelProtocolError.make({
              message: `Turn resume settled Tool Call ${settledCall.id} more than once`,
            }),
          );
        }
        settledIds.add(settledCall.id);
        trace.finalToolResultIds.add(settledCall.id);
        trace.applicationToolResults[declared.index] = {
          id: settledCall.id,
          name: declared.name,
          encodedResult: settledCall.result,
          isFailure: settledCall.isFailure,
        };
      }
      const settledChildJoinCallIds = resume.settledChildJoinCallIdsPastDeadline;
      if (settledChildJoinCallIds !== undefined) {
        const cleanupIds = new Set<string>(settledChildJoinCallIds);
        const openCalls = resume.calls.filter((call) => !settledIds.has(call.id));
        if (
          settledChildJoinCallIds.length === 0 ||
          cleanupIds.size !== settledChildJoinCallIds.length ||
          openCalls.length !== cleanupIds.size ||
          openCalls.some((call) => !cleanupIds.has(call.id) || !isDelegationToolName(call.name))
        ) {
          return failRunEventStream(
            ModelProtocolError.make({
              message:
                "Past-deadline cleanup authority must identify every and only still-open delegation Tool Call",
            }),
          );
        }
      }
      const policy = agent.definition.policy;
      const bounds = effectiveRunBounds(policy, options);
      const toolCalls = trace.toolCalls.size;
      const overToolBudget = toolCalls + context.programmaticToolCalls > bounds.maxToolCalls;
      if (overToolBudget && policy.onExhaustion === "fail") {
        return failRunEventStream(
          AgentPolicyError.make({
            limit: "tool-calls",
            message: `Agent exceeded its ${bounds.maxToolCalls} Tool Call limit`,
          }),
        );
      }
      const turnsBlocked =
        policy.onExhaustion === "fail" ? turn >= bounds.maxTurns : turn > bounds.maxTurns;
      if (turnsBlocked) {
        return failRunEventStream(
          AgentPolicyError.make({
            limit: "turns",
            message: `Agent exceeded its ${bounds.maxTurns} Turn limit`,
          }),
        );
      }
      const toolkit = yield* agent.definition.toolkit;
      const concurrency = yield* schedulingConcurrency(
        agent.definition.policy.toolConcurrency,
        options.scheduling,
      );

      // The pending Turn's committed LEADING messages (Turn-1 evaluated
      // instructions + input, or steering committed inside the pending
      // canonical response record) re-enter official history here, BEFORE the
      // rebuilt assistant tool-call message: a resumed Attempt's canonical
      // prompt boundary excludes the pending Turn, so without this thread the
      // resumed Run's live model context would silently drop them. Absent
      // `leadingMessages` keeps the prior behavior byte-for-byte.
      const leading = resume.leadingMessages;
      const resumedPrompt =
        leading === undefined || leading.content.length === 0
          ? prompt
          : Prompt.fromMessages([...prompt.content, ...leading.content]);
      if (resumedPrompt !== prompt) {
        yield* advanceHistory(context, resumedPrompt, options);
      }

      // The resumed Turn made its model call in a prior Attempt: no
      // ModelStarted event, no model metric, and no usage is consumed. The
      // TurnCompleted seam is re-emitted so downstream commit seams observe
      // the normal ordering (a Turn completes before its Tool batch executes).
      const started = Stream.fromEffect(
        Effect.gen(function* () {
          yield* Effect.logDebug("agent resumed a declared Tool batch").pipe(
            Effect.annotateLogs({
              agentId: context.agentId,
              runId: context.runId,
              turnId,
            }),
          );
          return [
            TurnStarted.make({
              ...(yield* eventBase(context)),
              turnId,
              turn,
            }),
            TurnCompleted.make({
              ...(yield* eventBase(context)),
              turnId,
              turn,
              finishReason: "tool-calls",
            }),
          ] satisfies ReadonlyArray<RunEvent>;
        }).pipe(Effect.withLogSpan("AgentRuntime.resume")),
      ).pipe(Stream.flatMap(Stream.fromIterable));
      const continueAfterBatch = () => {
        const continuation = toolBatchContinuation(
          agent,
          context,
          trace,
          resumedPrompt,
          turn,
          toolCalls,
          options,
        );
        return settledChildJoinCallIds === undefined
          ? continuation
          : enforceDurationDeadline(
              continuation,
              context.durationDeadlineMillis,
              durationLimitError(policy),
            );
      };

      // RUN-018 on the resume path: a canonically declared over-budget batch
      // settles synthetically under final-answer mode — recorded settled
      // results stand verbatim, only open calls get the synthetic failure,
      // and no handler starts. Once the synthetic settlements commit, the
      // pending batch is complete and recovery offers no further resume.
      if (overToolBudget) {
        const rejection = yield* settleRejectedBatch(
          context,
          turnId,
          trace,
          AgentPolicyError.make({
            limit: "tool-calls",
            message: `Tool Call budget exhausted: this Run's ${bounds.maxToolCalls} Tool Call limit was reached, so this call was rejected without executing. Do not request more tools; produce your final answer now from the information you already have.`,
          }),
          settledIds,
        );
        return started.pipe(
          Stream.concat(Stream.fromIterable(rejection)),
          Stream.concat(continueAfterBatch()),
        );
      }
      const toolResults = guardBudgetStream(
        executeToolBatch(
          context,
          turnId,
          turn,
          toolkit,
          trace.applicationToolCalls,
          trace,
          concurrency,
          options,
          {
            maxToolCalls: bounds.maxToolCalls,
            declaredToolCalls: toolCalls,
          },
          agent.definition.policy.toolResultBounds,
          settledIds,
        ),
        options.budget,
      );
      return started.pipe(Stream.concat(toolResults), Stream.concat(continueAfterBatch()));
    }),
  );

const failRunEventStream = <Error>(error: Error): Stream.Stream<RunEvent, Error> =>
  Stream.fail(error);

const durationLimitError = (policy: AgentPolicy): AgentPolicyError =>
  AgentPolicyError.make({
    limit: "duration",
    message: `Agent exceeded its ${Duration.format(policy.maxDuration)} duration limit`,
  });

const enforceDurationDeadline = <A, E, R>(
  execution: Stream.Stream<A, E, R>,
  durationDeadlineMillis: number,
  durationLimit: AgentPolicyError,
): Stream.Stream<A, E | AgentPolicyError, R> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const remaining = durationDeadlineMillis - now;
      if (remaining <= 0) {
        return Stream.fail(durationLimit);
      }
      return execution.pipe(
        Stream.interruptWhen(
          Effect.sleep(remaining).pipe(Effect.andThen(Effect.fail(durationLimit))),
        ),
      );
    }),
  );

const guardBudgetStream = <A, E, R, HookError, HookRequirements>(
  stream: Stream.Stream<A, E, R>,
  budget: RunOptions<HookError, HookRequirements>["budget"],
): Stream.Stream<A, E | HookError, R | HookRequirements> =>
  budget === undefined
    ? stream
    : Stream.transformPull(stream, (pull) => Effect.succeed(budget.guard(pull)));

/** Interpret a binding as an ordered semantic event stream. */
const stream = <
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  HookError = never,
  HookRequirements = never,
  InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
>(
  agent: RuntimeBinding<
    InputSchema,
    OutputSchema,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements,
    RunDispositionValue
  >,
  input: unknown,
  options: RunOptions<HookError, HookRequirements> = {},
): Stream.Stream<
  RunEvent,
  AgentRuntimeFailure<typeof agent, HookError, InstructionError>,
  AgentRuntimeRequirements<typeof agent, HookRequirements, InstructionRequirements>
> => {
  const interpreted = Stream.unwrap(
    Effect.gen(function* () {
      const resumeUsage =
        options.resumeUsage === undefined
          ? undefined
          : yield* decodeResumeUsage(options.resumeUsage);
      const attemptStartedAtMillis = yield* Clock.currentTimeMillis;
      const maxDurationMillis = Duration.toMillis(agent.definition.policy.maxDuration);
      const attemptDeadlineMillis = attemptStartedAtMillis + maxDurationMillis;
      // RUN-030: a durable coordinator supplies the logical Run deadline from
      // canonical Run-start evidence. Taking the earlier deadline keeps this
      // public option tightening-only for every other caller.
      const durationDeadlineMillis =
        options.durationDeadline === undefined
          ? attemptDeadlineMillis
          : Math.min(attemptDeadlineMillis, DateTime.toEpochMillis(options.durationDeadline));
      const startedAtMillis = durationDeadlineMillis - maxDurationMillis;
      const ids = yield* IdGenerator;
      const conversationId =
        options.conversationId === undefined
          ? yield* ids.nextConversationId
          : options.conversationId;
      const runId = options.runId === undefined ? yield* ids.nextRunId : options.runId;
      const context: RunContext = {
        agentId: agent.definition.id,
        conversationId,
        runId,
        input: undefined,
        pendingFollowUps: [],
        startedAtMillis,
        durationDeadlineMillis,
        history: options.history ?? Prompt.empty,
        // RUN-019: a resumed Attempt re-seeds cumulative usage from the
        // canonical response records so token budgets and the compaction
        // trigger keep accounting across ownership changes.
        modelCalls: resumeUsage?.modelCalls ?? 0,
        consecutiveToolFailures: 0,
        inputTokens: resumeUsage?.inputTokens ?? 0,
        outputTokens: resumeUsage?.outputTokens ?? 0,
        lastInputTokens: resumeUsage?.lastInputTokens ?? 0,
        lastOutputTokens: resumeUsage?.lastOutputTokens ?? 0,
        costMicrousd: resumeUsage?.costMicrousd ?? 0,
        lastCostMicrousd: 0,
        warnedLimits: new Set(),
        finalizing: false,
        tokenExhausted: false,
        exhaustedDimension: undefined,
        compaction: initialCompactionState(),
        bufferLimits: effectiveRunBufferLimits(options.bufferLimits),
        sequence: 0,
        programmaticToolCalls: 0,
      };
      // Restored totals can already breach the token budget (runtime spec §9):
      // the resumed Attempt must never issue an unconstrained external call.
      // "fail" rejects before any model call or resumed handler runs;
      // "final-answer" starts already constrained via the one-shot flag.
      if (resumeUsage !== undefined) {
        // Cost is an unconditional hard rail with no grace call in either
        // exhaustion mode (runtime spec §3): a resume whose seeded spend
        // already breaches the budget rejects before input, resumed
        // handlers, or any external model execution.
        const seededCostBudget = agent.definition.policy.costBudgetMicrousd;
        if (seededCostBudget !== undefined && context.costMicrousd > seededCostBudget) {
          return failRunEventStream(
            AgentPolicyError.make({
              limit: "cost",
              message: `Agent exceeded its ${seededCostBudget} microdollar cost budget`,
            }),
          );
        }
        const seededBudget = agent.definition.policy.tokenBudget;
        if (
          seededBudget !== undefined &&
          context.inputTokens + context.outputTokens > seededBudget
        ) {
          if (agent.definition.policy.onExhaustion === "fail") {
            return failRunEventStream(
              AgentPolicyError.make({
                limit: "tokens",
                message: `Agent exceeded its ${seededBudget} token budget`,
              }),
            );
          }
          context.tokenExhausted = true;
          context.exhaustedDimension ??= "tokens";
        }
      }
      if (options.input?.start !== undefined) {
        yield* options.input.start();
      }

      const started = Stream.fromEffect(
        Effect.gen(function* () {
          yield* Metric.update(runCounter, 1);
          yield* Effect.logDebug("agent run started").pipe(
            Effect.annotateLogs({
              agentId: context.agentId,
              runId: context.runId,
            }),
          );
          return yield* eventBase(context).pipe(Effect.map((base) => RunStarted.make(base)));
        }).pipe(Effect.withLogSpan("AgentRuntime.run")),
      );

      const execution = Stream.unwrap(
        Effect.gen(function* () {
          const decodedInput = yield* decodeInput(agent, input);
          const instructions = yield* evaluateInstructions<
            InputSchema["Type"],
            InstructionError,
            InstructionRequirements
          >(agent.definition.instructions, decodedInput);
          const encodedInput = yield* encodeInput(agent, decodedInput);
          context.input = encodedInput;
          const priorHistoryLength = context.history.content.length;
          const prompt = yield* makeInitialPrompt(instructions, encodedInput, context.history);
          // RUN-022: the instruction/input block is protected from compaction
          // by source index. A hook-prepared prompt (durable resume) replaces
          // the source, so index protection is unavailable there and the view
          // falls back to protecting system-role messages.
          if (options.context === undefined) {
            context.compaction.protectedStart = priorHistoryLength;
            context.compaction.protectedEnd = prompt.content.length;
          }
          yield* advanceHistory(context, prompt, options);
          if (options.resume !== undefined) {
            // A declared-batch resume re-enters mid-Turn: steering seams
            // reopen only after the resumed batch settles, so the initial
            // drain is skipped and the continuation drains at the safe seam.
            return makeResumeTurn(agent, context, prompt, options.resume, options);
          }
          const steering = yield* drainInputs(context, options);
          const initialPrompt = yield* appendInputs(context, prompt, steering, options);
          return makeTurn(agent, context, initialPrompt, 1, 0, options);
        }),
      );

      const durationLimit = durationLimitError(agent.definition.policy);
      // A coordinator-proven resume of exact already-settled attached-child
      // Calls is mandatory accepted-work cleanup. `makeResumeTurn` validates
      // and runs only that join batch past expiry, then restores this same
      // deadline guard around its continuation.
      const deadline =
        options.resume?.settledChildJoinCallIdsPastDeadline !== undefined
          ? execution
          : enforceDurationDeadline(execution, durationDeadlineMillis, durationLimit);

      // Engine-provided Tool services for this Run: a real `AgentSpawner`
      // bound to the Run's immutable identity and delegation depth, plus the
      // fail-closed `RunEventSink` and `DurableStep` defaults that each Tool
      // batch shadows with per-batch / per-call live services. Providing them
      // here is what removes these services from the runtime's public
      // requirements.
      const engineToolServices = Context.make(
        AgentSpawner,
        makeAgentSpawner(
          {
            agentId: context.agentId,
            conversationId: context.conversationId,
            runId: context.runId,
          },
          options.parentLink?.depth ?? 0,
        ),
      ).pipe(
        Context.add(RunEventSink, closedRunEventSink),
        Context.add(DurableStep, closedDurableStep),
        Context.add(SubagentDurability, closedSubagentDurability),
        Context.add(ToolBroker, closedToolBroker),
      );

      return started.pipe(
        Stream.concat(deadline),
        Stream.catch((error) => {
          const terminal = Stream.fromEffect(
            Effect.gen(function* () {
              if (error instanceof AgentApprovalPending || error instanceof AgentChildPending) {
                return RunSuspended.make({
                  ...(yield* terminalEventBase(context)),
                  reason: error.message,
                });
              }
              return RunFailed.make({
                ...(yield* terminalEventBase(context)),
                errorTag: errorTag(error),
                message: errorMessage(error),
              });
            }),
          );
          return terminal.pipe(Stream.concat(Stream.fail(error)));
        }),
        Stream.withSpan("AgentRuntime.run", {
          attributes: {
            agentId: context.agentId,
            runId: context.runId,
          },
        }),
        Stream.provide(engineToolServices),
      );
    }),
  );

  const finalized =
    options.input?.end === undefined
      ? interpreted
      : interpreted.pipe(Stream.ensuring(options.input.end()));
  return finalized.pipe(
    Stream.provide(agent.model, { local: true }),
    // The engine composition boundary owns span-lifecycle isolation while preserving the host's
    // ambient Tracer/Logger configuration. Individual Tool executions consume this capability.
    Stream.provide(ToolSpanTelemetry.layer),
  );
};

interface RunReduction {
  readonly completed?: Extract<RunEvent, { readonly _tag: "RunCompleted" }>;
}

/** Reduce one semantic Run event stream to its decoded terminal result. */
const reduceRunEvents = <AgentValue extends Agent.Any, Error, Requirements>(
  agent: AgentValue,
  events: Stream.Stream<RunEvent, Error, Requirements>,
): Effect.Effect<
  AgentResult<Agent.Output<AgentValue>>,
  Error | ModelProtocolError | AgentOutputError | Agent.RunDispositionFailure<AgentValue>,
  | Requirements
  | AgentValue["definition"]["output"]["DecodingServices"]
  | Agent.RunDispositionSchema<AgentValue>["DecodingServices"]
> =>
  Effect.gen(function* () {
    const reduction = yield* Stream.runFold(
      events,
      (): RunReduction => ({}),
      (state, event) => (event._tag === "RunCompleted" ? { completed: event } : state),
    );
    if (reduction.completed === undefined) {
      return yield* ModelProtocolError.make({
        message: "Agent stream ended without RunCompleted",
      });
    }
    const completed = reduction.completed;
    const candidateOutput: unknown = completed.output;
    const output = yield* Schema.decodeUnknownEffect(agent.definition.output)(candidateOutput).pipe(
      Effect.mapError((cause) =>
        AgentOutputError.make({
          message: cause.message,
        }),
      ),
    );
    const declaration = agent.definition.runDisposition;
    const runDisposition =
      completed.runDisposition === undefined
        ? undefined
        : completed.finishReason === "budget-exhausted"
          ? yield* ModelProtocolError.make({
              message: "A budget-exhausted RunCompleted event cannot declare a run disposition",
            })
          : declaration === undefined
            ? yield* ModelProtocolError.make({
                message:
                  "RunCompleted declared a run disposition without a definition-owned Schema",
              })
            : yield* decodeRunDisposition(agent, completed.runDisposition);
    return {
      output,
      conversationId: completed.conversationId,
      runId: completed.runId,
      turns: completed.turns,
      finishReason: completed.finishReason,
      ...(completed.exhausted !== undefined ? { exhausted: completed.exhausted } : {}),
      ...(runDisposition === undefined ? {} : { runDisposition: completed.runDisposition }),
    };
  });

/** Reduce the runtime's event stream to its decoded terminal result. */
const run = Effect.fn("AgentRuntime.run")(function* <
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  HookError = never,
  HookRequirements = never,
  InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
>(
  agent: RuntimeBinding<
    InputSchema,
    OutputSchema,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements,
    RunDispositionValue
  >,
  input: unknown,
  options: RunOptions<HookError, HookRequirements> = {},
): Effect.fn.Return<
  AgentResult<Agent.Output<typeof agent>>,
  AgentRuntimeFailure<typeof agent, HookError, InstructionError>,
  AgentRuntimeRequirements<typeof agent, HookRequirements, InstructionRequirements> | Scope.Scope
> {
  yield* Scope.Scope;
  return yield* reduceRunEvents(agent, stream(agent, input, options));
});

/**
 * Scoped detached execution whose observers cannot backpressure Run
 * completion.
 *
 * `observe` is a live multicast subscription: each subscription replays every
 * event the Run has already emitted, follows subsequent events as they occur,
 * and ends once the Run settles. The finite Run event ceiling sizes the replay
 * buffer so events are never dropped for a slow subscriber and publishing
 * never blocks the Run. `events` remains the complete bounded replay,
 * available after settlement. Both belong to the `start` Scope; observing
 * after that Scope closes interrupts the observer.
 */
export interface DetachedRun<Output, Error> {
  readonly await: Effect.Effect<AgentResult<Output>, Error>;
  readonly events: Effect.Effect<ReadonlyArray<RunEvent>>;
  readonly observe: Stream.Stream<RunEvent>;
}

const start = Effect.fn("AgentRuntime.start")(function* <
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  HookError = never,
  HookRequirements = never,
  InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
>(
  agent: RuntimeBinding<
    InputSchema,
    OutputSchema,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements,
    RunDispositionValue
  >,
  input: unknown,
  options: RunOptions<HookError, HookRequirements> = {},
): Effect.fn.Return<
  DetachedRun<
    Agent.Output<typeof agent>,
    AgentRuntimeFailure<typeof agent, HookError, InstructionError>
  >,
  never,
  AgentRuntimeRequirements<typeof agent, HookRequirements, InstructionRequirements> | Scope.Scope
> {
  yield* Scope.Scope;
  const bufferLimits = effectiveRunBufferLimits(options.bufferLimits);
  // Single-writer append-only trace owned by the Run fiber; readers only see
  // it after the fiber settles. `stream` admits at most `maxRunEvents`, so this
  // array has the same finite ceiling without per-event immutable copies.
  const captured: Array<RunEvent> = [];
  // One extra slot carries the terminal `Exit.void` Take after the bounded Run
  // event trace. Dropping is non-blocking, while the capacity proof means the
  // strategy never drops a valid event or the terminal marker.
  const observationCapacity = bufferLimits.maxRunEvents + 1;
  const pubsub = yield* PubSub.dropping<Take.Take<RunEvent>>({
    capacity: observationCapacity,
    replay: observationCapacity,
  });
  yield* Effect.addFinalizer(() => PubSub.shutdown(pubsub));
  const execution = reduceRunEvents(
    agent,
    stream(agent, input, options).pipe(
      Stream.tap((event) =>
        Effect.suspend(() => {
          captured.push(event);
          return PubSub.publish(pubsub, [event]);
        }),
      ),
    ),
  ).pipe(Effect.ensuring(PubSub.publish(pubsub, Exit.void)));
  const fiber = yield* execution.pipe(Effect.forkScoped);
  return {
    await: Fiber.join(fiber),
    events: Fiber.await(fiber).pipe(Effect.andThen(Effect.sync(() => captured.slice()))),
    observe: Stream.fromPubSubTake(pubsub),
  };
});

/**
 * Immutable parent Run identity carried by the locally provided
 * `AgentSpawner`. It exposes no mutable engine state and no Layer Context.
 */
export interface AgentSpawnerParent {
  readonly agentId: AgentId;
  readonly conversationId: ConversationId;
  readonly runId: RunId;
}

/**
 * Fail-closed Run-level `RunEventSink` default. Each Tool batch shadows it
 * with a live sink; any emission outside an active Tool batch fails with the
 * same typed error as emitting after a batch settled.
 */
const closedRunEventSink: RunEventSinkService = {
  emit: (payload) =>
    Effect.fail(
      RunEventSinkClosedError.make({
        message: `Subagent event ${payload._tag} was emitted outside an active Tool batch`,
      }),
    ),
};

/**
 * Fail-closed Run-level `DurableStep` default. Every executable Tool Call
 * shadows it with a live per-call service; a Step executed outside an active
 * Tool Call fails typed instead of silently running unrecorded.
 */
const closedDurableStep: DurableStepService = {
  do: (name) =>
    Effect.fail(
      DurableStepError.make({
        stepName: name,
        reason: "no-active-tool-call",
        message: "Durable Step was executed outside an active Tool Call",
      }),
    ),
};

/**
 * Fail-closed Run-level `ToolBroker` default. Every executable Tool Call
 * shadows it with a live per-call service bound to that call's identity and
 * held permit; opening a pass outside an active Tool Call fails typed.
 */
const closedToolBroker: ToolBrokerService = {
  openPass: () =>
    Effect.fail(
      ToolBrokerUnavailableError.make({
        message: "The programmatic Tool broker was used outside an active Tool Call",
      }),
    ),
};

/** Everything a live broker pass is bound to at its outer Tool Call. */
interface ToolBrokerBinding<HookError, HookRequirements> {
  readonly context: RunContext;
  readonly turnId: TurnId;
  readonly outerToolCallId: ToolCallId;
  readonly maxToolCalls: number;
  /** Model-declared Tool Calls committed through this batch (the outer call included). */
  readonly declaredToolCalls: number;
  readonly budget: RunOptions<HookError, HookRequirements>["budget"];
  readonly hookServices: Context.Context<HookRequirements>;
}

const programmaticOutcomeError = (
  index: number | undefined,
  tag: string,
  message: string,
): ProgrammaticCallOutcome => ({
  _tag: "ProgrammaticCallError",
  index,
  errorTag: tag,
  message,
});

/** A handler's typed failure captured as a value so defects stay defects. */
class BrokerHandlerFailure {
  constructor(readonly error: unknown) {}
}

/**
 * Measure one started programmatic handler with the same canonical span semantics as a
 * model-declared handler. A private marker gives value-level failures a failed exported status;
 * the marker is removed before the broker outcome returns, while an original Effect Cause is
 * restored unchanged outside the span.
 */
const stripProgrammaticToolSpanFailure = (
  cause: Cause.Cause<ToolSpanFailure>,
  marker: ToolSpanFailure,
): { readonly found: boolean; readonly residual: Cause.Cause<never> } => {
  let found = false;
  const residual: Array<Cause.Reason<never>> = [];
  for (const reason of cause.reasons) {
    if (Cause.isFailReason(reason)) {
      if (reason.error === marker) {
        found = true;
      } else {
        // This Effect's only typed failure is its fresh marker. Preserve a future invariant break
        // as a defect instead of consuming an unauthenticated same-class value.
        residual.push(Cause.makeDieReason(reason.error));
      }
    } else {
      residual.push(reason);
    }
  }
  return { found, residual: Cause.fromReasons(residual) };
};

const observeProgrammaticToolCall = <R>(
  telemetry: ToolSpanTelemetryService,
  descriptor: ToolTelemetryDescriptor,
  effect: Effect.Effect<ProgrammaticCallOutcome, never, R>,
): Effect.Effect<ProgrammaticCallOutcome, never, R> =>
  Effect.suspend(() => {
    const marker = ToolSpanFailure.marker();
    let terminalResult: ProgrammaticCallOutcome | undefined;
    let propagatedFailure: Cause.Cause<never> | undefined;

    const measured: Effect.Effect<ProgrammaticCallOutcome, ToolSpanFailure, R> = Effect.exit(
      effect,
    ).pipe(
      Effect.flatMap((exit): Effect.Effect<ProgrammaticCallOutcome, ToolSpanFailure> => {
        if (Exit.isFailure(exit)) {
          if (exit.cause.reasons.length > 0 && exit.cause.reasons.every(Cause.isInterruptReason)) {
            return Effect.failCause(exit.cause);
          }
          propagatedFailure = exit.cause;
          return isolateToolTerminalTelemetry(
            terminalToolTelemetry(descriptor, "failure", marker),
          ).pipe(Effect.andThen(Effect.fail(marker)));
        }

        terminalResult = exit.value;
        const outcome: ToolTelemetryOutcome =
          exit.value._tag === "ProgrammaticCallSuccess" ? "success" : "failure";
        return isolateToolTerminalTelemetry(
          terminalToolTelemetry(descriptor, outcome, outcome === "failure" ? marker : undefined),
        ).pipe(
          Effect.andThen(outcome === "failure" ? Effect.fail(marker) : Effect.succeed(exit.value)),
        );
      }),
      Effect.withSpan(`execute_tool ${descriptor.toolName}`, {
        kind: "internal",
        attributes: toolTelemetryAttributes(descriptor),
      }),
    );

    return telemetry.isolateEffectSpanLifecycle(measured).pipe(
      Effect.catchCause((cause) => {
        const { found, residual } = stripProgrammaticToolSpanFailure(cause, marker);
        const restored =
          propagatedFailure === undefined ? residual : Cause.combine(propagatedFailure, residual);
        if (!found) return Effect.failCause(restored);
        if (restored.reasons.length > 0) return Effect.failCause(restored);
        return terminalResult === undefined
          ? Effect.die("Programmatic Tool telemetry completed without a terminal result")
          : Effect.succeed(terminalResult);
      }),
    );
  });

const brokerEncodedByteLength = (value: unknown): number | undefined => {
  try {
    const encoded = JSON.stringify(value);
    return encoded === undefined ? undefined : utf8ByteLength(encoded);
  } catch {
    return undefined;
  }
};

const emptyProgrammaticUsage = Response.Usage.make({ inputTokens: {}, outputTokens: {} });

/** Hostile values can throw from trap getters mid-decode; stay fail-closed. */
const brokerDecodeJson = (value: unknown): Option.Option<Schema.Json> => {
  try {
    return Schema.decodeUnknownOption(Schema.Json)(value);
  } catch {
    return Option.none();
  }
};

/** One live broker bound to one outer Tool Call, plus its lifecycle closer. */
interface LiveToolBroker {
  readonly service: ToolBrokerService;
  /**
   * Marks every pass opened under this outer Tool Call closed. The batch
   * executor runs it when the call's stream settles, so a retained pass
   * cannot execute Tools outside the batch's scheduling authority.
   */
  readonly close: () => void;
}

/**
 * Live per-outer-call `ToolBroker` (runtime spec §12.1; RUN-016, RUN-017).
 * The pass executes under the outer Tool Call's already-held scheduling
 * permit — invocations run inside the outer handler's fiber and acquire no
 * batch permit of their own. Calls are strictly sequential, indices are
 * allocated from broker-owned monotonic state exactly when a handler starts,
 * and every started call consumes the Run's Tool-call budgets before its
 * handler is invoked. Inner calls produce no Run events and no Canonical
 * Records in the ephemeral slice; their evidence is telemetry only.
 */
const makeToolBrokerService = <HookError, HookRequirements>(
  binding: ToolBrokerBinding<HookError, HookRequirements>,
): Effect.Effect<LiveToolBroker, never, ToolSpanTelemetry> =>
  Effect.map(ToolSpanTelemetry, (toolSpanTelemetry) =>
    makeToolBrokerServiceWithTelemetry(binding, toolSpanTelemetry),
  );

const makeToolBrokerServiceWithTelemetry = <HookError, HookRequirements>(
  binding: ToolBrokerBinding<HookError, HookRequirements>,
  toolSpanTelemetry: ToolSpanTelemetryService,
): LiveToolBroker => {
  const lifecycle = { closed: false };
  const service: ToolBrokerService = {
    openPass: (toolkit, passOptions) =>
      Effect.gen(function* () {
        if (lifecycle.closed) {
          return yield* ToolBrokerUnavailableError.make({
            message: "The outer Tool Call for this broker has already settled",
          });
        }
        // A malformed result bound would fail open (`NaN` defeats every
        // comparison), so it is rejected typed before the pass opens.
        if (!Number.isSafeInteger(passOptions.maxResultBytes) || passOptions.maxResultBytes <= 0) {
          return yield* ToolBrokerConfigurationError.make({
            message: `maxResultBytes must be a positive safe integer; received ${String(passOptions.maxResultBytes)}`,
          });
        }
        // Capture the handler services present at the pass edge once; nothing
        // inside business execution can substitute them per invocation. The
        // same private-assertion contract as `provideHookServices` applies.
        const handlerServices = (yield* Effect.context<never>()) as Context.Context<unknown>;
        const state = { nextIndex: 0, inFlight: false };

        const body = (input: ProgrammaticToolInput): Effect.Effect<ProgrammaticCallOutcome> =>
          Effect.gen(function* () {
            if (!hasTool(toolkit.tools, input.toolName)) {
              return programmaticOutcomeError(
                undefined,
                "ProgrammaticToolUnknownError",
                `Tool ${input.toolName} is not part of this pass's allowlisted Toolkit`,
              );
            }
            const tool = toolkit.tools[input.toolName] as Tool.Any;
            const approval = tool.needsApproval;
            if (approval !== undefined && approval !== false) {
              return programmaticOutcomeError(
                undefined,
                "ProgrammaticApprovalUnsupportedError",
                `Tool ${input.toolName} requires approval; approval-requiring Tools never start programmatically in the ephemeral slice`,
              );
            }
            // Validate the encoded arguments against the owning parameter
            // Schema before the handler can start; the pinned `Toolkit.handle`
            // decodes internally, so the validated encoded form passes through
            // (the same inversion as `prepareToolCall`).
            const decodeParameters = Schema.decodeUnknownEffect(tool.parametersSchema) as (
              value: unknown,
            ) => Effect.Effect<unknown, Schema.SchemaError>;
            const invalidParameters = yield* decodeParameters(input.encodedArguments).pipe(
              Effect.map(() => undefined),
              Effect.catch((cause) =>
                Effect.succeed(
                  programmaticOutcomeError(
                    undefined,
                    "ModelProtocolError",
                    `Invalid parameters for Tool ${input.toolName}: ${cause.message}`,
                  ),
                ),
              ),
            );
            if (invalidParameters !== undefined) {
              return invalidParameters;
            }

            // Mid-pass budget consumption (RUN-017). The policy check and the
            // Run-wide reservation are adjacent with no interleaving point, so
            // parallel outer handlers cannot both observe the last remaining
            // slot; a rejection after the reservation rolls it back so the
            // counter always equals the handlers that actually started. A
            // budget-hook failure becomes a call outcome rather than failing
            // the Run mid-pass: exhaustion is re-enforced at the next Turn
            // seam, where `consumeUsage` and the stream guards stop the Run.
            const used = binding.declaredToolCalls + binding.context.programmaticToolCalls;
            if (used + 1 > binding.maxToolCalls) {
              return programmaticOutcomeError(
                undefined,
                "AgentPolicyError",
                `Agent exceeded its ${binding.maxToolCalls} Tool Call limit`,
              );
            }
            binding.context.programmaticToolCalls += 1;
            if (binding.budget !== undefined) {
              const exhausted = yield* provideHookServices(
                binding.budget.consume({
                  modelCalls: 0,
                  inputTokens: 0,
                  outputTokens: 0,
                  totalTokens: 0,
                  toolCalls: 1,
                  costMicrousd: 0,
                  usage: emptyProgrammaticUsage,
                }),
                binding.hookServices,
              ).pipe(
                Effect.map(() => undefined),
                Effect.catch((error) =>
                  Effect.succeed(
                    programmaticOutcomeError(undefined, errorTag(error), errorMessage(error)),
                  ),
                ),
              );
              if (exhausted !== undefined) {
                binding.context.programmaticToolCalls -= 1;
                return exhausted;
              }
            }

            const index = state.nextIndex++;
            const handleId = `${binding.outerToolCallId}#${index}`;
            const telemetryDescriptor: ToolTelemetryDescriptor = {
              context: binding.context,
              turnId: binding.turnId,
              toolCallId: handleId,
              toolName: input.toolName,
              executionClass: getToolExecutionClass(tool),
              invocationKind: "programmatic",
              parentToolCallId: binding.outerToolCallId,
              sequenceIndex: index,
            };
            return yield* observeProgrammaticToolCall(
              toolSpanTelemetry,
              telemetryDescriptor,
              Effect.gen(function* () {
                yield* Effect.logDebug("agent programmatic tool handler started").pipe(
                  Effect.annotateLogs({
                    agentId: binding.context.agentId,
                    runId: binding.context.runId,
                    turnId: binding.turnId,
                    toolCallId: handleId,
                    parentToolCallId: binding.outerToolCallId,
                    toolName: input.toolName,
                    sequenceIndex: index,
                  }),
                );
                yield* Metric.update(toolCounter, 1);

                let terminal:
                  | { readonly encodedResult: unknown; readonly isFailure: boolean }
                  | undefined;
                let resultAfterTerminal = false;
                const handlerFailed = yield* Stream.unwrap(
                  toolSpanTelemetry.isolateToolkitHandle(
                    (
                      toolkit.handle as (
                        name: string,
                        params: unknown,
                        id: string,
                      ) => Effect.Effect<
                        Stream.Stream<Tool.HandlerResult<Tool.Any>, unknown, unknown>,
                        unknown,
                        unknown
                      >
                    )(input.toolName, input.encodedArguments, handleId),
                  ),
                ).pipe(
                  Stream.runForEach((result) =>
                    Effect.sync(() => {
                      // The direct path rejects a second result after the terminal
                      // one; the broker preserves that protocol violation instead
                      // of silently keeping the last value.
                      if (terminal !== undefined) {
                        resultAfterTerminal = true;
                        return;
                      }
                      if (!result.preliminary) {
                        terminal = {
                          encodedResult: result.encodedResult,
                          isFailure: result.isFailure,
                        };
                      }
                    }),
                  ),
                  Effect.map(() => undefined),
                  Effect.catch((error) => Effect.succeed(new BrokerHandlerFailure(error))),
                );
                if (handlerFailed instanceof BrokerHandlerFailure) {
                  return programmaticOutcomeError(
                    index,
                    errorTag(handlerFailed.error),
                    errorMessage(handlerFailed.error),
                  );
                }
                if (resultAfterTerminal) {
                  return programmaticOutcomeError(
                    index,
                    "ModelProtocolError",
                    `Tool Call ${handleId} produced more than one terminal result`,
                  );
                }
                if (terminal === undefined) {
                  return programmaticOutcomeError(
                    index,
                    "ModelProtocolError",
                    `Tool Call ${handleId} completed without a terminal result`,
                  );
                }
                if (terminal.isFailure) {
                  return {
                    _tag: "ProgrammaticCallFailure",
                    index,
                    encodedResult: terminal.encodedResult,
                  } as const;
                }
                if (
                  Option.isNone(Schema.decodeUnknownOption(Schema.Json)(terminal.encodedResult))
                ) {
                  return programmaticOutcomeError(
                    index,
                    "ModelProtocolError",
                    `Tool ${input.toolName} produced a success encoding outside JSON`,
                  );
                }
                let encodedResult = terminal.encodedResult;
                if (passOptions.redactResult !== undefined) {
                  // A redactor is a substitution point: its replacement re-crosses
                  // the JSON boundary or the call fails closed.
                  const redacted = brokerDecodeJson(yield* passOptions.redactResult(encodedResult));
                  if (Option.isNone(redacted)) {
                    return programmaticOutcomeError(
                      index,
                      "ModelProtocolError",
                      `The redacted result for Tool ${input.toolName} is outside the JSON surface`,
                    );
                  }
                  encodedResult = redacted.value;
                }
                const bytes = brokerEncodedByteLength(encodedResult);
                if (bytes === undefined || bytes > passOptions.maxResultBytes) {
                  return programmaticOutcomeError(
                    index,
                    "ProgrammaticResultLimitError",
                    `Tool ${input.toolName} result of ${bytes ?? "unencodable"} bytes exceeds the ${passOptions.maxResultBytes}-byte broker bound`,
                  );
                }
                return { _tag: "ProgrammaticCallSuccess", index, encodedResult } as const;
              }),
            );
          }).pipe(Effect.provideContext(handlerServices)) as Effect.Effect<ProgrammaticCallOutcome>;

        const pass: ToolBrokerPass = {
          invoke: (input) =>
            Effect.suspend(() => {
              // A pass retained past its outer Tool Call cannot execute
              // Tools outside the batch's scheduling authority.
              if (lifecycle.closed) {
                return Effect.succeed(
                  programmaticOutcomeError(
                    undefined,
                    "ToolBrokerUnavailableError",
                    "The outer Tool Call for this pass has already settled",
                  ),
                );
              }
              // Strict sequentiality (RUN-016): a call issued while another
              // from this pass is unsettled fails typed without acquiring an
              // identity or consuming budget; the rejection does not release
              // the in-flight owner's claim.
              if (state.inFlight) {
                return Effect.succeed(
                  programmaticOutcomeError(
                    undefined,
                    "ProgrammaticCallConcurrencyError",
                    `Host call ${input.toolName} was issued while another call from this pass is unsettled; in-program Tool calls are strictly sequential`,
                  ),
                );
              }
              state.inFlight = true;
              return body(input).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    state.inFlight = false;
                  }),
                ),
              );
            }),
        };
        return pass;
      }),
  };
  return {
    service,
    close: () => {
      lifecycle.closed = true;
    },
  };
};

/**
 * Ephemeral pass-through `DurableStep` provided when `RunOptions.durability`
 * is absent: each Step body executes exactly once in-process and nothing is
 * recorded. Durable Tools stay runnable on the ephemeral runtime with honest
 * (weaker) semantics — the durable claim attaches to the runtime, not the
 * Tool. Duplicate Step names remain the same typed identity conflict as under
 * the durable service so authoring bugs fail identically on both runtimes.
 */
const passthroughDurableStep = (): DurableStepService => {
  const usedNames = new Set<string>();
  return {
    do: <Output extends Schema.Top, BodyError, BodyServices>(
      name: string,
      _output: Output,
      execute: Effect.Effect<Output["Type"], BodyError, BodyServices>,
    ) =>
      Effect.suspend(
        (): Effect.Effect<Output["Type"], DurableStepError | BodyError, BodyServices> => {
          if (usedNames.has(name)) {
            return Effect.fail(
              DurableStepError.make({
                stepName: name,
                reason: "duplicate-step-name",
                message: "Durable Step name was reused within one Tool Call",
              }),
            );
          }
          usedNames.add(name);
          return execute;
        },
      ),
  };
};

/**
 * Durable `DurableStep` service bound to one Tool Call over the coordinator's
 * `RunStepHook`.
 *
 * Semantics per durability §11: a committed result decodes through the
 * declared output Schema and returns without executing the body
 * (exactly-once-recorded); otherwise the body runs (at-least-once-executed —
 * a crash mid-body re-executes on the next Attempt and duplicate external
 * effects stay observable), the success is encoded through the Schema, and
 * only then committed. Failures are never recorded: a failing body fails the
 * Step call into the handler's error channel and re-entry re-executes it.
 * Step names must be deterministic and unique within one Tool Call; reuse is
 * a typed identity conflict because the second call would silently replay the
 * first call's recorded result. Hook failures and codec conflicts surface as
 * `DurableStepError` without widening the handler's error channel.
 */
/**
 * Provide a captured hook Context to a hook effect. TypeScript cannot reduce
 * the deferred conditional `Exclude<HookRequirements, HookRequirements>` on
 * an unresolved generic, so this private assertion pins the identity that
 * providing `Context<R>` to an `Effect<_, _, R>` leaves no requirements; it
 * never bypasses validation.
 */
const provideHookServices = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  services: Context.Context<R>,
): Effect.Effect<A, E> =>
  effect.pipe(Effect.provideContext(services)) as unknown as Effect.Effect<A, E>;

const makeDurableStepService = <HookError, HookRequirements>(
  toolCallId: ToolCallId,
  hook: RunStepHook<HookError, HookRequirements>,
  hookServices: Context.Context<HookRequirements>,
): DurableStepService => {
  const usedNames = new Set<string>();
  return {
    do: <Output extends Schema.Top, BodyError, BodyServices>(
      name: string,
      output: Output,
      execute: Effect.Effect<Output["Type"], BodyError, BodyServices>,
    ) =>
      Effect.gen(function* () {
        if (usedNames.has(name)) {
          return yield* DurableStepError.make({
            toolCallId,
            stepName: name,
            reason: "duplicate-step-name",
            message: "Durable Step name was reused within one Tool Call",
          });
        }
        usedNames.add(name);
        const key: RunStepKey = { toolCallId, stepName: name };
        const recorded = yield* provideHookServices(hook.lookup(key), hookServices).pipe(
          Effect.tapError((error) => ErrorReporter.report(Cause.fail(error))),
          Effect.mapError(() =>
            DurableStepError.make({
              toolCallId,
              stepName: name,
              reason: "lookup-failed",
              message: "Durable Step lookup failed",
            }),
          ),
        );
        if (Option.isSome(recorded)) {
          return yield* Schema.decodeUnknownEffect(output)(recorded.value.encodedOutput).pipe(
            Effect.mapError(() =>
              DurableStepError.make({
                toolCallId,
                stepName: name,
                reason: "recorded-result-invalid",
                message: "Recorded Durable Step result failed the declared output Schema",
              }),
            ),
          );
        }
        const value = yield* execute;
        const encodedOutput = yield* Schema.encodeEffect(output)(value).pipe(
          Effect.mapError(() =>
            DurableStepError.make({
              toolCallId,
              stepName: name,
              reason: "output-encoding-failed",
              message: "Durable Step output failed the declared output Schema",
            }),
          ),
        );
        yield* provideHookServices(hook.commit(key, encodedOutput), hookServices).pipe(
          Effect.tapError((error) => ErrorReporter.report(Cause.fail(error))),
          Effect.mapError(() =>
            DurableStepError.make({
              toolCallId,
              stepName: name,
              reason: "commit-failed",
              message: "Durable Step commit failed",
            }),
          ),
        );
        return value;
      }),
  };
};

/**
 * Typed engine-owned suspension signal raised by a delegation Tool handler
 * through the per-batch `SubagentDurability` service when its durable child
 * is established but not settled (S2 plan §2). It is NOT a Tool failure: the
 * batch executor treats the raising call as "stays open" — no terminal
 * result, no `ToolCallFailed`, no batch failure policy, no sibling
 * interruption — and terminates the Run with `AgentChildPending` after every
 * non-waiting sibling handler settled.
 */
export class ToolCallWaiting extends Schema.TaggedError<ToolCallWaiting>()("ToolCallWaiting", {
  toolCallId: ToolCallId,
  childConversationId: ConversationId,
  childSubmissionId: SubmissionId,
  childRunId: RunId,
  receiptId: ReceiptId,
  message: Schema.String,
}) {}

/**
 * The Run suspended `waitingForChild`: at least one durable delegation call
 * of the last Tool batch is waiting on its attached child (spec/subagents.md
 * §12 step 10, SUB-030). Mirrors `AgentApprovalPending`: the engine emits
 * `RunSuspended` and then fails the Run stream with this error; the durable
 * coordinator catches it and ends the Attempt's ownership period without
 * settling. `children` is listed in declaration order, deterministically.
 */
export class AgentChildPending extends Schema.TaggedError<AgentChildPending>()(
  "AgentChildPending",
  {
    children: Schema.NonEmptyArray(
      Schema.Struct({
        toolCallId: ToolCallId,
        childConversationId: ConversationId,
        childSubmissionId: SubmissionId,
        childRunId: RunId,
      }),
    ),
    message: Schema.String,
  },
) {}

/**
 * Typed failure of the engine-provided `SubagentDurability` operations.
 * `hook-failed` wraps a coordinator hook failure without leaking the hook's
 * error type into handler signatures (the `DurableStepError` precedent);
 * `no-active-tool-batch` is the fail-closed Run-level default — establishment
 * outside an active Tool batch never silently degrades to ephemeral spawning.
 */
export class SubagentDurabilityError extends Schema.TaggedError<SubagentDurabilityError>()(
  "SubagentDurabilityError",
  {
    operation: Schema.Literals(["establish", "join", "waiting"]),
    reason: Schema.Literals(["hook-failed", "no-active-tool-batch"]),
    message: Schema.String.check(Schema.isMaxLength(4_096)),
    toolCallId: Schema.optionalKey(ToolCallId),
  },
) {}

/**
 * Explicit ephemeral mode: no durable coordinator supplied
 * `RunOptions.subagent`, so no durable claim is being made and the delegation
 * handler keeps the S1 in-process spawn semantics honestly.
 */
export interface SubagentDurabilityEphemeral {
  readonly mode: "ephemeral";
}

/**
 * Durable mode: establishment and join run through the coordinator's
 * `RunSubagentHook` under the parent's ownership fence, and `waiting` raises
 * the engine-owned suspension signal for a still-running child.
 */
export interface SubagentDurabilityDurable {
  readonly mode: "durable";
  /** Idempotent durable child establishment (spec/subagents.md §12 steps 2-9, SUB-016). */
  readonly establish: (
    request: RunSubagentEstablishRequest,
  ) => Effect.Effect<ChildEstablishStatus, SubagentDurabilityError>;
  /** Atomic settlement join: `SubagentJoined` + parent `ToolCallSettled` in one canonical batch (SUB-019). */
  readonly join: (request: RunSubagentJoinRequest) => Effect.Effect<void, SubagentDurabilityError>;
  /**
   * Raise the waiting suspension signal for one delegation call whose
   * established child has not settled. Never returns: the handler ends here,
   * siblings run to completion, the Run suspends, and the resumed batch
   * re-executes the handler idempotently.
   */
  readonly waiting: (
    toolCallId: ToolCallId,
    child: RunSubagentChildIdentity,
  ) => Effect.Effect<never, ToolCallWaiting | SubagentDurabilityError>;
}

/**
 * Mode-dispatched service value seen by delegation Tool handlers: an explicit
 * ephemeral default when the Run carries no durable coordinator, the durable
 * establish/join/waiting surface otherwise.
 */
export type SubagentDurabilityService = SubagentDurabilityEphemeral | SubagentDurabilityDurable;

/**
 * Engine-owned durable-Subagent seam provided locally to every Tool batch,
 * constructed from `RunOptions.subagent` when present and the explicit
 * ephemeral-mode default when absent. Like the other engine-provided Tool
 * services it is excluded from the runtime's public requirements and MUST NOT
 * be satisfied from an application Layer; handlers resolve it per call (the
 * S1 innermost re-provide pattern) so a Layer built inside another Run's
 * batch never captures a stale mode.
 */
export class SubagentDurability extends Context.Service<
  SubagentDurability,
  SubagentDurabilityService
>()("@effect-agent/engine/SubagentDurability") {}

/** The one explicit ephemeral-mode value: absence of a durable coordinator is stated, not inferred. */
const ephemeralSubagentDurability: SubagentDurabilityService = { mode: "ephemeral" };

const closedSubagentDurabilityFailure = (
  operation: "establish" | "join" | "waiting",
  toolCallId?: ToolCallId,
): SubagentDurabilityError =>
  SubagentDurabilityError.make({
    operation,
    reason: "no-active-tool-batch",
    message: `Durable Subagent ${operation} was invoked outside an active Tool batch`,
    ...(toolCallId === undefined ? {} : { toolCallId }),
  });

/**
 * Fail-closed Run-level `SubagentDurability` default. Each Tool batch shadows
 * it with its live per-batch service; resolving the seam outside an active
 * Tool batch fails typed instead of silently spawning an ephemeral child
 * under a durable coordinator.
 */
const closedSubagentDurability: SubagentDurabilityService = {
  mode: "durable",
  establish: (request) =>
    Effect.fail(closedSubagentDurabilityFailure("establish", request.toolCallId)),
  join: (request) => Effect.fail(closedSubagentDurabilityFailure("join", request.toolCallId)),
  waiting: (toolCallId) => Effect.fail(closedSubagentDurabilityFailure("waiting", toolCallId)),
};

/**
 * Durable `SubagentDurability` service bound to one Tool batch over the
 * coordinator's `RunSubagentHook`. Hook failures surface as typed
 * `SubagentDurabilityError` values without widening handler error channels
 * (the coordinator keeps its own halt side channel, exactly like the Step
 * hook); `waiting` constructs the engine-owned `ToolCallWaiting` signal so a
 * handler can never counterfeit a foreign child identity shape.
 */
const makeSubagentDurabilityService = <HookError, HookRequirements>(
  hook: RunSubagentHook<HookError, HookRequirements>,
  hookServices: Context.Context<HookRequirements>,
): SubagentDurabilityService => ({
  mode: "durable",
  establish: (request) =>
    provideHookServices(hook.establish(request), hookServices).pipe(
      Effect.tapError((error) => ErrorReporter.report(Cause.fail(error))),
      Effect.mapError(() =>
        SubagentDurabilityError.make({
          operation: "establish",
          reason: "hook-failed",
          toolCallId: request.toolCallId,
          message: "Durable child establishment failed",
        }),
      ),
    ),
  join: (request) =>
    provideHookServices(hook.join(request), hookServices).pipe(
      Effect.tapError((error) => ErrorReporter.report(Cause.fail(error))),
      Effect.mapError(() =>
        SubagentDurabilityError.make({
          operation: "join",
          reason: "hook-failed",
          toolCallId: request.toolCallId,
          message: "Durable child join failed",
        }),
      ),
    ),
  waiting: (toolCallId, child) =>
    Effect.fail(
      ToolCallWaiting.make({
        toolCallId,
        childConversationId: child.childConversationId,
        childSubmissionId: child.childSubmissionId,
        childRunId: child.childRunId,
        receiptId: child.receiptId,
        message: `Tool Call ${toolCallId} is waiting on durable attached child ${child.childSubmissionId}`,
      }),
    ),
});

/**
 * Extract the waiting suspension signal from a handler cause. It normally
 * travels as a typed failure through the native `Toolkit.handle` error
 * channel; the squash fallback also recognizes it inside a defect so a
 * wrapped signal still suspends instead of manufacturing a Tool failure.
 */
const waitingFromCause = (cause: Cause.Cause<unknown>): ToolCallWaiting | undefined => {
  const failure = Cause.findErrorOption(cause);
  if (Option.isSome(failure) && failure.value instanceof ToolCallWaiting) {
    return failure.value;
  }
  const squashed = Cause.squash(cause);
  return squashed instanceof ToolCallWaiting ? squashed : undefined;
};

/** Stable delegation identity supplied by the invoking delegation Tool handler. */
export interface SpawnDelegation {
  readonly delegationId: DelegationId;
  readonly parentToolCallId: ToolCallId;
}

/**
 * Child Run options accepted by `AgentSpawner.spawn`. Child Conversation/Run
 * identity and the Parent Link are spawner-owned and cannot be overridden.
 */
export interface SpawnRunOptions<HookError = never, HookRequirements = never> extends Omit<
  RunOptions<HookError, HookRequirements>,
  "conversationId" | "runId" | "parentLink"
> {}

/**
 * Handle to one spawned Attached Child: its preallocated child identity, its
 * immutable Parent Link, and the same observation surface as `DetachedRun`.
 * The child fiber belongs to the Scope the caller provided to `spawn`.
 */
export interface SpawnedChildRun<Output, Error> extends DetachedRun<Output, Error> {
  readonly conversationId: ConversationId;
  readonly runId: RunId;
  readonly parentLink: SubagentParentLink;
}

/**
 * Run one child Agent Binding through the same interpreter as a top-level
 * Run.
 *
 * The spawner allocates a fresh child `ConversationId` and `RunId` through
 * `IdGenerator` (guaranteeing a fresh child Conversation per invocation, with
 * no Conversation reuse), constructs the immutable Parent Link at
 * `depth + 1`, and starts the child eagerly with `AgentRuntime.start` inside
 * the caller-provided Scope, so parent interruption always reaches the child
 * and its finalizers. Preflight policy (including S1's normative
 * nested-delegation rejection) belongs to the delegation capability and runs
 * before `spawn` is called.
 */
const spawnWithParent = (parent: AgentSpawnerParent, depth: number) =>
  Effect.fn("AgentSpawner.spawn")(function* <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    HookError = never,
    HookRequirements = never,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  >(
    binding: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements
    >,
    input: unknown,
    delegation: SpawnDelegation,
    options?: SpawnRunOptions<HookError, HookRequirements>,
  ): Effect.fn.Return<
    SpawnedChildRun<
      Agent.Output<typeof binding>,
      AgentRuntimeFailure<typeof binding, HookError, InstructionError>
    >,
    never,
    | Scope.Scope
    | AgentRuntimeRequirements<typeof binding, HookRequirements, InstructionRequirements>
  > {
    const ids = yield* IdGenerator;
    const conversationId = yield* ids.nextConversationId;
    const runId = yield* ids.nextRunId;
    // `depth + 1` is always an integer >= 1, so a decode failure is a defect.
    const childDepth = yield* Schema.decodeEffect(DelegationDepth)(depth + 1).pipe(Effect.orDie);
    const parentLink = SubagentParentLink.make({
      delegationId: delegation.delegationId,
      parentAgentId: parent.agentId,
      parentConversationId: parent.conversationId,
      parentRunId: parent.runId,
      parentToolCallId: delegation.parentToolCallId,
      depth: childDepth,
    });
    const child = yield* start(binding, input, {
      ...options,
      conversationId,
      runId,
      parentLink,
    });
    return {
      ...child,
      conversationId,
      runId,
      parentLink,
    };
  });

/**
 * Narrow parent execution value visible to Tool handlers. `depth` is the
 * current Run's root-relative delegation depth: `0` for a root Run and
 * `parentLink.depth` for a child, which the delegation preflight uses to
 * reject nested delegation (SUB-029).
 */
export interface AgentSpawnerService {
  readonly depth: number;
  readonly parent: AgentSpawnerParent;
  readonly spawn: ReturnType<typeof spawnWithParent>;
}

/**
 * Engine-owned service contract through which a declared delegation Tool
 * runs an Attached Child on the same interpreter.
 *
 * The engine provides this service locally to every Run, bound to the Run's
 * immutable identity and delegation depth; it is never satisfied from an
 * application Layer, is excluded from the runtime's public requirements, and
 * exposes neither the engine's mutable Run state nor any root Layer Context.
 */
export class AgentSpawner extends Context.Service<AgentSpawner, AgentSpawnerService>()(
  "@effect-agent/engine/AgentSpawner",
) {}

const makeAgentSpawner = (parent: AgentSpawnerParent, depth: number): AgentSpawnerService => ({
  depth,
  parent,
  spawn: spawnWithParent(parent, depth),
});

/** Bound applied to the rendered defect message of `withTerminalDefectEvent` (SEC-013). */
const DEFECT_MESSAGE_LIMIT = 2_048;

/**
 * Opt-in boundary combinator (P7 §7(h), decision point 8): the engine keeps defects as
 * defects — `RunFailed` covers EXPECTED failures only, and a defect still fails the event
 * stream with its full Cause (AGENTS.md: errors are never silently widened, and a defect is
 * never converted into a typed failure by default). A host boundary that forwards Run Events
 * to a UI or transport can wrap the stream with this helper to append ONE bounded terminal
 * `RunFailed { errorTag: "Defect" }` event before the original cause is rethrown, so a viewer
 * always observes a terminal event even when the Run dies.
 *
 * Contract (documented in docs/spec/runtime.md §10):
 *
 * - typed failures and interruptions pass through untouched — the engine already emitted
 *   their terminal event, so nothing is duplicated;
 * - a cause carrying a defect first emits `RunFailed` with `errorTag: "Defect"` and a
 *   bounded string rendering of the defect (never the raw value; hosts owning stricter
 *   redaction apply it downstream), then RETHROWS the original cause unchanged;
 * - identity fields come from the last event already streamed (`sequence` advances by one);
 *   a defect BEFORE the first event has no Run identity to attribute, so it is rethrown
 *   without an event — the helper never fabricates identities.
 */
export const withTerminalDefectEvent = <E, R>(
  events: Stream.Stream<RunEvent, E, R>,
): Stream.Stream<RunEvent, E, R> =>
  Stream.suspend(() => {
    let last: RunEvent | undefined;
    return events.pipe(
      Stream.tap((event) =>
        Effect.sync(() => {
          last = event;
        }),
      ),
      Stream.catchCause((cause): Stream.Stream<RunEvent, E, R> => {
        const base = last;
        if (base === undefined || !Cause.hasDies(cause) || Cause.hasInterruptsOnly(cause)) {
          return Stream.failCause(cause);
        }
        const terminal = Stream.fromEffect(
          Effect.gen(function* () {
            const timestamp = DateTime.makeUnsafe(yield* Clock.currentTimeMillis);
            return RunFailed.make({
              eventVersion: 1,
              runId: base.runId,
              conversationId: base.conversationId,
              agentId: base.agentId,
              sequence: base.sequence + 1,
              timestamp,
              errorTag: "Defect",
              message: errorMessage(Cause.squash(cause)).slice(0, DEFECT_MESSAGE_LIMIT),
            });
          }),
        );
        return terminal.pipe(Stream.concat(Stream.failCause(cause)));
      }),
    );
  });

/**
 * Ephemeral Agent interpreter whose `run` operation reduces the same semantic
 * event stream exposed by `stream`.
 *
 * The bound Model is provided locally; all remaining requirements stay visible
 * in the returned Effect or Stream. The interpreter owns no shared service or
 * Layer state.
 */
export const AgentRuntime = {
  run,
  start,
  stream,
} as const;
