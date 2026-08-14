import {
  Agent,
  AgentApprovalDenied,
  AgentApprovalPending,
  AgentInputError,
  AgentOutputError,
  AgentPolicyError,
  type AgentId,
  ApprovalRequested,
  ConversationId,
  type Definition,
  DelegationDepth,
  type DelegationId,
  IdGenerator,
  type InstructionSource,
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
  TurnCompleted,
  TurnStarted,
  type TurnId,
} from "@effect-agent/core";
import {
  Cause,
  Clock,
  Context,
  DateTime,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Metric,
  Option,
  PubSub,
  Queue,
  Schema,
  Scope,
  Semaphore,
  Stream,
  Take,
} from "effect";
import {
  type AiError,
  LanguageModel,
  type Model,
  Prompt,
  Response,
  Tool,
  type Toolkit,
} from "effect/unstable/ai";

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
> = {
  readonly definition: Definition<
    InputSchema,
    OutputSchema,
    Instructions,
    Toolkit.Toolkit<Tools>
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
  type RunOptions,
  type RunSchedulingHook,
  type RunSubagentChildIdentity,
  type RunSubagentEstablishRequest,
  type RunSubagentHook,
  type RunSubagentJoinRequest,
  type RunToolCallDescriptor,
  type RunTurnResume,
  type RunUsageDelta,
} from "./run-options.ts";

export * from "./durable-step.ts";
export * from "./run-events.ts";
export * from "./run-options.ts";

import {
  RunEventSink,
  RunEventSinkClosedError,
  type RunEventSinkService,
  type SubagentEventPayload,
} from "./run-events.ts";

/** Schema factory for the terminal value produced by reducing a completed agent event stream. */
export const AgentResultSchema = <Output extends Schema.Top>(output: Output) =>
  Schema.Struct({
    output,
    conversationId: ConversationId,
    runId: RunId,
    turns: Schema.Int.check(Schema.isGreaterThan(0)),
    finishReason: Schema.Literals(["completed", "model-stop"]),
  });

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
  | ModelProtocolError
  | AgentApprovalDenied
  | AgentApprovalPending
  | AgentChildPending
  | HookError
  | InstructionError;

/**
 * Services the engine itself provides locally to every Run: `AgentSpawner`
 * bound to the Run's immutable identity and delegation depth, `RunEventSink`
 * and `SubagentDurability` bound to the active Tool batch, and `DurableStep`
 * bound to the active Tool Call. They are excluded from the runtime's public
 * requirements and MUST NOT be satisfied from an application Layer.
 */
export type EngineProvidedToolServices =
  | AgentSpawner
  | RunEventSink
  | DurableStep
  | SubagentDurability;

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
  readonly pendingFollowUps: Array<Prompt.RawInput>;
  history: Prompt.Prompt;
  modelCalls: number;
  consecutiveToolFailures: number;
  inputTokens: number;
  outputTokens: number;
  costMicrousd: number;
  sequence: number;
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
  readonly applicationToolCalls: Array<Response.ToolCallPart<string, unknown>>;
  /** Durable-hook view of the application calls, in declaration order (encoded parameters). */
  readonly applicationCallDescriptors: Array<RunToolCallDescriptor>;
  readonly applicationToolResults: Array<{
    readonly id: string;
    readonly name: string;
    readonly encodedResult: unknown;
    readonly isFailure: boolean;
  }>;
  finished: boolean;
  finishReason: Response.FinishReason | undefined;
  usage: Response.Usage | undefined;
}

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
): Effect.fn.Return<RunEvent> {
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

const executePreparedToolCall = <Tools extends Record<string, Tool.Any>>(
  context: RunContext,
  turnId: TurnId,
  toolkit: Toolkit.WithHandler<Tools>,
  prepared: PreparedToolCall<Tools>,
  trace: TurnTrace,
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
  Tool.HandlerServices<ToolUnion<Tools>>
> => {
  const call = prepared.call;
  let terminal = false;

  const started = Stream.fromEffect(
    Effect.gen(function* () {
      const toolCallId = yield* decodeToolCallId(call.id);
      yield* Effect.logDebug("agent tool handler started").pipe(
        Effect.annotateLogs({
          agentId: context.agentId,
          runId: context.runId,
          turnId,
          toolCallId,
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

  const results = Stream.unwrap(
    toolkit
      .handle(prepared.name, prepared.nativeHandlerParams, call.id)
      .pipe(Effect.withSpan("AgentRuntime.toolkit.handle")),
  ).pipe(
    Stream.mapEffect((result) =>
      Effect.gen(function* () {
        if (terminal || trace.finalToolResultIds.has(call.id)) {
          return yield* ModelProtocolError.make({
            message: `Tool Call ${call.id} produced more than one terminal result`,
          });
        }
        const toolCallId = yield* decodeToolCallId(call.id);
        if (result.preliminary) {
          return ToolProgress.make({
            ...(yield* eventBase(context)),
            turnId,
            toolCallId,
            toolName: call.name,
            result: yield* decodeEventJson(result.encodedResult, "Tool result"),
            providerExecuted: false,
          });
        }

        terminal = true;
        trace.finalToolResultIds.add(call.id);
        trace.applicationToolResults[prepared.declarationIndex] = {
          id: call.id,
          name: call.name,
          encodedResult: result.encodedResult,
          isFailure: result.isFailure,
        };
        if (result.isFailure) {
          return ToolCallFailed.make({
            ...(yield* eventBase(context)),
            turnId,
            toolCallId,
            toolName: call.name,
            errorTag: errorTag(result.result),
            message: errorMessage(result.result),
            providerExecuted: false,
          });
        }
        return ToolCallSucceeded.make({
          ...(yield* eventBase(context)),
          turnId,
          toolCallId,
          toolName: call.name,
          result: yield* decodeEventJson(result.encodedResult, "Tool result"),
          providerExecuted: false,
        });
      }),
    ),
  );

  const requireTerminal = Stream.fromEffect(
    Effect.suspend(() =>
      terminal
        ? Effect.void
        : Effect.fail(
            ModelProtocolError.make({
              message: `Tool Call ${call.id} completed without a terminal result`,
            }),
          ),
    ),
  ).pipe(Stream.drain);

  return started.pipe(
    Stream.concat(results),
    Stream.concat(requireTerminal),
    Stream.catchCause((cause) => {
      const waiting = waitingFromCause(cause);
      if (waiting !== undefined) {
        if (terminal || trace.finalToolResultIds.has(call.id)) {
          // A handler that produced a terminal result cannot also wait: the
          // anomaly fails loud and typed instead of suspending a settled call.
          return Stream.fail(
            ModelProtocolError.make({
              message: `Tool Call ${call.id} raised the waiting signal after its terminal result`,
            }),
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
        return Stream.failCause(cause);
      }
      terminal = true;
      trace.finalToolResultIds.add(call.id);
      return Stream.fromEffect(
        makeToolFailedEvent(context, turnId, call, Cause.squash(cause)),
      ).pipe(Stream.concat(Stream.failCause(cause)));
    }),
    Stream.withSpan("AgentRuntime.tool", {
      attributes: {
        agentId: context.agentId,
        runId: context.runId,
        turnId,
        toolCallId: call.id,
        toolName: call.name,
      },
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
  toolkit: Toolkit.WithHandler<Tools>,
  calls: ReadonlyArray<Response.ToolCallPart<string, unknown>>,
  trace: TurnTrace,
  concurrency: number,
  options: RunOptions<HookError, HookRequirements>,
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
  | AgentChildPending
  | AiError.AiError
  | Tool.HandlerError<ToolUnion<Tools>>,
  HookRequirements | Tool.HandlerServices<ToolUnion<Tools>>
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

      const durability = options.durability;
      // The hook's requirements are captured here (they are already part of
      // this stream's requirements) so the per-call `DurableStep` service can
      // run hook effects without leaking `HookRequirements` into handler
      // signatures.
      const hookServices = yield* Effect.context<HookRequirements>();

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
                const descriptors = prepared.flatMap((call) => {
                  const executionClass = getToolExecutionClass(call.tool);
                  return executionClass === "readonly"
                    ? []
                    : [
                        {
                          toolCallId: call.toolCallId,
                          toolName: call.name,
                          parameters: call.nativeHandlerParams,
                          executionClass,
                        } satisfies RunToolCallDescriptor,
                      ];
                });
                return descriptors.length === 0
                  ? Effect.void
                  : durability.prepareToolCalls(descriptors);
              }),
            ).pipe(Stream.drain);

      // Each executable call gets its own locally provided `DurableStep`,
      // bound to that call's identity: durable over the coordinator's step
      // hook, honest pass-through otherwise.
      const stepServiceFor = (call: PreparedToolCall<Tools>): DurableStepService =>
        durability === undefined
          ? passthroughDurableStep()
          : makeDurableStepService(call.toolCallId, durability.step, hookServices);

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

      const executable =
        settledCallIds === undefined
          ? prepared
          : prepared.filter((call) => !settledCallIds.has(call.call.id));

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
          Tool.HandlerServices<ToolUnion<Tools>>
        >
      >((stream, group) => {
        const next = Stream.mergeAll(
          group.map((call) =>
            withSemaphorePermit(
              semaphore,
              executePreparedToolCall(context, turnId, toolkit, call, trace, (waiting) => {
                waitingByDeclaration.set(call.declarationIndex, waiting);
              }).pipe(Stream.provideService(DurableStep, stepServiceFor(call))),
            ),
          ),
          { concurrency: "unbounded" },
        );
        return stream.pipe(Stream.concat(next));
      }, Stream.empty);

      // This batch's live sink. The queue is unbounded and drained by the
      // Run's own stream below, matching the Run's existing buffering:
      // emission never blocks a handler and no external observer can
      // backpressure the batch. Emitting after the batch settled fails closed
      // with `RunEventSinkClosedError`.
      const sinkQueue = yield* Queue.unbounded<SubagentEventPayload, Cause.Done>();
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
        Tool.HandlerServices<ToolUnion<Tools>>
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
      return approvalPreflight.pipe(Stream.concat(preparation), Stream.concat(settled));
    }),
  );

const ErrorMessage = Schema.Struct({ message: Schema.String });
const ErrorTag = Schema.Struct({ _tag: Schema.NonEmptyString });

const errorMessage = (error: unknown): string =>
  Option.match(Schema.decodeUnknownOption(ErrorMessage)(error), {
    onNone: () => String(error),
    onSome: ({ message }) => message,
  });

const errorTag = (error: unknown): string =>
  Option.match(Schema.decodeUnknownOption(ErrorTag)(error), {
    onNone: () => "UnknownError",
    onSome: ({ _tag }) => _tag,
  });

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
    if (result !== undefined) {
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

const consumeUsage = <AgentValue extends Agent.Any, HookError, HookRequirements>(
  agent: AgentValue,
  context: RunContext,
  trace: TurnTrace,
  options: RunOptions<HookError, HookRequirements>,
): Effect.Effect<void, AgentPolicyError | HookError, HookRequirements> =>
  Effect.gen(function* () {
    if (trace.usage === undefined) {
      return yield* AgentPolicyError.make({
        limit: "usage",
        message: "A completed model response did not report usage",
      });
    }
    const inputTokens = Math.max(
      0,
      trace.usage.inputTokens.total ??
        (trace.usage.inputTokens.uncached ?? 0) +
          (trace.usage.inputTokens.cacheRead ?? 0) +
          (trace.usage.inputTokens.cacheWrite ?? 0),
    );
    const outputTokens = Math.max(
      0,
      trace.usage.outputTokens.total ??
        (trace.usage.outputTokens.text ?? 0) + (trace.usage.outputTokens.reasoning ?? 0),
    );
    const totalTokens = inputTokens + outputTokens;
    const costMicrousd =
      options.estimateCostMicrousd === undefined
        ? 0
        : yield* options.estimateCostMicrousd(trace.usage);
    if (!Number.isInteger(costMicrousd) || costMicrousd < 0) {
      return yield* AgentPolicyError.make({
        limit: "cost",
        message: "Model cost estimation must produce a non-negative integer number of microdollars",
      });
    }
    context.modelCalls += 1;
    context.inputTokens += inputTokens;
    context.outputTokens += outputTokens;
    context.costMicrousd += costMicrousd;

    const tokenBudget = agent.definition.policy.tokenBudget;
    if (tokenBudget !== undefined && context.inputTokens + context.outputTokens > tokenBudget) {
      return yield* AgentPolicyError.make({
        limit: "tokens",
        message: `Agent exceeded its ${tokenBudget} token budget`,
      });
    }
    const costBudget = agent.definition.policy.costBudgetMicrousd;
    if (costBudget !== undefined) {
      if (options.estimateCostMicrousd === undefined) {
        return yield* AgentPolicyError.make({
          limit: "cost",
          message: "Agent cost budget requires a model cost estimator",
        });
      }
      if (context.costMicrousd > costBudget) {
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
        toolCalls: trace.toolCalls.size,
        costMicrousd,
        usage: trace.usage,
      };
      yield* options.budget.consume(delta);
    }
  });

// `Effect.fnUntraced`: this helper runs for every streamed Response Part, so a
// named span here would emit one span per TextDelta/ReasoningDelta. Spans stay
// on per-Run, per-Turn, and per-Tool operations.
const eventBase = Effect.fnUntraced(function* (context: RunContext) {
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
  if (trace.finished) {
    return yield* ModelProtocolError.make({
      message: "Model response emitted content after its finish part",
    });
  }
  trace.parts.push(part);
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
      // Official history carries the wire form: rebuild the just-pushed trace
      // part with the Schema-encoded parameters (the provider re-serializes
      // them on the next request regardless), while `applicationToolCalls`
      // below keeps the decoded part for the handler path — see TurnTrace.
      trace.parts[trace.parts.length - 1] = Response.makePart("tool-call", {
        id: part.id,
        name: part.name,
        params: parameters,
        providerExecuted: part.providerExecuted,
        metadata: part.metadata,
      });
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
      const result = yield* decodeEventJson(part.encodedResult, "Tool result");
      if (part.preliminary === true) {
        return [
          ToolProgress.make({
            ...(yield* eventBase(context)),
            turnId,
            toolCallId,
            toolName: part.name,
            result,
            providerExecuted: part.providerExecuted,
          }),
        ];
      }
      if (trace.finalToolResultIds.has(part.id)) {
        return yield* ModelProtocolError.make({
          message: `Tool Call ${part.id} produced more than one terminal result`,
        });
      }
      trace.finalToolResultIds.add(part.id);
      if (part.isFailure) {
        return [
          ToolCallFailed.make({
            ...(yield* eventBase(context)),
            turnId,
            toolCallId,
            toolName: part.name,
            errorTag: errorTag(part.result),
            message: errorMessage(part.result),
            providerExecuted: part.providerExecuted,
          }),
        ];
      }
      return [
        ToolCallSucceeded.make({
          ...(yield* eventBase(context)),
          turnId,
          toolCallId,
          toolName: part.name,
          result,
          providerExecuted: part.providerExecuted,
        }),
      ];
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
    default: {
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
  Schema.Json,
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
  yield* Schema.decodeUnknownEffect(agent.definition.output)(candidateOutput).pipe(
    Effect.mapError((cause) =>
      AgentOutputError.make({
        message: cause.message,
      }),
    ),
  );
  return eventJson;
});

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
    InstructionRequirements
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
      const ids = yield* IdGenerator;
      const turnId = yield* ids.nextTurnId;
      const modelContext =
        options.context === undefined
          ? { prompt }
          : yield* options.context.prepare({
              conversationId: context.conversationId,
              runId: context.runId,
              turnId,
              turn,
              source: prompt,
            });
      const trace: TurnTrace = {
        parts: [],
        text: [],
        textParts: new Map(),
        reasoningParts: new Map(),
        toolParameterParts: new Map(),
        toolCalls: new Map(),
        finalToolResultIds: new Set(),
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

      const response = guardBudgetStream(
        LanguageModel.streamText({
          prompt: modelContext.prompt,
          toolkit: agent.definition.toolkit,
          disableToolCallResolution: true,
        }),
        options.budget,
      ).pipe(
        Stream.mapEffect((part) =>
          eventsForPart(context, turnId, turn, agent.definition.toolkit.tools, trace, part),
        ),
        Stream.flatMap(Stream.fromIterable),
        Stream.withSpan("AgentRuntime.model", {
          attributes: {
            agentId: context.agentId,
            runId: context.runId,
            turnId,
          },
        }),
      );

      const continuation = Stream.unwrap(
        Effect.gen(function* () {
          if (!trace.finished) {
            return failRunEventStream(
              ModelProtocolError.make({
                message: "Model response ended without a finish part",
              }),
            );
          }
          yield* consumeUsage(agent, context, trace, options);
          const toolCalls = priorToolCalls + trace.toolCalls.size;
          if (toolCalls > agent.definition.policy.maxToolCalls) {
            return failRunEventStream(
              AgentPolicyError.make({
                limit: "tool-calls",
                message: `Agent exceeded its ${agent.definition.policy.maxToolCalls} Tool Call limit`,
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

          /** Official history advanced through this Turn's response, plus any Tool message. */
          const historyWithResponse = (...additions: ReadonlyArray<Prompt.Message>) =>
            Prompt.fromMessages([
              ...prompt.content,
              ...promptFromTurnParts(trace).content,
              ...additions,
            ]);

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
                if (turn >= agent.definition.policy.maxTurns) {
                  return failRunEventStream(
                    AgentPolicyError.make({
                      limit: "turns",
                      message: `Agent exceeded its ${agent.definition.policy.maxTurns} Turn limit`,
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
              const completed = RunCompleted.make({
                ...(yield* eventBase(context)),
                output,
                turns: turn,
                finishReason: "model-stop",
              });
              return Stream.succeed<RunEvent>(completed);
            });

          if (providerOnly && trace.finishReason === "stop") {
            return yield* settleOrFollowUp(historyWithResponse());
          }

          if (trace.toolCalls.size > 0) {
            if (trace.finishReason !== "tool-calls") {
              return failRunEventStream(
                ModelProtocolError.make({
                  message: `Model declared Tool Calls with incompatible finish reason ${trace.finishReason}`,
                }),
              );
            }
            if (turn >= agent.definition.policy.maxTurns) {
              return failRunEventStream(
                AgentPolicyError.make({
                  limit: "turns",
                  message: `Agent exceeded its ${agent.definition.policy.maxTurns} Turn limit`,
                }),
              );
            }
            if (trace.applicationToolCalls.length === 0) {
              yield* applyRepeatedFailurePolicy(
                context,
                trace,
                agent.definition.policy.repeatedFailureLimit,
              );
              return yield* continueTurn(historyWithResponse());
            }
            const toolkit = yield* agent.definition.toolkit;
            const concurrency = yield* schedulingConcurrency(
              agent.definition.policy.toolConcurrency,
              options.scheduling,
            );
            if (options.durability !== undefined) {
              // Durable turn-commit seam: the response becomes canonical
              // after the finish-part validations and before approval
              // preflight or preparation, creating the provably-safe window
              // of durability §15 ("after model item commit, before tool
              // preparation" resumes the declared batch without model
              // re-invocation). No-tool Turns never reach this branch and
              // keep their late single-batch commit.
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
                toolkit,
                trace.applicationToolCalls,
                trace,
                concurrency,
                options,
              ),
              options.budget,
            );
            return toolResults.pipe(
              Stream.concat(
                toolBatchContinuation(agent, context, trace, prompt, turn, toolCalls, options),
              ),
            );
          }

          if (trace.finishReason !== "stop") {
            return failRunEventStream(
              ModelProtocolError.make({
                message: `Model stopped without a final answer (${trace.finishReason})`,
              }),
            );
          }
          return yield* settleOrFollowUp(historyWithResponse());
        }),
      );

      return started.pipe(Stream.concat(response), Stream.concat(continuation));
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
    InstructionRequirements
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
    InstructionRequirements
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
        parts: [],
        text: [],
        textParts: new Map(),
        reasoningParts: new Map(),
        toolParameterParts: new Map(),
        toolCalls: new Map(),
        finalToolResultIds: new Set(),
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
      for (const settledCall of resume.settled) {
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
      const toolCalls = trace.toolCalls.size;
      if (toolCalls > agent.definition.policy.maxToolCalls) {
        return failRunEventStream(
          AgentPolicyError.make({
            limit: "tool-calls",
            message: `Agent exceeded its ${agent.definition.policy.maxToolCalls} Tool Call limit`,
          }),
        );
      }
      if (turn >= agent.definition.policy.maxTurns) {
        return failRunEventStream(
          AgentPolicyError.make({
            limit: "turns",
            message: `Agent exceeded its ${agent.definition.policy.maxTurns} Turn limit`,
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

      const toolResults = guardBudgetStream(
        executeToolBatch(
          context,
          turnId,
          toolkit,
          trace.applicationToolCalls,
          trace,
          concurrency,
          options,
          settledIds,
        ),
        options.budget,
      );
      return started.pipe(
        Stream.concat(toolResults),
        Stream.concat(
          toolBatchContinuation(agent, context, trace, resumedPrompt, turn, toolCalls, options),
        ),
      );
    }),
  );

const failRunEventStream = <Error>(error: Error): Stream.Stream<RunEvent, Error> =>
  Stream.fail(error);

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
    InstructionRequirements
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
      const startedAtMillis = yield* Clock.currentTimeMillis;
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
        pendingFollowUps: [],
        history: options.history ?? Prompt.empty,
        modelCalls: 0,
        consecutiveToolFailures: 0,
        inputTokens: 0,
        outputTokens: 0,
        costMicrousd: 0,
        sequence: 0,
      };
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
          const prompt = yield* makeInitialPrompt(instructions, encodedInput, context.history);
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

      const durationLimit = AgentPolicyError.make({
        limit: "duration",
        message: `Agent exceeded its ${Duration.format(agent.definition.policy.maxDuration)} duration limit`,
      });
      const deadlineEffect = Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const remaining =
          startedAtMillis + Duration.toMillis(agent.definition.policy.maxDuration) - now;
        if (remaining > 0) {
          yield* Effect.sleep(remaining);
        }
        return yield* durationLimit;
      });
      const deadline = execution.pipe(Stream.interruptWhen(deadlineEffect));

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
      );

      return started.pipe(
        Stream.concat(deadline),
        Stream.catch((error) => {
          const terminal = Stream.fromEffect(
            Effect.gen(function* () {
              if (error instanceof AgentApprovalPending || error instanceof AgentChildPending) {
                return RunSuspended.make({
                  ...(yield* eventBase(context)),
                  reason: error.message,
                });
              }
              return RunFailed.make({
                ...(yield* eventBase(context)),
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
  return finalized.pipe(Stream.provide(agent.model, { local: true }));
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
  Error | ModelProtocolError | AgentOutputError,
  Requirements | AgentValue["definition"]["output"]["DecodingServices"]
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
    return {
      output,
      conversationId: completed.conversationId,
      runId: completed.runId,
      turns: completed.turns,
      finishReason: completed.finishReason,
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
    InstructionRequirements
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
 * and ends once the Run settles. Events are never dropped for a slow
 * subscriber, and publishing never blocks the Run. `events` remains the
 * complete replay, available after settlement. Both belong to the `start`
 * Scope; observing after that Scope closes interrupts the observer.
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
    InstructionRequirements
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
  // Single-writer append-only trace owned by the Run fiber; readers only see
  // it after the fiber settles, so a plain array avoids the quadratic cost of
  // copying an immutable Ref array per event.
  const captured: Array<RunEvent> = [];
  // The unbounded replay window keeps `observe` subscriptions live without
  // backpressuring the Run: it retains references to the same events as
  // `captured`, replays them to subscribers that attach mid-Run or after
  // settlement, and the terminal `Exit.void` Take ends every subscription.
  const pubsub = yield* PubSub.unbounded<Take.Take<RunEvent>>({
    replay: Number.MAX_SAFE_INTEGER,
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
        message: `Durable Step ${name} was executed outside an active Tool Call`,
      }),
    ),
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
                message: `Durable Step name ${name} was reused within one Tool Call`,
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
            message: `Durable Step name ${name} was reused within Tool Call ${toolCallId}`,
          });
        }
        usedNames.add(name);
        const key: RunStepKey = { toolCallId, stepName: name };
        const recorded = yield* provideHookServices(hook.lookup(key), hookServices).pipe(
          Effect.mapError((cause) =>
            DurableStepError.make({
              toolCallId,
              stepName: name,
              reason: "lookup-failed",
              message: `Durable Step lookup failed: ${errorMessage(cause)}`,
            }),
          ),
        );
        if (Option.isSome(recorded)) {
          return yield* Schema.decodeUnknownEffect(output)(recorded.value.encodedOutput).pipe(
            Effect.mapError((cause) =>
              DurableStepError.make({
                toolCallId,
                stepName: name,
                reason: "recorded-result-invalid",
                message: `Recorded Durable Step result did not decode through the declared output Schema: ${cause.message}`,
              }),
            ),
          );
        }
        const value = yield* execute;
        const encodedOutput = yield* Schema.encodeEffect(output)(value).pipe(
          Effect.mapError((cause) =>
            DurableStepError.make({
              toolCallId,
              stepName: name,
              reason: "output-encoding-failed",
              message: `Durable Step output did not encode through the declared output Schema: ${cause.message}`,
            }),
          ),
        );
        yield* provideHookServices(hook.commit(key, encodedOutput), hookServices).pipe(
          Effect.mapError((cause) =>
            DurableStepError.make({
              toolCallId,
              stepName: name,
              reason: "commit-failed",
              message: `Durable Step commit failed: ${errorMessage(cause)}`,
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
    message: Schema.String,
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
      Effect.mapError((cause) =>
        SubagentDurabilityError.make({
          operation: "establish",
          reason: "hook-failed",
          toolCallId: request.toolCallId,
          message: `Durable child establishment failed: ${errorMessage(cause)}`,
        }),
      ),
    ),
  join: (request) =>
    provideHookServices(hook.join(request), hookServices).pipe(
      Effect.mapError((cause) =>
        SubagentDurabilityError.make({
          operation: "join",
          reason: "hook-failed",
          toolCallId: request.toolCallId,
          message: `Durable child join failed: ${errorMessage(cause)}`,
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
 * in the returned Effect or Stream. `layer` is empty because this interpreter
 * owns no shared runtime state.
 */
export const AgentRuntime = {
  layer: Layer.empty,
  run,
  start,
  stream,
} as const;
