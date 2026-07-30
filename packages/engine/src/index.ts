import {
  Cause,
  Clock,
  DateTime,
  Duration,
  Effect,
  Fiber,
  Layer,
  Metric,
  Option,
  Ref,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import {
  Agent,
  AgentApprovalDenied,
  AgentApprovalPending,
  AgentInputError,
  AgentOutputError,
  AgentPolicyError,
  ApprovalRequested,
  ConversationId,
  type Definition,
  IdGenerator,
  type InstructionSource,
  ModelStarted,
  ModelProtocolError,
  ReasoningDelta,
  RunCompleted,
  RunFailed,
  RunStarted,
  RunSuspended,
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
  type AiError,
  LanguageModel,
  type Model,
  Prompt,
  Response,
  Tool,
  type Toolkit,
} from "effect/unstable/ai";

type RuntimeBinding<
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
  type CommandDrainPolicy,
  type RunApprovalDecision,
  type RunOptions,
  type RunSchedulingHook,
  type RunUsageDelta,
} from "./run-options.ts";

export * from "./run-options.ts";

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
  | HookError
  | InstructionError;

/** Inferred agent services plus the runtime's identity-generation authority. */
export type AgentRuntimeRequirements<
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
 * the owning parameter Schema. The pinned `Toolkit.handle` implementation
 * decodes once more internally, despite its decoded-parameter signature, so
 * transformed values must be encoded back to that native runtime boundary.
 * These private function-type assertions restore correlation lost by dynamic
 * record lookup only around a successful Schema encode; they never bypass
 * validation.
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
  const encodeParameters = Schema.encodeUnknownEffect(tool.parametersSchema) as (
    input: Tool.Parameters<ToolUnion<Tools>>,
  ) => Effect.Effect<unknown, Schema.SchemaError, Tool.HandlerServices<ToolUnion<Tools>>>;
  return encodeParameters(decodedParams).pipe(
    Effect.mapError((cause) =>
      ModelProtocolError.make({
        message: `Invalid parameters for Tool ${call.name}: ${cause.message}`,
      }),
    ),
    Effect.map((encodedParams) => ({
      call,
      name,
      decodedParams,
      nativeHandlerParams: encodedParams as Tool.Parameters<ToolUnion<Tools>>,
      tool,
      declarationIndex,
    })),
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

/** Execute a wholly preflighted native Toolkit batch under one finite engine semaphore. */
const executeToolBatch = <Tools extends Record<string, Tool.Any>, HookError, HookRequirements>(
  context: RunContext,
  turnId: TurnId,
  toolkit: Toolkit.WithHandler<Tools>,
  calls: ReadonlyArray<Response.ToolCallPart<string, unknown>>,
  trace: TurnTrace,
  concurrency: number,
  options: RunOptions<HookError, HookRequirements>,
): Stream.Stream<
  RunEvent,
  | HookError
  | ModelProtocolError
  | AgentApprovalDenied
  | AgentApprovalPending
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

      const groups: Array<ReadonlyArray<PreparedToolCall<Tools>>> = [];
      let parallel: Array<PreparedToolCall<Tools>> = [];
      for (const call of prepared) {
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
              executePreparedToolCall(context, turnId, toolkit, call, trace),
            ),
          ),
          { concurrency: "unbounded" },
        );
        return stream.pipe(Stream.concat(next));
      }, Stream.empty);
      return approvalPreflight.pipe(Stream.concat(handlers));
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

const inputsToPrompt = (
  inputs: ReadonlyArray<Prompt.RawInput>,
): Effect.Effect<Prompt.Prompt, ModelProtocolError> =>
  Effect.try({
    try: () => Prompt.fromMessages(inputs.flatMap((input) => Prompt.make(input).content)),
    catch: (cause) =>
      ModelProtocolError.make({
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
): Effect.Effect<Prompt.Prompt, HookError | ModelProtocolError, HookRequirements> =>
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
        (trace.usage.inputTokens.uncached ?? 0) + (trace.usage.inputTokens.cacheRead ?? 0),
    );
    const outputTokens = Math.max(0, trace.usage.outputTokens.total ?? 0);
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

const eventBase = Effect.fn("AgentRuntime.eventBase")(function* (context: RunContext) {
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
          ...instructionPrompt.content,
          ...history.content,
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

const eventsForPart = Effect.fn("AgentRuntime.eventsForPart")(function* <
  Tools extends Record<string, Tool.Any>,
>(
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
      const encodeParameters = Schema.encodeUnknownEffect(tool.parametersSchema) as (
        input: Tool.Parameters<ToolUnion<Tools>>,
      ) => Effect.Effect<unknown, Schema.SchemaError, Tool.HandlerServices<ToolUnion<Tools>>>;
      const encodedParameters = yield* encodeParameters(
        part.params as Tool.Parameters<ToolUnion<Tools>>,
      ).pipe(
        Effect.mapError((cause) =>
          ModelProtocolError.make({
            message: `Invalid parameters for Tool ${part.name}: ${cause.message}`,
          }),
        ),
      );
      const parameters = yield* decodeEventJson(encodedParameters, "Tool parameters");
      trace.toolCalls.set(part.id, {
        name: part.name,
        providerExecuted: part.providerExecuted,
      });
      if (!part.providerExecuted) {
        trace.applicationToolCalls.push(part);
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
  AgentRuntimeRequirements<typeof agent, HookRequirements, InstructionRequirements>
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
          if (providerOnly && trace.finishReason === "stop") {
            const responsePrompt = promptFromTurnParts(trace);
            const history = Prompt.fromMessages([...prompt.content, ...responsePrompt.content]);
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
              const responsePrompt = promptFromTurnParts(trace);
              const history = Prompt.fromMessages([...prompt.content, ...responsePrompt.content]);
              yield* advanceHistory(context, history, options);
              const steering = yield* drainInputs(context, options);
              const nextPrompt = yield* appendInputs(context, history, steering, options);
              return makeTurn(agent, context, nextPrompt, turn + 1, toolCalls, options);
            }
            const toolkit = yield* agent.definition.toolkit;
            const concurrency = yield* schedulingConcurrency(
              agent.definition.policy.toolConcurrency,
              options.scheduling,
            );
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
            const continuationAfterTools = Stream.unwrap(
              Effect.gen(function* () {
                if (trace.finalToolResultIds.size !== trace.toolCalls.size) {
                  return failRunEventStream(
                    ModelProtocolError.make({
                      message: "A Tool Call turn completed without one final result per Tool Call",
                    }),
                  );
                }
                const responsePrompt = promptFromTurnParts(trace);
                const orderedResults: Array<(typeof trace.applicationToolResults)[number]> = [];
                for (const call of trace.applicationToolCalls) {
                  const result = trace.applicationToolResults.find(
                    (candidate) => candidate.id === call.id,
                  );
                  if (result === undefined) {
                    return failRunEventStream(
                      ModelProtocolError.make({ message: "Tool batch did not settle completely" }),
                    );
                  }
                  orderedResults.push(result);
                }
                const toolMessage = Prompt.makeMessage("tool", {
                  content: orderedResults.map((result) =>
                    Prompt.makePart("tool-result", {
                      id: result.id,
                      name: result.name,
                      result: result.encodedResult,
                      isFailure: result.isFailure,
                    }),
                  ),
                });
                const nextPrompt = Prompt.fromMessages([
                  ...prompt.content,
                  ...responsePrompt.content,
                  toolMessage,
                ]);
                yield* advanceHistory(context, nextPrompt, options);
                const steering = yield* drainInputs(context, options);
                const nextHistory = yield* appendInputs(context, nextPrompt, steering, options);
                return makeTurn(agent, context, nextHistory, turn + 1, toolCalls, options);
              }),
            );
            return toolResults.pipe(Stream.concat(continuationAfterTools));
          }

          if (trace.finishReason !== "stop") {
            return failRunEventStream(
              ModelProtocolError.make({
                message: `Model stopped without a final answer (${trace.finishReason})`,
              }),
            );
          }
          const responsePrompt = promptFromTurnParts(trace);
          const history = Prompt.fromMessages([...prompt.content, ...responsePrompt.content]);
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
        }),
      );

      return started.pipe(Stream.concat(response), Stream.concat(continuation));
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
      const runId = yield* ids.nextRunId;
      const context: RunContext = {
        agentId: agent.definition.id,
        conversationId,
        runId,
        pendingFollowUps: [],
        history: options.history ?? Prompt.empty,
        modelCalls: 0,
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
      return started.pipe(
        Stream.concat(deadline),
        Stream.catch((error) => {
          const terminal = Stream.fromEffect(
            Effect.gen(function* () {
              if (error instanceof AgentApprovalPending) {
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
  const reduction = yield* Stream.runFold(
    stream(agent, input, options),
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

/** Scoped detached execution whose replay observer cannot backpressure Run completion. */
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
  const captured = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
  const execution = Effect.gen(function* () {
    const reduction = yield* Stream.runFold(
      stream(agent, input, options).pipe(
        Stream.tap((event) => Ref.update(captured, (events) => [...events, event])),
      ),
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
  const fiber = yield* execution.pipe(Effect.forkScoped);
  const events = Fiber.await(fiber).pipe(Effect.andThen(Ref.get(captured)));
  return {
    await: Fiber.join(fiber),
    events,
    observe: Stream.unwrap(events.pipe(Effect.map(Stream.fromIterable))),
  };
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
