import {
  Cause,
  Clock,
  DateTime,
  Duration,
  Effect,
  Layer,
  Metric,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import {
  Agent,
  AgentInputError,
  AgentOutputError,
  AgentPolicyError,
  type ConversationId,
  IdGenerator,
  ModelProtocolError,
  type RunEvent,
  type RunId,
  ToolCallId,
  type TurnId,
} from "@effect-agent/core";
import {
  type AiError,
  LanguageModel,
  Prompt,
  type Response,
  Tool,
  type Toolkit,
} from "effect/unstable/ai";

/** Decoded terminal value produced by reducing a completed agent event stream. */
export interface AgentResult<Output> {
  readonly output: Output;
  readonly conversationId: ConversationId;
  readonly runId: RunId;
  /** Number of model turns completed by the run. */
  readonly turns: number;
  /** Normalized reason that the runtime accepted the terminal output. */
  readonly finishReason: "completed" | "model-stop";
}

/** Expected agent, policy, and model-protocol failures exposed by the runtime. */
export type AgentRuntimeFailure<AgentValue extends Agent.Any> =
  | Agent.Failure<AgentValue>
  | AgentPolicyError
  | ModelProtocolError;

/** Inferred agent services plus the runtime's identity-generation authority. */
export type AgentRuntimeRequirements<AgentValue extends Agent.Any> =
  | Agent.Requirements<AgentValue>
  | IdGenerator;

interface RunContext {
  readonly agentId: Agent.AnyDefinition["id"];
  readonly conversationId: ConversationId;
  readonly runId: RunId;
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
  readonly toolCallIds: Set<string>;
  readonly toolCallProviderExecutions: Map<string, boolean>;
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
}

type PartLifecycle = "open" | "closed";

type ToolUnion<Tools extends Record<string, Tool.Any>> = Tools[keyof Tools];

interface PreparedToolCall<Tools extends Record<string, Tool.Any>> {
  readonly call: Response.ToolCallPart<string, unknown>;
  readonly name: keyof Tools & string;
  readonly nativeHandlerParams: Tool.Parameters<ToolUnion<Tools>>;
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
      new ModelProtocolError({
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
        new ModelProtocolError({
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
      new ModelProtocolError({ message: `Model requested unknown Tool ${call.name}` }),
    );
  }
  const tool = toolkit.tools[name];
  const encodeParameters = Schema.encodeUnknownEffect(tool.parametersSchema) as (
    input: Tool.Parameters<ToolUnion<Tools>>,
  ) => Effect.Effect<unknown, Schema.SchemaError, Tool.HandlerServices<ToolUnion<Tools>>>;
  return encodeParameters(call.params as Tool.Parameters<ToolUnion<Tools>>).pipe(
    Effect.mapError(
      (cause) =>
        new ModelProtocolError({
          message: `Invalid parameters for Tool ${call.name}: ${cause.message}`,
        }),
    ),
    Effect.map((encodedParams) => ({
      call,
      name,
      nativeHandlerParams: encodedParams as Tool.Parameters<ToolUnion<Tools>>,
      declarationIndex,
    })),
  );
};

const makeToolFailedEvent = (
  context: RunContext,
  turnId: TurnId,
  call: Response.ToolCallPart<string, unknown>,
  error: unknown,
): Effect.Effect<RunEvent, ModelProtocolError> =>
  Effect.gen(function* () {
    const toolCallId = yield* decodeToolCallId(call.id);
    return {
      ...(yield* eventBase(context)),
      _tag: "ToolCallFailed" as const,
      turnId,
      toolCallId,
      toolName: call.name,
      errorTag: errorTag(error),
      message: errorMessage(error),
      providerExecuted: false,
    } satisfies RunEvent;
  });

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
      return {
        ...(yield* eventBase(context)),
        _tag: "ToolCallStarted" as const,
        turnId,
        toolCallId,
        toolName: call.name,
      } satisfies RunEvent;
    }).pipe(Effect.withLogSpan("AgentRuntime.tool")),
  );

  const results = Stream.unwrap(
    toolkit
      .handle(prepared.name, prepared.nativeHandlerParams, call.id)
      .pipe(Effect.withSpan("AgentRuntime.toolkit.handle"), Effect.withTracerEnabled(false)),
  ).pipe(
    Stream.mapEffect((result) =>
      Effect.gen(function* () {
        if (terminal || trace.finalToolResultIds.has(call.id)) {
          return yield* new ModelProtocolError({
            message: `Tool Call ${call.id} produced more than one terminal result`,
          });
        }
        const toolCallId = yield* decodeToolCallId(call.id);
        if (result.preliminary) {
          return {
            ...(yield* eventBase(context)),
            _tag: "ToolProgress" as const,
            turnId,
            toolCallId,
            toolName: call.name,
            result: yield* decodeEventJson(result.encodedResult, "Tool result"),
            providerExecuted: false,
          } satisfies RunEvent;
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
          return {
            ...(yield* eventBase(context)),
            _tag: "ToolCallFailed" as const,
            turnId,
            toolCallId,
            toolName: call.name,
            errorTag: errorTag(result.result),
            message: errorMessage(result.result),
            providerExecuted: false,
          } satisfies RunEvent;
        }
        return {
          ...(yield* eventBase(context)),
          _tag: "ToolCallSucceeded" as const,
          turnId,
          toolCallId,
          toolName: call.name,
          result: yield* decodeEventJson(result.encodedResult, "Tool result"),
          providerExecuted: false,
        } satisfies RunEvent;
      }),
    ),
  );

  const requireTerminal = Stream.fromEffect(
    Effect.suspend(() =>
      terminal
        ? Effect.void
        : Effect.fail(
            new ModelProtocolError({
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
const executeToolBatch = <Tools extends Record<string, Tool.Any>>(
  context: RunContext,
  turnId: TurnId,
  toolkit: Toolkit.WithHandler<Tools>,
  calls: ReadonlyArray<Response.ToolCallPart<string, unknown>>,
  trace: TurnTrace,
  concurrency: number,
): Stream.Stream<
  RunEvent,
  ModelProtocolError | AiError.AiError | Tool.HandlerError<ToolUnion<Tools>>,
  Tool.HandlerServices<ToolUnion<Tools>>
> =>
  Stream.unwrap(
    Effect.gen(function* () {
      // Resolve every name and decode every parameter object before any call stream is constructed.
      const prepared = yield* Effect.forEach(calls, (call, declarationIndex) =>
        prepareToolCall(toolkit, call, declarationIndex),
      );
      const semaphore = yield* Semaphore.make(concurrency);
      return Stream.mergeAll(
        prepared.map((call) =>
          withSemaphorePermit(
            semaphore,
            executePreparedToolCall(context, turnId, toolkit, call, trace),
          ),
        ),
        { concurrency: "unbounded" },
      );
    }),
  );

const errorMessage = (error: unknown): string => {
  if (
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof error.message === "string"
  ) {
    return error.message;
  }
  return String(error);
};

const errorTag = (error: unknown): string => {
  if (
    typeof error === "object" &&
    error !== null &&
    "_tag" in error &&
    typeof error._tag === "string" &&
    error._tag.length > 0
  ) {
    return error._tag;
  }
  return "UnknownError";
};

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

const decodeInput = <AgentValue extends Agent.Any>(
  agent: AgentValue,
  input: unknown,
): Effect.Effect<
  Agent.Input<AgentValue>,
  AgentInputError,
  AgentValue["definition"]["input"]["DecodingServices"]
> =>
  Schema.decodeUnknownEffect(agent.definition.input)(input).pipe(
    Effect.mapError(
      (cause) =>
        new AgentInputError({
          message: cause.message,
        }),
    ),
  );

const evaluateInstructions = <AgentValue extends Agent.Any>(
  agent: AgentValue,
  input: Agent.Input<AgentValue>,
) =>
  Effect.suspend(() => {
    const instruction = agent.definition.instructions;
    const result = typeof instruction === "function" ? instruction(input) : instruction;
    return Effect.isEffect(result) ? result : Effect.succeed(result);
  });

const encodeInput = <AgentValue extends Agent.Any>(
  agent: AgentValue,
  input: Agent.Input<AgentValue>,
): Effect.Effect<
  AgentValue["definition"]["input"]["Encoded"],
  AgentInputError,
  AgentValue["definition"]["input"]["EncodingServices"]
> =>
  Schema.encodeEffect(agent.definition.input)(input).pipe(
    Effect.mapError(
      (cause) =>
        new AgentInputError({
          message: `Unable to encode Agent input: ${cause.message}`,
        }),
    ),
  );

const makeInitialPrompt = (
  instructions: Prompt.RawInput,
  input: unknown,
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
        Prompt.makeMessage("user", {
          content: [Prompt.makePart("text", { text: encodedInput })],
        }),
      ]);
    },
    catch: (cause) =>
      new AgentInputError({
        message: `Unable to materialize Agent input: ${errorMessage(cause)}`,
      }),
  });

const decodeToolCallId = (id: string): Effect.Effect<ToolCallId, ModelProtocolError> =>
  Schema.decodeEffect(ToolCallId)(id).pipe(
    Effect.mapError(
      (cause) =>
        new ModelProtocolError({
          message: `Invalid Tool Call ID: ${cause.message}`,
        }),
    ),
  );

const decodeEventJson = (
  value: unknown,
  label: string,
): Effect.Effect<Schema.Json, ModelProtocolError> =>
  Schema.decodeUnknownEffect(Schema.Json)(value).pipe(
    Effect.mapError(
      (cause) =>
        new ModelProtocolError({
          message: `${label} is not JSON: ${cause.message}`,
        }),
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

const eventsForPart = (
  context: RunContext,
  turnId: TurnId,
  turn: number,
  trace: TurnTrace,
  part: Response.AnyPart,
): Effect.Effect<ReadonlyArray<RunEvent>, ModelProtocolError> =>
  Effect.gen(function* () {
    if (trace.finished) {
      return yield* new ModelProtocolError({
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
          {
            ...(yield* eventBase(context)),
            _tag: "TextDelta" as const,
            turnId,
            text: part.delta,
          },
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
          {
            ...(yield* eventBase(context)),
            _tag: "ReasoningDelta" as const,
            turnId,
            text: part.delta,
          },
        ];
      }
      case "reasoning-end": {
        yield* endPart(trace.reasoningParts, part.id, "reasoning");
        return [];
      }
      case "tool-params-start": {
        if (trace.toolParameterParts.has(part.id)) {
          return yield* new ModelProtocolError({
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
          return yield* new ModelProtocolError({
            message: `Model response emitted Tool parameter delta for inactive part ${part.id}`,
          });
        }
        return [];
      }
      case "tool-params-end": {
        const parameterPart = trace.toolParameterParts.get(part.id);
        if (parameterPart?.state !== "open") {
          return yield* new ModelProtocolError({
            message: `Model response emitted Tool parameter end for inactive part ${part.id}`,
          });
        }
        parameterPart.state = "closed";
        return [];
      }
      case "tool-call": {
        if (trace.toolCallIds.has(part.id)) {
          return yield* new ModelProtocolError({
            message: `Model response repeated Tool Call ID ${part.id}`,
          });
        }
        const parameterPart = trace.toolParameterParts.get(part.id);
        if (parameterPart?.state === "open") {
          return yield* new ModelProtocolError({
            message: `Model response declared Tool Call ${part.id} before its parameters completed`,
          });
        }
        if (
          parameterPart !== undefined &&
          (parameterPart.name !== part.name ||
            parameterPart.providerExecuted !== part.providerExecuted)
        ) {
          return yield* new ModelProtocolError({
            message: `Completed Tool parameters did not match Tool Call ${part.id}`,
          });
        }
        const toolCallId = yield* decodeToolCallId(part.id);
        const parameters = yield* decodeEventJson(part.params, "Tool parameters");
        trace.toolCallIds.add(part.id);
        trace.toolCallProviderExecutions.set(part.id, part.providerExecuted);
        if (!part.providerExecuted) {
          trace.applicationToolCalls.push(part);
        }
        const declared = {
          ...(yield* eventBase(context)),
          _tag: "ToolCallDeclared" as const,
          turnId,
          toolCallId,
          toolName: part.name,
          parameters,
          providerExecuted: part.providerExecuted,
        };
        return [declared];
      }
      case "tool-result": {
        if (!trace.toolCallIds.has(part.id)) {
          return yield* new ModelProtocolError({
            message: `Model response returned an unrequested Tool result ${part.id}`,
          });
        }
        if (trace.toolCallProviderExecutions.get(part.id) !== part.providerExecuted) {
          return yield* new ModelProtocolError({
            message: `Tool result execution boundary did not match Tool Call ${part.id}`,
          });
        }
        if (!part.providerExecuted) {
          return yield* new ModelProtocolError({
            message: `Model response included an application Tool result before engine execution for ${part.id}`,
          });
        }
        const toolCallId = yield* decodeToolCallId(part.id);
        const result = yield* decodeEventJson(part.encodedResult, "Tool result");
        if (part.preliminary === true) {
          return [
            {
              ...(yield* eventBase(context)),
              _tag: "ToolProgress" as const,
              turnId,
              toolCallId,
              toolName: part.name,
              result,
              providerExecuted: part.providerExecuted,
            },
          ];
        }
        if (trace.finalToolResultIds.has(part.id)) {
          return yield* new ModelProtocolError({
            message: `Tool Call ${part.id} produced more than one terminal result`,
          });
        }
        trace.finalToolResultIds.add(part.id);
        if (part.isFailure) {
          return [
            {
              ...(yield* eventBase(context)),
              _tag: "ToolCallFailed" as const,
              turnId,
              toolCallId,
              toolName: part.name,
              errorTag: errorTag(part.result),
              message: errorMessage(part.result),
              providerExecuted: part.providerExecuted,
            },
          ];
        }
        return [
          {
            ...(yield* eventBase(context)),
            _tag: "ToolCallSucceeded" as const,
            turnId,
            toolCallId,
            toolName: part.name,
            result,
            providerExecuted: part.providerExecuted,
          },
        ];
      }
      case "finish": {
        const openPart = firstOpenPart(trace);
        if (openPart !== undefined) {
          return yield* new ModelProtocolError({
            message: `Model response finished before completing ${openPart}`,
          });
        }
        trace.finished = true;
        trace.finishReason = part.reason;
        return [
          {
            ...(yield* eventBase(context)),
            _tag: "TurnCompleted" as const,
            turnId,
            turn,
            finishReason: part.reason,
          },
        ];
      }
      case "error": {
        return yield* new ModelProtocolError({
          message: `Model response failed: ${errorMessage(part.error)}`,
        });
      }
      default: {
        return [];
      }
    }
  });

const decodeFinalOutput = <AgentValue extends Agent.Any>(
  agent: AgentValue,
  text: string,
): Effect.Effect<
  Schema.Json,
  AgentOutputError,
  AgentValue["definition"]["output"]["DecodingServices"]
> =>
  Effect.gen(function* () {
    const json: unknown = yield* Effect.try({
      try: () => JSON.parse(text),
      catch: (cause) =>
        new AgentOutputError({
          message: `Agent output is not valid JSON: ${errorMessage(cause)}`,
        }),
    });
    const eventJson = yield* Schema.decodeUnknownEffect(Schema.Json)(json).pipe(
      Effect.mapError(
        (cause) =>
          new AgentOutputError({
            message: `Agent output is not JSON: ${cause.message}`,
          }),
      ),
    );
    const candidateOutput: unknown = eventJson;
    yield* Schema.decodeUnknownEffect(agent.definition.output)(candidateOutput).pipe(
      Effect.mapError(
        (cause) =>
          new AgentOutputError({
            message: cause.message,
          }),
      ),
    );
    return eventJson;
  });

const makeTurn = <AgentValue extends Agent.Any>(
  agent: AgentValue,
  context: RunContext,
  prompt: Prompt.Prompt,
  turn: number,
  priorToolCalls: number,
): Stream.Stream<RunEvent, AgentRuntimeFailure<AgentValue>, AgentRuntimeRequirements<AgentValue>> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const ids = yield* IdGenerator;
      const turnId = yield* ids.nextTurnId;
      const trace: TurnTrace = {
        parts: [],
        text: [],
        textParts: new Map(),
        reasoningParts: new Map(),
        toolParameterParts: new Map(),
        toolCallIds: new Set(),
        toolCallProviderExecutions: new Map(),
        finalToolResultIds: new Set(),
        applicationToolCalls: [],
        applicationToolResults: [],
        finished: false,
        finishReason: undefined,
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
            {
              ...(yield* eventBase(context)),
              _tag: "TurnStarted" as const,
              turnId,
              turn,
            },
            {
              ...(yield* eventBase(context)),
              _tag: "ModelStarted" as const,
              turnId,
              turn,
            },
          ] satisfies ReadonlyArray<RunEvent>;
        }).pipe(Effect.withLogSpan("AgentRuntime.model")),
      ).pipe(Stream.flatMap(Stream.fromIterable));

      const response = LanguageModel.streamText({
        prompt,
        toolkit: agent.definition.toolkit,
        disableToolCallResolution: true,
      }).pipe(
        Stream.mapEffect((part) => eventsForPart(context, turnId, turn, trace, part)),
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
              new ModelProtocolError({
                message: "Model response ended without a finish part",
              }),
            );
          }
          const toolCalls = priorToolCalls + trace.toolCallIds.size;
          if (toolCalls > agent.definition.policy.maxToolCalls) {
            return failRunEventStream(
              new AgentPolicyError({
                limit: "tool-calls",
                message: `Agent exceeded its ${agent.definition.policy.maxToolCalls} Tool Call limit`,
              }),
            );
          }
          const providerOnly =
            trace.toolCallIds.size > 0 &&
            Array.from(trace.toolCallProviderExecutions.values()).every(
              (providerExecuted) => providerExecuted,
            );
          const missingProviderResult = Array.from(trace.toolCallProviderExecutions.entries()).find(
            ([id, providerExecuted]) => providerExecuted && !trace.finalToolResultIds.has(id),
          );
          if (missingProviderResult !== undefined) {
            return failRunEventStream(
              new ModelProtocolError({
                message: `Provider-executed Tool Call ${missingProviderResult[0]} completed without a terminal result`,
              }),
            );
          }
          if (providerOnly && trace.finishReason === "stop") {
            const output = yield* decodeFinalOutput(agent, trace.text.join(""));
            const completed = {
              ...(yield* eventBase(context)),
              _tag: "RunCompleted" as const,
              output,
              turns: turn,
              finishReason: "model-stop" as const,
            } satisfies RunEvent;
            return Stream.succeed<RunEvent>(completed);
          }

          if (trace.toolCallIds.size > 0) {
            if (trace.finishReason !== "tool-calls") {
              return failRunEventStream(
                new ModelProtocolError({
                  message: `Model declared Tool Calls with incompatible finish reason ${trace.finishReason}`,
                }),
              );
            }
            if (turn >= agent.definition.policy.maxTurns) {
              return failRunEventStream(
                new AgentPolicyError({
                  limit: "turns",
                  message: `Agent exceeded its ${agent.definition.policy.maxTurns} Turn limit`,
                }),
              );
            }
            if (trace.applicationToolCalls.length === 0) {
              const responsePrompt = promptFromTurnParts(trace);
              return makeTurn(
                agent,
                context,
                Prompt.fromMessages([...prompt.content, ...responsePrompt.content]),
                turn + 1,
                toolCalls,
              );
            }
            const toolkit = yield* agent.definition.toolkit;
            const toolResults = executeToolBatch(
              context,
              turnId,
              toolkit,
              trace.applicationToolCalls,
              trace,
              agent.definition.policy.toolConcurrency,
            );
            const continuationAfterTools = Stream.unwrap(
              Effect.sync(() => {
                if (trace.finalToolResultIds.size !== trace.toolCallIds.size) {
                  return failRunEventStream(
                    new ModelProtocolError({
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
                      new ModelProtocolError({ message: "Tool batch did not settle completely" }),
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
                return makeTurn(agent, context, nextPrompt, turn + 1, toolCalls);
              }),
            );
            return toolResults.pipe(Stream.concat(continuationAfterTools));
          }

          if (trace.finishReason !== "stop") {
            return failRunEventStream(
              new ModelProtocolError({
                message: `Model stopped without a final answer (${trace.finishReason})`,
              }),
            );
          }
          const output = yield* decodeFinalOutput(agent, trace.text.join(""));
          const completed = {
            ...(yield* eventBase(context)),
            _tag: "RunCompleted" as const,
            output,
            turns: turn,
            finishReason: "model-stop" as const,
          } satisfies RunEvent;
          return Stream.succeed<RunEvent>(completed);
        }),
      );

      return started.pipe(Stream.concat(response), Stream.concat(continuation));
    }),
  );

const failRunEventStream = <Error>(error: Error): Stream.Stream<RunEvent, Error> =>
  Stream.fail(error);

/** Interpret a binding as an ordered semantic event stream. */
const stream = <AgentValue extends Agent.Any>(
  agent: AgentValue,
  input: unknown,
): Stream.Stream<
  RunEvent,
  AgentRuntimeFailure<AgentValue>,
  AgentRuntimeRequirements<AgentValue>
> => {
  const interpreted = Stream.unwrap(
    Effect.gen(function* () {
      const startedAtMillis = yield* Clock.currentTimeMillis;
      const ids = yield* IdGenerator;
      const conversationId = yield* ids.nextConversationId;
      const runId = yield* ids.nextRunId;
      const context: RunContext = {
        agentId: agent.definition.id,
        conversationId,
        runId,
        sequence: 0,
      };

      const started = Stream.fromEffect(
        Effect.gen(function* () {
          yield* Metric.update(runCounter, 1);
          yield* Effect.logDebug("agent run started").pipe(
            Effect.annotateLogs({
              agentId: context.agentId,
              runId: context.runId,
            }),
          );
          return yield* eventBase(context).pipe(
            Effect.map(
              (base) =>
                ({
                  ...base,
                  _tag: "RunStarted" as const,
                }) satisfies RunEvent,
            ),
          );
        }).pipe(Effect.withLogSpan("AgentRuntime.run")),
      );

      const execution = Stream.unwrap(
        Effect.gen(function* () {
          const decodedInput = yield* decodeInput(agent, input);
          const instructions = yield* evaluateInstructions(agent, decodedInput);
          const encodedInput = yield* encodeInput(agent, decodedInput);
          const prompt = yield* makeInitialPrompt(instructions, encodedInput);
          return makeTurn(agent, context, prompt, 1, 0);
        }),
      );

      const durationLimit = new AgentPolicyError({
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
        Stream.catch((error) =>
          Stream.fromEffect(
            Effect.gen(function* () {
              return {
                ...(yield* eventBase(context)),
                _tag: "RunFailed" as const,
                errorTag: errorTag(error),
                message: errorMessage(error),
              } satisfies RunEvent;
            }),
          ).pipe(Stream.concat(Stream.fail(error))),
        ),
        Stream.withSpan("AgentRuntime.run", {
          attributes: {
            agentId: context.agentId,
            runId: context.runId,
          },
        }),
      );
    }),
  );

  return interpreted.pipe(Stream.provide(agent.model, { local: true }));
};

interface RunReduction {
  readonly completed?: Extract<RunEvent, { readonly _tag: "RunCompleted" }>;
}

/** Reduce the runtime's event stream to its decoded terminal result. */
const run = <AgentValue extends Agent.Any>(
  agent: AgentValue,
  input: unknown,
): Effect.Effect<
  AgentResult<Agent.Output<AgentValue>>,
  AgentRuntimeFailure<AgentValue>,
  AgentRuntimeRequirements<AgentValue> | Scope.Scope
> =>
  Effect.gen(function* () {
    yield* Scope.Scope;
    const reduction = yield* Stream.runFold(
      stream(agent, input),
      (): RunReduction => ({}),
      (state, event) => (event._tag === "RunCompleted" ? { completed: event } : state),
    );
    if (reduction.completed === undefined) {
      return yield* new ModelProtocolError({
        message: "Agent stream ended without RunCompleted",
      });
    }
    const completed = reduction.completed;
    const candidateOutput: unknown = completed.output;
    const output = yield* Schema.decodeUnknownEffect(agent.definition.output)(candidateOutput).pipe(
      Effect.mapError(
        (cause) =>
          new AgentOutputError({
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
  stream,
} as const;
