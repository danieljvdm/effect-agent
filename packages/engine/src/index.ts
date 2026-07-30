import { Clock, DateTime, Effect, Layer, Schema, Scope, Stream } from "effect";
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
import { LanguageModel, Prompt, type Response } from "effect/unstable/ai";

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

interface TurnTrace {
  readonly parts: Array<Response.AnyPart>;
  readonly text: Array<string>;
  readonly toolCallIds: Set<string>;
  readonly toolCallProviderExecutions: Map<string, boolean>;
  readonly finalToolResultIds: Set<string>;
  finished: boolean;
  finishReason: Response.FinishReason | undefined;
}

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
  const responsePrompt = Prompt.fromResponseParts(trace.parts);
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
      const content = message.content.filter(
        (part) =>
          part.type !== "tool-result" || trace.toolCallProviderExecutions.get(part.id) !== true,
      );
      if (content.length > 0) {
        messages.push(
          Prompt.makeMessage("tool", {
            content,
            options: message.options,
          }),
        );
      }
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
      case "text-delta": {
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
      case "reasoning-delta": {
        return [
          {
            ...(yield* eventBase(context)),
            _tag: "ReasoningDelta" as const,
            turnId,
            text: part.delta,
          },
        ];
      }
      case "tool-call": {
        if (trace.toolCallIds.has(part.id)) {
          return yield* new ModelProtocolError({
            message: `Model response repeated Tool Call ID ${part.id}`,
          });
        }
        const toolCallId = yield* decodeToolCallId(part.id);
        const parameters = yield* decodeEventJson(part.params, "Tool parameters");
        trace.toolCallIds.add(part.id);
        trace.toolCallProviderExecutions.set(part.id, part.providerExecuted);
        const declared = {
          ...(yield* eventBase(context)),
          _tag: "ToolCallDeclared" as const,
          turnId,
          toolCallId,
          toolName: part.name,
          parameters,
          providerExecuted: part.providerExecuted,
        };
        if (part.providerExecuted) {
          return [declared];
        }
        return [
          declared,
          {
            ...(yield* eventBase(context)),
            _tag: "ToolCallStarted" as const,
            turnId,
            toolCallId,
            toolName: part.name,
          },
        ];
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
        if (trace.finished) {
          return yield* new ModelProtocolError({
            message: "Model response emitted more than one finish part",
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
        toolCallIds: new Set(),
        toolCallProviderExecutions: new Map(),
        finalToolResultIds: new Set(),
        finished: false,
        finishReason: undefined,
      };

      const started = Stream.fromEffect(
        Effect.gen(function* () {
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
        }),
      ).pipe(Stream.flatMap(Stream.fromIterable));

      const response = LanguageModel.streamText({
        prompt,
        toolkit: agent.definition.toolkit,
        concurrency: agent.definition.policy.toolConcurrency,
      }).pipe(
        Stream.mapEffect((part) => eventsForPart(context, turnId, turn, trace, part)),
        Stream.flatMap(Stream.fromIterable),
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
            if (trace.finalToolResultIds.size !== trace.toolCallIds.size) {
              return failRunEventStream(
                new ModelProtocolError({
                  message: "A Tool Call turn completed without one final result per Tool Call",
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
            const responsePrompt = promptFromTurnParts(trace);
            const nextPrompt = Prompt.fromMessages([...prompt.content, ...responsePrompt.content]);
            return makeTurn(agent, context, nextPrompt, turn + 1, toolCalls);
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
        eventBase(context).pipe(
          Effect.map(
            (base) =>
              ({
                ...base,
                _tag: "RunStarted" as const,
              }) satisfies RunEvent,
          ),
        ),
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

      return started.pipe(
        Stream.concat(execution),
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
