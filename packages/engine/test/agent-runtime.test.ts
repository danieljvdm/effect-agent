import { expect, layer } from "@effect/vitest";

import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Ref, Schema, Stream } from "effect";
import {
  Agent,
  AgentInputError,
  AgentOutputError,
  AgentPolicy,
  ConversationId,
  IdGenerator,
  ModelProtocolError,
  RunId,
  ToolCallId,
  TurnId,
  type RunEvent,
} from "@effect-agent/core";
import { LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";

import { AgentRuntime } from "../src/index.ts";

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const identifiers = Layer.succeed(IdGenerator, {
  nextConversationId: Effect.succeed(Schema.decodeSync(ConversationId)("conversation-1")),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-1")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-1")),
});

const modelFromParts = (parts: ReadonlyArray<Response.StreamPartEncoded>) =>
  Model.make(
    "scripted",
    "engine-test",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.fromIterable(parts),
      }),
    ),
  );

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const runtimeDefinition = Agent.define("runtime-test", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: ({ question }) => `Answer ${question} as JSON.`,
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const makeAgent = (parts: ReadonlyArray<Response.StreamPartEncoded>) =>
  Agent.withModel(runtimeDefinition, modelFromParts(parts));

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the Effect to fail");
  }

  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) {
    throw new Error("Expected a typed failure in the Cause");
  }
  return failure.value;
};

layer(identifiers)("RUN-001 Phase 0 AgentRuntime", (it) => {
  it.effect("uses the native Toolkit for a deterministic two-Turn flow", () => {
    const Search = Tool.make("search", {
      parameters: Schema.Struct({ query: Schema.String }),
      success: Schema.Struct({ available: Schema.Boolean }),
    });
    const tools = Toolkit.make(Search);
    const model = Model.make(
      "scripted",
      "two-turn",
      Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          const turn = yield* Ref.make(0);
          return yield* LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                  Effect.map((value) =>
                    Stream.fromIterable<Response.StreamPartEncoded>(
                      value === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "search-1",
                              name: "search",
                              params: { query: "sea" },
                              providerExecuted: false,
                            },
                            {
                              type: "finish",
                              reason: "tool-calls",
                              usage,
                            },
                          ]
                        : finalParts('{"answer":"A flight is available."}'),
                    ),
                  ),
                ),
              ),
          });
        }),
      ),
    );
    const definition = Agent.define("two-turn", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Search before answering.",
      toolkit: tools,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    });
    const agent = Agent.withModel(definition, model);
    const toolLayer = tools.toLayer({
      search: () => Effect.succeed({ available: true }),
    });

    return Effect.gen(function* () {
      const result = yield* AgentRuntime.stream(agent, {
        question: "Is a flight available?",
      }).pipe(Stream.runCollect, Effect.provide(toolLayer));

      expect(result.map((event) => event._tag)).toEqual([
        "RunStarted",
        "TurnStarted",
        "ModelStarted",
        "ToolCallDeclared",
        "ToolCallStarted",
        "ToolCallSucceeded",
        "TurnCompleted",
        "TurnStarted",
        "ModelStarted",
        "TextDelta",
        "TurnCompleted",
        "RunCompleted",
      ]);
      expect(result.find((event) => event._tag === "RunCompleted")?.output).toEqual({
        answer: "A flight is available.",
      });
    });
  });

  it.effect("run reduces the same semantic trace exposed by stream", () => {
    const agent = makeAgent(finalParts('{"answer":"blue"}'));

    return Effect.gen(function* () {
      const events = yield* AgentRuntime.stream(agent, {
        question: "What color?",
      }).pipe(Stream.runCollect);
      const reduced = events.find(
        (event): event is Extract<RunEvent, { readonly _tag: "RunCompleted" }> =>
          event._tag === "RunCompleted",
      );
      const runResult = yield* AgentRuntime.run(agent, {
        question: "What color?",
      });

      expect(events.map((event) => event._tag)).toEqual([
        "RunStarted",
        "TurnStarted",
        "ModelStarted",
        "TextDelta",
        "TurnCompleted",
        "RunCompleted",
      ]);
      expect(reduced?.output).toEqual(runResult.output);
      expect(reduced?.conversationId).toBe(runResult.conversationId);
      expect(reduced?.runId).toBe(runResult.runId);
      expect(reduced?.turns).toBe(runResult.turns);
    });
  });

  it.effect("keeps input and output decode failures typed", () => {
    const agent = makeAgent(finalParts('{"answer":42}'));

    return Effect.gen(function* () {
      const inputExit = yield* AgentRuntime.run(agent, {
        question: 42,
      }).pipe(Effect.exit);
      const outputExit = yield* AgentRuntime.run(agent, {
        question: "What color?",
      }).pipe(Effect.exit);

      const input = failureFrom(inputExit);
      const output = failureFrom(outputExit);
      expect(input).toBeInstanceOf(AgentInputError);
      expect(input.message).toContain("Expected string, got 42");
      expect(input.message).toContain('at ["question"]');
      expect(output).toBeInstanceOf(AgentOutputError);
      expect(output.message).toContain("Expected string, got 42");
      expect(output.message).toContain('at ["answer"]');
    });
  });

  it.effect("does not turn a truncated model response into success", () => {
    const agent = makeAgent([
      { type: "text-start", id: "answer" },
      {
        type: "text-delta",
        id: "answer",
        delta: '{"answer":"incomplete"}',
      },
      { type: "text-end", id: "answer" },
      { type: "finish", reason: "length", usage },
    ]);

    return Effect.gen(function* () {
      const exit = yield* AgentRuntime.run(agent, { question: "What color?" }).pipe(Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("without a final answer");
    });
  });

  it.effect("interrupts native model and Tool resources", () =>
    Effect.gen(function* () {
      const toolStarted = yield* Deferred.make<void>();
      const toolFinalized = yield* Deferred.make<void>();
      const modelFinalized = yield* Deferred.make<void>();

      const Wait = Tool.make("wait", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const tools = Toolkit.make(Wait);
      const toolLayer = tools.toLayer({
        wait: () =>
          Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.acquireRelease(Deferred.succeed(toolStarted, undefined), () =>
                Deferred.succeed(toolFinalized, undefined),
              );
              return yield* Effect.never;
            }),
          ),
      });
      const model = Model.make(
        "scripted",
        "interrupt-test",
        Layer.effect(
          LanguageModel.LanguageModel,
          Effect.acquireRelease(
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: "tool-call",
                    id: Schema.decodeSync(ToolCallId)("tool-call-1"),
                    name: "wait",
                    params: {},
                    providerExecuted: false,
                  },
                  {
                    type: "finish",
                    reason: "tool-calls",
                    usage,
                  },
                ]),
            }),
            () => Deferred.succeed(modelFinalized, undefined),
          ),
        ),
      );
      const definition = Agent.define("interrupt-test", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Wait for the Tool.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const agent = Agent.withModel(definition, model);

      const fiber = yield* AgentRuntime.stream(agent, {
        question: "wait",
      }).pipe(Stream.runDrain, Effect.provide(toolLayer), Effect.forkChild);
      yield* Deferred.await(toolStarted);
      yield* Fiber.interrupt(fiber);

      expect({
        model: yield* Deferred.isDone(modelFinalized),
        tool: yield* Deferred.isDone(toolFinalized),
      }).toEqual({ model: true, tool: true });
    }),
  );
});
