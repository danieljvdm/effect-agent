import { expect, layer } from "@effect/vitest";

import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Ref, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  Agent,
  AgentInputError,
  AgentOutputError,
  AgentPolicy,
  AgentPolicyError,
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

class ScheduledToolFailure extends Schema.TaggedErrorClass<ScheduledToolFailure>()(
  "ScheduledToolFailure",
  { message: Schema.String },
) {}

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

const HostedSearch = Tool.providerDefined({
  id: "test.web_search",
  customName: "HostedSearch",
  providerName: "web_search",
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
})(undefined);
const hostedTools = Toolkit.make(HostedSearch);
const hostedDefinition = Agent.define("hosted-tool", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Search before answering.",
  toolkit: hostedTools,
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

const errorMessageForTest = (error: unknown): string =>
  typeof error === "object" &&
  error !== null &&
  "message" in error &&
  typeof error.message === "string"
    ? error.message
    : String(error);

layer(identifiers)("RUN-001 Phase 1 AgentRuntime", (it) => {
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
        "TurnCompleted",
        "ToolCallStarted",
        "ToolCallSucceeded",
        "TurnStarted",
        "ModelStarted",
        "TextDelta",
        "TurnCompleted",
        "RunCompleted",
      ]);
      const declared = result.find((event) => event._tag === "ToolCallDeclared");
      const succeeded = result.find((event) => event._tag === "ToolCallSucceeded");
      expect(declared).toMatchObject({
        parameters: { query: "sea" },
        providerExecuted: false,
      });
      expect(succeeded).toMatchObject({ providerExecuted: false });
      expect(result.find((event) => event._tag === "RunCompleted")?.output).toEqual({
        answer: "A flight is available.",
      });
    });
  });

  it.effect("keeps provider-executed Tools distinct from application handlers", () => {
    let providerResultStayedAssistant = false;
    const model = Model.make(
      "scripted",
      "hosted-tool",
      Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          const turn = yield* Ref.make(0);
          return yield* LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) =>
              Stream.unwrap(
                Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                  Effect.map((value) => {
                    if (value > 0) {
                      providerResultStayedAssistant = request.prompt.content.some(
                        (message) =>
                          message.role === "assistant" &&
                          message.content.some(
                            (part) => part.type === "tool-result" && part.id === "hosted-search-1",
                          ),
                      );
                      expect(
                        request.prompt.content.some((message) => message.role === "tool"),
                      ).toBe(false);
                    }
                    return Stream.fromIterable<Response.StreamPartEncoded>(
                      value === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "hosted-search-1",
                              name: "HostedSearch",
                              params: { query: "current London travel context" },
                              providerExecuted: true,
                            },
                            {
                              type: "tool-result",
                              id: "hosted-search-1",
                              name: "HostedSearch",
                              result: { status: "completed" },
                              isFailure: false,
                              providerExecuted: true,
                            },
                            {
                              type: "finish",
                              reason: "tool-calls",
                              usage,
                            },
                          ]
                        : finalParts('{"answer":"Search completed."}'),
                    );
                  }),
                ),
              ),
          });
        }),
      ),
    );

    return Effect.gen(function* () {
      const result = yield* AgentRuntime.stream(Agent.withModel(hostedDefinition, model), {
        question: "What is current?",
      }).pipe(Stream.runCollect);

      expect(result.map((event) => event._tag)).toEqual([
        "RunStarted",
        "TurnStarted",
        "ModelStarted",
        "ToolCallDeclared",
        "ToolCallSucceeded",
        "TurnCompleted",
        "TurnStarted",
        "ModelStarted",
        "TextDelta",
        "TurnCompleted",
        "RunCompleted",
      ]);
      expect(result.find((event) => event._tag === "ToolCallDeclared")).toMatchObject({
        parameters: { query: "current London travel context" },
        providerExecuted: true,
      });
      expect(result.find((event) => event._tag === "ToolCallSucceeded")).toMatchObject({
        result: { status: "completed" },
        providerExecuted: true,
      });
      expect(providerResultStayedAssistant).toBe(true);
      expect(result.some((event) => event._tag === "ToolCallStarted")).toBe(false);
    });
  });

  it.effect("accepts provider-only Tools that finish with final output in the same Turn", () => {
    const parts: ReadonlyArray<Response.StreamPartEncoded> = [
      {
        type: "tool-call",
        id: "hosted-search-1",
        name: "HostedSearch",
        params: { query: "current London travel context" },
        providerExecuted: true,
      },
      {
        type: "tool-result",
        id: "hosted-search-1",
        name: "HostedSearch",
        result: { status: "completed" },
        isFailure: false,
        providerExecuted: true,
      },
      ...finalParts('{"answer":"Search completed in one response."}'),
    ];

    return Effect.gen(function* () {
      const result = yield* AgentRuntime.stream(
        Agent.withModel(hostedDefinition, modelFromParts(parts)),
        { question: "What is current?" },
      ).pipe(Stream.runCollect);

      expect(result.filter((event) => event._tag === "TurnStarted")).toHaveLength(1);
      expect(result.at(-1)).toMatchObject({
        _tag: "RunCompleted",
        output: { answer: "Search completed in one response." },
        turns: 1,
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

  it.effect("rejects malformed streaming part lifecycles", () => {
    const agent = makeAgent([
      {
        type: "text-delta",
        id: "orphan",
        delta: '{"answer":"invalid"}',
      },
      { type: "finish", reason: "stop", usage },
    ]);

    return Effect.gen(function* () {
      const exit = yield* AgentRuntime.run(agent, { question: "What color?" }).pipe(Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("text delta for inactive part orphan");
    });
  });

  it.effect("never executes a Tool whose streamed parameters are incomplete", () => {
    let handlerStarted = false;
    const Search = Tool.make("search", {
      parameters: Schema.Struct({ query: Schema.String }),
      success: Schema.String,
    });
    const tools = Toolkit.make(Search);
    const definition = Agent.define("truncated-tool-parameters", {
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
    const model = modelFromParts([
      {
        type: "tool-params-start",
        id: "truncated-search",
        name: "search",
        providerExecuted: false,
      },
      {
        type: "tool-params-delta",
        id: "truncated-search",
        delta: '{"query":"Lon',
      },
      { type: "finish", reason: "length", usage },
    ]);

    return Effect.gen(function* () {
      const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "Search London",
      }).pipe(
        Effect.provide(
          tools.toLayer({
            search: () =>
              Effect.sync(() => {
                handlerStarted = true;
                return "unexpected";
              }),
          }),
        ),
        Effect.scoped,
        Effect.exit,
      );
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain(
        "finished before completing Tool parameter part truncated-search",
      );
      expect(handlerStarted).toBe(false);
    });
  });

  it.effect(
    "bounds concurrent handlers, emits live progress, and preserves declaration order in the next prompt",
    () =>
      Effect.gen(function* () {
        const entered = [
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
        ] as const;
        const release = [
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
        ] as const;
        const startedObserved = [
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
        ] as const;
        const completed = [
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
        ] as const;
        const succeededObserved = [
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
          yield* Deferred.make<void>(),
        ] as const;
        const active = yield* Ref.make(0);
        const maximumActive = yield* Ref.make(0);
        const completionOrder = yield* Ref.make<ReadonlyArray<number>>([]);
        let promptOrder: ReadonlyArray<string> = [];

        const Lookup = Tool.make("lookup", {
          parameters: Schema.Struct({ index: Schema.Int }),
          success: Schema.Struct({ value: Schema.String }),
        });
        const tools = Toolkit.make(Lookup);
        const toolLayer = tools.toLayer({
          lookup: ({ index }, context) =>
            Effect.gen(function* () {
              const current = yield* Ref.updateAndGet(active, (value) => value + 1);
              yield* Ref.update(maximumActive, (value) => Math.max(value, current));
              yield* Deferred.succeed(entered[index], undefined);
              yield* context.preliminary({ value: `progress-${index}` });
              yield* Deferred.await(release[index]);
              yield* Ref.update(completionOrder, (order) => [...order, index]);
              yield* Deferred.succeed(completed[index], undefined);
              return { value: `result-${index}` };
            }).pipe(Effect.ensuring(Ref.update(active, (value) => value - 1))),
        });
        const model = Model.make(
          "scripted",
          "bounded-tools",
          Layer.effect(
            LanguageModel.LanguageModel,
            Effect.gen(function* () {
              const turn = yield* Ref.make(0);
              return yield* LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: (request) =>
                  Stream.unwrap(
                    Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                      Effect.map((value) => {
                        if (value === 1) {
                          const toolMessage = request.prompt.content.find(
                            (message) => message.role === "tool",
                          );
                          promptOrder =
                            toolMessage?.content
                              .filter((part) => part.type === "tool-result")
                              .map((part) => part.id) ?? [];
                        }
                        return Stream.fromIterable<Response.StreamPartEncoded>(
                          value === 0
                            ? [
                                {
                                  type: "tool-call",
                                  id: "lookup-1",
                                  name: "lookup",
                                  params: { index: 0 },
                                  providerExecuted: false,
                                },
                                {
                                  type: "tool-call",
                                  id: "lookup-2",
                                  name: "lookup",
                                  params: { index: 1 },
                                  providerExecuted: false,
                                },
                                {
                                  type: "tool-call",
                                  id: "lookup-3",
                                  name: "lookup",
                                  params: { index: 2 },
                                  providerExecuted: false,
                                },
                                { type: "finish", reason: "tool-calls", usage },
                              ]
                            : finalParts('{"answer":"ordered"}'),
                        );
                      }),
                    ),
                  ),
              });
            }),
          ),
        );
        const definition = Agent.define("bounded-tools", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Run all lookups.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 3,
            maxDuration: "30 seconds",
            toolConcurrency: 2,
          }),
        });

        const fiber = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "lookup",
        }).pipe(
          Stream.tap((event) =>
            event._tag === "ToolCallStarted"
              ? Deferred.succeed(startedObserved[Number(event.toolCallId.slice(-1)) - 1], undefined)
              : event._tag === "ToolCallSucceeded"
                ? Deferred.succeed(
                    succeededObserved[Number(event.toolCallId.slice(-1)) - 1],
                    undefined,
                  )
                : Effect.void,
          ),
          Stream.runCollect,
          Effect.provide(toolLayer),
          Effect.forkChild,
        );

        yield* Deferred.await(entered[0]);
        yield* Deferred.await(entered[1]);
        yield* Deferred.await(startedObserved[0]);
        yield* Deferred.await(startedObserved[1]);
        expect({
          firstStartedWasLive: yield* Deferred.isDone(startedObserved[0]),
          secondStartedWasLive: yield* Deferred.isDone(startedObserved[1]),
          thirdEntered: yield* Deferred.isDone(entered[2]),
          maximumActive: yield* Ref.get(maximumActive),
        }).toEqual({
          firstStartedWasLive: true,
          secondStartedWasLive: true,
          thirdEntered: false,
          maximumActive: 2,
        });

        yield* Deferred.succeed(release[1], undefined);
        yield* Deferred.await(entered[2]);
        yield* Deferred.succeed(release[2], undefined);
        yield* Deferred.await(completed[2]);
        yield* Deferred.await(succeededObserved[2]);
        yield* Deferred.succeed(release[0], undefined);
        const events = yield* Fiber.join(fiber);

        expect(yield* Ref.get(completionOrder)).toEqual([1, 2, 0]);
        expect(
          events
            .filter((event) => event._tag === "ToolCallSucceeded")
            .map((event) => event.toolCallId),
        ).toEqual(["lookup-2", "lookup-3", "lookup-1"]);
        expect(events.filter((event) => event._tag === "ToolProgress")).toHaveLength(3);
        for (const callId of ["lookup-1", "lookup-2", "lookup-3"]) {
          const callEvents = events.filter(
            (event) => "toolCallId" in event && event.toolCallId === callId,
          );
          expect(callEvents.map((event) => event._tag)).toEqual([
            "ToolCallDeclared",
            "ToolCallStarted",
            "ToolProgress",
            "ToolCallSucceeded",
          ]);
        }
        expect(promptOrder).toEqual(["lookup-1", "lookup-2", "lookup-3"]);
      }),
  );

  it.effect("preflights the complete Tool batch before starting any handler", () =>
    Effect.gen(function* () {
      const starts = yield* Ref.make(0);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const Lookup = Tool.make("lookup", {
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
      });
      const tools = Toolkit.make(Lookup);
      const definition = Agent.define("batch-preflight", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Run lookups.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 2,
        }),
      });
      const model = modelFromParts([
        {
          type: "tool-call",
          id: "lookup-valid",
          name: "lookup",
          params: { value: 1 },
          providerExecuted: false,
        },
        {
          type: "tool-call",
          id: "lookup-invalid",
          name: "lookup",
          params: { value: "not-an-int" },
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const toolLayer = tools.toLayer({
        lookup: () => Ref.update(starts, (value) => value + 1).pipe(Effect.as("unexpected")),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "lookup",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(errorMessageForTest(failure)).toContain('Expected number, got "not-an-int"');
      expect(yield* Ref.get(starts)).toBe(0);
      expect(observed.some((event) => event._tag === "ToolCallStarted")).toBe(false);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
    }),
  );

  it.effect("preserves transformed parameters across the native Toolkit handler boundary", () => {
    let handlerParameter: unknown;
    let promptResult: unknown;
    const Increment = Tool.make("increment", {
      parameters: Schema.Struct({ value: Schema.FiniteFromString }),
      success: Schema.Struct({ value: Schema.Finite }),
    });
    const tools = Toolkit.make(Increment);
    const model = Model.make(
      "scripted",
      "transformed-tool-parameters",
      Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          const turn = yield* Ref.make(0);
          return yield* LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) =>
              Stream.unwrap(
                Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                  Effect.map((value) => {
                    if (value === 1) {
                      const toolMessage = request.prompt.content.find(
                        (message) => message.role === "tool",
                      );
                      promptResult = toolMessage?.content.find(
                        (part) => part.type === "tool-result",
                      )?.result;
                    }
                    return Stream.fromIterable<Response.StreamPartEncoded>(
                      value === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "increment-1",
                              name: "increment",
                              params: { value: "41" },
                              providerExecuted: false,
                            },
                            { type: "finish", reason: "tool-calls", usage },
                          ]
                        : finalParts('{"answer":"42"}'),
                    );
                  }),
                ),
              ),
          });
        }),
      ),
    );
    const definition = Agent.define("transformed-tool-parameters", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Increment the encoded number.",
      toolkit: tools,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    });

    return AgentRuntime.run(Agent.withModel(definition, model), {
      question: "increment",
    }).pipe(
      Effect.provide(
        tools.toLayer({
          increment: ({ value }) => {
            handlerParameter = value;
            return Effect.succeed({ value: value + 1 });
          },
        }),
      ),
      Effect.scoped,
      Effect.tap(() =>
        Effect.sync(() => {
          expect(handlerParameter).toBe(41);
          expect(promptResult).toEqual({ value: 42 });
        }),
      ),
    );
  });

  it.effect("emits Started and Failed before propagating the concrete typed Tool failure", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const finalized = yield* Deferred.make<void>();
      const Fail = Tool.make("fail", {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: ScheduledToolFailure,
      });
      const tools = Toolkit.make(Fail);
      const definition = Agent.define("typed-tool-failure", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Call the failing Tool.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const model = modelFromParts([
        {
          type: "tool-call",
          id: "failure-1",
          name: "fail",
          params: {},
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const expected = new ScheduledToolFailure({ message: "scheduled failure" });
      const toolLayer = tools.toLayer({
        fail: () =>
          Effect.acquireUseRelease(
            Effect.void,
            () => Effect.fail(expected),
            () => Deferred.succeed(finalized, undefined),
          ),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "fail",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBe(expected);
      expect(
        observed
          .filter(
            (event) =>
              event._tag === "ToolCallStarted" ||
              event._tag === "ToolCallFailed" ||
              event._tag === "RunFailed",
          )
          .map((event) => event._tag),
      ).toEqual(["ToolCallStarted", "ToolCallFailed", "RunFailed"]);
      expect(observed.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);
      expect(yield* Deferred.isDone(finalized)).toBe(true);
    }),
  );

  it.effect("treats Option.none as a successful empty Tool result", () =>
    Effect.gen(function* () {
      let promptResult: unknown;
      const Maybe = Tool.make("maybe", {
        parameters: Schema.Struct({}),
        success: Schema.OptionFromNullOr(Schema.String),
      });
      const tools = Toolkit.make(Maybe);
      const model = Model.make(
        "scripted",
        "empty-success",
        Layer.effect(
          LanguageModel.LanguageModel,
          Effect.gen(function* () {
            const turn = yield* Ref.make(0);
            return yield* LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (request) =>
                Stream.unwrap(
                  Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                    Effect.map((value) => {
                      if (value === 1) {
                        const toolMessage = request.prompt.content.find(
                          (message) => message.role === "tool",
                        );
                        promptResult = toolMessage?.content.find(
                          (part) => part.type === "tool-result",
                        )?.result;
                      }
                      return Stream.fromIterable<Response.StreamPartEncoded>(
                        value === 0
                          ? [
                              {
                                type: "tool-call",
                                id: "maybe-1",
                                name: "maybe",
                                params: {},
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]
                          : finalParts('{"answer":"empty is valid"}'),
                      );
                    }),
                  ),
                ),
            });
          }),
        ),
      );
      const definition = Agent.define("empty-success", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Call maybe.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });

      const events = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "maybe",
      }).pipe(
        Stream.runCollect,
        Effect.provide(tools.toLayer({ maybe: () => Effect.succeed(Option.none()) })),
      );

      expect(events.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(1);
      expect(events.some((event) => event._tag === "ToolCallFailed")).toBe(false);
      expect(promptResult).toBe(null);
    }),
  );

  it.effect("rejects duplicate terminal Tool results without inventing a second terminal", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const agent = Agent.withModel(
        hostedDefinition,
        modelFromParts([
          {
            type: "tool-call",
            id: "hosted-duplicate",
            name: "HostedSearch",
            params: { query: "duplicate" },
            providerExecuted: true,
          },
          {
            type: "tool-result",
            id: "hosted-duplicate",
            name: "HostedSearch",
            result: { status: "first" },
            isFailure: false,
            providerExecuted: true,
          },
          {
            type: "tool-result",
            id: "hosted-duplicate",
            name: "HostedSearch",
            result: { status: "second" },
            isFailure: false,
            providerExecuted: true,
          },
          { type: "finish", reason: "tool-calls", usage },
        ]),
      );

      const exit = yield* AgentRuntime.stream(agent, { question: "duplicate" }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("more than one terminal result");
      expect(observed.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(1);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
    }),
  );

  it.effect("enforces maxDuration from Run start with TestClock and one RunFailed event", () =>
    Effect.gen(function* () {
      const modelStarted = yield* Deferred.make<void>();
      const modelFinalized = yield* Deferred.make<void>();
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const model = Model.make(
        "scripted",
        "absolute-timeout",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.never.pipe(Stream.ensuring(Deferred.succeed(modelFinalized, undefined))),
          }),
        ),
      );
      const definition = Agent.define("absolute-timeout", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: () => Effect.sleep("4 seconds").pipe(Effect.as("Wait, then answer.")),
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "5 seconds",
          toolConcurrency: 1,
        }),
      });
      const fiber = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "wait",
      }).pipe(
        Stream.tap((event) =>
          Effect.gen(function* () {
            yield* Ref.update(events, (all) => [...all, event]);
            if (event._tag === "ModelStarted") {
              yield* Deferred.succeed(modelStarted, undefined);
            }
          }),
        ),
        Stream.runDrain,
        Effect.forkChild,
      );

      yield* TestClock.adjust("4 seconds");
      yield* Deferred.await(modelStarted);
      yield* TestClock.adjust("1 second");
      const exit = yield* Fiber.await(fiber);
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect(failure).toMatchObject({ limit: "duration" });
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunFailed",
        errorTag: "AgentPolicyError",
      });
      expect(yield* Deferred.isDone(modelFinalized)).toBe(true);
    }),
  );

  it.effect(
    "preserves a handler defect while emitting one failed classification and finalizing",
    () =>
      Effect.gen(function* () {
        const finalized = yield* Deferred.make<void>();
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const defect = new Error("handler defect");
        const Defect = Tool.make("defect", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        });
        const tools = Toolkit.make(Defect);
        const definition = Agent.define("tool-defect", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Call defect.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const model = modelFromParts([
          {
            type: "tool-call",
            id: "defect-1",
            name: "defect",
            params: {},
            providerExecuted: false,
          },
          { type: "finish", reason: "tool-calls", usage },
        ]);
        const toolLayer = tools.toLayer({
          defect: () =>
            Effect.acquireUseRelease(
              Effect.void,
              () => Effect.die(defect),
              () => Deferred.succeed(finalized, undefined),
            ),
        });

        const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "defect",
        }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
          Effect.exit,
        );
        if (Exit.isSuccess(exit)) {
          throw new Error("Expected the handler defect to fail the Run");
        }
        const observed = yield* Ref.get(events);

        expect(Cause.squash(exit.cause)).toBe(defect);
        expect(yield* Deferred.isDone(finalized)).toBe(true);
        expect(observed.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);
        expect(observed.some((event) => event._tag === "RunFailed")).toBe(false);
      }),
  );

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
