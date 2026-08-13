import { expect, layer } from "@effect/vitest";

import {
  Cause,
  Context,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import {
  Agent,
  AgentApprovalDenied,
  AgentApprovalPending,
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
import {
  AiError,
  LanguageModel,
  Model,
  Prompt,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";

import {
  AgentRuntime,
  type RunBudgetHook,
  type RunTurnResume,
  type RunUsageDelta,
} from "../src/index.ts";

class ScheduledToolFailure extends Schema.TaggedErrorClass<ScheduledToolFailure>()(
  "ScheduledToolFailure",
  { message: Schema.String },
) {}

class HookFailure extends Schema.TaggedErrorClass<HookFailure>()("HookFailure", {
  message: Schema.String,
}) {}

class BudgetGuardFailure extends Schema.TaggedErrorClass<BudgetGuardFailure>()(
  "BudgetGuardFailure",
  {
    message: Schema.String,
  },
) {}

class HookService extends Context.Service<HookService, { readonly enabled: true }>()(
  "@effect-agent/engine/test/HookService",
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
  it.effect("preserves official prior history as the exact prefix of a new Run", () => {
    const priorHistory = Prompt.fromMessages([
      Prompt.makeMessage("system", { content: "Original conversation instructions." }),
      Prompt.makeMessage("user", {
        content: [Prompt.makePart("text", { text: "Which city is best?" })],
      }),
      Prompt.makeMessage("assistant", {
        content: [Prompt.makePart("text", { text: "Edinburgh." })],
      }),
    ]);
    let observedPrompt: Prompt.Prompt | undefined;
    const model = Model.make(
      "scripted",
      "history-prefix",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (request) => {
            observedPrompt = request.prompt;
            return Stream.fromIterable(finalParts('{"answer":"Edinburgh."}'));
          },
        }),
      ),
    );

    return Effect.gen(function* () {
      yield* AgentRuntime.run(
        Agent.withModel(runtimeDefinition, model),
        { question: "Which city did you recommend?" },
        { history: priorHistory },
      );

      expect(observedPrompt).toBeDefined();
      if (observedPrompt === undefined) {
        throw new Error("Expected the model request Prompt to be captured");
      }
      const encodedPrior = yield* Schema.encodeEffect(Prompt.Prompt)(priorHistory);
      const encodedObserved = yield* Schema.encodeEffect(Prompt.Prompt)(observedPrompt);

      expect(encodedObserved.content.slice(0, encodedPrior.content.length)).toEqual(
        encodedPrior.content,
      );
      expect(encodedObserved.content.at(-2)?.role).toBe("system");
      expect(encodedObserved.content.at(-1)?.role).toBe("user");
    });
  });

  it.effect("keeps Run hook failures and requirements visible in Effect E and R", () => {
    const program = AgentRuntime.run(
      makeAgent(finalParts('{"answer":"typed"}')),
      { question: "typed hooks" },
      {
        budget: {
          guard: (effect) => Effect.andThen(HookService, effect),
          consume: () =>
            Effect.gen(function* () {
              yield* HookService;
              return yield* HookFailure.make({ message: "budget hook failed" });
            }),
        },
      },
    );
    type ErrorProof = HookFailure extends Effect.Error<typeof program> ? true : false;
    type RequirementsProof = HookService extends Effect.Services<typeof program> ? true : false;
    const errorProof: ErrorProof = true;
    const requirementsProof: RequirementsProof = true;

    expect(errorProof).toBe(true);
    expect(requirementsProof).toBe(true);
    return Effect.void;
  });

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

  it.effect("rejects a provider Tool result whose name differs from its declared call", () => {
    const HostedLookup = Tool.providerDefined({
      id: "test.lookup",
      customName: "HostedLookup",
      providerName: "lookup",
      parameters: Schema.Struct({ query: Schema.String }),
      success: Schema.Struct({ status: Schema.String }),
    })(undefined);
    const tools = Toolkit.make(HostedSearch, HostedLookup);
    const definition = Agent.define("hosted-tool-name-correlation", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Use one hosted Tool.",
      toolkit: tools,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 2,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    });
    const model = modelFromParts([
      {
        type: "tool-call",
        id: "hosted-call-1",
        name: "HostedSearch",
        params: { query: "current context" },
        providerExecuted: true,
      },
      {
        type: "tool-result",
        id: "hosted-call-1",
        name: "HostedLookup",
        result: { status: "completed" },
        isFailure: false,
        providerExecuted: true,
      },
      { type: "finish", reason: "tool-calls", usage },
    ]);

    return Effect.gen(function* () {
      const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "What is current?",
      }).pipe(Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("name did not match");
      expect(failure.message).toContain("HostedSearch");
      expect(failure.message).toContain("HostedLookup");
    });
  });

  it.effect("preserves native Effect AI rejection of an unknown provider Tool", () =>
    Effect.gen(function* () {
      const seen = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const model = modelFromParts([
        {
          type: "tool-call",
          id: "unknown-hosted-call",
          name: "UnconfiguredProviderTool",
          params: { query: "current context" },
          providerExecuted: true,
        },
        {
          type: "tool-result",
          id: "unknown-hosted-call",
          name: "UnconfiguredProviderTool",
          result: { status: "completed" },
          isFailure: false,
          providerExecuted: true,
        },
        ...finalParts('{"answer":"Untrusted result."}'),
      ]);
      const exit = yield* AgentRuntime.stream(Agent.withModel(runtimeDefinition, model), {
        question: "What is current?",
      }).pipe(
        Stream.tap((event) => Ref.update(seen, (events) => [...events, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const events = yield* Ref.get(seen);

      expect(failure).toBeInstanceOf(AiError.AiError);
      expect(failure).toMatchObject({
        _tag: "AiError",
        module: "LanguageModel",
        method: "streamText",
        reason: { _tag: "InvalidOutputError" },
      });
      expect(events.some((event) => event._tag === "ToolCallDeclared")).toBe(false);
    }),
  );

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
      const expected = ScheduledToolFailure.make({ message: "scheduled failure" });
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

  it.effect("never starts a handler while native Tool approval is unresolved", () =>
    Effect.gen(function* () {
      const handlerStarted = yield* Ref.make(false);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const Hold = Tool.make("hold", {
        parameters: Schema.Struct({ itineraryId: Schema.String }),
        success: Schema.String,
        needsApproval: true,
      });
      const tools = Toolkit.make(Hold);
      const definition = Agent.define("approval-no-start", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Hold the itinerary.",
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
          id: "hold-1",
          name: "hold",
          params: { itineraryId: "trip-1" },
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const agent = Agent.withModel(definition, model);
      const toolLayer = tools.toLayer({
        hold: () => Ref.set(handlerStarted, true).pipe(Effect.as("held")),
      });
      const exit = yield* AgentRuntime.stream(
        agent,
        { question: "hold" },
        {
          approval: {
            request: () => Effect.succeed({ _tag: "unresolved" as const }),
          },
        },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );

      expect(failureFrom(exit)).toBeInstanceOf(AgentApprovalPending);
      expect(yield* Ref.get(handlerStarted)).toBe(false);
      expect((yield* Ref.get(events)).map((event) => event._tag)).toContain("ApprovalRequested");
      expect((yield* Ref.get(events)).map((event) => event._tag)).toContain("RunSuspended");
      expect((yield* Ref.get(events)).some((event) => event._tag === "ToolCallStarted")).toBe(
        false,
      );

      const deniedExit = yield* AgentRuntime.run(
        agent,
        { question: "hold" },
        {
          approval: {
            request: () => Effect.succeed({ _tag: "denied" as const, reason: "traveler declined" }),
          },
        },
      ).pipe(Effect.provide(toolLayer), Effect.exit);
      expect(failureFrom(deniedExit)).toBeInstanceOf(AgentApprovalDenied);
      expect(yield* Ref.get(handlerStarted)).toBe(false);
    }),
  );

  it.effect("fails token exhaustion before accepting a successful model stop", () =>
    Effect.gen(function* () {
      const definition = Agent.define("token-budget", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          tokenBudget: 3,
        }),
      });
      const parts: ReadonlyArray<Response.StreamPartEncoded> = [
        ...finalParts('{"answer":"done"}').slice(0, -1),
        {
          type: "finish",
          reason: "stop",
          usage: {
            inputTokens: { total: 2 },
            outputTokens: { total: 2 },
          },
        },
      ];

      const exit = yield* AgentRuntime.run(Agent.withModel(definition, modelFromParts(parts)), {
        question: "answer",
      }).pipe(Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      if (!(failure instanceof AgentPolicyError)) {
        throw new Error("Expected AgentPolicyError");
      }
      expect(failure.limit).toBe("tokens");
    }),
  );

  it.effect("sums component-only usage reports for both token fallbacks", () =>
    Effect.gen(function* () {
      const deltas = yield* Ref.make<ReadonlyArray<RunUsageDelta>>([]);
      const parts: ReadonlyArray<Response.StreamPartEncoded> = [
        { type: "text-start", id: "answer" },
        { type: "text-delta", id: "answer", delta: '{"answer":"counted"}' },
        { type: "text-end", id: "answer" },
        {
          type: "finish",
          reason: "stop",
          usage: {
            inputTokens: { uncached: 2, cacheRead: 3, cacheWrite: 4 },
            outputTokens: { text: 5, reasoning: 6 },
          },
        },
      ];
      const budget: RunBudgetHook = {
        guard: (effect) => effect,
        consume: (delta) => Ref.update(deltas, (all) => [...all, delta]),
      };

      yield* AgentRuntime.run(makeAgent(parts), { question: "count usage" }, { budget });

      const observed = yield* Ref.get(deltas);
      expect(observed).toHaveLength(1);
      expect(observed[0]).toMatchObject({
        inputTokens: 9,
        outputTokens: 11,
        totalTokens: 20,
      });
    }),
  );

  it.effect("fails typed Turn exhaustion before executing the pending Tool batch", () =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const Search = Tool.make("search", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const tools = Toolkit.make(Search);
      const definition = Agent.define("turn-exhaustion", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Search until done.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const model = modelFromParts([
        {
          type: "tool-call",
          id: "search-1",
          name: "search",
          params: {},
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const toolLayer = tools.toLayer({
        search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "loop",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect(failure).toMatchObject({ limit: "turns" });
      expect(yield* Ref.get(handlerStarts)).toBe(0);
      expect(observed.filter((event) => event._tag === "ModelStarted")).toHaveLength(1);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunFailed",
        errorTag: "AgentPolicyError",
      });
    }),
  );

  it.effect("fails typed Tool Call exhaustion before executing the exceeding batch", () =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const turns = yield* Ref.make(0);
      const Search = Tool.make("search", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const tools = Toolkit.make(Search);
      const model = Model.make(
        "scripted",
        "tool-call-exhaustion",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.getAndUpdate(turns, (value) => value + 1).pipe(
                  Effect.map((turn) =>
                    Stream.fromIterable<Response.StreamPartEncoded>([
                      {
                        type: "tool-call",
                        id: `search-${turn + 1}`,
                        name: "search",
                        params: {},
                        providerExecuted: false,
                      },
                      { type: "finish", reason: "tool-calls", usage },
                    ]),
                  ),
                ),
              ),
          }),
        ),
      );
      const definition = Agent.define("tool-call-exhaustion", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Search until done.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 5,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const toolLayer = tools.toLayer({
        search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "loop",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect(failure).toMatchObject({ limit: "tool-calls" });
      expect(yield* Ref.get(handlerStarts)).toBe(1);
      expect(observed.filter((event) => event._tag === "ModelStarted")).toHaveLength(2);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunFailed",
        errorTag: "AgentPolicyError",
      });
    }),
  );

  it.effect(
    "stops a Run whose consecutive Tool Call failures reach the repeated-failure limit",
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const turns = yield* Ref.make(0);
        const Flaky = Tool.make("flaky", {
          parameters: Schema.Struct({}),
          success: Schema.String,
          failure: ScheduledToolFailure,
          failureMode: "return",
        });
        const tools = Toolkit.make(Flaky);
        const model = Model.make(
          "scripted",
          "repeated-failures",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Ref.getAndUpdate(turns, (value) => value + 1).pipe(
                    Effect.map((turn) =>
                      Stream.fromIterable<Response.StreamPartEncoded>([
                        {
                          type: "tool-call",
                          id: `flaky-${turn + 1}`,
                          name: "flaky",
                          params: {},
                          providerExecuted: false,
                        },
                        { type: "finish", reason: "tool-calls", usage },
                      ]),
                    ),
                  ),
                ),
            }),
          ),
        );
        const definition = Agent.define("repeated-failures", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Keep trying.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            repeatedFailureLimit: 2,
          }),
        });
        const toolLayer = tools.toLayer({
          flaky: () => Effect.fail(ScheduledToolFailure.make({ message: "still unavailable" })),
        });

        const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "retry",
        }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
          Effect.exit,
        );
        const failure = failureFrom(exit);
        const observed = yield* Ref.get(events);

        expect(failure).toBeInstanceOf(AgentPolicyError);
        expect(failure).toMatchObject({ limit: "repeated-failures" });
        expect(observed.filter((event) => event._tag === "ModelStarted")).toHaveLength(2);
        expect(observed.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(2);
        expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
        expect(observed.at(-1)).toMatchObject({
          _tag: "RunFailed",
          errorTag: "AgentPolicyError",
        });
      }),
  );

  it.effect("resets the repeated-failure counter when an interleaved Tool Call succeeds", () =>
    Effect.gen(function* () {
      const turns = yield* Ref.make(0);
      const Flaky = Tool.make("flaky", {
        parameters: Schema.Struct({ fail: Schema.Boolean }),
        success: Schema.String,
        failure: ScheduledToolFailure,
        failureMode: "return",
      });
      const tools = Toolkit.make(Flaky);
      const model = Model.make(
        "scripted",
        "repeated-failure-reset",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.getAndUpdate(turns, (value) => value + 1).pipe(
                  Effect.map((turn) =>
                    Stream.fromIterable<Response.StreamPartEncoded>(
                      turn === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "flaky-1",
                              name: "flaky",
                              params: { fail: true },
                              providerExecuted: false,
                            },
                            {
                              type: "tool-call",
                              id: "flaky-2",
                              name: "flaky",
                              params: { fail: false },
                              providerExecuted: false,
                            },
                            {
                              type: "tool-call",
                              id: "flaky-3",
                              name: "flaky",
                              params: { fail: true },
                              providerExecuted: false,
                            },
                            { type: "finish", reason: "tool-calls", usage },
                          ]
                        : finalParts('{"answer":"recovered"}'),
                    ),
                  ),
                ),
              ),
          }),
        ),
      );
      const definition = Agent.define("repeated-failure-reset", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Keep trying.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          repeatedFailureLimit: 2,
        }),
      });
      const toolLayer = tools.toLayer({
        flaky: ({ fail }) =>
          fail
            ? Effect.fail(ScheduledToolFailure.make({ message: "still unavailable" }))
            : Effect.succeed("recovered"),
      });

      const events = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "retry",
      }).pipe(Stream.runCollect, Effect.provide(toolLayer));

      expect(events.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(2);
      expect(events.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(1);
      expect(events.at(-1)).toMatchObject({
        _tag: "RunCompleted",
        output: { answer: "recovered" },
      });
    }),
  );

  it.effect("classifies malformed queued Run input as a typed Agent input failure", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      // Simulates an untyped application caller handing the engine a
      // structurally invalid queued input value at the hook boundary.
      const malformed = [{ role: "user", content: 42 }] as unknown as Prompt.RawInput;

      const exit = yield* AgentRuntime.stream(
        makeAgent(finalParts('{"answer":"unreachable"}')),
        { question: "steer" },
        {
          input: {
            drain: () => Effect.succeed([{ kind: "steering" as const, input: malformed }]),
          },
        },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(AgentInputError);
      expect(failure.message).toContain("Unable to materialize queued Run input");
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunFailed",
        errorTag: "AgentInputError",
      });
    }),
  );

  it.effect("keeps source history authoritative when model context is compacted", () =>
    Effect.gen(function* () {
      const sources = yield* Ref.make<ReadonlyArray<number>>([]);
      const received = yield* Ref.make<ReadonlyArray<Prompt.Prompt>>([]);
      const turns = yield* Ref.make(0);
      const Search = Tool.make("search", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const tools = Toolkit.make(Search);
      const model = Model.make(
        "scripted",
        "compaction-source",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  yield* Ref.update(received, (all) => [...all, request.prompt]);
                  const turn = yield* Ref.getAndUpdate(turns, (value) => value + 1);
                  return Stream.fromIterable<Response.StreamPartEncoded>(
                    turn === 0
                      ? [
                          {
                            type: "tool-call",
                            id: "search-1",
                            name: "search",
                            params: {},
                            providerExecuted: false,
                          },
                          { type: "finish", reason: "tool-calls", usage },
                        ]
                      : finalParts('{"answer":"done"}'),
                  );
                }),
              ),
          }),
        ),
      );
      const definition = Agent.define("compaction-source", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Search.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });

      yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "search" },
        {
          context: {
            prepare: ({ source }) =>
              Ref.update(sources, (all) => [...all, source.content.length]).pipe(
                Effect.as({ prompt: Prompt.make("compacted model context") }),
              ),
          },
        },
      ).pipe(Effect.provide(tools.toLayer({ search: () => Effect.succeed("found") })));

      const observed = yield* Ref.get(sources);
      expect(observed).toHaveLength(2);
      expect(observed[1]).toBeGreaterThan(observed[0] ?? 0);
      const receivedPrompts = yield* Ref.get(received);
      expect(receivedPrompts).toHaveLength(2);
      for (const receivedPrompt of receivedPrompts) {
        expect(receivedPrompt.content.map((message) => message.role)).toEqual(["user"]);
        const encoded = JSON.stringify(receivedPrompt.content);
        expect(encoded).toContain("compacted model context");
        expect(encoded).not.toContain("Search.");
        expect(encoded).not.toContain("found");
      }
    }),
  );

  it.effect(
    "delivers steering offered during Tool execution only before the next model request",
    () =>
      Effect.gen(function* () {
        const toolStarted = yield* Deferred.make<void>();
        const releaseTool = yield* Deferred.make<void>();
        const queued = yield* Ref.make(false);
        const requests = yield* Ref.make<ReadonlyArray<Prompt.Prompt>>([]);
        const turns = yield* Ref.make(0);
        const Wait = Tool.make("wait_for_change", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        });
        const tools = Toolkit.make(Wait);
        const model = Model.make(
          "scripted",
          "steering-safe-seam",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (request) =>
                Stream.unwrap(
                  Effect.gen(function* () {
                    yield* Ref.update(requests, (all) => [...all, request.prompt]);
                    const turn = yield* Ref.getAndUpdate(turns, (value) => value + 1);
                    return Stream.fromIterable<Response.StreamPartEncoded>(
                      turn === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "wait-change-1",
                              name: "wait_for_change",
                              params: {},
                              providerExecuted: false,
                            },
                            { type: "finish", reason: "tool-calls", usage },
                          ]
                        : finalParts('{"answer":"changed"}'),
                    );
                  }),
                ),
            }),
          ),
        );
        const definition = Agent.define("steering-safe-seam", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Wait, then incorporate changes.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const fiber = yield* AgentRuntime.run(
          Agent.withModel(definition, model),
          { question: "initial dates" },
          {
            input: {
              drain: () =>
                Ref.getAndSet(queued, false).pipe(
                  Effect.map((available) =>
                    available
                      ? [{ kind: "steering" as const, input: "change dates to August" }]
                      : [],
                  ),
                ),
            },
          },
        ).pipe(
          Effect.provide(
            tools.toLayer({
              wait_for_change: () =>
                Deferred.succeed(toolStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseTool)),
                  Effect.as("ready"),
                ),
            }),
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(toolStarted);
        yield* Ref.set(queued, true);
        expect(yield* Ref.get(requests)).toHaveLength(1);
        yield* Deferred.succeed(releaseTool, undefined);
        yield* Fiber.join(fiber);

        const observed = yield* Ref.get(requests);
        expect(observed).toHaveLength(2);
        expect(JSON.stringify(observed[0])).not.toContain("change dates to August");
        expect(JSON.stringify(observed[1])).toContain("change dates to August");
      }),
  );

  it.effect("buffers follow-up until the otherwise-stop seam and honors all-drain input", () =>
    Effect.gen(function* () {
      const drains = yield* Ref.make(0);
      const requests = yield* Ref.make<ReadonlyArray<Prompt.Prompt>>([]);
      const turns = yield* Ref.make(0);
      const Search = Tool.make("search_once", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const tools = Toolkit.make(Search);
      const model = Model.make(
        "scripted",
        "follow-up-stop-seam",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  yield* Ref.update(requests, (all) => [...all, request.prompt]);
                  const turn = yield* Ref.getAndUpdate(turns, (value) => value + 1);
                  return Stream.fromIterable<Response.StreamPartEncoded>(
                    turn === 0
                      ? [
                          {
                            type: "tool-call",
                            id: "search-once-1",
                            name: "search_once",
                            params: {},
                            providerExecuted: false,
                          },
                          { type: "finish", reason: "tool-calls", usage },
                        ]
                      : finalParts(
                          turn === 1 ? '{"answer":"candidate"}' : '{"answer":"with preferences"}',
                        ),
                  );
                }),
              ),
          }),
        ),
      );
      const definition = Agent.define("follow-up-stop-seam", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Search and answer.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 2,
        }),
      });

      yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "plan" },
        {
          commandDrainPolicy: "all",
          input: {
            drain: (policy) =>
              Ref.getAndUpdate(drains, (value) => value + 1).pipe(
                Effect.map((drain) => {
                  expect(policy).toBe("all");
                  return drain === 1
                    ? [
                        { kind: "follow-up" as const, input: "prefer quiet hotels" },
                        { kind: "follow-up" as const, input: "avoid red-eyes" },
                      ]
                    : [];
                }),
              ),
          },
        },
      ).pipe(Effect.provide(tools.toLayer({ search_once: () => Effect.succeed("found") })));

      const observed = yield* Ref.get(requests);
      expect(observed).toHaveLength(3);
      expect(JSON.stringify(observed[1])).not.toContain("prefer quiet hotels");
      expect(JSON.stringify(observed[2])).toContain("prefer quiet hotels");
      expect(JSON.stringify(observed[2])).toContain("avoid red-eyes");
    }),
  );

  it.effect("applies per-Tool exclusivity over the finite scheduler", () =>
    Effect.gen(function* () {
      const active = yield* Ref.make(0);
      const maximum = yield* Ref.make(0);
      const exclusiveStartedWith = yield* Ref.make<number | undefined>(undefined);
      const toolResultOrder = yield* Ref.make<ReadonlyArray<string>>([]);
      const Ordinary = Tool.make("ordinary", {
        parameters: Schema.Struct({ value: Schema.String }),
        success: Schema.String,
      });
      const Exclusive = Tool.make("exclusive", {
        parameters: Schema.Struct({ value: Schema.String }),
        success: Schema.String,
      });
      const tools = Toolkit.make(Ordinary, Exclusive);
      const turns = yield* Ref.make(0);
      const model = Model.make(
        "scripted",
        "scheduling-overrides",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) =>
              Stream.unwrap(
                Ref.getAndUpdate(turns, (value) => value + 1).pipe(
                  Effect.map((turn) => {
                    if (turn > 0) {
                      const message = request.prompt.content.find(
                        (candidate) => candidate.role === "tool",
                      );
                      const ids =
                        message?.content.flatMap((part) =>
                          part.type === "tool-result" ? [part.id] : [],
                        ) ?? [];
                      return Stream.fromEffect(Ref.set(toolResultOrder, ids)).pipe(
                        Stream.drain,
                        Stream.concat(Stream.fromIterable(finalParts('{"answer":"done"}'))),
                      );
                    }
                    return Stream.fromIterable<Response.StreamPartEncoded>([
                      {
                        type: "tool-call",
                        id: "ordinary-1",
                        name: "ordinary",
                        params: { value: "one" },
                        providerExecuted: false,
                      },
                      {
                        type: "tool-call",
                        id: "ordinary-2",
                        name: "ordinary",
                        params: { value: "two" },
                        providerExecuted: false,
                      },
                      {
                        type: "tool-call",
                        id: "exclusive-1",
                        name: "exclusive",
                        params: { value: "exclusive" },
                        providerExecuted: false,
                      },
                      {
                        type: "finish",
                        reason: "tool-calls",
                        usage,
                      },
                    ]);
                  }),
                ),
              ),
          }),
        ),
      );
      const definition = Agent.define("scheduling-overrides", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Run tools.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 3,
          maxDuration: "30 seconds",
          toolConcurrency: 3,
        }),
      });
      const enter = Effect.fn(function* () {
        const now = yield* Ref.updateAndGet(active, (value) => value + 1);
        yield* Ref.update(maximum, (value) => Math.max(value, now));
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
      });
      const leave = Ref.update(active, (value) => value - 1);

      yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "run" },
        {
          scheduling: {
            runOverride: { mode: "bounded", concurrency: 3 },
            toolRequiresSequential: (name) => name === "exclusive",
          },
        },
      ).pipe(
        Effect.provide(
          tools.toLayer({
            ordinary: ({ value }) => enter().pipe(Effect.ensuring(leave), Effect.as(value)),
            exclusive: ({ value }) =>
              Ref.get(active).pipe(
                Effect.tap((count) => Ref.set(exclusiveStartedWith, count)),
                Effect.andThen(enter()),
                Effect.ensuring(leave),
                Effect.as(value),
              ),
          }),
        ),
      );

      expect(yield* Ref.get(maximum)).toBe(2);
      expect(yield* Ref.get(exclusiveStartedWith)).toBe(0);
      expect(yield* Ref.get(toolResultOrder)).toEqual(["ordinary-1", "ordinary-2", "exclusive-1"]);
    }),
  );

  it.effect("applies a sequential Run override to the entire Tool batch", () =>
    Effect.gen(function* () {
      const active = yield* Ref.make(0);
      const maximum = yield* Ref.make(0);
      const turns = yield* Ref.make(0);
      const Work = Tool.make("work", {
        parameters: Schema.Struct({ value: Schema.String }),
        success: Schema.String,
      });
      const tools = Toolkit.make(Work);
      const model = Model.make(
        "scripted",
        "sequential-run",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.getAndUpdate(turns, (value) => value + 1).pipe(
                  Effect.map((turn) =>
                    Stream.fromIterable<Response.StreamPartEncoded>(
                      turn === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "work-1",
                              name: "work",
                              params: { value: "one" },
                              providerExecuted: false,
                            },
                            {
                              type: "tool-call",
                              id: "work-2",
                              name: "work",
                              params: { value: "two" },
                              providerExecuted: false,
                            },
                            { type: "finish", reason: "tool-calls", usage },
                          ]
                        : finalParts('{"answer":"done"}'),
                    ),
                  ),
                ),
              ),
          }),
        ),
      );
      const definition = Agent.define("sequential-run", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Work.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 2,
        }),
      });

      yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "work" },
        { scheduling: { runOverride: { mode: "sequential" } } },
      ).pipe(
        Effect.provide(
          tools.toLayer({
            work: ({ value }) =>
              Effect.gen(function* () {
                const now = yield* Ref.updateAndGet(active, (count) => count + 1);
                yield* Ref.update(maximum, (count) => Math.max(count, now));
                yield* Effect.yieldNow;
                yield* Ref.update(active, (count) => count - 1);
                return value;
              }),
          }),
        ),
      );

      expect(yield* Ref.get(maximum)).toBe(1);
    }),
  );

  it.effect("reuses explicit Conversation identity and returned history across Runs", () =>
    Effect.gen(function* () {
      const conversationId = yield* Schema.decodeEffect(ConversationId)("shared-conversation");
      let history = Prompt.empty;
      const first = yield* AgentRuntime.run(
        makeAgent(finalParts('{"answer":"first run"}')),
        { question: "first" },
        {
          conversationId,
          onHistory: (next) =>
            Effect.sync(() => {
              history = next;
            }),
        },
      );
      let secondPrompt = "";
      const secondModel = Model.make(
        "scripted",
        "conversation-second-run",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              secondPrompt = JSON.stringify(request.prompt);
              return Stream.fromIterable(finalParts('{"answer":"second run"}'));
            },
          }),
        ),
      );
      const second = yield* AgentRuntime.run(
        Agent.withModel(runtimeDefinition, secondModel),
        { question: "second" },
        { conversationId, history },
      );

      expect(first.conversationId).toBe(conversationId);
      expect(second.conversationId).toBe(conversationId);
      expect(secondPrompt).toContain("first run");
      expect(secondPrompt).toContain("second");
    }),
  );

  it.effect("requires cost estimation and fails typed cost exhaustion", () =>
    Effect.gen(function* () {
      const definition = Agent.define("cost-budget", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          costBudgetMicrousd: 1,
        }),
      });
      const agent = Agent.withModel(definition, modelFromParts(finalParts('{"answer":"done"}')));

      const missing = failureFrom(
        yield* AgentRuntime.run(agent, { question: "missing estimator" }).pipe(Effect.exit),
      );
      expect(missing).toBeInstanceOf(AgentPolicyError);
      if (!(missing instanceof AgentPolicyError)) {
        throw new Error("Expected AgentPolicyError");
      }
      expect(missing.limit).toBe("cost");
      expect(missing.message).toContain("requires a model cost estimator");

      const exhausted = failureFrom(
        yield* AgentRuntime.run(
          agent,
          { question: "too expensive" },
          { estimateCostMicrousd: () => Effect.succeed(2) },
        ).pipe(Effect.exit),
      );
      expect(exhausted).toBeInstanceOf(AgentPolicyError);
      if (!(exhausted instanceof AgentPolicyError)) {
        throw new Error("Expected AgentPolicyError");
      }
      expect(exhausted.limit).toBe("cost");
      expect(exhausted.message).toContain("exceeded");
    }),
  );

  it.effect("guards a stalled model with the active hierarchical budget", () =>
    Effect.gen(function* () {
      const budget: RunBudgetHook<BudgetGuardFailure> = {
        guard: () => Effect.fail(BudgetGuardFailure.make({ message: "model deadline reached" })),
        consume: () => Effect.void,
      };
      const stalledModel = Model.make(
        "scripted",
        "guarded-model",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => Stream.fromEffect(Effect.never),
          }),
        ),
      );
      const failure = failureFrom(
        yield* AgentRuntime.run(
          Agent.withModel(runtimeDefinition, stalledModel),
          { question: "stall" },
          { budget },
        ).pipe(Effect.exit),
      );

      expect(failure).toBeInstanceOf(BudgetGuardFailure);
      if (!(failure instanceof BudgetGuardFailure)) {
        throw new Error("Expected BudgetGuardFailure");
      }
      expect(failure.message).toContain("model deadline");
    }),
  );

  it.effect("guards an already-started Tool with the active hierarchical budget", () =>
    Effect.gen(function* () {
      const expired = yield* Deferred.make<void>();
      const toolStarted = yield* Deferred.make<void>();
      const Wait = Tool.make("wait_for_budget", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const tools = Toolkit.make(Wait);
      const definition = Agent.define("guarded-tool", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Wait.",
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
          id: "guarded-tool-1",
          name: "wait_for_budget",
          params: {},
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const budgetFailure = BudgetGuardFailure.make({ message: "tool deadline reached" });
      const budget: RunBudgetHook<BudgetGuardFailure> = {
        guard: (effect) =>
          Effect.raceFirst(
            effect,
            Deferred.await(expired).pipe(Effect.andThen(Effect.fail(budgetFailure))),
          ),
        consume: () => Effect.void,
      };
      const program = AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "stall in Tool" },
        { budget },
      ).pipe(
        Effect.provide(
          tools.toLayer({
            wait_for_budget: () =>
              Deferred.succeed(toolStarted, undefined).pipe(Effect.andThen(Effect.never)),
          }),
        ),
      );
      const fiber = yield* program.pipe(Effect.forkChild);
      yield* Deferred.await(toolStarted);
      yield* Deferred.succeed(expired, undefined);
      const failure = failureFrom(yield* Fiber.join(fiber).pipe(Effect.exit));

      expect(failure).toBe(budgetFailure);
    }),
  );

  it.effect("does not let a slow detached observer determine completion", () =>
    Effect.gen(function* () {
      const detached = yield* AgentRuntime.start(makeAgent(finalParts('{"answer":"detached"}')), {
        question: "complete independently",
      });
      const slowObserver = yield* detached.observe.pipe(
        Stream.runForEach(() => Effect.never),
        Effect.forkChild,
      );
      const result = yield* detached.await;

      expect(result.output).toEqual({ answer: "detached" });
      expect((yield* detached.events).at(-1)?._tag).toBe("RunCompleted");
      yield* Fiber.interrupt(slowObserver);
    }),
  );

  it.effect(
    "delivers live events to an observer attached mid-run and replays after settlement",
    () =>
      Effect.gen(function* () {
        const toolStarted = yield* Deferred.make<void>();
        const releaseTool = yield* Deferred.make<void>();
        const observerSawResult = yield* Deferred.make<void>();
        const turns = yield* Ref.make(0);
        const Wait = Tool.make("wait_for_release", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        });
        const tools = Toolkit.make(Wait);
        const model = Model.make(
          "scripted",
          "live-observer",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Ref.getAndUpdate(turns, (value) => value + 1).pipe(
                    Effect.flatMap((turn) =>
                      turn === 0
                        ? Effect.succeed(
                            Stream.fromIterable<Response.StreamPartEncoded>([
                              {
                                type: "tool-call",
                                id: "wait-release-1",
                                name: "wait_for_release",
                                params: {},
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]),
                          )
                        : // The Run cannot settle until the mid-Run observer has
                          // received the Tool result, so completion below proves
                          // the observer is live rather than replay-after-settle.
                          Deferred.await(observerSawResult).pipe(
                            Effect.as(
                              Stream.fromIterable(finalParts('{"answer":"observed live"}')),
                            ),
                          ),
                    ),
                  ),
                ),
            }),
          ),
        );
        const definition = Agent.define("live-observer", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Wait for the release signal.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const toolLayer = tools.toLayer({
          wait_for_release: () =>
            Deferred.succeed(toolStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseTool)),
              Effect.as("released"),
            ),
        });

        const detached = yield* AgentRuntime.start(Agent.withModel(definition, model), {
          question: "observe",
        }).pipe(Effect.provide(toolLayer));
        yield* Deferred.await(toolStarted);
        const observer = yield* detached.observe.pipe(
          Stream.tap((event) =>
            event._tag === "ToolCallSucceeded"
              ? Deferred.succeed(observerSawResult, undefined)
              : Effect.void,
          ),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Deferred.succeed(releaseTool, undefined);
        const result = yield* detached.await;
        const collected = yield* Fiber.join(observer);

        const expectedTrace = [
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
        ];
        expect(result.output).toEqual({ answer: "observed live" });
        expect(collected.map((event) => event._tag)).toEqual(expectedTrace);
        expect((yield* detached.events).map((event) => event._tag)).toEqual(expectedTrace);
        const replayed = yield* detached.observe.pipe(Stream.runCollect);
        expect(replayed.map((event) => event._tag)).toEqual(expectedTrace);
      }),
  );

  it.effect("official history carries encoded tool-call parameters for class-shaped schemas", () =>
    Effect.gen(function* () {
      class ItineraryParams extends Schema.Class<ItineraryParams>("ItineraryParams")({
        city: Schema.String,
        departAt: Schema.DateTimeUtcFromString,
      }) {}
      const Plan = Tool.make("plan_itinerary", {
        parameters: ItineraryParams,
        success: Schema.String,
      });
      const tools = Toolkit.make(Plan);
      const definition = Agent.define("class-shaped-parameters", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Plan the itinerary.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const turns = yield* Ref.make(0);
      const model = Model.make(
        "scripted",
        "class-shaped-parameters",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.getAndUpdate(turns, (value) => value + 1).pipe(
                  Effect.map((turn) =>
                    Stream.fromIterable<Response.StreamPartEncoded>(
                      turn === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "plan-1",
                              name: "plan_itinerary",
                              params: { city: "Kyoto", departAt: "2026-08-12T09:00:00Z" },
                              providerExecuted: false,
                            },
                            { type: "finish", reason: "tool-calls", usage },
                          ]
                        : finalParts('{"answer":"planned"}'),
                    ),
                  ),
                ),
              ),
          }),
        ),
      );
      const histories = yield* Ref.make<ReadonlyArray<Prompt.Prompt>>([]);

      const result = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "plan" },
        { onHistory: (history) => Ref.update(histories, (all) => [...all, history]) },
      ).pipe(
        Effect.provide(tools.toLayer({ plan_itinerary: () => Effect.succeed("planned") })),
        Effect.scoped,
      );

      const finalHistory = (yield* Ref.get(histories)).at(-1);
      expect(finalHistory).toBeDefined();
      if (finalHistory === undefined) {
        throw new Error("Expected official history to advance");
      }
      const callPart = finalHistory.content
        .filter((message) => message.role === "assistant")
        .flatMap((message) => message.content)
        .find((part) => part.type === "tool-call");
      expect(callPart).toBeDefined();
      if (callPart === undefined || callPart.type !== "tool-call") {
        throw new Error("Expected the official history to carry the tool-call part");
      }
      // Official history carries the wire form: plain JSON, not the decoded
      // Schema.Class instance with its DateTime field.
      expect(Option.isSome(Schema.decodeUnknownOption(Schema.Json)(callPart.params))).toBe(true);
      const params = callPart.params as { readonly city: string; readonly departAt: unknown };
      expect(params.city).toBe("Kyoto");
      expect(typeof params.departAt).toBe("string");
      // The full official history round-trips through the Prompt codec into
      // plain JSON — the property the canonical persistence boundary needs.
      const encodedHistory = yield* Schema.encodeEffect(Prompt.Prompt)(finalHistory);
      expect(Option.isSome(Schema.decodeUnknownOption(Schema.Json)(encodedHistory))).toBe(true);
      expect(result.output).toEqual({ answer: "planned" });
    }),
  );

  it.effect("handlers still receive decoded class-shaped parameters", () =>
    Effect.gen(function* () {
      class ItineraryParams extends Schema.Class<ItineraryParams>("ItineraryParams")({
        city: Schema.String,
        departAt: Schema.DateTimeUtcFromString,
      }) {}
      let handlerParams: ItineraryParams | undefined;
      const Plan = Tool.make("plan_itinerary", {
        parameters: ItineraryParams,
        success: Schema.String,
      });
      const tools = Toolkit.make(Plan);
      const definition = Agent.define("decoded-handler-parameters", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Plan the itinerary.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const turns = yield* Ref.make(0);
      const model = Model.make(
        "scripted",
        "decoded-handler-parameters",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.getAndUpdate(turns, (value) => value + 1).pipe(
                  Effect.map((turn) =>
                    Stream.fromIterable<Response.StreamPartEncoded>(
                      turn === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "plan-1",
                              name: "plan_itinerary",
                              params: { city: "Kyoto", departAt: "2026-08-12T09:00:00Z" },
                              providerExecuted: false,
                            },
                            { type: "finish", reason: "tool-calls", usage },
                          ]
                        : finalParts('{"answer":"planned"}'),
                    ),
                  ),
                ),
              ),
          }),
        ),
      );

      yield* AgentRuntime.run(Agent.withModel(definition, model), { question: "plan" }).pipe(
        Effect.provide(
          tools.toLayer({
            plan_itinerary: (params) =>
              Effect.sync(() => {
                handlerParams = params;
                return "planned";
              }),
          }),
        ),
        Effect.scoped,
      );

      expect(handlerParams).toBeInstanceOf(ItineraryParams);
      expect(handlerParams?.city).toBe("Kyoto");
      expect(DateTime.isDateTime(handlerParams?.departAt)).toBe(true);
    }),
  );

  it.effect(
    "threads resume leading messages into the model context before the assistant tool-call message",
    () =>
      Effect.gen(function* () {
        const leadingText = "steering committed inside the pending turn";
        const resumeLeadingTurnId = yield* Schema.decodeEffect(TurnId)("turn-resume-leading").pipe(
          Effect.orDie,
        );
        const Lookup = Tool.make("lookup", {
          parameters: Schema.Struct({ key: Schema.String }),
          success: Schema.String,
        });
        const tools = Toolkit.make(Lookup);
        const definition = Agent.define("resume-leading-messages", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Look everything up.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const toolLayer = tools.toLayer({
          lookup: ({ key }) => Effect.succeed(`handled-${key}`),
        });
        const runResumed = (leadingMessages: Prompt.Prompt | undefined) =>
          Effect.gen(function* () {
            let captured: Prompt.Prompt | undefined;
            const model = Model.make(
              "scripted",
              "resume-leading",
              Layer.effect(
                LanguageModel.LanguageModel,
                LanguageModel.make({
                  generateText: () => Effect.succeed([]),
                  streamText: (request) => {
                    captured = request.prompt;
                    return Stream.fromIterable(finalParts('{"answer":"resumed"}'));
                  },
                }),
              ),
            );
            const resume: RunTurnResume = {
              turn: 1,
              turnId: resumeLeadingTurnId,
              calls: [{ id: "lookup-1", name: "lookup", params: { key: "a" } }],
              settled: [],
              ...(leadingMessages === undefined ? {} : { leadingMessages }),
            };
            const result = yield* AgentRuntime.run(
              Agent.withModel(definition, model),
              { question: "resume" },
              { resume },
            ).pipe(Effect.provide(toolLayer), Effect.scoped);
            expect(result.output).toEqual({ answer: "resumed" });
            expect(captured).toBeDefined();
            if (captured === undefined) {
              throw new Error("Expected the follow-up model request to be captured");
            }
            return captured;
          });

        const leadingMessages = Prompt.fromMessages([
          Prompt.makeMessage("user", {
            content: [Prompt.makePart("text", { text: leadingText })],
          }),
        ]);
        const prompt = yield* runResumed(leadingMessages);

        const leadingIndex = prompt.content.findIndex(
          (message) =>
            message.role === "user" &&
            message.content.some((part) => part.type === "text" && part.text === leadingText),
        );
        const assistantIndex = prompt.content.findIndex(
          (message) =>
            message.role === "assistant" &&
            message.content.some((part) => part.type === "tool-call" && part.id === "lookup-1"),
        );
        const toolIndex = prompt.content.findIndex((message) => message.role === "tool");
        const inputIndex = prompt.content.findIndex(
          (message) =>
            message.role === "user" &&
            message.content.some((part) => part.type === "text" && part.text.includes("resume")),
        );
        // The pending Turn's committed leading messages sit between the
        // re-evaluated initial prompt and the rebuilt assistant tool-call
        // message, and precede the Tool results.
        expect(inputIndex).toBeGreaterThanOrEqual(0);
        expect(leadingIndex).toBeGreaterThan(inputIndex);
        expect(assistantIndex).toBeGreaterThan(leadingIndex);
        expect(toolIndex).toBeGreaterThan(assistantIndex);

        // Absent leadingMessages keeps the prior behavior: the steering text
        // never enters the resumed model context.
        const bare = yield* runResumed(undefined);
        const bareLeading = bare.content.findIndex(
          (message) =>
            message.role === "user" &&
            message.content.some((part) => part.type === "text" && part.text === leadingText),
        );
        expect(bareLeading).toBe(-1);
      }),
  );
});
