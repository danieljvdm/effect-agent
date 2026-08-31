import {
  Agent,
  AgentApprovalDenied,
  AgentApprovalPending,
  AgentInputError,
  AgentOutputError,
  AgentRunDispositionError,
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
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Context,
  DateTime,
  Deferred,
  Effect,
  ErrorReporter,
  Exit,
  Fiber,
  Layer,
  Logger,
  Option,
  Redacted,
  Ref,
  References,
  Schema,
  Stream,
  Tracer,
} from "effect";
import { TestClock } from "effect/testing";
import {
  AiError,
  LanguageModel,
  Model,
  Prompt,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";

import { boundedValueFootprint } from "../src/bounded-value-internal.ts";
import { ConversationHistory } from "../src/conversation-history.ts";
import { errorMessage, errorTag } from "../src/error-diagnostic-internal.ts";
import {
  AgentResultSchema,
  AgentRuntime,
  ToolExecutionClass,
  withTerminalDefectEvent,
  type RunBudgetHook,
  type RunToolAuthorizationRequest,
  type RunTurnResume,
  type RunUsageDelta,
} from "../src/index.ts";
import { boundedJsonSnapshot } from "../src/provider-result-staging-internal.ts";
import { emitThenAfter, isolateToolDerivative } from "../src/tool-derivative-internal.ts";
import {
  annotateToolSpanTerminalOutcome,
  makeIsolatedToolTracer,
  restoreToolSpanFailureCause,
  stripToolSpanFailures,
  ToolSpanTelemetry,
  ToolSpanFailure,
} from "../src/tool-telemetry-internal.ts";

class ScheduledToolFailure extends Schema.TaggedError<ScheduledToolFailure>()(
  "ScheduledToolFailure",
  { message: Schema.String },
) {}

class HookFailure extends Schema.TaggedError<HookFailure>()("HookFailure", {
  message: Schema.String,
}) {}

class TestCauseSource extends Context.Service<TestCauseSource, string>()(
  "@effect-agent/engine/test/TestCauseSource",
) {}

class BudgetGuardFailure extends Schema.TaggedError<BudgetGuardFailure>()("BudgetGuardFailure", {
  message: Schema.String,
}) {}

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

/** Traverse every OTLP-facing observation value, including Maps and non-enumerable Error fields. */
const reachableTelemetryValues = (root: unknown): ReadonlyArray<unknown> => {
  const values: Array<unknown> = [];
  const visited = new WeakSet<object>();
  const visit = (value: unknown): void => {
    values.push(value);
    if ((typeof value !== "object" && typeof value !== "function") || value === null) return;
    if (visited.has(value)) return;
    visited.add(value);
    if (value instanceof Map) {
      for (const [key, item] of value) {
        visit(key);
        visit(item);
      }
    } else if (value instanceof Set) {
      for (const item of value) visit(item);
    }
    for (const key of Reflect.ownKeys(value)) {
      visit(key);
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor !== undefined && "value" in descriptor) visit(descriptor.value);
    }
  };
  visit(root);
  return values;
};

/** Snapshot every field Effect's OTLP tracer exports, without retaining a successful Exit value. */
const exportedSpanObservation = (span: Tracer.NativeSpan) => ({
  name: span.name,
  spanId: span.spanId,
  traceId: span.traceId,
  parentSpanId: Option.getOrUndefined(span.parent)?.spanId,
  sampled: span.sampled,
  kind: span.kind,
  startTime: span.startTime,
  status:
    span.status._tag === "Started"
      ? span.status
      : {
          _tag: span.status._tag,
          startTime: span.status.startTime,
          endTime: span.status.endTime,
          failureCause: Exit.isFailure(span.status.exit) ? span.status.exit.cause : undefined,
        },
  attributes: Object.fromEntries(span.attributes),
  events: span.events.map(([name, startTime, attributes]) => ({
    name,
    startTime,
    attributes: { ...attributes },
  })),
  links: span.links.map(({ span: linkedSpan, attributes }) => ({
    traceId: linkedSpan.traceId,
    spanId: linkedSpan.spanId,
    attributes: { ...attributes },
  })),
});

/** Snapshot every input or context field Effect's pinned OTLP logger can export. */
const exportedLogObservation = ({ cause, fiber, logLevel, message }: Logger.Options<unknown>) => ({
  message,
  level: logLevel,
  cause,
  annotations: { ...fiber.getRef(References.CurrentLogAnnotations) },
  logSpans: fiber
    .getRef(References.CurrentLogSpans)
    .map(([label, startTime]) => ({ label, startTime })),
  fiberId: fiber.id,
  currentSpan:
    fiber.currentSpan === undefined
      ? undefined
      : {
          traceId: fiber.currentSpan.traceId,
          spanId: fiber.currentSpan.spanId,
        },
});

type ExportedLogObservation = ReturnType<typeof exportedLogObservation>;

const renderedLogMessage = (message: unknown): string =>
  Array.isArray(message) ? message.join(" ") : String(message);

const testLayer = Layer.merge(identifiers, ConversationHistory.layerTransient);

layer(testLayer)("RUN-001 Phase 1 AgentRuntime", (it) => {
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
      // The outgoing tail is instructions, decoded input, then the derived
      // run-status message (RUN-020) — official history keeps only the first two.
      expect(encodedObserved.content.at(-3)?.role).toBe("system");
      expect(encodedObserved.content.at(-2)?.role).toBe("user");
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
        "BudgetWarning",
        "ToolCallStarted",
        "ToolCallSucceeded",
        "TurnStarted",
        "ModelStarted",
        "TextDelta",
        "TurnCompleted",
        "BudgetWarning",
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
        "BudgetWarning",
        "TurnStarted",
        "ModelStarted",
        "TextDelta",
        "TurnCompleted",
        "BudgetWarning",
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

  it.effect("retains an owned normalized snapshot of provider Tool results", () => {
    const providerResult = { status: "completed" };
    let observedPromptResult: unknown;
    const model = Model.make(
      "scripted",
      "owned-provider-result",
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
                      const observedPart = request.prompt.content
                        .find((message) => message.role === "assistant")
                        ?.content.find(
                          (part) => part.type === "tool-result" && part.id === "owned-result-1",
                        );
                      observedPromptResult =
                        observedPart?.type === "tool-result" ? observedPart.result : undefined;
                      return Stream.fromIterable(finalParts('{"answer":"owned"}'));
                    }
                    return Stream.fromIterable<Response.StreamPartEncoded>([
                      {
                        type: "tool-call",
                        id: "owned-result-1",
                        name: "HostedSearch",
                        params: { query: "ownership" },
                        providerExecuted: true,
                      },
                      {
                        type: "tool-result",
                        id: "owned-result-1",
                        name: "HostedSearch",
                        result: providerResult,
                        isFailure: false,
                        providerExecuted: true,
                      },
                    ]).pipe(
                      Stream.concat(
                        Stream.fromEffect(
                          Effect.sync(() => {
                            providerResult.status = "mutated-after-emission";
                            return {
                              type: "finish" as const,
                              reason: "tool-calls" as const,
                              usage,
                            };
                          }),
                        ),
                      ),
                    );
                  }),
                ),
              ),
          });
        }),
      ),
    );

    return Effect.gen(function* () {
      const events = yield* AgentRuntime.stream(Agent.withModel(hostedDefinition, model), {
        question: "Who owns the result?",
      }).pipe(Stream.runCollect);

      expect(events.at(-1)?._tag).toBe("RunCompleted");
      expect(observedPromptResult).toEqual({ status: "completed" });
      expect(observedPromptResult).not.toBe(providerResult);
    });
  });

  it.effect("exports content-free canonical Tool spans and bounded terminal logs", () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const logs: Array<ExportedLogObservation> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const logger = Logger.make<unknown, void>((options) => {
      const observation = exportedLogObservation(options);
      logs.push(observation);
    });
    const handlerSecret = "handler-result-must-not-be-exported";
    const failureSecret = "failure-message-must-not-be-exported";
    const failureToolCallId = "fail-observed-1";
    const returnedFailure = ScheduledToolFailure.make({ message: failureSecret });
    const Read = Tool.make("read", {
      parameters: Schema.Struct({ path: Schema.String }),
      success: Schema.String,
    }).annotate(ToolExecutionClass, "readonly");
    const Fail = Tool.make("fail", {
      parameters: Schema.Struct({ command: Schema.String }),
      success: Schema.String,
      failure: ScheduledToolFailure,
      failureMode: "return",
    });
    const tools = Toolkit.make(Read, Fail);
    const model = Model.make(
      "scripted",
      "tool-observability",
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
                    value === 0
                      ? Stream.fromIterable<Response.StreamPartEncoded>([
                          {
                            type: "tool-call",
                            id: "read-observed-1",
                            name: "read",
                            params: { path: "/private/source.ts" },
                            providerExecuted: false,
                          },
                          {
                            type: "tool-call",
                            id: failureToolCallId,
                            name: "fail",
                            params: { command: "printenv SECRET" },
                            providerExecuted: false,
                          },
                          { type: "finish", reason: "tool-calls", usage },
                        ])
                      : Stream.fromIterable(finalParts('{"answer":"observed"}')),
                  ),
                ),
              ),
          });
        }),
      ),
    );
    const definition = Agent.define("tool-observability", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Call both Tools.",
      toolkit: tools,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 2,
        maxDuration: "30 seconds",
        toolConcurrency: 2,
      }),
    });
    const toolLayer = tools.toLayer({
      read: () => Effect.succeed(handlerSecret).pipe(Effect.withSpan("host.read")),
      fail: () => Effect.fail(returnedFailure),
    });

    return Effect.gen(function* () {
      const events = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "Which Tools ran?",
      }).pipe(Stream.runCollect, Effect.provide(toolLayer));

      expect(events.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(1);
      expect(events.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);

      const toolSpans = spans
        .filter((span) => span.name.startsWith("execute_tool "))
        .toSorted((left, right) => left.name.localeCompare(right.name));
      expect(toolSpans).toHaveLength(2);
      expect(toolSpans.map((span) => span.name)).toEqual([
        "execute_tool fail",
        "execute_tool read",
      ]);
      expect(spans.some((span) => span.name === "AgentRuntime.toolkit.handle")).toBe(false);
      const hostOwnedSpan = spans.find((span) => span.name === "host.read");
      expect(hostOwnedSpan?.status._tag).toBe("Ended");
      expect(Object.fromEntries(hostOwnedSpan?.attributes ?? [])).toEqual({});
      expect(hostOwnedSpan?.events).toEqual([]);
      expect(hostOwnedSpan?.links).toEqual([]);
      const hostParent = Option.getOrUndefined(hostOwnedSpan?.parent ?? Option.none());
      expect(hostParent?._tag).toBe("Span");
      if (hostParent?._tag !== "Span") {
        throw new Error("Expected the host-owned handler span to have a canonical Tool parent");
      }
      expect(hostParent.name).toBe("execute_tool read");
      expect(hostOwnedSpan?.traceId).toBe(hostParent.traceId);

      const [failedSpan, succeededSpan] = toolSpans;
      expect(failedSpan?.kind).toBe("internal");
      const failedAttributes = Object.fromEntries(failedSpan?.attributes ?? []);
      expect(failedAttributes).toMatchObject({
        "gen_ai.operation.name": "execute_tool",
        "gen_ai.tool.name": "fail",
        "gen_ai.tool.type": "function",
        "gen_ai.agent.name": "tool-observability",
        "gen_ai.conversation.id": "conversation-1",
        "effect_agent.tool.execution_class": "uncertain",
        "effect_agent.tool.outcome": "failure",
        agentId: "tool-observability",
        conversationId: "conversation-1",
        runId: "run-1",
        turnId: "turn-1",
        toolCallId: failureToolCallId,
        toolName: "fail",
        "gen_ai.tool.call.id": failureToolCallId,
      });
      expect(Object.fromEntries(succeededSpan?.attributes ?? [])).toMatchObject({
        "gen_ai.tool.name": "read",
        "gen_ai.tool.call.id": "read-observed-1",
        "effect_agent.tool.execution_class": "readonly",
        "effect_agent.tool.outcome": "success",
        toolCallId: "read-observed-1",
      });
      expect(failedSpan?.status._tag).toBe("Ended");
      expect(succeededSpan?.status._tag).toBe("Ended");
      if (failedSpan?.status._tag !== "Ended" || !Exit.isFailure(failedSpan.status.exit)) {
        throw new Error("Expected the returned Tool failure span to end failed");
      }
      expect(Cause.pretty(failedSpan.status.exit.cause)).not.toContain(failureSecret);
      if (succeededSpan?.status._tag !== "Ended") {
        throw new Error("Expected the successful Tool span to end");
      }
      expect(Exit.isSuccess(succeededSpan.status.exit)).toBe(true);
      const spanNames = spans.map((span) => span.name);
      expect(spanNames).not.toContain("AgentRuntime.decodeToolCallId");
      expect(spanNames).not.toContain("AgentRuntime.decodeProviderToolCallId");
      expect(spanNames).not.toContain("AgentRuntime.decodeProviderResponsePartId");

      const terminalLogs = logs.filter((entry) =>
        ["agent tool execution completed", "agent tool execution failed"].includes(
          renderedLogMessage(entry.message),
        ),
      );
      expect(terminalLogs).toHaveLength(2);
      expect(
        terminalLogs
          .map((entry) => ({
            level: entry.level,
            message: renderedLogMessage(entry.message),
            outcome: entry.annotations.toolOutcome,
            toolName: entry.annotations.toolName,
          }))
          .toSorted((left, right) => String(left.toolName).localeCompare(String(right.toolName))),
      ).toEqual([
        {
          level: "Warn",
          message: "agent tool execution failed",
          outcome: "failure",
          toolName: "fail",
        },
        {
          level: "Info",
          message: "agent tool execution completed",
          outcome: "success",
          toolName: "read",
        },
      ]);
      expect(terminalLogs.map(({ annotations }) => annotations["gen_ai.operation.name"])).toEqual([
        "execute_tool",
        "execute_tool",
      ]);
      expect(terminalLogs.map(({ annotations }) => annotations["gen_ai.conversation.id"])).toEqual([
        "conversation-1",
        "conversation-1",
      ]);
      expect(terminalLogs.map(({ annotations }) => annotations.conversationId)).toEqual([
        "conversation-1",
        "conversation-1",
      ]);
      const failedLog = terminalLogs.find(({ annotations }) => annotations.toolName === "fail");
      expect(failedLog?.annotations).toMatchObject({
        "gen_ai.tool.call.id": failureToolCallId,
        toolCallId: failureToolCallId,
      });
      const succeededLog = terminalLogs.find(({ annotations }) => annotations.toolName === "read");
      expect(succeededLog?.annotations).toMatchObject({
        "gen_ai.tool.call.id": "read-observed-1",
        toolCallId: "read-observed-1",
      });
      const exportedValues = reachableTelemetryValues({
        spans: spans.map(exportedSpanObservation),
        logs,
      });
      expect(exportedValues).not.toContain(returnedFailure);
      const exportedStrings = exportedValues
        .filter((value) => typeof value === "string")
        .join("\n");
      expect(exportedStrings).not.toContain("/private/source.ts");
      expect(exportedStrings).not.toContain("printenv SECRET");
      expect(exportedStrings).not.toContain(handlerSecret);
      expect(exportedStrings).not.toContain(failureSecret);
    }).pipe(Effect.provideService(Tracer.Tracer, tracer), Effect.provide(Logger.layer([logger])));
  });

  it.effect("rejects invalid model Tool Call IDs before correlation or handler execution", () =>
    Effect.gen(function* () {
      const handlerCalls = yield* Ref.make(0);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const invalidIdSecret = "invalid-tool-call-id-must-not-reach-a-handler";
      const invalidId = `unsafe/${invalidIdSecret}/${"x".repeat(256)}`;
      const Read = Tool.make("read_invalid_id", {
        parameters: Schema.Struct({ path: Schema.String }),
        success: Schema.String,
      });
      const tools = Toolkit.make(Read);
      const definition = Agent.define("invalid-tool-call-id", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Call the Tool.",
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
          id: invalidId,
          name: "read_invalid_id",
          params: { path: "/private/source.ts" },
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const toolLayer = tools.toLayer({
        read_invalid_id: () =>
          Ref.update(handlerCalls, (calls) => calls + 1).pipe(Effect.as("unexpected")),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "Which Tool ran?",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("invalid Tool Call ID");
      expect(failure.message).not.toContain(invalidIdSecret);
      expect(yield* Ref.get(handlerCalls)).toBe(0);
      expect(observed.some((event) => event._tag === "ToolCallDeclared")).toBe(false);
      expect(observed.some((event) => event._tag === "ToolCallStarted")).toBe(false);
      expect(observed.some((event) => event._tag === "ToolCallSucceeded")).toBe(false);
      expect(observed.some((event) => event._tag === "ToolCallFailed")).toBe(false);
    }),
  );

  it.effect("rejects every retained or correlated model response-part identifier", () => {
    const invalidIdSecret = "invalid-response-part-id-must-not-enter-runtime-state";
    const invalidId = `unsafe/${invalidIdSecret}/${"x".repeat(256)}`;
    const cases: ReadonlyArray<{
      readonly label: string;
      readonly part: Response.StreamPartEncoded;
    }> = [
      { label: "text start", part: { type: "text-start", id: invalidId } },
      { label: "text delta", part: { type: "text-delta", id: invalidId, delta: "ignored" } },
      { label: "text end", part: { type: "text-end", id: invalidId } },
      { label: "reasoning start", part: { type: "reasoning-start", id: invalidId } },
      {
        label: "reasoning delta",
        part: { type: "reasoning-delta", id: invalidId, delta: "ignored" },
      },
      { label: "reasoning end", part: { type: "reasoning-end", id: invalidId } },
      {
        label: "Tool parameters start",
        part: {
          type: "tool-params-start",
          id: invalidId,
          name: "unknown",
          providerExecuted: false,
        },
      },
      {
        label: "Tool parameters delta",
        part: { type: "tool-params-delta", id: invalidId, delta: "{}" },
      },
      { label: "Tool parameters end", part: { type: "tool-params-end", id: invalidId } },
      {
        label: "source",
        part: {
          type: "source",
          sourceType: "url",
          id: invalidId,
          url: "https://example.invalid/source",
          title: "source",
        },
      },
      {
        label: "response metadata",
        part: { type: "response-metadata", id: invalidId },
      },
      {
        label: "approval",
        part: {
          type: "tool-approval-request",
          approvalId: invalidId,
          toolCallId: "valid-tool-call",
        },
      },
      {
        label: "approval Tool Call",
        part: {
          type: "tool-approval-request",
          approvalId: "valid-approval",
          toolCallId: invalidId,
        },
      },
    ];

    return Effect.gen(function* () {
      yield* Effect.forEach(
        cases,
        ({ label, part }) =>
          AgentRuntime.stream(makeAgent([part]), { question: label }).pipe(
            Stream.runDrain,
            Effect.exit,
            Effect.map((exit) => {
              const failure = failureFrom(exit);
              expect(failure).toBeInstanceOf(ModelProtocolError);
              expect(failure.message).toContain("invalid");
              expect(failure.message).not.toContain(invalidIdSecret);
            }),
          ),
        { discard: true },
      );

      const toolResultExit = yield* AgentRuntime.stream(
        Agent.withModel(
          hostedDefinition,
          modelFromParts([
            {
              type: "tool-result",
              id: invalidId,
              name: "HostedSearch",
              result: { status: "ignored" },
              isFailure: false,
              providerExecuted: true,
            },
          ]),
        ),
        { question: "Tool result" },
      ).pipe(Stream.runDrain, Effect.exit);
      const toolResultFailure = failureFrom(toolResultExit);
      expect(toolResultFailure).toBeInstanceOf(ModelProtocolError);
      expect(toolResultFailure.message).toContain("invalid Tool Call ID");
      expect(toolResultFailure.message).not.toContain(invalidIdSecret);
    });
  });

  it.effect("exports one failed Tool outcome when downstream stops at its terminal event", () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const logs: Array<ExportedLogObservation> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const logger = Logger.make<unknown, void>((options) => {
      logs.push(exportedLogObservation(options));
    });
    const failureSecret = "early-close-returned-failure-must-not-be-exported";
    const returnedFailure = ScheduledToolFailure.make({ message: failureSecret });
    const Fail = Tool.make("fail_early", {
      parameters: Schema.Struct({ command: Schema.String }),
      success: Schema.String,
      failure: ScheduledToolFailure,
      failureMode: "return",
    });
    const tools = Toolkit.make(Fail);
    const model = modelFromParts([
      {
        type: "tool-call",
        id: "fail-early-1",
        name: "fail_early",
        params: { command: "private command" },
        providerExecuted: false,
      },
      { type: "finish", reason: "tool-calls", usage },
    ]);
    const definition = Agent.define("early-close-tool-observability", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Call the Tool.",
      toolkit: tools,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    });

    return Effect.gen(function* () {
      const events = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "Fail once",
      }).pipe(
        Stream.takeUntil((event) => event._tag === "ToolCallFailed"),
        Stream.runCollect,
        Effect.provide(tools.toLayer({ fail_early: () => Effect.fail(returnedFailure) })),
      );

      expect(events.at(-1)).toMatchObject({
        _tag: "ToolCallFailed",
        toolCallId: "fail-early-1",
        toolName: "fail_early",
        providerExecuted: false,
      });
      const terminalLogs = logs.filter(
        (entry) => renderedLogMessage(entry.message) === "agent tool execution failed",
      );
      expect(terminalLogs).toHaveLength(1);
      expect(terminalLogs[0]?.annotations).toMatchObject({
        "effect_agent.tool.outcome": "failure",
        toolName: "fail_early",
        toolOutcome: "failure",
      });

      const span = spans.find((candidate) => candidate.name === "execute_tool fail_early");
      expect(Object.fromEntries(span?.attributes ?? [])).toMatchObject({
        "effect_agent.tool.outcome": "failure",
      });
      if (span?.status._tag !== "Ended" || !Exit.isFailure(span.status.exit)) {
        throw new Error("Expected the early-closed returned Tool failure span to end failed");
      }
      const exportedValues = reachableTelemetryValues({
        spans: [exportedSpanObservation(span)],
        logs: terminalLogs,
      });
      expect(exportedValues).not.toContain(returnedFailure);
      expect(exportedValues.filter((value) => typeof value === "string").join("\n")).not.toContain(
        failureSecret,
      );
    }).pipe(Effect.provideService(Tracer.Tracer, tracer), Effect.provide(Logger.layer([logger])));
  });

  it.effect("runs deferred work once from an ordinary second pull", () =>
    Effect.gen(function* () {
      const attempts = yield* Ref.make(0);
      const observed = yield* emitThenAfter(
        Effect.succeed("terminal-event"),
        Ref.update(attempts, (count) => count + 1),
      ).pipe(Stream.runCollect);

      expect([...observed]).toEqual(["terminal-event"]);
      expect(yield* Ref.get(attempts)).toBe(1);
    }),
  );

  it.effect("runs deferred work once when downstream closes after the first value", () =>
    Effect.gen(function* () {
      const attempts = yield* Ref.make(0);
      const observed = yield* emitThenAfter(
        Effect.succeed("terminal-event"),
        Ref.update(attempts, (count) => count + 1),
      ).pipe(Stream.take(1), Stream.runCollect);

      expect([...observed]).toEqual(["terminal-event"]);
      expect(yield* Ref.get(attempts)).toBe(1);
    }),
  );

  it.effect("preserves exact interruption during an in-flight early-close finalizer", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const blocked = yield* Deferred.make<void>();
      const interruptor = 7332;
      const fiber = yield* emitThenAfter(
        Effect.succeed("terminal-event"),
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(blocked))),
      ).pipe(Stream.take(1), Stream.runCollect, Effect.forkChild);

      yield* Deferred.await(entered);
      fiber.interruptUnsafe(interruptor);
      const exit = yield* Fiber.await(fiber);

      if (Exit.isSuccess(exit)) throw new Error("Expected early-close finalizer interruption");
      expect(exit.cause.reasons).toHaveLength(1);
      expect(exit.cause.reasons[0]?._tag).toBe("Interrupt");
      if (exit.cause.reasons[0]?._tag !== "Interrupt") {
        throw new Error("Expected exact early-close interruption Cause");
      }
      expect(exit.cause.reasons[0].fiberId).toBe(interruptor);
    }),
  );

  it.effect("reports typed failures and defects owned by early-close finalization", () => {
    const typedFailure = new Error("early-close-typed-derivative-failure");
    const defect = new Error("early-close-derivative-defect");
    const reports: Array<Cause.Cause<unknown>> = [];
    const reporter = ErrorReporter.make(({ cause }) => {
      reports.push(cause);
    });

    return Effect.gen(function* () {
      const typedObserved = yield* emitThenAfter(
        Effect.succeed("typed-event"),
        Effect.fail(typedFailure),
      ).pipe(Stream.take(1), Stream.runCollect);
      const defectObserved = yield* emitThenAfter(
        Effect.succeed("defect-event"),
        Effect.die(defect),
      ).pipe(Stream.take(1), Stream.runCollect);

      expect([...typedObserved]).toEqual(["typed-event"]);
      expect([...defectObserved]).toEqual(["defect-event"]);
      expect(reports).toHaveLength(2);
      expect(reports[0]?.reasons).toHaveLength(1);
      expect(reports[0]?.reasons[0]?._tag).toBe("Fail");
      if (reports[0]?.reasons[0]?._tag !== "Fail") {
        return yield* Effect.die(
          new Error("Expected the early-close typed failure to reach ErrorReporter"),
        );
      }
      expect(reports[0].reasons[0].error).toBe(typedFailure);
      expect(reports[1]?.reasons).toHaveLength(1);
      expect(reports[1]?.reasons[0]?._tag).toBe("Die");
      if (reports[1]?.reasons[0]?._tag !== "Die") {
        return yield* Effect.die(
          new Error("Expected the early-close defect to reach ErrorReporter"),
        );
      }
      expect(reports[1].reasons[0].defect).toBe(defect);
    }).pipe(Effect.provide(ErrorReporter.layer([reporter])));
  });

  it.effect("does not arm deferred work when the event fails", () =>
    Effect.gen(function* () {
      const attempts = yield* Ref.make(0);
      const eventFailure = new Error("terminal event was not committed");
      const exit = yield* emitThenAfter(
        Effect.fail(eventFailure),
        Ref.update(attempts, (count) => count + 1),
      ).pipe(Stream.runDrain, Effect.exit);

      if (Exit.isSuccess(exit)) throw new Error("Expected the event Effect to fail");
      expect(Cause.findErrorOption(exit.cause)).toEqual(Option.some(eventFailure));
      expect(yield* Ref.get(attempts)).toBe(0);
    }),
  );

  it.effect("does not arm deferred work when downstream takes zero values", () =>
    Effect.gen(function* () {
      const attempts = yield* Ref.make(0);
      const observed = yield* emitThenAfter(
        Effect.succeed("terminal-event"),
        Ref.update(attempts, (count) => count + 1),
      ).pipe(Stream.take(0), Stream.runCollect);

      expect([...observed]).toEqual([]);
      expect(yield* Ref.get(attempts)).toBe(0);
    }),
  );

  it.effect("does not arm deferred work when an opened scope closes before its first pull", () =>
    Effect.gen(function* () {
      const attempts = yield* Ref.make(0);
      const stream = emitThenAfter(
        Effect.succeed("terminal-event"),
        Ref.update(attempts, (count) => count + 1),
      );

      yield* Effect.scoped(Stream.toPull(stream).pipe(Effect.asVoid));
      expect(yield* Ref.get(attempts)).toBe(0);
    }),
  );

  it.effect("emits the event first and never retries an interrupted deferred action", () =>
    Effect.gen(function* () {
      const attempts = yield* Ref.make(0);
      const observed: Array<string> = [];
      const exit = yield* Effect.scoped(
        Effect.gen(function* () {
          const telemetryEntered = yield* Deferred.make<void>();
          const telemetryBlocked = yield* Deferred.make<void>();
          const stream = emitThenAfter(
            Effect.succeed("terminal-event"),
            Ref.update(attempts, (count) => count + 1).pipe(
              Effect.andThen(Deferred.succeed(telemetryEntered, undefined)),
              Effect.andThen(Deferred.await(telemetryBlocked)),
            ),
          ).pipe(
            Stream.tap((event) =>
              Effect.sync(() => {
                observed.push(event);
              }),
            ),
          );
          const pull = yield* Stream.toPull(stream);

          expect(yield* pull).toEqual(["terminal-event"]);
          expect(observed).toEqual(["terminal-event"]);

          const secondPull = yield* Effect.forkChild(pull);
          yield* Deferred.await(telemetryEntered);
          yield* Fiber.interrupt(secondPull);
          return yield* Fiber.await(secondPull);
        }),
      );

      expect(observed).toEqual(["terminal-event"]);
      if (Exit.isSuccess(exit)) throw new Error("Expected terminal telemetry interruption");
      expect(Cause.hasInterrupts(exit.cause)).toBe(true);
      expect(yield* Ref.get(attempts)).toBe(1);
    }),
  );

  it.effect("keeps committed terminal outcomes authoritative over telemetry interruption", () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });

    return Effect.gen(function* () {
      const telemetry = yield* ToolSpanTelemetry;
      const failureMarker = ToolSpanFailure.marker();
      yield* Effect.forEach(
        ["success", "failure"] as const,
        (outcome) =>
          Effect.gen(function* () {
            const entered = yield* Deferred.make<void>();
            const blocked = yield* Deferred.make<void>();
            const pullExit = yield* Effect.scoped(
              Effect.gen(function* () {
                const stream = emitThenAfter(
                  Effect.succeed(`terminal-${outcome}`),
                  annotateToolSpanTerminalOutcome(
                    outcome,
                    outcome === "failure" ? failureMarker : undefined,
                  ).pipe(
                    Effect.andThen(
                      Effect.flatMap(Effect.currentSpan, (span) =>
                        Effect.sync(() => {
                          // Host/application code can mutate this public observation map. The
                          // authenticated private terminal state must remain authoritative.
                          span.attribute(
                            "effect_agent.tool.outcome",
                            outcome === "success" ? "failure" : "success",
                          );
                        }),
                      ),
                    ),
                    Effect.andThen(Deferred.succeed(entered, undefined)),
                    Effect.andThen(Deferred.await(blocked)),
                  ),
                ).pipe(
                  Stream.withSpan(`execute_tool interrupted-${outcome}`),
                  telemetry.isolateSpanLifecycle,
                );
                const pull = yield* Stream.toPull(stream);
                expect(yield* pull).toEqual([`terminal-${outcome}`]);
                const secondPull = yield* Effect.forkChild(pull);
                yield* Deferred.await(entered);
                yield* Fiber.interrupt(secondPull);
                return yield* Fiber.await(secondPull);
              }),
            );
            if (Exit.isSuccess(pullExit)) throw new Error("Expected telemetry interruption");
            expect(Cause.hasInterrupts(pullExit.cause)).toBe(true);
          }),
        { discard: true },
      );

      for (const outcome of ["success", "failure"] as const) {
        const span = spans.find(
          (candidate) => candidate.name === `execute_tool interrupted-${outcome}`,
        );
        expect(Object.fromEntries(span?.attributes ?? [])).toMatchObject({
          "effect_agent.tool.outcome": outcome,
        });
        if (span?.status._tag !== "Ended") throw new Error("Expected the Tool span to end");
        expect(Exit.isSuccess(span.status.exit)).toBe(outcome === "success");
        if (outcome === "failure") {
          if (Exit.isSuccess(span.status.exit)) throw new Error("Expected a failed Tool span");
          expect(Cause.findErrorOption(span.status.exit.cause)).toEqual(Option.some(failureMarker));
        }
        expect(Object.fromEntries(span.attributes)).not.toHaveProperty(
          "@effect-agent/engine/ToolSpanFailureMarker",
        );
      }
    }).pipe(Effect.provide(ToolSpanTelemetry.layer), Effect.provideService(Tracer.Tracer, tracer));
  });

  it.effect("preserves the enclosing interruption when no terminal outcome exists", () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });

    return Effect.gen(function* () {
      const telemetry = yield* ToolSpanTelemetry;
      const entered = yield* Deferred.make<void>();
      const blocked = yield* Deferred.make<void>();
      const pullExit = yield* Effect.scoped(
        Effect.gen(function* () {
          const stream = emitThenAfter(
            Effect.succeed("nonterminal-event"),
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(blocked))),
          ).pipe(Stream.withSpan("execute_tool without-outcome"), telemetry.isolateSpanLifecycle);
          const pull = yield* Stream.toPull(stream);
          expect(yield* pull).toEqual(["nonterminal-event"]);
          const secondPull = yield* Effect.forkChild(pull);
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(secondPull);
          return yield* Fiber.await(secondPull);
        }),
      );
      if (Exit.isSuccess(pullExit)) throw new Error("Expected telemetry interruption");

      const span = spans.find((candidate) => candidate.name === "execute_tool without-outcome");
      if (span?.status._tag !== "Ended" || Exit.isSuccess(span.status.exit)) {
        throw new Error("Expected the unannotated span to preserve interruption");
      }
      expect(Cause.hasInterrupts(span.status.exit.cause)).toBe(true);
    }).pipe(Effect.provide(ToolSpanTelemetry.layer), Effect.provideService(Tracer.Tracer, tracer));
  });

  it.effect("runs Toolkit handling unchanged when no current span exists", () => {
    const tracer = Tracer.make({
      span: (options) => new Tracer.NativeSpan(options),
    });
    const expectedFailure = ScheduledToolFailure.make({ message: "typed-no-parent-span" });
    let attempts = 0;

    return Effect.gen(function* () {
      const telemetry = yield* ToolSpanTelemetry;
      const success = yield* telemetry.isolateToolkitHandle(
        Effect.sync(() => {
          attempts += 1;
          return "handled-without-parent";
        }),
      );
      const failureExit = yield* telemetry
        .isolateToolkitHandle(
          Effect.suspend(() => {
            attempts += 1;
            return Effect.fail(expectedFailure);
          }),
        )
        .pipe(Effect.exit);

      expect(success).toBe("handled-without-parent");
      expect(failureFrom(failureExit)).toBe(expectedFailure);
      expect(attempts).toBe(2);
    }).pipe(Effect.provide(ToolSpanTelemetry.layer), Effect.provideService(Tracer.Tracer, tracer));
  });

  it.effect("allocates span isolation for each execution of a reusable Stream", () => {
    let isolationAllocations = 0;
    const delegateContext: NonNullable<Tracer.Tracer["context"]> = <X>(
      primitive: Tracer.EffectPrimitive<X>,
      fiber: Fiber.Fiber<unknown, unknown>,
    ): X => primitive["~effect/Effect/evaluate"](fiber);
    const instrumentedContext = new Proxy(delegateContext, {
      get(target, property, receiver) {
        if (property === "bind") {
          return (thisArgument: unknown) => {
            isolationAllocations += 1;
            return target.bind(thisArgument);
          };
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const delegate = Tracer.make({
      span: (options) => new Tracer.NativeSpan(options),
      context: instrumentedContext,
    });

    return Effect.gen(function* () {
      const telemetry = yield* ToolSpanTelemetry;
      const reusable = Stream.succeed("value").pipe(
        Stream.withSpan("reusable-isolated-stream"),
        telemetry.isolateSpanLifecycle,
      );
      const beforeExecutions = isolationAllocations;
      const [left, right] = yield* Effect.all(
        [Stream.runCollect(reusable), Stream.runCollect(reusable)],
        { concurrency: "unbounded" },
      );

      expect([...left]).toEqual(["value"]);
      expect([...right]).toEqual(["value"]);
      expect(isolationAllocations - beforeExecutions).toBe(2);
    }).pipe(
      Effect.provide(ToolSpanTelemetry.layer),
      Effect.provideService(Tracer.Tracer, delegate),
    );
  });

  it.effect("drains a queued terminal event before a merged producer interruption", () =>
    Effect.gen(function* () {
      const observed: Array<string> = [];
      const exit = yield* Stream.mergeAll(
        [emitThenAfter(Effect.succeed("terminal-event"), Effect.interrupt)],
        { concurrency: "unbounded" },
      ).pipe(
        Stream.tap((event) =>
          Effect.sync(() => {
            observed.push(event);
          }),
        ),
        Stream.runDrain,
        Effect.exit,
      );

      expect(observed).toEqual(["terminal-event"]);
      if (Exit.isSuccess(exit)) throw new Error("Expected merged producer interruption");
      expect(Cause.hasInterrupts(exit.cause)).toBe(true);
    }),
  );

  it.effect("preserves external interruption at the terminal telemetry boundary", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const blocked = yield* Deferred.make<void>();
      const fiber = yield* isolateToolDerivative(
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(blocked))),
      ).pipe(Effect.forkChild);

      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isSuccess(exit)) throw new Error("Expected terminal telemetry interruption");
      expect(Cause.hasInterrupts(exit.cause)).toBe(true);
    }),
  );

  it.effect("preserves the host tracer's final sampling decision", () => {
    const requestedSampling: Array<boolean> = [];
    const delegate = Tracer.make({
      span(options) {
        requestedSampling.push(options.sampled);
        return new Tracer.NativeSpan({ ...options, sampled: false });
      },
    });

    return Effect.gen(function* () {
      const telemetry = yield* ToolSpanTelemetry;
      const observed = yield* Stream.fromEffect(
        Effect.map(Effect.currentSpan, (span) => span.sampled),
      )
        .pipe(Stream.withSpan("sampling-decision"), telemetry.isolateSpanLifecycle)
        .pipe(Stream.runCollect);

      expect(requestedSampling).toEqual([true]);
      expect([...observed]).toEqual([false]);
    }).pipe(
      Effect.provide(ToolSpanTelemetry.layer),
      Effect.provideService(Tracer.Tracer, delegate),
    );
  });

  it.effect("preserves the host tracer context receiver", () => {
    const receiverToken = Symbol("host-tracer-receiver");
    let contextCalls = 0;
    class ReceiverAwareTracer implements Tracer.Tracer {
      readonly receiver = receiverToken;

      span(options: Parameters<Tracer.Tracer["span"]>[0]): Tracer.Span {
        return new Tracer.NativeSpan(options);
      }

      context<X>(primitive: Tracer.EffectPrimitive<X>, fiber: Fiber.Fiber<unknown, unknown>): X {
        if (this.receiver !== receiverToken) throw new Error("host tracer receiver was lost");
        contextCalls += 1;
        return primitive["~effect/Effect/evaluate"](fiber);
      }
    }
    const delegate = new ReceiverAwareTracer();
    const isolated = makeIsolatedToolTracer(delegate);
    const primitive: Tracer.EffectPrimitive<string> = {
      ["~effect/Effect/evaluate"]: () => "completed",
    };

    return Effect.withFiber((fiber) =>
      Effect.sync(() => {
        const context = isolated.tracer.context;
        if (context === undefined) throw new Error("Expected the isolated tracer context");

        expect(context(primitive, fiber)).toBe("completed");
        expect(contextCalls).toBe(1);
      }),
    );
  });

  it.effect("isolates a defecting host tracer context getter", () => {
    const contextDefect = new Error("host-tracer-context-getter-defect");
    const reports: Array<Cause.Cause<unknown>> = [];
    const delegate = Object.defineProperty(
      Tracer.make({
        span: (options) => new Tracer.NativeSpan(options),
      }),
      "context",
      {
        get: () => {
          throw contextDefect;
        },
      },
    );
    const isolated = makeIsolatedToolTracer(delegate);
    const reporter = ErrorReporter.make(({ cause }) => {
      reports.push(cause);
    });

    return Effect.gen(function* () {
      expect(isolated.tracer.context).toBeUndefined();
      yield* isolated.reportLifecycleDefects;
      expect(reports).toHaveLength(1);
      expect(reports[0]?.reasons.filter(Cause.isDieReason).map(({ defect }) => defect)).toEqual([
        contextDefect,
      ]);
    }).pipe(Effect.provide(ErrorReporter.layer([reporter])));
  });

  it.effect("preserves interruption raised while reporting span lifecycle defects", () => {
    const contextDefect = new Error("host-tracer-context-defect-before-reporter-interruption");
    const interruptor = 7331;
    const delegate = Object.defineProperty(
      Tracer.make({
        span: (options) => new Tracer.NativeSpan(options),
      }),
      "context",
      {
        get: () => {
          throw contextDefect;
        },
      },
    );
    const isolated = makeIsolatedToolTracer(delegate);
    const reporter = ErrorReporter.make(({ cause, fiber }) => {
      expect(cause.reasons).toHaveLength(1);
      expect(cause.reasons[0]?._tag).toBe("Die");
      if (cause.reasons[0]?._tag !== "Die") throw new Error("Expected lifecycle defect");
      expect(cause.reasons[0].defect).toBe(contextDefect);
      fiber.interruptUnsafe(interruptor);
    });

    return Effect.gen(function* () {
      const reportingFiber = yield* Effect.forkChild(isolated.reportLifecycleDefects);
      const exit = yield* Fiber.await(reportingFiber);
      if (Exit.isSuccess(exit)) throw new Error("Expected reporter interruption");
      expect(exit.cause.reasons).toHaveLength(1);
      expect(exit.cause.reasons[0]?._tag).toBe("Interrupt");
      if (exit.cause.reasons[0]?._tag !== "Interrupt") {
        throw new Error("Expected exact reporter interruption Cause");
      }
      expect(exit.cause.reasons[0].fiberId).toBe(interruptor);
    }).pipe(Effect.provide(ErrorReporter.layer([reporter])));
  });

  it.effect(
    "evaluates the primitive once when host tracer context defects before evaluation",
    () => {
      const contextDefect = new Error("host-tracer-context-before-evaluation-defect");
      const reports: Array<Cause.Cause<unknown>> = [];
      let evaluations = 0;
      const delegate = Tracer.make({
        span: (options) => new Tracer.NativeSpan(options),
        context: () => {
          throw contextDefect;
        },
      });
      const isolated = makeIsolatedToolTracer(delegate);
      const reporter = ErrorReporter.make(({ cause }) => {
        reports.push(cause);
      });
      const primitive: Tracer.EffectPrimitive<string> = {
        ["~effect/Effect/evaluate"]: () => {
          evaluations += 1;
          return "completed";
        },
      };

      return Effect.withFiber((fiber) =>
        Effect.gen(function* () {
          const context = isolated.tracer.context;
          if (context === undefined) throw new Error("Expected the isolated tracer context");

          expect(context(primitive, fiber)).toBe("completed");
          expect(evaluations).toBe(1);
          yield* isolated.reportLifecycleDefects;
          expect(reports).toHaveLength(1);
          expect(reports[0]?.reasons.filter(Cause.isDieReason).map(({ defect }) => defect)).toEqual(
            [contextDefect],
          );
        }),
      ).pipe(Effect.provide(ErrorReporter.layer([reporter])));
    },
  );

  it.effect(
    "returns the recorded primitive value when host tracer context defects afterward",
    () => {
      const contextDefect = new Error("host-tracer-context-after-evaluation-defect");
      const reports: Array<Cause.Cause<unknown>> = [];
      let evaluations = 0;
      const delegate = Tracer.make({
        span: (options) => new Tracer.NativeSpan(options),
        context: <X>(
          primitive: Tracer.EffectPrimitive<X>,
          fiber: Fiber.Fiber<unknown, unknown>,
        ): X => {
          primitive["~effect/Effect/evaluate"](fiber);
          throw contextDefect;
        },
      });
      const isolated = makeIsolatedToolTracer(delegate);
      const reporter = ErrorReporter.make(({ cause }) => {
        reports.push(cause);
      });
      const primitive: Tracer.EffectPrimitive<string> = {
        ["~effect/Effect/evaluate"]: () => {
          evaluations += 1;
          return "completed";
        },
      };

      return Effect.withFiber((fiber) =>
        Effect.gen(function* () {
          const context = isolated.tracer.context;
          if (context === undefined) throw new Error("Expected the isolated tracer context");

          expect(context(primitive, fiber)).toBe("completed");
          expect(evaluations).toBe(1);
          yield* isolated.reportLifecycleDefects;
          expect(reports).toHaveLength(1);
          expect(reports[0]?.reasons.filter(Cause.isDieReason).map(({ defect }) => defect)).toEqual(
            [contextDefect],
          );
        }),
      ).pipe(Effect.provide(ErrorReporter.layer([reporter])));
    },
  );

  it.effect("does not let host tracer context substitute a primitive result", () => {
    let evaluations = 0;
    const delegateContext: NonNullable<Tracer.Tracer["context"]> = <X>(
      primitive: Tracer.EffectPrimitive<X>,
      fiber: Fiber.Fiber<unknown, unknown>,
    ): X => primitive["~effect/Effect/evaluate"](fiber);
    const substitutingContext = new Proxy(delegateContext, {
      apply(target, thisArgument, argumentsList) {
        Reflect.apply(target, thisArgument, argumentsList);
        return "host-substituted-result";
      },
    });
    const isolated = makeIsolatedToolTracer(
      Tracer.make({
        span: (options) => new Tracer.NativeSpan(options),
        context: substitutingContext,
      }),
    );
    const primitive: Tracer.EffectPrimitive<string> = {
      ["~effect/Effect/evaluate"]: () => {
        evaluations += 1;
        return "engine-result";
      },
    };

    return Effect.withFiber((fiber) =>
      Effect.sync(() => {
        const context = isolated.tracer.context;
        if (context === undefined) throw new Error("Expected the isolated tracer context");

        expect(context(primitive, fiber)).toBe("engine-result");
        expect(evaluations).toBe(1);
      }),
    );
  });

  it.effect("rethrows the original primitive error once when host tracer context wraps it", () => {
    const primitiveError = new Error("primitive-evaluation-error");
    const contextDefect = new Error("host-tracer-context-wrapper-defect");
    const reports: Array<Cause.Cause<unknown>> = [];
    let evaluations = 0;
    const delegate = Tracer.make({
      span: (options) => new Tracer.NativeSpan(options),
      context: <X>(
        primitive: Tracer.EffectPrimitive<X>,
        fiber: Fiber.Fiber<unknown, unknown>,
      ): X => {
        try {
          return primitive["~effect/Effect/evaluate"](fiber);
        } catch {
          throw contextDefect;
        }
      },
    });
    const isolated = makeIsolatedToolTracer(delegate);
    const reporter = ErrorReporter.make(({ cause }) => {
      reports.push(cause);
    });
    const primitive: Tracer.EffectPrimitive<string> = {
      ["~effect/Effect/evaluate"]: () => {
        evaluations += 1;
        throw primitiveError;
      },
    };

    return Effect.withFiber((fiber) =>
      Effect.gen(function* () {
        const context = isolated.tracer.context;
        if (context === undefined) throw new Error("Expected the isolated tracer context");

        let observed: unknown;
        try {
          context(primitive, fiber);
        } catch (error) {
          observed = error;
        }
        expect(observed).toBe(primitiveError);
        expect(evaluations).toBe(1);
        yield* isolated.reportLifecycleDefects;
        expect(reports).toEqual([]);
      }),
    ).pipe(Effect.provide(ErrorReporter.layer([reporter])));
  });

  it.effect("keeps Tool success unchanged when terminal telemetry defects", () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const logs: Array<ExportedLogObservation> = [];
    const reports: Array<Cause.Cause<unknown>> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const telemetryDefect = new Error("terminal-logger-defect-must-not-reach-exported-span-or-log");
    const reporterDefect = new Error("terminal-error-reporter-defect-must-be-isolated");
    let defectiveReporterCalls = 0;
    const logger = Logger.make<unknown, void>((options) => {
      const rendered = renderedLogMessage(options.message);
      logs.push(exportedLogObservation(options));
      if (rendered === "agent tool execution completed") throw telemetryDefect;
    });
    const reporter = ErrorReporter.make(({ cause }) => {
      reports.push(cause);
      defectiveReporterCalls += 1;
      throw reporterDefect;
    });
    let handlerAttempts = 0;

    const Read = Tool.make("read_once", {
      parameters: Schema.Struct({ path: Schema.String }),
      success: Schema.String,
    });
    const tools = Toolkit.make(Read);
    const model = Model.make(
      "scripted",
      "terminal-telemetry-defect",
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
                              id: "read-once-1",
                              name: "read_once",
                              params: { path: "/private/tool-input" },
                              providerExecuted: false,
                            },
                            { type: "finish", reason: "tool-calls", usage },
                          ]
                        : finalParts('{"answer":"done"}'),
                    ),
                  ),
                ),
              ),
          });
        }),
      ),
    );
    const definition = Agent.define("terminal-telemetry-defect", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Read once, then answer.",
      toolkit: tools,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    });

    return Effect.gen(function* () {
      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "Read the file",
      }).pipe(
        Stream.runCollect,
        Effect.provide(
          tools.toLayer({
            read_once: () =>
              Effect.sync(() => {
                handlerAttempts += 1;
                return "original-success";
              }),
          }),
        ),
        Effect.exit,
      );

      if (Exit.isFailure(exit)) throw new Error("Terminal telemetry changed Tool success");
      expect(handlerAttempts).toBe(1);
      expect(exit.value.filter((event) => event._tag === "ToolCallSucceeded")).toEqual([
        expect.objectContaining({
          toolCallId: "read-once-1",
          toolName: "read_once",
          result: "original-success",
          providerExecuted: false,
        }),
      ]);
      expect(exit.value.filter((event) => event._tag === "RunCompleted")).toHaveLength(1);

      expect(reports).toHaveLength(1);
      expect(defectiveReporterCalls).toBe(1);
      expect(reports[0]?.reasons.filter(Cause.isDieReason).map(({ defect }) => defect)).toEqual([
        telemetryDefect,
      ]);

      const span = spans.find((candidate) => candidate.name === "execute_tool read_once");
      expect(Object.fromEntries(span?.attributes ?? [])).toMatchObject({
        "effect_agent.tool.outcome": "success",
      });
      if (span?.status._tag !== "Ended") throw new Error("Expected the Tool span to end");
      expect(Exit.isSuccess(span.status.exit)).toBe(true);

      const exportedValues = reachableTelemetryValues({
        spans: spans.map(exportedSpanObservation),
        logs,
      });
      expect(exportedValues).not.toContain(telemetryDefect);
      expect(exportedValues).not.toContain(reporterDefect);
      expect(exportedValues.filter((value) => typeof value === "string").join("\n")).not.toContain(
        telemetryDefect.message,
      );
      expect(exportedValues.filter((value) => typeof value === "string").join("\n")).not.toContain(
        reporterDefect.message,
      );
      expect(exportedValues.filter((value) => typeof value === "string").join("\n")).not.toContain(
        "/private/tool-input",
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.effect(Tracer.Tracer, Effect.succeed(tracer)),
          Logger.layer([logger]),
          ErrorReporter.layer([reporter]),
        ),
      ),
    );
  });

  it.effect(
    "keeps completed Tool results when canonical span create, wrap, or close defects",
    () => {
      const spans: Array<Tracer.NativeSpan> = [];
      const reports: Array<Cause.Cause<unknown>> = [];
      const createDefect = new Error("canonical-span-create-defect");
      const wrapDefect = new Error("canonical-span-wrap-defect");
      const wrapCloseDefect = new Error("canonical-span-wrap-close-defect");
      const closeDefect = new Error("canonical-span-close-defect");
      class CloseDefectSpan extends Tracer.NativeSpan {
        override end(): void {
          throw closeDefect;
        }
      }
      const tracer = Tracer.make({
        span(options) {
          if (options.name === "execute_tool create_safe") throw createDefect;
          if (options.name === "execute_tool wrap_safe") {
            const allocated = new Tracer.NativeSpan(options);
            spans.push(allocated);
            return new Proxy(allocated, {
              get(target, property) {
                if (property === "sampled") throw wrapDefect;
                if (property === "end") {
                  return (endTime: bigint, exit: Exit.Exit<unknown, unknown>): void => {
                    target.end(endTime, exit);
                    throw wrapCloseDefect;
                  };
                }
                const value = Reflect.get(target, property, target);
                return typeof value === "function" ? value.bind(target) : value;
              },
            });
          }
          const span =
            options.name === "execute_tool close_safe"
              ? new CloseDefectSpan(options)
              : new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      const reporter = ErrorReporter.make(({ cause }) => {
        reports.push(cause);
      });
      const CreateSafe = Tool.make("create_safe", {
        parameters: Schema.Struct({ value: Schema.String }),
        success: Schema.String,
      });
      const WrapSafe = Tool.make("wrap_safe", {
        parameters: Schema.Struct({ value: Schema.String }),
        success: Schema.String,
      });
      const CloseSafe = Tool.make("close_safe", {
        parameters: Schema.Struct({ value: Schema.String }),
        success: Schema.String,
      });
      const tools = Toolkit.make(CreateSafe, WrapSafe, CloseSafe);
      const model = Model.make(
        "scripted",
        "span-lifecycle-defects",
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
                                id: "span-create-1",
                                name: "create_safe",
                                params: { value: "create" },
                                providerExecuted: false,
                              },
                              {
                                type: "tool-call",
                                id: "span-wrap-1",
                                name: "wrap_safe",
                                params: { value: "wrap" },
                                providerExecuted: false,
                              },
                              {
                                type: "tool-call",
                                id: "span-close-1",
                                name: "close_safe",
                                params: { value: "close" },
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]
                          : finalParts('{"answer":"done"}'),
                      ),
                    ),
                  ),
                ),
            });
          }),
        ),
      );
      const definition = Agent.define("span-lifecycle-defects", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Call all three Tools, then answer.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 3,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      let createAttempts = 0;
      let wrapAttempts = 0;
      let closeAttempts = 0;

      return Effect.gen(function* () {
        const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "exercise span lifecycle",
        }).pipe(
          Stream.runCollect,
          Effect.provide(
            tools.toLayer({
              create_safe: () =>
                Effect.sync(() => {
                  createAttempts += 1;
                  return "create-result";
                }),
              wrap_safe: () =>
                Effect.sync(() => {
                  wrapAttempts += 1;
                  return "wrap-result";
                }),
              close_safe: () =>
                Effect.sync(() => {
                  closeAttempts += 1;
                  return "close-result";
                }),
            }),
          ),
          Effect.exit,
        );

        if (Exit.isFailure(exit)) throw new Error("Span lifecycle telemetry changed Tool success");
        expect(createAttempts).toBe(1);
        expect(wrapAttempts).toBe(1);
        expect(closeAttempts).toBe(1);
        expect(exit.value.filter((event) => event._tag === "ToolCallSucceeded")).toEqual([
          expect.objectContaining({
            toolCallId: "span-create-1",
            toolName: "create_safe",
            result: "create-result",
          }),
          expect.objectContaining({
            toolCallId: "span-wrap-1",
            toolName: "wrap_safe",
            result: "wrap-result",
          }),
          expect.objectContaining({
            toolCallId: "span-close-1",
            toolName: "close_safe",
            result: "close-result",
          }),
        ]);
        expect(exit.value.filter((event) => event._tag === "RunCompleted")).toHaveLength(1);
        expect(
          reports.flatMap((cause) =>
            cause.reasons.filter(Cause.isDieReason).map(({ defect }) => defect),
          ),
        ).toEqual([createDefect, wrapDefect, wrapCloseDefect, closeDefect]);
        expect(spans.some((span) => span.name === "execute_tool create_safe")).toBe(false);
        const wrapSpan = spans.find((span) => span.name === "execute_tool wrap_safe");
        expect(wrapSpan?.status._tag).toBe("Ended");
        if (wrapSpan?.status._tag !== "Ended" || !Exit.isFailure(wrapSpan.status.exit)) {
          throw new Error("Expected the allocated wrapper-failure span to close failed");
        }
        expect(spans.find((span) => span.name === "execute_tool close_safe")?.status._tag).toBe(
          "Started",
        );

        const exportedValues = reachableTelemetryValues(spans.map(exportedSpanObservation));
        expect(exportedValues).not.toContain(createDefect);
        expect(exportedValues).not.toContain(wrapDefect);
        expect(exportedValues).not.toContain(wrapCloseDefect);
        expect(exportedValues).not.toContain(closeDefect);
        const exportedStrings = exportedValues
          .filter((value) => typeof value === "string")
          .join("\n");
        expect(exportedStrings).not.toContain(createDefect.message);
        expect(exportedStrings).not.toContain(wrapDefect.message);
        expect(exportedStrings).not.toContain(wrapCloseDefect.message);
        expect(exportedStrings).not.toContain(closeDefect.message);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.effect(Tracer.Tracer, Effect.succeed(tracer)),
            ErrorReporter.layer([reporter]),
          ),
        ),
      );
    },
  );

  it.effect("overrides apparent Tool success when the handler stream later fails", () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const logs: Array<ExportedLogObservation> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const logger = Logger.make<unknown, void>((options) => {
      logs.push(exportedLogObservation(options));
    });
    const postTerminalSecret = "post-terminal-handler-secret";
    const postTerminalFailure = AiError.make({
      module: "Toolkit",
      method: "lookup.handle",
      reason: AiError.UnknownError.make({ description: postTerminalSecret }),
    });
    const postTerminalInterruptor = 4242;
    const Lookup = Tool.make("lookup", {
      parameters: Schema.Struct({ query: Schema.String }),
      success: Schema.String,
    });
    const tools = Toolkit.make(Lookup);
    const anomalousRuntime = Effect.map(
      tools,
      (native) =>
        ({
          tools: native.tools,
          handle: <Name extends keyof typeof tools.tools>(
            name: Name,
            parameters: Tool.Parameters<(typeof tools.tools)[Name]>,
            toolCallId?: string,
          ) =>
            native.handle(name, parameters, toolCallId).pipe(
              Effect.map((results) =>
                results.pipe(
                  // Defects and interruption have error channel `never`, so this adversarial
                  // composed Cause works for every generic Tool without widening HandlerError.
                  Stream.concat(
                    Stream.failCause(
                      Cause.combine(
                        Cause.die(postTerminalFailure),
                        Cause.interrupt(postTerminalInterruptor),
                      ),
                    ),
                  ),
                ),
              ),
            ),
        }) satisfies Toolkit.WithHandler<typeof tools.tools>,
    );
    // Test-only Effect AI Toolkit seam: runtime execution consumes the Toolkit Effect and
    // `tools`; handler Layer construction stays on the native `tools` value below.
    const anomalousTools = Object.assign(anomalousRuntime, {
      "~effect/ai/Toolkit": "~effect/ai/Toolkit" as const,
      tools: tools.tools,
    }) as Toolkit.Toolkit<typeof tools.tools>;
    const model = modelFromParts([
      {
        type: "tool-call",
        id: "post-terminal-1",
        name: "lookup",
        params: { query: "status" },
        providerExecuted: false,
      },
      { type: "finish", reason: "tool-calls", usage },
    ]);
    const definition = Agent.define("post-terminal-tool-failure", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Look up the status.",
      toolkit: anomalousTools,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    });
    const finalized = Ref.makeUnsafe(0);
    const toolLayer = tools.toLayer({
      lookup: () =>
        Effect.succeed("apparently successful").pipe(
          Effect.ensuring(Ref.update(finalized, (count) => count + 1)),
        ),
    });

    return Effect.gen(function* () {
      const observed = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "status",
      }).pipe(
        Stream.tap((event) => Ref.update(observed, (events) => [...events, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isSuccess(exit)) {
        throw new Error("Expected the post-terminal Tool failure to escape as a defect");
      }
      expect(
        exit.cause.reasons
          .filter(Cause.isDieReason)
          .map((reason) => reason.defect)
          .includes(postTerminalFailure),
      ).toBe(true);
      expect(Cause.interruptors(exit.cause).has(postTerminalInterruptor)).toBe(true);
      expect(exit.cause.reasons.filter(Cause.isFailReason)).toEqual([]);
      expect(yield* Ref.get(finalized)).toBe(1);
      const terminalEvents = (yield* Ref.get(observed)).filter(
        (event) => event._tag === "ToolCallSucceeded" || event._tag === "ToolCallFailed",
      );
      expect(terminalEvents).toEqual([
        expect.objectContaining({
          _tag: "ToolCallFailed",
          toolCallId: "post-terminal-1",
          toolName: "lookup",
          providerExecuted: false,
        }),
      ]);
      expect(terminalEvents.some((event) => event._tag === "ToolCallSucceeded")).toBe(false);
      const span = spans.find((candidate) => candidate.name === "execute_tool lookup");
      expect(span).toBeDefined();
      expect(Object.fromEntries(span?.attributes ?? [])).toMatchObject({
        "effect_agent.tool.outcome": "failure",
      });
      if (span?.status._tag !== "Ended" || !Exit.isFailure(span.status.exit)) {
        throw new Error("Expected the post-terminal Tool span to end failed");
      }
      const exportedValues = reachableTelemetryValues({
        spans: [exportedSpanObservation(span)],
        logs,
      });
      expect(exportedValues).not.toContain(postTerminalFailure);
      expect(exportedValues.filter((value) => typeof value === "string").join("\n")).not.toContain(
        postTerminalSecret,
      );

      const terminalLogs = logs.filter((entry) =>
        ["agent tool execution completed", "agent tool execution failed"].includes(
          renderedLogMessage(entry.message),
        ),
      );
      expect(
        terminalLogs.map((entry) => ({
          message: renderedLogMessage(entry.message),
          outcome: entry.annotations.toolOutcome,
        })),
      ).toEqual([{ message: "agent tool execution failed", outcome: "failure" }]);
    }).pipe(Effect.provideService(Tracer.Tracer, tracer), Effect.provide(Logger.layer([logger])));
  });

  it("preserves a marker-free Tool Cause by referential identity", () => {
    const failureReason = Cause.makeFailReason(new Error("ordinary tool failure"));
    const defectReason = Cause.makeDieReason(new Error("ordinary tool defect"));
    const interruptReason = Cause.makeInterruptReason(4242);
    const original = Cause.fromReasons([failureReason, defectReason, interruptReason]);

    const { found, restored } = restoreToolSpanFailureCause(
      original,
      ToolSpanFailure.marker(),
      undefined,
    );

    expect(found).toBe(false);
    expect(restored).toBe(original);
    expect(restored.reasons).toEqual([failureReason, defectReason, interruptReason]);
  });

  it("does not consume an independently constructed Tool span failure", () => {
    const privateMarker = ToolSpanFailure.marker();
    const handlerFailure = ToolSpanFailure.marker();
    const handlerCause = Cause.fail(handlerFailure);

    const stripped = stripToolSpanFailures(handlerCause, privateMarker);

    expect(privateMarker).toBeInstanceOf(ToolSpanFailure);
    expect(handlerFailure).toBeInstanceOf(ToolSpanFailure);
    expect(handlerFailure).not.toBe(privateMarker);
    expect(stripped.found).toBe(false);
    expect(stripped.residual).toBe(handlerCause);
    expect(stripped.residual.reasons.filter(Cause.isFailReason).map(({ error }) => error)).toEqual([
      handlerFailure,
    ]);
  });

  it("removes only Tool span marker Reasons while preserving residual identity and order", () => {
    const originalFailure = new Error("original tool cause");
    const originalFailureReason = Cause.makeFailReason(originalFailure).annotate(
      Context.make(TestCauseSource, "handler"),
    );
    const originalInterruptReason = Cause.makeInterruptReason(4242).annotate(
      Context.make(TestCauseSource, "interrupt"),
    );
    const residualDefect = new Error("span wrapper residual");
    const residualFailure = new Error("span wrapper typed residual");
    const residualDefectReason = Cause.makeDieReason(residualDefect).annotate(
      Context.make(TestCauseSource, "span-finalizer"),
    );
    const residualFailureReason = Cause.makeFailReason(residualFailure).annotate(
      Context.make(TestCauseSource, "span-wrapper"),
    );
    const marker = ToolSpanFailure.marker();
    const firstMarker = Cause.makeFailReason(marker);
    const secondMarker = Cause.makeFailReason(marker);
    const foreignMarker = Cause.makeFailReason(ToolSpanFailure.marker()).annotate(
      Context.make(TestCauseSource, "handler-lookalike"),
    );
    const original = Cause.fromReasons([originalFailureReason, originalInterruptReason]);
    const observed = Cause.fromReasons<Error | ToolSpanFailure>([
      residualDefectReason,
      firstMarker,
      residualFailureReason,
      foreignMarker,
      secondMarker,
    ]);

    const stripped = stripToolSpanFailures<Error | ToolSpanFailure>(observed, marker);
    expect(stripped.found).toBe(true);
    expect(stripped.residual.reasons).toEqual([
      residualDefectReason,
      residualFailureReason,
      foreignMarker,
    ]);
    expect(stripped.residual.reasons[0]).toBe(residualDefectReason);
    expect(stripped.residual.reasons[1]).toBe(residualFailureReason);
    expect(stripped.residual.reasons[2]).toBe(foreignMarker);
    expect([...stripped.residual.reasons[0]!.annotations.values()]).toContain("span-finalizer");
    expect([...stripped.residual.reasons[1]!.annotations.values()]).toContain("span-wrapper");
    expect([...stripped.residual.reasons[2]!.annotations.values()]).toContain("handler-lookalike");

    const { found, restored } = restoreToolSpanFailureCause(observed, marker, original);

    expect(found).toBe(true);
    expect(restored.reasons).toEqual([
      originalFailureReason,
      originalInterruptReason,
      residualDefectReason,
      residualFailureReason,
      foreignMarker,
    ]);
    expect(restored.reasons[0]).toBe(originalFailureReason);
    expect(restored.reasons[1]).toBe(originalInterruptReason);
    expect(restored.reasons[2]).toBe(residualDefectReason);
    expect(restored.reasons[3]).toBe(residualFailureReason);
    expect(restored.reasons[4]).toBe(foreignMarker);
    expect(restored.reasons.filter(Cause.isFailReason).some(({ error }) => error === marker)).toBe(
      false,
    );
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

  it("AgentResultSchema rejects a disposition on budget exhaustion", () => {
    const Result = AgentResultSchema(Schema.Struct({ answer: Schema.String }));
    const ordinary = {
      output: { answer: "done" },
      conversationId: "conversation-1",
      runId: "run-1",
      turns: 1,
      finishReason: "completed",
      runDisposition: "application-complete",
    } as const;
    const budgetExhausted = {
      ...ordinary,
      finishReason: "budget-exhausted",
      exhausted: "turns",
    } as const;
    const budgetWithoutDisposition = {
      output: ordinary.output,
      conversationId: ordinary.conversationId,
      runId: ordinary.runId,
      turns: ordinary.turns,
      finishReason: budgetExhausted.finishReason,
      exhausted: budgetExhausted.exhausted,
    } as const;

    expect(Schema.decodeUnknownSync(Result)(ordinary).runDisposition).toBe("application-complete");
    expect(Schema.decodeUnknownSync(Result)(budgetWithoutDisposition).finishReason).toBe(
      "budget-exhausted",
    );
    expect(() => Schema.decodeUnknownSync(Result)(budgetExhausted)).toThrow(
      /runDisposition only when finishReason is not budget-exhausted/,
    );
  });

  it.effect("RUN-029 validates and exposes an application run disposition", () => {
    const RunDisposition = Schema.String.check(
      Schema.makeFilter((value) =>
        value === "application-complete" ? undefined : `Rejected disposition: ${value}`,
      ),
    );
    const definition = Agent.define("run-disposition", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({
        answer: Schema.String,
        runDisposition: Schema.optionalKey(Schema.String),
      }),
      instructions: "Answer as JSON.",
      toolkit: Toolkit.empty,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
      runDisposition: {
        schema: RunDisposition,
        fromOutput: (output) => output.runDisposition,
      },
    });

    return Effect.gen(function* () {
      const valid = Agent.withModel(
        definition,
        modelFromParts(finalParts('{"answer":"done","runDisposition":"application-complete"}')),
      );
      const events = yield* AgentRuntime.stream(valid, { question: "done?" }).pipe(
        Stream.runCollect,
      );
      const result = yield* AgentRuntime.run(valid, { question: "done?" });

      expect(events.at(-1)).toMatchObject({
        _tag: "RunCompleted",
        runDisposition: "application-complete",
      });
      expect(result.runDisposition).toBe("application-complete");

      const secret = "sensitive-run-disposition-must-not-enter-events";
      const invalidDefinition = Agent.define("invalid-run-disposition", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer as JSON.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
        runDisposition: {
          schema: RunDisposition,
          fromOutput: () => secret,
        },
      });
      const invalid = Agent.withModel(
        invalidDefinition,
        modelFromParts(finalParts('{"answer":"done"}')),
      );
      const observed: Array<RunEvent> = [];
      const invalidExit = yield* AgentRuntime.stream(invalid, { question: "done?" }).pipe(
        Stream.tap((event) =>
          Effect.sync(() => {
            observed.push(event);
          }),
        ),
        Stream.runDrain,
        Effect.exit,
      );
      const failure = failureFrom(invalidExit);
      const isRunDispositionError = Schema.is(AgentRunDispositionError)(failure);
      expect(isRunDispositionError).toBe(true);
      if (!isRunDispositionError) {
        throw new Error("Expected AgentRunDispositionError");
      }
      expect(failure.message).toBe("Run disposition failed Schema encoding");
      expect(failure.message).not.toContain(secret);
      expect(String(failure.cause)).toContain(secret);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunFailed",
        errorTag: "AgentRunDispositionError",
        message: "Run disposition failed Schema encoding",
      });
      expect(JSON.stringify(observed)).not.toContain(secret);
    });
  });

  it.effect("keeps a thrown run-disposition selector cause out of canonical events", () => {
    const RunDisposition = Schema.Literal("application-complete");
    const secret = "selector-secret-must-not-enter-events";
    const structuredCause = {
      _tag: "SelectorDependencyFailure",
      code: "E_SELECTOR",
      detail: secret,
    } as const;
    const selectorFailure = new Error(`selector failed with ${secret}`, {
      cause: structuredCause,
    });
    selectorFailure.name = "SelectorFailure";
    const definition = Agent.define("throwing-run-disposition", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Answer as JSON.",
      toolkit: Toolkit.empty,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
      runDisposition: {
        schema: RunDisposition,
        fromOutput: () => {
          throw selectorFailure;
        },
      },
    });
    const agent = Agent.withModel(definition, modelFromParts(finalParts('{"answer":"done"}')));

    return Effect.gen(function* () {
      const observed: Array<RunEvent> = [];
      const exit = yield* AgentRuntime.stream(agent, { question: "done?" }).pipe(
        Stream.tap((event) =>
          Effect.sync(() => {
            observed.push(event);
          }),
        ),
        Stream.runDrain,
        Effect.exit,
      );
      const failure = failureFrom(exit);

      const isRunDispositionError = Schema.is(AgentRunDispositionError)(failure);
      expect(isRunDispositionError).toBe(true);
      if (!isRunDispositionError) {
        throw new Error("Expected AgentRunDispositionError");
      }
      expect(failure.message).toBe("Run disposition selector failed");
      expect(failure.message).not.toContain(secret);
      expect(failure.cause).toBe(selectorFailure);
      expect(selectorFailure.name).toBe("SelectorFailure");
      expect(selectorFailure.stack).toContain("SelectorFailure");
      expect(selectorFailure.cause).toBe(structuredCause);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunFailed",
        errorTag: "AgentRunDispositionError",
        message: "Run disposition selector failed",
      });
      expect(observed.some((event) => event._tag === "RunCompleted")).toBe(false);
      expect(JSON.stringify(observed)).not.toContain(secret);
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
      expect(input.message).toContain("Expected string");
      expect(input.message).toContain('at ["question"]');
      expect(input.message).not.toContain("got 42");
      expect(output).toBeInstanceOf(AgentOutputError);
      expect(output.message).toContain("Expected string");
      expect(output.message).toContain('at ["answer"]');
      expect(output.message).not.toContain("got 42");
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

      expect(errorMessageForTest(failure)).toContain("Expected number");
      expect(errorMessageForTest(failure)).toContain('at [1]["params"]["value"]');
      expect(errorMessageForTest(failure)).not.toContain("not-an-int");
      expect(yield* Ref.get(starts)).toBe(0);
      expect(observed.some((event) => event._tag === "ToolCallStarted")).toBe(false);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
    }),
  );

  it.effect("preserves transformed parameters across the native Toolkit handler boundary", () => {
    let handlerParameter: unknown;
    let authorizationCall: RunToolAuthorizationRequest["call"] | undefined;
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

    return AgentRuntime.run(
      Agent.withModel(definition, model),
      { question: "increment" },
      {
        toolAuthorization: {
          authorize: (request) =>
            Effect.sync(() => {
              authorizationCall = request.call;
              return { _tag: "allowed" } as const;
            }),
        },
      },
    ).pipe(
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
          expect(authorizationCall?.parameters).toEqual({ value: "41" });
          expect(handlerParameter).toBe(41);
          expect(promptResult).toEqual({ value: 42 });
        }),
      ),
    );
  });

  it.effect("emits Started and Failed before propagating the concrete typed Tool failure", () =>
    Effect.gen(function* () {
      const spans: Array<Tracer.NativeSpan> = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
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
      const failureSecret = "typed-scheduled-failure-must-not-be-exported";
      const expected = ScheduledToolFailure.make({ message: failureSecret });
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
        Effect.provideService(Tracer.Tracer, tracer),
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
      const span = spans.find((candidate) => candidate.name === "execute_tool fail");
      expect(span?.attributes.get("effect_agent.tool.outcome")).toBe("failure");
      expect(span?.status._tag).toBe("Ended");
      if (span?.status._tag !== "Ended" || !Exit.isFailure(span.status.exit)) {
        throw new Error("Expected the typed Tool failure to close its span with a failed Exit");
      }
      const exportedValues = reachableTelemetryValues(exportedSpanObservation(span));
      expect(exportedValues).not.toContain(expected);
      expect(exportedValues.filter((value) => typeof value === "string").join("\n")).not.toContain(
        failureSecret,
      );
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

  it.effect("stamps staged provider results only in their actual emission order", () =>
    Effect.gen(function* () {
      const parts: ReadonlyArray<Response.StreamPartEncoded> = [
        {
          type: "tool-call",
          id: "hosted-sequence",
          name: "HostedSearch",
          params: { query: "sequence" },
          providerExecuted: true,
        },
        {
          type: "tool-result",
          id: "hosted-sequence",
          name: "HostedSearch",
          result: { status: "searching" },
          isFailure: false,
          providerExecuted: true,
          preliminary: true,
        },
        { type: "text-start", id: "hosted-answer" },
        {
          type: "text-delta",
          id: "hosted-answer",
          delta: '{"answer":"sequence-safe"}',
        },
        { type: "text-end", id: "hosted-answer" },
        {
          type: "tool-result",
          id: "hosted-sequence",
          name: "HostedSearch",
          result: { status: "complete" },
          isFailure: false,
          providerExecuted: true,
        },
        { type: "finish", reason: "stop", usage },
      ];

      const events = yield* AgentRuntime.stream(
        Agent.withModel(hostedDefinition, modelFromParts(parts)),
        { question: "sequence" },
      ).pipe(Stream.runCollect);
      const observed = [...events];

      expect(observed.map(({ sequence }) => sequence)).toEqual(
        Array.from({ length: observed.length }, (_, sequence) => sequence),
      );
      expect(observed.map(({ _tag }) => _tag)).toEqual([
        "RunStarted",
        "TurnStarted",
        "ModelStarted",
        "ToolCallDeclared",
        "TextDelta",
        "ToolProgress",
        "ToolCallSucceeded",
        "TurnCompleted",
        // The single-Turn policy sits at 100% of maxTurns, so the one-shot
        // RUN-025 advisory fires before settlement.
        "BudgetWarning",
        "RunCompleted",
      ]);
    }),
  );

  it.effect("bounds progress, terminal, and Turn-completion staged provider events together", () =>
    Effect.gen(function* () {
      const preliminaryResults: Array<Response.StreamPartEncoded> = Array.from(
        { length: 255 },
        (_, index) => ({
          type: "tool-result" as const,
          id: "hosted-count-bound",
          name: "HostedSearch",
          result: { status: String(index) },
          isFailure: false,
          providerExecuted: true,
          preliminary: true,
        }),
      );
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const exit = yield* AgentRuntime.stream(
        Agent.withModel(
          hostedDefinition,
          modelFromParts([
            {
              type: "tool-call",
              id: "hosted-count-bound",
              name: "HostedSearch",
              params: { query: "count" },
              providerExecuted: true,
            },
            ...preliminaryResults,
            {
              type: "tool-result",
              id: "hosted-count-bound",
              name: "HostedSearch",
              result: { status: "complete" },
              isFailure: false,
              providerExecuted: true,
            },
            { type: "finish", reason: "stop", usage },
          ]),
        ),
        { question: "count" },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("256-event staged provider event limit");
      expect(observed.filter((event) => event._tag === "ToolProgress")).toHaveLength(0);
      expect(observed.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(0);
      expect(observed.filter((event) => event._tag === "TurnCompleted")).toHaveLength(0);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
    }),
  );

  it.effect("bounds the encoded bytes of staged provider results", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const exit = yield* AgentRuntime.stream(
        Agent.withModel(
          hostedDefinition,
          modelFromParts([
            {
              type: "tool-call",
              id: "hosted-byte-bound",
              name: "HostedSearch",
              params: { query: "bytes" },
              providerExecuted: true,
            },
            {
              type: "tool-result",
              id: "hosted-byte-bound",
              name: "HostedSearch",
              result: { status: "x".repeat(1024 * 1024) },
              isFailure: false,
              providerExecuted: true,
              preliminary: true,
            },
          ]),
        ),
        { question: "bytes" },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("1048576-byte staged provider event limit");
      expect(observed.filter((event) => event._tag === "ToolProgress")).toHaveLength(0);
      expect(observed.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(0);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
    }),
  );

  it.effect("bounds every decoded model response by part count and retained bytes", () =>
    Effect.gen(function* () {
      const metadataExit = yield* AgentRuntime.stream(
        makeAgent([
          {
            type: "response-metadata",
            id: "response-1",
            modelId: "scripted",
            timestamp: "2026-01-01T00:00:00.000Z",
            request: {
              method: "POST",
              url: "https://example.invalid/responses",
              urlParams: [],
              headers: { authorization: Redacted.make("secret") },
            },
          },
          ...finalParts('{"answer":"owned metadata"}'),
        ]),
        { question: "metadata" },
      ).pipe(Stream.runDrain, Effect.exit);
      expect(Exit.isSuccess(metadataExit)).toBe(true);

      const countEvents = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const countExit = yield* AgentRuntime.stream(
        makeAgent(finalParts('{"answer":"count"}')),
        { question: "count" },
        { bufferLimits: { maxModelResponseParts: 2 } },
      ).pipe(
        Stream.tap((event) => Ref.update(countEvents, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const countFailure = failureFrom(countExit);
      expect(countFailure).toBeInstanceOf(ModelProtocolError);
      expect(countFailure.message).toContain("2-part response limit");
      expect((yield* Ref.get(countEvents)).at(-1)?._tag).toBe("RunFailed");

      const byteEvents = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const byteExit = yield* AgentRuntime.stream(
        makeAgent(finalParts(`{"answer":"${"x".repeat(4_096)}"}`)),
        { question: "bytes" },
        { bufferLimits: { maxModelResponseBytes: 1_024 } },
      ).pipe(
        Stream.tap((event) => Ref.update(byteEvents, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const byteFailure = failureFrom(byteExit);
      expect(byteFailure).toBeInstanceOf(ModelProtocolError);
      expect(byteFailure.message).toContain("1024-byte retained response limit");
      expect((yield* Ref.get(byteEvents)).at(-1)?._tag).toBe("RunFailed");
    }),
  );

  it.effect("reserves one bounded Run event slot for a typed terminal failure", () =>
    Effect.gen(function* () {
      const observed = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const exit = yield* AgentRuntime.stream(
        makeAgent(finalParts('{"answer":"event bound"}')),
        { question: "events" },
        { bufferLimits: { maxRunEvents: 4 } },
      ).pipe(
        Stream.tap((event) => Ref.update(observed, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const events = yield* Ref.get(observed);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("4-event buffer limit");
      expect(events).toHaveLength(4);
      expect(events.map(({ sequence }) => sequence)).toEqual([0, 1, 2, 3]);
      expect(events.at(-1)).toMatchObject({
        _tag: "RunFailed",
        errorTag: "ModelProtocolError",
        message: "Run exceeded the 4-event buffer limit",
      });
    }),
  );

  it.effect("rejects unmeasurable and oversized provider failures without reading accessors", () =>
    Effect.gen(function* () {
      let hostileReads = 0;
      const hostile = Object.create(null) as Record<PropertyKey, unknown>;
      Object.defineProperty(hostile, "message", {
        enumerable: true,
        get: () => {
          hostileReads += 1;
          throw new Error("message getter must not escape");
        },
      });
      Object.defineProperty(hostile, "_tag", {
        enumerable: true,
        get: () => {
          hostileReads += 1;
          throw new Error("tag getter must not escape");
        },
      });
      Object.defineProperty(hostile, Symbol.toPrimitive, {
        value: () => {
          throw new Error("coercion must not run");
        },
      });

      const hostileEvents = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const hostileExit = yield* AgentRuntime.stream(
        makeAgent([{ type: "error", error: hostile }]),
        { question: "hostile" },
      ).pipe(
        Stream.tap((event) => Ref.update(hostileEvents, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const hostileFailure = failureFrom(hostileExit);
      expect(hostileFailure).toBeInstanceOf(ModelProtocolError);
      expect(hostileFailure.message).toContain("retained response limit");
      expect((yield* Ref.get(hostileEvents)).at(-1)).toMatchObject({
        _tag: "RunFailed",
      });
      expect(hostileReads).toBe(0);

      const diagnosticFailure = Object.create(null) as Record<PropertyKey, unknown>;
      Object.defineProperty(diagnosticFailure, "message", {
        get: () => {
          hostileReads += 1;
          throw new Error("diagnostic message getter must not run");
        },
      });
      Object.defineProperty(diagnosticFailure, "_tag", {
        get: () => {
          hostileReads += 1;
          throw new Error("diagnostic tag getter must not run");
        },
      });
      expect(errorMessage(diagnosticFailure)).toBe("Unknown error");
      expect(errorTag(diagnosticFailure)).toBe("UnknownError");
      expect(hostileReads).toBe(0);

      const oversizedEvents = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const oversizedExit = yield* AgentRuntime.stream(
        makeAgent([{ type: "error", error: { message: "x".repeat(8_192) } }]),
        { question: "oversized" },
      ).pipe(
        Stream.tap((event) => Ref.update(oversizedEvents, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const oversizedFailure = failureFrom(oversizedExit);
      expect(oversizedFailure).toBeInstanceOf(ModelProtocolError);
      const terminal = (yield* Ref.get(oversizedEvents)).at(-1);
      expect(terminal?._tag).toBe("RunFailed");
      if (terminal?._tag === "RunFailed") {
        expect(terminal.message.length).toBeLessThanOrEqual(4_096);
      }
    }),
  );

  it.effect("preserves a canonical encoder defect as a defect", () =>
    Effect.gen(function* () {
      const encoderDefect = new Error("response encoder defect");
      const target: Response.StreamPartEncoded = { type: "text-start", id: "answer" };
      const defectingPart = new Proxy(target, {
        get: (part, key, receiver) => {
          if (key === "id") throw encoderDefect;
          return Reflect.get(part, key, receiver);
        },
      });
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const exit = yield* AgentRuntime.stream(makeAgent([defectingPart]), {
        question: "defect",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isSuccess(exit)) throw new Error("Expected the encoder defect to fail the Run");
      expect(Cause.squash(exit.cause)).toBe(encoderDefect);
      expect((yield* Ref.get(events)).some((event) => event._tag === "RunFailed")).toBe(false);
    }),
  );

  it("normalizes one bounded JSON snapshot without invoking dynamic serialization behavior", () => {
    let suffixReads = 0;
    const payload: Array<unknown> = ["x".repeat(32)];
    Object.defineProperty(payload, 1, {
      enumerable: true,
      get: () => {
        suffixReads += 1;
        throw new Error("the rejected suffix must not be traversed");
      },
    });

    expect(boundedJsonSnapshot(payload, 8)).toBeUndefined();
    expect(suffixReads).toBe(0);

    let toJsonCalls = 0;
    const inheritedToJson = Object.create({
      toJSON: () => {
        toJsonCalls += 1;
        return "x".repeat(2_000_000);
      },
    });
    inheritedToJson.safe = "small";
    expect(boundedJsonSnapshot(inheritedToJson, 128)).toBeUndefined();

    const ownToJson = { safe: "small" };
    Object.defineProperty(ownToJson, "toJSON", {
      value: () => {
        toJsonCalls += 1;
        return "x".repeat(2_000_000);
      },
    });
    expect(boundedJsonSnapshot(ownToJson, 128)).toBeUndefined();

    let accessorReads = 0;
    const accessorBacked = {};
    Object.defineProperty(accessorBacked, "result", {
      enumerable: true,
      get: () => {
        accessorReads += 1;
        return accessorReads === 1 ? "small" : "x".repeat(2_000_000);
      },
    });
    expect(boundedJsonSnapshot(accessorBacked, 128)).toBeUndefined();
    expect(accessorReads).toBe(0);
    expect(toJsonCalls).toBe(0);

    let proxyReads = 0;
    const proxyTarget = { result: "small" };
    const dynamicProxy = new Proxy(proxyTarget, {
      get: (target, key, receiver) => {
        proxyReads += 1;
        return key === "result" ? "x".repeat(2_000_000) : Reflect.get(target, key, receiver);
      },
    });
    const proxySnapshot = boundedJsonSnapshot(dynamicProxy, 128);
    expect(proxySnapshot).toBeDefined();
    expect(proxyReads).toBe(0);
    expect(proxySnapshot?.value).toEqual({ result: "small" });
    expect(JSON.stringify(proxySnapshot?.value)).toBe('{"result":"small"}');
    proxyTarget.result = "changed-after-normalization";
    expect(JSON.stringify(proxySnapshot?.value)).toBe('{"result":"small"}');

    expect(boundedJsonSnapshot('é\n"', 8)).toEqual({ value: 'é\n"', bytes: 8 });
    expect(boundedJsonSnapshot('é\n"', 7)).toBeUndefined();
  });

  it("rejects invalid bounded-snapshot limits before traversing provider data", () => {
    let reflectionAttempts = 0;
    const payload = new Proxy(
      { result: "small" },
      {
        getPrototypeOf: (target) => {
          reflectionAttempts += 1;
          return Reflect.getPrototypeOf(target);
        },
      },
    );

    for (const maxBytes of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(boundedJsonSnapshot(payload, maxBytes)).toBeUndefined();
    }
    for (const maxDepth of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(boundedJsonSnapshot(payload, 128, maxDepth)).toBeUndefined();
    }
    expect(reflectionAttempts).toBe(0);
  });

  it("fails closed for retained closures and hidden object storage", () => {
    const cleanBacking = new ArrayBuffer(4_096);
    const cleanView = new Uint8Array(cleanBacking, 0, 1);
    const backing = new ArrayBuffer(4_096);
    const view = new Uint8Array(backing, 0, 1);
    let binaryAccessorReads = 0;
    Object.defineProperty(view, "buffer", {
      get: () => {
        binaryAccessorReads += 1;
        throw new Error("view buffer accessor must not run");
      },
    });
    Object.defineProperty(backing, "byteLength", {
      get: () => {
        binaryAccessorReads += 1;
        throw new Error("buffer byteLength accessor must not run");
      },
    });

    expect(boundedValueFootprint(view, 1_024)).toBeUndefined();
    expect(boundedValueFootprint(view, 8_192)).toBeUndefined();
    expect(boundedValueFootprint(backing, 8_192)).toBeUndefined();
    expect(boundedValueFootprint(cleanView, 8_192)).toBe(4_128);
    expect(boundedValueFootprint(cleanBacking, 8_192)).toBe(4_128);
    // The backing bytes fit, but materializing thousands of indexed keys does not.
    expect(boundedValueFootprint(new Uint8Array(4_096), 8_192)).toBeUndefined();
    expect(
      boundedValueFootprint(
        Array.from({ length: 4_096 }, () => 0),
        8_192,
      ),
    ).toBeUndefined();
    expect(binaryAccessorReads).toBe(0);
    const expandedView = new Uint8Array(1);
    Object.defineProperty(expandedView, "payload", { value: "x".repeat(2_048) });
    expect(boundedValueFootprint(expandedView, 1_024)).toBeUndefined();
    class HiddenView extends Uint8Array {
      readonly #payload = "x".repeat(2_048);
      retainedPayload(): string {
        return this.#payload;
      }
    }
    class HiddenBuffer extends ArrayBuffer {
      readonly #payload = "x".repeat(2_048);
      retainedPayload(): string {
        return this.#payload;
      }
    }
    expect(boundedValueFootprint(new HiddenView(1), 1_024)).toBeUndefined();
    expect(boundedValueFootprint(new HiddenBuffer(1), 1_024)).toBeUndefined();
    expect(boundedValueFootprint(1n, 1_024)).toBeUndefined();
    expect(boundedValueFootprint(Symbol("small"), 1_024)).toBeUndefined();
    expect(boundedValueFootprint({ [Symbol("key")]: "value" }, 1_024)).toBeUndefined();
    expect(boundedValueFootprint(Redacted.make("secret"), 1_024)).toBe(40);
    expect(boundedValueFootprint(DateTime.makeUnsafe(0), 1_024)).toBeDefined();
    expect(
      boundedValueFootprint(Redacted.make(new Map([["small", "value"]])), 1_024),
    ).toBeUndefined();
    const forgedRedacted = Object.create(Object.getPrototypeOf(Redacted.make("secret")));
    expect(boundedValueFootprint(forgedRedacted, 1_024)).toBeUndefined();
    expect(boundedValueFootprint(new Map([["small", "value"]]), 1_024)).toBeUndefined();
    expect(boundedValueFootprint(new Set(["small"]), 1_024)).toBeUndefined();
    const forgedEffectValue = Object.create({ "~effect/forged": "~effect/forged" });
    forgedEffectValue.value = "small";
    expect(boundedValueFootprint(forgedEffectValue, 1_024)).toBeUndefined();
    class UnknownEnvelope extends Schema.Class<UnknownEnvelope>("UnknownEnvelope")({
      value: Schema.Unknown,
    }) {}
    expect(
      boundedValueFootprint(Schema.decodeSync(UnknownEnvelope)({ value: "small" }), 1_024),
    ).toBeUndefined();
    expect(
      boundedValueFootprint(
        Schema.decodeSync(UnknownEnvelope)({ value: forgedEffectValue }),
        1_024,
      ),
    ).toBeUndefined();
    expect(boundedValueFootprint({ callback: () => undefined }, 1_024)).toBeUndefined();

    let accessorReads = 0;
    const accessorBacked = {};
    Object.defineProperty(accessorBacked, "value", {
      enumerable: true,
      get: () => {
        accessorReads += 1;
        return "small";
      },
    });
    expect(boundedValueFootprint(accessorBacked, 1_024)).toBeUndefined();
    expect(accessorReads).toBe(0);
  });

  it.effect("rejects duplicate provider terminals before appending any Tool success", () =>
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
      expect(observed.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(0);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
    }),
  );

  it.effect("rejects post-finish provider content before appending staged terminal events", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const agent = Agent.withModel(
        hostedDefinition,
        modelFromParts([
          {
            type: "tool-call",
            id: "hosted-late-content",
            name: "HostedSearch",
            params: { query: "late" },
            providerExecuted: true,
          },
          {
            type: "tool-result",
            id: "hosted-late-content",
            name: "HostedSearch",
            result: { status: "complete" },
            isFailure: false,
            providerExecuted: true,
          },
          { type: "finish", reason: "tool-calls", usage },
          { type: "text-start", id: "after-finish" },
        ]),
      );

      const exit = yield* AgentRuntime.stream(agent, { question: "late" }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("content after its finish part");
      expect(observed.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(0);
      expect(observed.filter((event) => event._tag === "TurnCompleted")).toHaveLength(0);
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

  it.effect("preserves interruption-only Tool handler Causes without failure telemetry", () =>
    Effect.gen(function* () {
      const spans: Array<Tracer.NativeSpan> = [];
      const logs: Array<ExportedLogObservation> = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      const logger = Logger.make<unknown, void>((options) => {
        logs.push(exportedLogObservation(options));
      });
      const toolStarted = yield* Deferred.make<void>();
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const Wait = Tool.make("wait_interrupted", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const tools = Toolkit.make(Wait);
      const definition = Agent.define("interrupted-tool-observability", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Wait for interruption.",
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
          id: "wait-interrupted-1",
          name: "wait_interrupted",
          params: {},
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const toolLayer = tools.toLayer({
        wait_interrupted: () =>
          Deferred.succeed(toolStarted, undefined).pipe(Effect.andThen(Effect.never)),
      });

      const fiber = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "wait",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.provideService(Tracer.Tracer, tracer),
        Effect.provide(Logger.layer([logger])),
        Effect.forkChild,
      );
      yield* Deferred.await(toolStarted);
      const interruptor = 7335;
      fiber.interruptUnsafe(interruptor);
      const exit = yield* Fiber.await(fiber);

      if (Exit.isSuccess(exit)) throw new Error("Expected the Tool handler to be interrupted");
      expect(exit.cause.reasons).toHaveLength(1);
      expect(exit.cause.reasons[0]?._tag).toBe("Interrupt");
      if (!Cause.isInterruptReason(exit.cause.reasons[0])) {
        throw new Error("Expected an interruption-only Tool Cause");
      }
      expect(exit.cause.reasons[0].fiberId).toBe(interruptor);

      const observed = yield* Ref.get(events);
      expect(
        observed.filter(
          (event) => event._tag === "ToolCallSucceeded" || event._tag === "ToolCallFailed",
        ),
      ).toEqual([]);
      const span = spans.find((candidate) => candidate.name === "execute_tool wait_interrupted");
      expect(Object.fromEntries(span?.attributes ?? [])).not.toHaveProperty(
        "effect_agent.tool.outcome",
      );
      if (span?.status._tag !== "Ended" || !Exit.isFailure(span.status.exit)) {
        throw new Error("Expected the interrupted Tool span to end with interruption");
      }
      expect(span.status.exit.cause.reasons).toHaveLength(1);
      expect(span.status.exit.cause.reasons[0]?._tag).toBe("Interrupt");
      if (!Cause.isInterruptReason(span.status.exit.cause.reasons[0])) {
        throw new Error("Expected the Tool span to retain interruption");
      }
      expect(
        logs.filter((entry) =>
          ["agent tool execution completed", "agent tool execution failed"].includes(
            renderedLogMessage(entry.message),
          ),
        ),
      ).toEqual([]);
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

  it.effect('fails token exhaustion before a successful stop with onExhaustion "fail"', () =>
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
          onExhaustion: "fail",
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

  it.effect(
    "RUN-019 fail mode fails typed Turn exhaustion before executing the pending Tool batch",
    () =>
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
            onExhaustion: "fail",
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

  it.effect(
    "RUN-018 fail mode fails typed Tool Call exhaustion before executing the exceeding batch",
    () =>
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
            onExhaustion: "fail",
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
          // This test pins the context hook's exact outgoing shape (RUN-020
          // status coverage lives in context-economics.test.ts).
          runStatus: "off",
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
        // The model-visible output contract (RUN-028) is
        // applied after context preparation, so compaction cannot drop it.
        expect(receivedPrompt.content.map((message) => message.role)).toEqual(["system", "user"]);
        const encoded = JSON.stringify(receivedPrompt.content);
        expect(encoded).toContain("Final output contract:");
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

  it.effect("leaves incremental history caller-owned when later output validation fails", () =>
    Effect.gen(function* () {
      const conversationId = yield* Schema.decodeEffect(ConversationId)("shared-conversation");
      let history = Prompt.empty;
      const first = yield* AgentRuntime.run(
        makeAgent(finalParts("invalid final output")),
        { question: "first" },
        {
          conversationId,
          onHistory: (next) =>
            Effect.sync(() => {
              history = next;
            }),
        },
      ).pipe(Effect.flip);
      expect(first).toBeInstanceOf(AgentOutputError);
      expect(JSON.stringify(history)).toContain("invalid final output");
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

      expect(second.conversationId).toBe(conversationId);
      expect(secondPrompt).toContain("invalid final output");
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
      let bufferLimitReads = 0;
      const options = {
        get bufferLimits() {
          bufferLimitReads += 1;
          return { maxRunEvents: 8 };
        },
      };
      const detached = yield* AgentRuntime.start(
        makeAgent(finalParts('{"answer":"detached"}')),
        { question: "complete independently" },
        options,
      );
      const slowObserver = yield* detached.observe.pipe(
        Stream.runForEach(() => Effect.never),
        Effect.forkChild,
      );
      const result = yield* detached.await;

      expect(result.output).toEqual({ answer: "detached" });
      expect((yield* detached.events).at(-1)?._tag).toBe("RunCompleted");
      expect(bufferLimitReads).toBe(1);
      yield* Fiber.interrupt(slowObserver);

      const frozenDetached = yield* AgentRuntime.start(
        makeAgent(finalParts('{"answer":"frozen"}')),
        { question: "accept frozen options" },
        Object.freeze({ bufferLimits: Object.freeze({ maxRunEvents: 8 }) }),
      );
      expect((yield* frozenDetached.await).output).toEqual({ answer: "frozen" });
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
          "BudgetWarning",
          "ToolCallStarted",
          "ToolCallSucceeded",
          "TurnStarted",
          "ModelStarted",
          "TextDelta",
          "TurnCompleted",
          "BudgetWarning",
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
      const params = yield* Schema.decodeUnknownEffect(
        Schema.Struct({ city: Schema.String, departAt: Schema.String }),
      )(callPart.params);
      expect(params).toEqual({ city: "Kyoto", departAt: "2026-08-12T09:00:00.000Z" });
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
              {
                resume,
                resumeUsage: {
                  committedTurns: 1,
                  toolCalls: 1,
                  programmaticToolCalls: 0,
                  consecutiveToolFailures: 0,
                  finalizationUsed: false,
                  modelCalls: 1,
                  inputTokens: 0,
                  outputTokens: 0,
                  lastInputTokens: 0,
                  lastOutputTokens: 0,
                  costMicrousd: 0,
                },
              },
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

/**
 * Budget soft landing (RUN-018/RUN-019/RUN-020): under the default
 * `onExhaustion: "final-answer"`, Turn and Tool Call exhaustion settle the Run
 * through one constrained final-answer opportunity — the over-budget batch
 * settles synthetically without a handler start, subsequent model requests
 * forbid tool use, and the Run completes with the honest
 * `finishReason: "budget-exhausted"` (RUN-011).
 */
layer(testLayer)("RUN-018 budget soft landing", (it) => {
  const Search = Tool.make("search", {
    parameters: Schema.Struct({}),
    success: Schema.String,
  });
  const softLandingTools = Toolkit.make(Search);
  const softLandingDefinition = (policy: AgentPolicy) =>
    Agent.define("soft-landing", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Search until done.",
      toolkit: softLandingTools,
      policy,
    });
  const searchCall = (id: string): Response.StreamPartEncoded => ({
    type: "tool-call",
    id,
    name: "search",
    params: {},
    providerExecuted: false,
  });
  const turnScriptedModel = (
    name: string,
    turns: Ref.Ref<number>,
    toolChoices: Ref.Ref<ReadonlyArray<unknown>>,
    prompts: Ref.Ref<ReadonlyArray<string>>,
    script: (turn: number) => ReadonlyArray<Response.StreamPartEncoded>,
  ) =>
    Model.make(
      "scripted",
      name,
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (request) =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* Ref.update(toolChoices, (all) => [...all, request.toolChoice]);
                yield* Ref.update(prompts, (all) => [...all, JSON.stringify(request.prompt)]);
                const turn = yield* Ref.getAndUpdate(turns, (value) => value + 1);
                return Stream.fromIterable(script(turn));
              }),
            ),
        }),
      ),
    );

  it.effect(
    "RUN-018 an over-budget Tool batch settles synthetically and the Run completes budget-exhausted",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const turns = yield* Ref.make(0);
        const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
        const definition = softLandingDefinition(
          AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        );
        const model = turnScriptedModel(
          "soft-landing-batch",
          turns,
          toolChoices,
          prompts,
          (turn) =>
            turn === 0
              ? [
                  searchCall("search-1"),
                  searchCall("search-2"),
                  { type: "finish", reason: "tool-calls", usage },
                ]
              : finalParts('{"answer":"partial findings"}'),
        );
        const toolLayer = softLandingTools.toLayer({
          search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
        });

        const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "research",
        }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
          Effect.exit,
        );
        const observed = yield* Ref.get(events);
        const observedChoices = yield* Ref.get(toolChoices);
        const observedPrompts = yield* Ref.get(prompts);

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(yield* Ref.get(handlerStarts)).toBe(0);
        expect(observed.filter((event) => event._tag === "ToolCallDeclared")).toHaveLength(2);
        expect(observed.filter((event) => event._tag === "ToolCallStarted")).toHaveLength(0);
        const failed = observed.filter((event) => event._tag === "ToolCallFailed");
        expect(failed).toHaveLength(2);
        for (const event of failed) {
          expect(event).toMatchObject({ errorTag: "AgentPolicyError" });
        }
        expect(observed.at(-1)).toMatchObject({
          _tag: "RunCompleted",
          turns: 2,
          finishReason: "budget-exhausted",
        });
        expect(observedChoices).toEqual(["auto", "none"]);
        // The rejected batch is model-visible: the final-answer request
        // carries one failed tool result per rejected call, in declaration
        // order, each with the synthetic policy failure as its payload.
        const secondPrompt = JSON.parse(observedPrompts[1] ?? "{}") as {
          readonly content?: ReadonlyArray<{
            readonly role: string;
            readonly content: ReadonlyArray<{
              readonly type: string;
              readonly id?: string;
              readonly isFailure?: boolean;
              readonly result?: { readonly _tag?: string; readonly message?: string };
            }>;
          }>;
        };
        const toolResults = (secondPrompt.content ?? [])
          .filter((message) => message.role === "tool")
          .flatMap((message) => message.content)
          .filter((part) => part.type === "tool-result");
        expect(toolResults.map((part) => part.id)).toEqual(["search-1", "search-2"]);
        for (const part of toolResults) {
          expect(part.isFailure).toBe(true);
          expect(part.result?._tag).toBe("AgentPolicyError");
          expect(part.result?.message).toContain("Tool Call budget exhausted");
        }
      }),
  );

  it.effect("RUN-018 synthetic rejections do not advance the repeated-failure counter", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const turns = yield* Ref.make(0);
      const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
      const definition = softLandingDefinition(
        AgentPolicy.make({
          maxTurns: 5,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          repeatedFailureLimit: 3,
        }),
      );
      const model = turnScriptedModel("soft-landing-exempt", turns, toolChoices, prompts, (turn) =>
        turn === 0
          ? [
              searchCall("search-1"),
              searchCall("search-2"),
              searchCall("search-3"),
              searchCall("search-4"),
              { type: "finish", reason: "tool-calls", usage },
            ]
          : finalParts('{"answer":"still landed"}'),
      );
      const toolLayer = softLandingTools.toLayer({
        search: () => Effect.succeed("found"),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "research",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const observed = yield* Ref.get(events);

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(observed.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(4);
      expect(observed.some((event) => event._tag === "RunFailed")).toBe(false);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunCompleted",
        finishReason: "budget-exhausted",
      });
    }),
  );

  it.effect("RUN-018 a batch landing exactly on the Tool Call cap keeps the model-stop path", () =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const turns = yield* Ref.make(0);
      const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
      const definition = softLandingDefinition(
        AgentPolicy.make({
          maxTurns: 5,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      );
      const model = turnScriptedModel("soft-landing-exact", turns, toolChoices, prompts, (turn) =>
        turn === 0
          ? [searchCall("search-1"), { type: "finish", reason: "tool-calls", usage }]
          : finalParts('{"answer":"done"}'),
      );
      const toolLayer = softLandingTools.toLayer({
        search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "research",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const observed = yield* Ref.get(events);
      const observedChoices = yield* Ref.get(toolChoices);

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(yield* Ref.get(handlerStarts)).toBe(1);
      expect(observedChoices).toEqual(["auto", "auto"]);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunCompleted",
        finishReason: "model-stop",
      });
    }),
  );

  it.effect(
    "RUN-018 a resumed over-budget batch settles synthetically without handler starts",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const turns = yield* Ref.make(0);
        const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
        const resumeTurnId = yield* Schema.decodeEffect(TurnId)("turn-resume-soft").pipe(
          Effect.orDie,
        );
        const definition = softLandingDefinition(
          AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        );
        const model = turnScriptedModel("soft-landing-resume", turns, toolChoices, prompts, () =>
          finalParts('{"answer":"resumed partial"}'),
        );
        const toolLayer = softLandingTools.toLayer({
          search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
        });
        const resume: RunTurnResume = {
          turn: 1,
          turnId: resumeTurnId,
          calls: [
            { id: "search-1", name: "search", params: {} },
            { id: "search-2", name: "search", params: {} },
          ],
          settled: [{ id: "search-1", result: "recorded-before-crash", isFailure: false }],
        };

        const exit = yield* AgentRuntime.stream(
          Agent.withModel(definition, model),
          { question: "resume" },
          {
            resume,
            resumeUsage: {
              committedTurns: 1,
              toolCalls: 2,
              programmaticToolCalls: 0,
              consecutiveToolFailures: 0,
              finalizationUsed: false,
              modelCalls: 1,
              inputTokens: 0,
              outputTokens: 0,
              lastInputTokens: 0,
              lastOutputTokens: 0,
              costMicrousd: 0,
            },
          },
        ).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
          Effect.exit,
        );
        const observed = yield* Ref.get(events);
        const observedPrompts = yield* Ref.get(prompts);

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(yield* Ref.get(handlerStarts)).toBe(0);
        expect(observed.filter((event) => event._tag === "ToolCallStarted")).toHaveLength(0);
        // Only the open call settles synthetically; the recorded result stands verbatim.
        expect(observed.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);
        expect(observed.at(-1)).toMatchObject({
          _tag: "RunCompleted",
          finishReason: "budget-exhausted",
        });
        expect(yield* Ref.get(toolChoices)).toEqual(["none"]);
        expect(observedPrompts[0]).toContain("recorded-before-crash");
        expect(observedPrompts[0]).toContain("Tool Call budget exhausted");
      }),
  );

  it.effect("RUN-019 Turn exhaustion grants exactly one final-answer grace Turn", () =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const turns = yield* Ref.make(0);
      const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
      const definition = softLandingDefinition(
        AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      );
      const model = turnScriptedModel("soft-landing-grace", turns, toolChoices, prompts, (turn) =>
        turn === 0
          ? [searchCall("search-1"), { type: "finish", reason: "tool-calls", usage }]
          : finalParts('{"answer":"grace"}'),
      );
      const toolLayer = softLandingTools.toLayer({
        search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "research",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const observed = yield* Ref.get(events);

      expect(Exit.isSuccess(exit)).toBe(true);
      // The pending batch at the final permitted Turn executes normally.
      expect(yield* Ref.get(handlerStarts)).toBe(1);
      expect(yield* Ref.get(toolChoices)).toEqual(["auto", "none"]);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunCompleted",
        turns: 2,
        finishReason: "budget-exhausted",
      });
    }),
  );

  it.effect("RUN-019 no second grace Turn is granted at the grace Turn's stop seam", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const turns = yield* Ref.make(0);
      const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
      const followUpOffered = yield* Ref.make(false);
      const definition = softLandingDefinition(
        AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      );
      const model = turnScriptedModel(
        "soft-landing-no-second-grace",
        turns,
        toolChoices,
        prompts,
        (turn) =>
          turn === 0
            ? [searchCall("search-1"), { type: "finish", reason: "tool-calls", usage }]
            : finalParts('{"answer":"grace"}'),
      );
      const toolLayer = softLandingTools.toLayer({
        search: () => Effect.succeed("found"),
      });

      const exit = yield* AgentRuntime.stream(
        Agent.withModel(definition, model),
        { question: "research" },
        {
          input: {
            drain: () =>
              Ref.getAndSet(followUpOffered, true).pipe(
                Effect.map((offered) =>
                  offered ? [] : [{ kind: "follow-up" as const, input: "one more thing" }],
                ),
              ),
          },
        },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect(failure).toMatchObject({ limit: "turns" });
      expect(observed.filter((event) => event._tag === "ModelStarted")).toHaveLength(2);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunFailed",
        errorTag: "AgentPolicyError",
      });
    }),
  );

  it.effect("RUN-020 declaring Tool Calls under toolChoice none fails the Run typed", () =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const turns = yield* Ref.make(0);
      const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
      const definition = softLandingDefinition(
        AgentPolicy.make({
          maxTurns: 5,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      );
      const model = turnScriptedModel(
        "soft-landing-fail-closed",
        turns,
        toolChoices,
        prompts,
        (turn) =>
          turn === 0
            ? [
                searchCall("search-1"),
                searchCall("search-2"),
                { type: "finish", reason: "tool-calls", usage },
              ]
            : [searchCall("search-3"), { type: "finish", reason: "tool-calls", usage }],
      );
      const toolLayer = softLandingTools.toLayer({
        search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "research",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(errorMessageForTest(failure)).toContain('toolChoice "none"');
      expect(yield* Ref.get(handlerStarts)).toBe(0);
    }),
  );

  it.effect(
    "RUN-021 a per-Run Tool Call allowance tightens below the policy and soft-lands at the effective limit",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const turns = yield* Ref.make(0);
        const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
        const definition = softLandingDefinition(
          AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        );
        const model = turnScriptedModel(
          "allowance-tightens",
          turns,
          toolChoices,
          prompts,
          (turn) =>
            turn === 0
              ? [
                  searchCall("search-1"),
                  searchCall("search-2"),
                  { type: "finish", reason: "tool-calls", usage },
                ]
              : finalParts('{"answer":"partial under allowance"}'),
        );
        const toolLayer = softLandingTools.toLayer({
          search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
        });

        const exit = yield* AgentRuntime.stream(
          Agent.withModel(definition, model),
          { question: "research" },
          { toolCallAllowance: 1 },
        ).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
          Effect.exit,
        );
        const observed = yield* Ref.get(events);

        // The batch of 2 exceeds the effective limit of min(5, 1) = 1: it is
        // rejected without a handler start and the Run soft-lands.
        expect(Exit.isSuccess(exit)).toBe(true);
        expect(yield* Ref.get(handlerStarts)).toBe(0);
        expect(observed.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(2);
        expect(observed.at(-1)).toMatchObject({
          _tag: "RunCompleted",
          finishReason: "budget-exhausted",
        });
        expect(yield* Ref.get(toolChoices)).toEqual(["auto", "none"]);
      }),
  );

  it.effect("RUN-021 an allowance can never widen the policy ceiling", () =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      const turns = yield* Ref.make(0);
      const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
      const definition = softLandingDefinition(
        AgentPolicy.make({
          maxTurns: 5,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          onExhaustion: "fail",
        }),
      );
      const model = turnScriptedModel("allowance-no-widen", turns, toolChoices, prompts, () => [
        searchCall("search-1"),
        searchCall("search-2"),
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const toolLayer = softLandingTools.toLayer({
        search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
      });

      const exit = yield* AgentRuntime.stream(
        Agent.withModel(definition, model),
        { question: "research" },
        { toolCallAllowance: 100 },
      ).pipe(Stream.runDrain, Effect.provide(toolLayer), Effect.exit);
      const failure = failureFrom(exit);

      // min(policy 1, allowance 100) = 1: the policy ceiling holds.
      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect(failure).toMatchObject({ limit: "tool-calls" });
      expect(errorMessageForTest(failure)).toContain("1 Tool Call limit");
      expect(yield* Ref.get(handlerStarts)).toBe(0);
    }),
  );

  it.effect(
    "RUN-021 a non-finite allowance is ignored fail-closed and the policy bound holds",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const turns = yield* Ref.make(0);
        const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
        const definition = softLandingDefinition(
          AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            onExhaustion: "fail",
          }),
        );
        const model = turnScriptedModel("allowance-nan", turns, toolChoices, prompts, () => [
          searchCall("search-1"),
          searchCall("search-2"),
          { type: "finish", reason: "tool-calls", usage },
        ]);
        const toolLayer = softLandingTools.toLayer({
          search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
        });

        // NaN would poison every `>` comparison (always false) and silently
        // erase the bound; the engine must keep the policy limit instead.
        const exit = yield* AgentRuntime.stream(
          Agent.withModel(definition, model),
          { question: "research" },
          { toolCallAllowance: Number.NaN },
        ).pipe(Stream.runDrain, Effect.provide(toolLayer), Effect.exit);
        const failure = failureFrom(exit);

        expect(failure).toBeInstanceOf(AgentPolicyError);
        expect(failure).toMatchObject({ limit: "tool-calls" });
        expect(yield* Ref.get(handlerStarts)).toBe(0);
      }),
  );

  it.effect("RUN-021 a per-Run Turn allowance tightens maxTurns with the same grace", () =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const turns = yield* Ref.make(0);
      const toolChoices = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
      const definition = softLandingDefinition(
        AgentPolicy.make({
          maxTurns: 5,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      );
      const model = turnScriptedModel("turn-allowance", turns, toolChoices, prompts, (turn) =>
        turn === 0
          ? [searchCall("search-1"), { type: "finish", reason: "tool-calls", usage }]
          : finalParts('{"answer":"grace under allowance"}'),
      );
      const toolLayer = softLandingTools.toLayer({
        search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
      });

      const exit = yield* AgentRuntime.stream(
        Agent.withModel(definition, model),
        { question: "research" },
        { turnAllowance: 1 },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );
      const observed = yield* Ref.get(events);

      // The pending batch at the effective final Turn executes; the single
      // grace Turn runs tool-free and settles honestly.
      expect(Exit.isSuccess(exit)).toBe(true);
      expect(yield* Ref.get(handlerStarts)).toBe(1);
      expect(yield* Ref.get(toolChoices)).toEqual(["auto", "none"]);
      expect(observed.at(-1)).toMatchObject({
        _tag: "RunCompleted",
        turns: 2,
        finishReason: "budget-exhausted",
      });
    }),
  );
});

/**
 * P7 §7(h): the opt-in defect boundary. The engine keeps defects as defects — `RunFailed`
 * covers expected failures only — and `withTerminalDefectEvent` lets a host boundary append
 * ONE bounded terminal `RunFailed { errorTag: "Defect" }` before the cause is rethrown.
 */
layer(testLayer)("RUN-004 withTerminalDefectEvent boundary (P7 §7(h))", (it) => {
  it.effect("a defect appends one bounded terminal RunFailed and rethrows the original cause", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const model = Model.make(
        "scripted",
        "defect-boundary",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.concat(
                Stream.fromIterable<Response.StreamPartEncoded>([
                  { type: "text-start", id: "answer" },
                  { type: "text-delta", id: "answer", delta: "partial" },
                ]),
                Stream.fromEffect(Effect.die(new Error("the supplier catalog crashed"))),
              ),
          }),
        ),
      );

      const exit = yield* AgentRuntime.stream(Agent.withModel(runtimeDefinition, model), {
        question: "defect",
      }).pipe(
        withTerminalDefectEvent,
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );

      // The defect stays a defect: the cause is rethrown unchanged, never converted into a
      // typed failure or a successful stream end.
      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        expect(Cause.hasDies(exit.cause)).toBe(true);
      }

      const observed = yield* Ref.get(events);
      const failures = observed.filter((event) => event._tag === "RunFailed");
      expect(failures).toHaveLength(1);
      const terminal = observed.at(-1);
      expect(terminal?._tag).toBe("RunFailed");
      if (terminal?._tag === "RunFailed") {
        expect(terminal.errorTag).toBe("Defect");
        expect(terminal.message).toContain("the supplier catalog crashed");
        // Identity comes from the already-streamed events, one sequence later.
        const previous = observed.at(-2);
        expect(previous).toBeDefined();
        if (previous !== undefined) {
          expect(terminal.runId).toBe(previous.runId);
          expect(terminal.conversationId).toBe(previous.conversationId);
          expect(terminal.sequence).toBe(previous.sequence + 1);
        }
      }
    }),
  );

  it.effect(
    "a typed failure is never double-terminalized — the engine's RunFailed stands alone",
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        // Invalid final JSON fails typed (AgentOutputError): the engine already emits the
        // terminal RunFailed, so the boundary helper must pass the cause through untouched.
        const exit = yield* AgentRuntime.stream(makeAgent(finalParts("not json")), {
          question: "typed",
        }).pipe(
          withTerminalDefectEvent,
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.exit,
        );
        const failure = failureFrom(exit);
        expect(errorMessageForTest(failure)).toContain("not valid JSON");
        const observed = yield* Ref.get(events);
        const failures = observed.filter((event) => event._tag === "RunFailed");
        expect(failures).toHaveLength(1);
        if (failures[0]?._tag === "RunFailed") {
          expect(failures[0].errorTag).not.toBe("Defect");
        }
      }),
  );

  it.effect("a completed Run streams unchanged through the boundary", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      yield* AgentRuntime.stream(makeAgent(finalParts('{"answer":"fine"}')), {
        question: "ok",
      }).pipe(
        withTerminalDefectEvent,
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
      );
      const observed = yield* Ref.get(events);
      expect(observed.some((event) => event._tag === "RunFailed")).toBe(false);
      expect(observed.at(-1)?._tag).toBe("RunCompleted");
    }),
  );
});
