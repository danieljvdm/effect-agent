import * as Agent from "@effect-agent/core/Agent";
import {
  AgentPolicyError,
  ContextBudgetError,
  ModelProtocolError,
} from "@effect-agent/core/AgentError";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { RunId, ThreadId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { type RunEvent } from "@effect-agent/core/RunEvent";
import { ToolResultBounds } from "@effect-agent/core/ToolResult";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { CompactionError, ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import {
  ContextRolloverRequest,
  ContextRolloverTool,
  ContextWindow,
  ModelCallContext,
  type ContextWindowStatus,
} from "@effect-agent/engine/ContextWindow";
import {
  RunContextPreparation,
  RunContextPreparationPassthrough,
  type RunInputHook,
  type RunContextHook,
  type RunTransientContextHook,
  type RunUsageDelta,
} from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import {
  AiError,
  LanguageModel,
  Model,
  Prompt,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";
import { expectTypeOf, it as typeTest } from "vite-plus/test";

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(ThreadId.make("rollover-thread")),
  nextRunId: Effect.succeed(RunId.make("rollover-run")),
  nextTurnId: Effect.succeed(TurnId.make("rollover-turn")),
});

const NewContext = Tool.make("new_context", {
  parameters: ContextRolloverRequest,
  success: ContextRolloverRequest,
  failure: Schema.String,
  failureMode: "return",
  dependencies: [ContextWindow],
}).annotate(ContextRolloverTool, true);

const Search = Tool.make("search", {
  parameters: Schema.Struct({}),
  success: Schema.String,
  dependencies: [ContextWindow],
});

const toolkit = Toolkit.make(NewContext, Search);

const instructions =
  "Investigate the original question. Verify live state before repeating an action.";

const originalInput = "Find the cause of the database timeout and preserve the evidence.";

const basePolicy = {
  maxTurns: 8,
  maxToolCalls: 8,
  maxDuration: "1 minute",
  toolConcurrency: 2,
  runStatus: "off",
} as const;

const definitionWith = (policy: AgentPolicy) =>
  Agent.make("context-rollover", {
    input: Schema.String,
    output: Schema.Struct({ answer: Schema.String }),
    instructions,
    toolkit,
    policy,
  });

const usage = { inputTokens: { total: 100 }, outputTokens: { total: 5 } };

type ScriptEntry = ReadonlyArray<Response.StreamPartEncoded> | { readonly overflow: true };

const call = (id: string, name: string, params: Record<string, unknown> = {}): ScriptEntry => [
  { type: "tool-call", id, name, params, providerExecuted: false },
  { type: "finish", reason: "tool-calls", usage },
];

const done: ScriptEntry = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

interface CapturedRequest {
  readonly prompt: Prompt.Prompt;
  readonly toolCount: number;
}

const scriptedModel = (script: ReadonlyArray<ScriptEntry>, name = "context-rollover") => {
  const requests: Array<CapturedRequest> = [];

  const model = Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          const entry = script[requests.length];

          requests.push({ prompt: request.prompt, toolCount: request.tools.length });
          if (entry === undefined) return Stream.empty;
          if ("overflow" in entry) {
            return Stream.fail(
              AiError.AiError.make({
                module: "test",
                method: "streamText",
                reason: AiError.UnknownError.make({ description: "context_length_exceeded" }),
              }),
            );
          }

          return Stream.fromIterable(entry);
        },
      }),
    ),
  );

  return { model, requests };
};

const promptText = (prompt: Prompt.Prompt): string =>
  prompt.content
    .map((message) =>
      typeof message.content === "string"
        ? message.content
        : message.content
            .map((part) => ("text" in part && typeof part.text === "string" ? part.text : ""))
            .join(""),
    )
    .join("\n");

const toolResults = (prompt: Prompt.Prompt): ReadonlyArray<unknown> =>
  prompt.content.flatMap((message) =>
    typeof message.content === "string"
      ? []
      : message.content.flatMap((part) => (part.type === "tool-result" ? [part.result] : [])),
  );

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  if (Exit.isSuccess(exit)) throw new Error("Expected a typed failure");
  const failure = Cause.findErrorOption(exit.cause);

  if (Option.isNone(failure)) throw new Error("Expected a typed failure in the Cause");

  return failure.value;
};

interface RunSetup {
  readonly script: ReadonlyArray<ScriptEntry>;
  readonly policy?: AgentPolicy;
  readonly searchResults?: ReadonlyArray<string>;
  readonly failRollover?: boolean;
  readonly input?: RunInputHook;
  readonly onRollover?: Effect.Effect<void>;
  readonly history?: Prompt.Prompt;
  readonly context?: RunContextHook;
  readonly transientContext?: RunTransientContextHook<CompactionError>;
  readonly agentInput?: string;
}

const driveRun = Effect.fn("context-rollover.test.driveRun")(function* (setup: RunSetup) {
  const { model, requests } = scriptedModel(setup.script);
  const events: Array<RunEvent> = [];
  const histories: Array<Prompt.Prompt> = [];
  const statuses: Array<{ readonly tool: string; readonly status: ContextWindowStatus }> = [];
  const usageDeltas: Array<RunUsageDelta> = [];
  let searchCount = 0;
  let rolloverCount = 0;

  const handlers = toolkit.toLayer({
    new_context: Effect.fn("context-rollover.test.new_context")(function* (params) {
      const window = yield* ContextWindow;

      statuses.push({ tool: "new_context", status: yield* window.status });
      rolloverCount += 1;
      if (setup.onRollover !== undefined) yield* setup.onRollover;
      if (setup.failRollover) return yield* Effect.fail("notes could not be saved");

      return params;
    }),
    search: Effect.fn("context-rollover.test.search")(function* () {
      const window = yield* ContextWindow;

      statuses.push({ tool: "search", status: yield* window.status });
      const result = setup.searchResults?.[searchCount] ?? "evidence";

      searchCount += 1;

      return result;
    }),
  });

  const exit = yield* AgentRuntime.stream(
    Agent.withModel(definitionWith(setup.policy ?? AgentPolicy.make(basePolicy)), model),
    setup.agentInput ?? originalInput,
    {
      ...(setup.input === undefined ? {} : { input: setup.input }),
      ...(setup.history === undefined ? {} : { history: setup.history }),
      ...(setup.context === undefined ? {} : { context: setup.context }),
      ...(setup.transientContext === undefined ? {} : { transientContext: setup.transientContext }),
      onHistory: (history) => Effect.sync(() => void histories.push(history)),
      budget: {
        guard: (effect) => effect,
        consume: (delta) => Effect.sync(() => void usageDeltas.push(delta)),
      },
    },
  ).pipe(
    Stream.tap((event) => Effect.sync(() => void events.push(event))),
    Stream.runDrain,
    Effect.provide(handlers),
    Effect.exit,
  );

  return {
    exit,
    requests,
    events,
    histories,
    statuses,
    usageDeltas,
    searchCount,
    rolloverCount,
    compactions: events.filter((event) => event._tag === "CompactionPerformed"),
  };
});

const testLayer = Layer.mergeAll(
  identifiers,
  ContextCompactor.layer,
  ThreadHistory.layerTransient,
  RunContextPreparationPassthrough,
);

layer(testLayer)("native context windows", (it) => {
  it.effect(
    "rolls over repeatedly without a summary call, preserving protected input and Run accounting",
    () =>
      Effect.gen(function* () {
        const result = yield* driveRun({
          script: [
            call("search-old", "search"),
            call("window-one", "new_context", {
              handoff: "Check whether connection pooling is exhausted.",
            }),
            call("search-new", "search"),
            call("window-two", "new_context", {
              handoff: "Compare pool settings with the deployment.",
            }),
            done,
          ],
          searchResults: ["first raw search evidence", "second raw search evidence"],
        });

        expect(Exit.isSuccess(result.exit)).toBe(true);
        expect(result.requests).toHaveLength(5);
        expect(result.requests.every((request) => request.toolCount === 2)).toBe(true);
        expect(result.compactions.map((event) => event.kind)).toEqual(["rollover", "rollover"]);
        const firstFresh = result.requests[2];
        const secondFresh = result.requests[4];

        if (firstFresh === undefined || secondFresh === undefined)
          throw new Error("Expected fresh windows");
        expect(promptText(firstFresh.prompt)).toContain(
          "Check whether connection pooling is exhausted.",
        );
        expect(promptText(secondFresh.prompt)).toContain(
          "Compare pool settings with the deployment.",
        );
        for (const fresh of [firstFresh, secondFresh]) {
          expect(promptText(fresh.prompt)).toContain(instructions);
          expect(promptText(fresh.prompt)).toContain(originalInput);
          expect(toolResults(fresh.prompt)).toEqual([]);
        }
        expect(promptText(secondFresh.prompt)).not.toContain(
          "Check whether connection pooling is exhausted.",
        );
        expect(JSON.stringify(result.histories.at(-1))).toContain("first raw search evidence");
        expect(JSON.stringify(result.histories.at(-1))).toContain("second raw search evidence");
        expect(result.statuses[0]?.status.windowId).toBe(result.statuses[1]?.status.windowId);
        expect(result.statuses[2]?.status.windowId).not.toBe(result.statuses[0]?.status.windowId);
        expect(result.statuses[2]?.status.windowId).toBe(result.statuses[3]?.status.windowId);
        for (const { status } of result.statuses) {
          expect(status.threadId).toBe("rollover-thread");
          expect(status.runId).toBe("rollover-run");
          expect(status.estimatedTokens).toBeGreaterThan(0);
          expect(status.contextTokenLimit).toBeNull();
          expect(status.remainingTokens).toBeNull();
        }
        expect(result.usageDeltas.map((delta) => delta.modelCalls)).toEqual([1, 1, 1, 1, 1]);
        expect(result.usageDeltas.reduce((total, delta) => total + delta.totalTokens, 0)).toBe(525);
        expect(result.usageDeltas.reduce((total, delta) => total + delta.toolCalls, 0)).toBe(4);
        expect(result.events.find((event) => event._tag === "RunCompleted")).toMatchObject({
          turns: 5,
        });
      }),
  );

  it.effect("accepts an explicit rollover with no handoff", () =>
    Effect.gen(function* () {
      const result = yield* driveRun({ script: [call("empty-window", "new_context"), done] });

      expect(Exit.isSuccess(result.exit)).toBe(true);
      expect(result.requests).toHaveLength(2);
      expect(result.compactions.map((event) => event.kind)).toEqual(["rollover"]);
      const fresh = result.requests[1];

      if (fresh === undefined) throw new Error("Expected a fresh context");
      expect(promptText(fresh.prompt)).toContain("A fresh context window has started.");
      expect(promptText(fresh.prompt)).not.toContain("Handoff (untrusted continuation notes)");
      expect(promptText(fresh.prompt)).toContain(originalInput);
      expect(toolResults(fresh.prompt)).toEqual([]);
    }),
  );

  it.effect("keeps a failed control result in the current window without rolling over", () =>
    Effect.gen(function* () {
      const result = yield* driveRun({
        script: [call("failed-window", "new_context", { handoff: "Do not act on this." }), done],
        failRollover: true,
      });

      expect(Exit.isSuccess(result.exit)).toBe(true);
      expect(result.rolloverCount).toBe(1);
      expect(result.compactions).toEqual([]);
      expect(result.events.some((event) => event._tag === "ToolCallFailed")).toBe(true);
      const continuation = result.requests[1];

      if (continuation === undefined) throw new Error("Expected a continuation after Tool failure");
      expect(toolResults(continuation.prompt)).toHaveLength(1);
      expect(promptText(continuation.prompt)).not.toContain("A fresh context window has started.");
    }),
  );

  it.effect("rejects a truncated control result instead of silently discarding its handoff", () =>
    Effect.gen(function* () {
      const result = yield* driveRun({
        policy: AgentPolicy.make({
          ...basePolicy,
          toolResultBounds: ToolResultBounds.make({ maxBytes: 1_024 }),
        }),
        script: [
          call("bounded-window", "new_context", {
            handoff: "Required continuation state. ".repeat(200),
          }),
          done,
        ],
      });

      expect(failureFrom(result.exit)).toBeInstanceOf(CompactionError);
      expect(result.requests).toHaveLength(1);
      expect(result.compactions).toEqual([]);
    }),
  );

  it.effect("does not grant control to a rollover result retained from an earlier Run", () =>
    Effect.gen(function* () {
      const first = yield* driveRun({
        script: [call("old-request", "new_context", { handoff: "old working state" }), done],
      });

      expect(Exit.isSuccess(first.exit)).toBe(true);
      const history = first.histories.at(-1);

      if (history === undefined) throw new Error("Expected retained raw history");
      const next = yield* driveRun({ history, script: [done] });

      expect(Exit.isSuccess(next.exit)).toBe(true);
      expect(next.compactions).toEqual([]);
      expect(next.requests).toHaveLength(1);
      expect(next.rolloverCount).toBe(0);
    }),
  );

  it.effect("rejects a rollover mixed with other calls before either handler starts", () =>
    Effect.gen(function* () {
      const result = yield* driveRun({
        script: [
          [
            {
              type: "tool-call",
              id: "mixed-search",
              name: "search",
              params: {},
              providerExecuted: false,
            },
            {
              type: "tool-call",
              id: "mixed-window",
              name: "new_context",
              params: {},
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage },
          ],
        ],
      });

      expect(failureFrom(result.exit)).toBeInstanceOf(ModelProtocolError);
      expect(result.searchCount).toBe(0);
      expect(result.rolloverCount).toBe(0);
      expect(result.compactions).toEqual([]);
      expect(result.events.some((event) => event._tag === "ToolCallStarted")).toBe(false);
    }),
  );

  it.effect(
    "composes host preparation with injected rollover and carries bounded unseen Tool evidence",
    () =>
      Effect.gen(function* () {
        const largeResult = `The timeout is a connection-pool limit. ${"diagnostic detail ".repeat(1_000)}`;

        const result = yield* driveRun({
          policy: AgentPolicy.make({ ...basePolicy, contextTokenLimit: 2_000 }),
          script: [call("unseen-evidence", "search"), done],
          searchResults: [largeResult],
        }).pipe(
          Effect.provide(ContextCompactor.layerRollover),
          Effect.provideService(RunContextPreparation, {
            hook: { prepare: ({ source }) => Effect.succeed({ prompt: source }) },
            transientContext: {
              load: () => Effect.succeed("Host reference: verify the active pool configuration."),
            },
          }),
        );

        expect(Exit.isSuccess(result.exit)).toBe(true);
        expect(result.requests).toHaveLength(2);
        expect(result.compactions.map((event) => event.kind)).toEqual(["rollover"]);
        const fresh = result.requests[1];

        if (fresh === undefined) throw new Error("Expected pressure recovery");
        expect(promptText(fresh.prompt)).toContain("The timeout is a connection-pool limit.");
        expect(promptText(fresh.prompt)).toContain("unseen-evidence");
        expect(promptText(fresh.prompt)).toContain(originalInput);
        expect(promptText(fresh.prompt)).not.toContain(largeResult);
        expect(toolResults(fresh.prompt)).toEqual([]);
        expect(result.requests.every((request) => request.toolCount === 2)).toBe(true);
        expect(
          result.requests.every((request) =>
            promptText(request.prompt).includes(
              "Host reference: verify the active pool configuration.",
            ),
          ),
        ).toBe(true);
        expect(result.statuses[0]?.status.contextTokenLimit).toBe(2_000);
        expect(result.statuses[0]?.status.remainingTokens).toBeGreaterThan(0);
      }),
  );

  it.effect("recovers a provider overflow with a fresh window and no summarizer request", () =>
    Effect.gen(function* () {
      const result = yield* driveRun({
        policy: AgentPolicy.make({ ...basePolicy, contextTokenLimit: 20_000 }),
        script: [call("overflow-evidence", "search"), { overflow: true }, done],
        searchResults: [`Evidence to recover. ${"detail ".repeat(1_000)}`],
      }).pipe(Effect.provide(ContextCompactor.layerRollover));

      if (Exit.isFailure(result.exit)) throw new Error(Cause.pretty(result.exit.cause));
      expect(Exit.isSuccess(result.exit)).toBe(true);
      expect(result.requests).toHaveLength(3);
      expect(result.compactions.map((event) => event.kind)).toEqual(["rollover"]);
      const fresh = result.requests[2];

      if (fresh === undefined) throw new Error("Expected an overflow retry");
      expect(promptText(fresh.prompt)).toContain("Evidence to recover.");
      expect(promptText(fresh.prompt)).toContain("overflow-evidence");
      expect(toolResults(fresh.prompt)).toEqual([]);
      expect(result.requests.every((request) => request.toolCount === 2)).toBe(true);
      expect(result.usageDeltas).toHaveLength(2);
    }),
  );

  it.effect("preserves steering arriving after the rollover call as a new user message", () =>
    Effect.gen(function* () {
      const queued = yield* Ref.make(false);
      const steering = "Investigate the staging environment before production.";

      const result = yield* driveRun({
        script: [call("steered-window", "new_context", { handoff: "Inspect production." }), done],
        onRollover: Ref.set(queued, true),
        input: {
          drain: () =>
            Ref.getAndSet(queued, false).pipe(
              Effect.map((ready) =>
                ready ? [{ kind: "steering" as const, input: steering }] : [],
              ),
            ),
        },
      });

      expect(Exit.isSuccess(result.exit)).toBe(true);
      expect(result.compactions).toHaveLength(1);
      const initial = result.requests[0];
      const fresh = result.requests[1];

      if (initial === undefined || fresh === undefined)
        throw new Error("Expected two model requests");
      expect(promptText(initial.prompt)).not.toContain(steering);
      expect(promptText(fresh.prompt)).toContain(originalInput);

      const steeringMessage = fresh.prompt.content.find(
        (message) =>
          message.role === "user" && promptText(Prompt.fromMessages([message])) === steering,
      );

      expect(steeringMessage).toBeDefined();
      expect(fresh.prompt.content.at(-1)).toBe(steeringMessage);
    }),
  );

  it.effect("does not replenish the cumulative Tool Call budget when the window changes", () =>
    Effect.gen(function* () {
      const result = yield* driveRun({
        policy: AgentPolicy.make({ ...basePolicy, maxToolCalls: 2, onExhaustion: "fail" }),
        script: [
          call("budget-window", "new_context"),
          call("budget-search", "search"),
          call("unaffordable-window", "new_context"),
          done,
        ],
      });

      const failure = failureFrom(result.exit);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect(failure).toMatchObject({ limit: "tool-calls" });
      expect(result.rolloverCount).toBe(1);
      expect(result.searchCount).toBe(1);
      expect(result.compactions).toHaveLength(1);
      expect(result.events.some((event) => event._tag === "RunCompleted")).toBe(false);
    }),
  );
});

// Regression seam: https://linear.app/reve/issue/KOM-125
layer(testLayer)("resolved model context", (it) => {
  it.effect("freezes routing before transient preparation and admits a smaller next model", () =>
    Effect.gen(function* () {
      const large = scriptedModel([call("switch-model", "search")], "large");
      const small = scriptedModel([call("small-status", "search"), done], "small");
      const selected = yield* Ref.make("large");
      const resolved: Array<string> = [];

      const result = yield* driveRun({
        script: [],
        searchResults: ["retained search evidence ".repeat(800), "small result"],
        context: {
          prepare: (request) =>
            Effect.gen(function* () {
              const route = yield* Ref.get(selected);

              resolved.push(route);

              return {
                prompt: request.source,
                modelCall: {
                  model: route === "large" ? large.model : small.model,
                  context: ModelCallContext.make({
                    contextCapacity: route === "large" ? 20_000 : 4_000,
                    maxInputTokens: route === "large" ? 15_000 : 2_000,
                    outputReserveTokens: 400,
                    uncountedOverheadTokens: 100,
                  }),
                },
              };
            }),
        },
        transientContext: {
          load: () => Ref.set(selected, "small").pipe(Effect.as(Prompt.empty)),
        },
      }).pipe(Effect.provide(ContextCompactor.layerRollover));

      expect(Exit.isSuccess(result.exit)).toBe(true);
      expect(result.requests).toHaveLength(0);
      expect(large.requests).toHaveLength(1);
      expect(small.requests).toHaveLength(2);
      expect(resolved).toEqual(["large", "small", "small"]);
      expect(result.compactions.map((event) => event.kind)).toEqual(["rollover"]);
      expect(result.statuses.map(({ status }) => status.contextTokenLimit)).toEqual([
        14_900, 1_900,
      ]);
      const firstSmall = small.requests[0];

      if (firstSmall === undefined) throw new Error("Expected the smaller model request");
      expect(promptText(firstSmall.prompt)).toContain("A fresh context window has started.");
      expect(promptText(firstSmall.prompt)).toContain(originalInput);
      expect(toolResults(firstSmall.prompt)).toEqual([]);
      expect(result.usageDeltas.map((entry) => entry.modelUsage?.model)).toEqual([
        "large",
        "small",
        "small",
      ]);
    }),
  );

  it.effect("commits a host-selected fresh window below pressure without a model Tool Call", () =>
    Effect.gen(function* () {
      const history = Prompt.make([
        { role: "user", content: "obsolete completed request" },
        { role: "assistant", content: "completed execution evidence" },
      ]);

      const result = yield* driveRun({
        script: [done],
        history,
        policy: AgentPolicy.make({ ...basePolicy, contextTokenLimit: 20_000 }),
        context: {
          prepare: (request) =>
            Effect.succeed({
              prompt: request.source,
              rollover: { through: history.content.length },
            }),
        },
      });

      expect(Exit.isSuccess(result.exit)).toBe(true);
      expect(result.rolloverCount).toBe(0);
      expect(result.compactions.map((event) => event.kind)).toEqual(["rollover"]);
      const first = result.requests[0];

      if (first === undefined) throw new Error("Expected the host rollover request");
      expect(promptText(first.prompt)).toContain(originalInput);
      expect(promptText(first.prompt)).not.toContain("completed execution evidence");
      expect(JSON.stringify(result.histories.at(-1))).toContain("completed execution evidence");
    }),
  );

  it.effect("rejects protected input beyond routed capacity before model I/O", () =>
    Effect.gen(function* () {
      const routed = scriptedModel([done]);

      const result = yield* driveRun({
        script: [],
        agentInput: "protected user content ".repeat(500),
        context: {
          prepare: (request) =>
            Effect.succeed({
              prompt: request.source,
              modelCall: {
                model: routed.model,
                context: ModelCallContext.make({
                  contextCapacity: 1_000,
                  outputReserveTokens: 200,
                  uncountedOverheadTokens: 100,
                }),
              },
            }),
        },
      }).pipe(Effect.provide(ContextCompactor.layerRollover));

      expect(failureFrom(result.exit)).toBeInstanceOf(ContextBudgetError);
      expect(routed.requests).toHaveLength(0);
      expect(result.compactions).toHaveLength(0);
    }),
  );

  it.effect("keeps pressure rollover available when a default host reset has no prior prefix", () =>
    Effect.gen(function* () {
      const result = yield* driveRun({
        script: [call("current-search", "search"), done],
        searchResults: ["completed action evidence ".repeat(600)],
        policy: AgentPolicy.make({ ...basePolicy, contextTokenLimit: 2_000 }),
        context: {
          prepare: (request) => Effect.succeed({ prompt: request.source, rollover: {} }),
        },
      }).pipe(Effect.provide(ContextCompactor.layerRollover));

      expect(Exit.isSuccess(result.exit)).toBe(true);
      expect(result.compactions).toHaveLength(1);
      expect(result.requests).toHaveLength(2);
      const first = result.requests[0];
      const fresh = result.requests[1];

      if (first === undefined || fresh === undefined) throw new Error("Expected both model calls");
      expect(promptText(first.prompt)).not.toContain("A fresh context window has started.");
      expect(promptText(fresh.prompt)).toContain("A fresh context window has started.");
      expect(promptText(fresh.prompt)).toContain(originalInput);
      expect(toolResults(fresh.prompt)).toEqual([]);
    }),
  );

  for (const ending of [
    "completed",
    "preparation-failed",
    "interrupted",
    "reserve-exceeds-capacity",
    "overhead-exhausts-capacity",
  ] as const) {
    it.effect(`owns resolved model resources per Turn when ${ending}`, () =>
      Effect.gen(function* () {
        const lifecycle: Array<string> = [];
        const routed = scriptedModel([call("resource-search", "search"), done]);

        const result = yield* driveRun({
          script: [],
          context: {
            prepare: (request) =>
              Effect.sync(() => ({
                prompt: request.source,
                modelCall: {
                  model: Layer.merge(
                    routed.model,
                    Layer.effectDiscard(
                      Effect.acquireRelease(
                        Effect.sync(() => {
                          lifecycle.push(`open:${request.turn}`);
                        }),
                        () =>
                          Effect.sync(() => {
                            lifecycle.push(`close:${request.turn}`);
                          }),
                      ),
                    ),
                  ),
                  context: ModelCallContext.make({
                    contextCapacity: 10_000,
                    outputReserveTokens:
                      request.turn === 2 && ending === "reserve-exceeds-capacity" ? 10_001 : 400,
                    uncountedOverheadTokens:
                      request.turn === 2 && ending === "overhead-exhausts-capacity" ? 9_600 : 0,
                  }),
                },
              })),
          },
          transientContext: {
            load: (request) =>
              request.turn !== 2
                ? Effect.succeed(Prompt.empty)
                : ending === "preparation-failed"
                  ? CompactionError.make({ message: "transient preparation failed" })
                  : ending === "interrupted"
                    ? Effect.interrupt
                    : Effect.succeed(Prompt.empty),
          },
        });

        expect(lifecycle).toEqual(["open:1", "close:1", "open:2", "close:2"]);
        expect(routed.requests).toHaveLength(ending === "completed" ? 2 : 1);
        if (ending === "completed") expect(Exit.isSuccess(result.exit)).toBe(true);
        else if (ending === "interrupted") {
          expect(Exit.isFailure(result.exit) && Cause.hasInterrupts(result.exit.cause)).toBe(true);
        } else if (ending === "preparation-failed") {
          expect(failureFrom(result.exit)).toBeInstanceOf(CompactionError);
        } else expect(failureFrom(result.exit)).toBeInstanceOf(ContextBudgetError);
      }),
    );
  }

  it.effect("reuses the resolved native model for one provider-overflow retry", () =>
    Effect.gen(function* () {
      const routed = scriptedModel([{ overflow: true }, done]);
      let resolutions = 0;

      const result = yield* driveRun({
        script: [],
        history: Prompt.make([
          { role: "user", content: "old request" },
          { role: "assistant", content: "old execution evidence ".repeat(100) },
        ]),
        context: {
          prepare: (request) =>
            Effect.sync(() => {
              resolutions += 1;

              return {
                prompt: request.source,
                modelCall: {
                  model: routed.model,
                  context: ModelCallContext.make({
                    contextCapacity: 10_000,
                    outputReserveTokens: 200,
                    uncountedOverheadTokens: 0,
                  }),
                },
              };
            }),
        },
      }).pipe(Effect.provide(ContextCompactor.layerRollover));

      expect(Exit.isSuccess(result.exit)).toBe(true);
      expect(resolutions).toBe(1);
      expect(routed.requests).toHaveLength(2);
      expect(result.compactions.map((event) => event.kind)).toEqual(["rollover"]);
    }),
  );
});

typeTest("keeps handler dependencies typed while supplying the engine-owned ContextWindow", () => {
  const definition = definitionWith(AgentPolicy.make(basePolicy));
  const run = AgentRuntime.run(definition, originalInput);
  const stream = AgentRuntime.stream(definition, originalInput);

  expectTypeOf<
    Extract<Agent.DefinitionRequirements<typeof definition>, ContextWindow>
  >().toEqualTypeOf<ContextWindow>();
  expectTypeOf<Extract<Effect.Services<typeof run>, ContextWindow>>().toEqualTypeOf<never>();
  expectTypeOf<Extract<Stream.Services<typeof stream>, ContextWindow>>().toEqualTypeOf<never>();
  expectTypeOf<
    Extract<Effect.Services<typeof run>, Tool.HandlersFor<typeof toolkit.tools>>
  >().toEqualTypeOf<Tool.HandlersFor<typeof toolkit.tools>>();
});
