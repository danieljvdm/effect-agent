import { expect, layer } from "@effect/vitest";
import { Effect, Exit, Layer, Ref, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import type { CompactionError } from "effect-agent/context-compactor";
import { ContextCompactor } from "effect-agent/context-compactor";
import {
  ContextRolloverRequest,
  ContextRolloverTool,
  ContextWindow,
  ModelCallContext,
} from "effect-agent/context-window";
import { IdGenerator } from "effect-agent/id-generator";
import { RunId, ThreadId, TurnId } from "effect-agent/identifiers";
import { type RunEvent } from "effect-agent/run-event";
import {
  RunContextPreparationPassthrough,
  type RunInputHook,
  type RunContextHook,
  type RunTransientContextHook,
} from "effect-agent/run-options";
import { ThreadHistory } from "effect-agent/thread-history";
import { LanguageModel, Model, Prompt, type Response, Tool, Toolkit } from "effect/unstable/ai";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => ThreadId.make(`rollover-thread-${++threadSequence}`)),
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

type ScriptEntry = ReadonlyArray<Response.StreamPartEncoded>;

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

interface RunSetup {
  readonly script: ReadonlyArray<ScriptEntry>;
  readonly searchResults?: ReadonlyArray<string>;
  readonly input?: RunInputHook;
  readonly onRollover?: Effect.Effect<void>;
  readonly context?: RunContextHook;
  readonly transientContext?: RunTransientContextHook<CompactionError>;
}

const driveRun = Effect.fn("context-rollover.test.driveRun")(function* (setup: RunSetup) {
  const { model, requests } = scriptedModel(setup.script);
  const events: Array<RunEvent> = [];
  let searchCount = 0;

  const handlers = toolkit.toLayer({
    new_context: Effect.fn("context-rollover.test.new_context")(function* (params) {
      if (setup.onRollover !== undefined) yield* setup.onRollover;

      return params;
    }),
    search: Effect.fn("context-rollover.test.search")(() =>
      Effect.sync(() => {
        const result = setup.searchResults?.[searchCount] ?? "evidence";

        searchCount += 1;

        return result;
      }),
    ),
  });

  const exit = yield* AgentRuntime.stream(
    Agent.withModel(definitionWith(AgentPolicy.make(basePolicy)), model),
    originalInput,
    {
      ...(setup.input === undefined ? {} : { input: setup.input }),
      ...(setup.context === undefined ? {} : { context: setup.context }),
      ...(setup.transientContext === undefined ? {} : { transientContext: setup.transientContext }),
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
    compactions: events.filter((event) => event._tag === "CompactionPerformed"),
  };
});

const testLayer = Layer.mergeAll(
  identifiers,
  ContextCompactor.layer,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer)("native context windows", (it) => {
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
});

// Regression seam: https://linear.app/reve-ai/issue/KOM-125
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
      const firstSmall = small.requests[0];

      if (firstSmall === undefined) throw new Error("Expected the smaller model request");
      expect(promptText(firstSmall.prompt)).toContain("A fresh context window has started.");
      expect(promptText(firstSmall.prompt)).toContain(originalInput);
      expect(toolResults(firstSmall.prompt)).toEqual([]);
    }),
  );
});
