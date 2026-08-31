import {
  Agent,
  AgentPolicy,
  AgentPolicyError,
  ConversationId,
  ModelProtocolError,
  IdGenerator,
  RunId,
  ToolResultBounds,
  TruncatedToolResult,
  TurnId,
  UnserializableToolResult,
  type RunCompleted,
  type RunEvent,
} from "@effect-agent/core";
import { expect, layer } from "@effect/vitest";
import { Cause, DateTime, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  LanguageModel,
  Model,
  type Prompt,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";

import { ConversationHistory } from "../src/conversation-history.ts";
import {
  AgentRuntime,
  formatRunStatus,
  type RunDurabilityHook,
  type RunTurnResume,
  type RunUsageDelta,
} from "../src/index.ts";

const identifiers = Layer.succeed(IdGenerator, {
  nextConversationId: Effect.succeed(Schema.decodeSync(ConversationId)("conversation-1")),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-1")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-1")),
});

type FinishUsage = Extract<Response.StreamPartEncoded, { readonly type: "finish" }>["usage"];

const emptyUsage: FinishUsage = { inputTokens: {}, outputTokens: {} };

const emptyPolicyUsage = {
  committedTurns: 0,
  toolCalls: 0,
  programmaticToolCalls: 0,
  consecutiveToolFailures: 0,
  finalizationUsed: false,
};

const emptyResumeUsage = {
  ...emptyPolicyUsage,
  modelCalls: 0,
  inputTokens: 0,
  outputTokens: 0,
  lastInputTokens: 0,
  lastOutputTokens: 0,
  costMicrousd: 0,
};

const usageOf = (input: number, output: number): FinishUsage => ({
  inputTokens: { total: input },
  outputTokens: { total: output },
});

const finalParts = (
  text: string,
  usage: FinishUsage = emptyUsage,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolCallParts = (
  id: string,
  name: string,
  params: Record<string, unknown>,
  usage: FinishUsage = emptyUsage,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "tool-call", id, name, params, providerExecuted: false },
  { type: "finish", reason: "tool-calls", usage },
];

interface CapturedRequest {
  readonly prompt: Prompt.Prompt;
  readonly toolCount: number;
  readonly toolChoice: unknown;
}

/** Scripted multi-call model: one parts script per model request, with request capture. */
const scriptedModel = (script: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>) => {
  const requests: Array<CapturedRequest> = [];
  const model = Model.make(
    "scripted",
    "context-economics",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          const index = Math.min(requests.length, script.length - 1);
          requests.push({
            prompt: request.prompt,
            toolCount: request.tools.length,
            toolChoice: request.toolChoice,
          });
          return Stream.fromIterable(script[index] ?? []);
        },
      }),
    ),
  );
  return { model, requests };
};

const messageText = (message: Prompt.Prompt["content"][number]): string => {
  if (typeof message.content === "string") {
    return message.content;
  }
  return message.content
    .map((part) => ("text" in part && typeof part.text === "string" ? part.text : ""))
    .join("");
};

const promptText = (prompt: Prompt.Prompt): string =>
  prompt.content.map((message) => messageText(message)).join("\n");

const occurrences = (haystack: string, needle: string): number => haystack.split(needle).length - 1;

const toolResultValues = (prompt: Prompt.Prompt): ReadonlyArray<unknown> =>
  prompt.content.flatMap((message) =>
    typeof message.content === "string"
      ? []
      : message.content.flatMap((part) =>
          part.type === "tool-result" ? [part.result as unknown] : [],
        ),
  );

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

const EmitTool = Tool.make("emit", {
  parameters: Schema.Struct({}),
  success: Schema.Struct({ data: Schema.String }),
});
const emitToolkit = Toolkit.make(EmitTool);

const SearchTool = Tool.make("search", {
  parameters: Schema.Struct({}),
  success: Schema.String,
});
const searchToolkit = Toolkit.make(SearchTool);

const PostMessageTool = Tool.make("post_message", {
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.Struct({ messageId: Schema.String }),
});
const postMessageToolkit = Toolkit.make(PostMessageTool);

const answerOutput = Schema.Struct({ answer: Schema.String });

const testLayer = Layer.merge(identifiers, ConversationHistory.layerTransient);

layer(testLayer)("context economics — bounding, tracking, status, exhaustion", (it) => {
  // ---------------------------------------------------------------- RUN-022

  it.effect(
    "RUN-022: bounds an oversized application Tool result into the TruncatedToolResult envelope for prompt and events",
    () =>
      Effect.gen(function* () {
        const bigData = "x".repeat(3_000);
        const definition = Agent.make("bounds-oversized", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Use the tool once, then answer.",
          toolkit: emitToolkit,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            toolResultBounds: ToolResultBounds.make({ maxBytes: 1_024 }),
          }),
        });
        const { model, requests } = scriptedModel([
          toolCallParts("emit-1", "emit", {}),
          finalParts('{"answer":"done"}'),
        ]);
        const toolLayer = emitToolkit.toLayer({
          emit: () => Effect.succeed({ data: bigData }),
        });
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);

        yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "big",
        }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
        );

        expect(requests).toHaveLength(2);
        const second = requests[1];
        if (second === undefined) throw new Error("expected a second model request");
        const results = toolResultValues(second.prompt);
        expect(results).toHaveLength(1);
        const envelope = Schema.decodeUnknownSync(TruncatedToolResult)(results[0]);
        const originalEncoded = JSON.stringify({ data: bigData });
        expect(envelope.originalBytes).toBe(originalEncoded.length);
        expect(originalEncoded.startsWith(envelope.head)).toBe(true);
        expect(originalEncoded.endsWith(envelope.tail)).toBe(true);
        expect(JSON.stringify(results[0]).length).toBeLessThanOrEqual(1_024);

        // The success event carries the same bounded value as the prompt.
        const succeeded = (yield* Ref.get(events)).find(
          (event) => event._tag === "ToolCallSucceeded",
        );
        expect(succeeded).toBeDefined();
        if (succeeded === undefined || succeeded._tag !== "ToolCallSucceeded") {
          throw new Error("expected ToolCallSucceeded");
        }
        expect(succeeded.result).toEqual(results[0]);
      }),
  );

  it.effect("RUN-022: leaves within-bounds Tool results unchanged", () =>
    Effect.gen(function* () {
      const definition = Agent.make("bounds-small", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Use the tool once, then answer.",
        toolkit: emitToolkit,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model, requests } = scriptedModel([
        toolCallParts("emit-1", "emit", {}),
        finalParts('{"answer":"done"}'),
      ]);
      const toolLayer = emitToolkit.toLayer({
        emit: () => Effect.succeed({ data: "small" }),
      });

      yield* AgentRuntime.run(Agent.withModel(definition, model), { question: "small" }).pipe(
        Effect.provide(toolLayer),
      );

      const second = requests[1];
      if (second === undefined) throw new Error("expected a second model request");
      expect(toolResultValues(second.prompt)).toEqual([{ data: "small" }]);
    }),
  );

  it.effect("RUN-022: the default policy bounds Tool results at 50 KiB", () =>
    Effect.gen(function* () {
      const definition = Agent.make("bounds-default", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Use the tool once, then answer.",
        toolkit: emitToolkit,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model, requests } = scriptedModel([
        toolCallParts("emit-1", "emit", {}),
        finalParts('{"answer":"done"}'),
      ]);
      const toolLayer = emitToolkit.toLayer({
        emit: () => Effect.succeed({ data: "y".repeat(200_000) }),
      });

      yield* AgentRuntime.run(Agent.withModel(definition, model), { question: "huge" }).pipe(
        Effect.provide(toolLayer),
      );

      const second = requests[1];
      if (second === undefined) throw new Error("expected a second model request");
      const results = toolResultValues(second.prompt);
      const envelope = Schema.decodeUnknownSync(TruncatedToolResult)(results[0]);
      expect(envelope.originalBytes).toBe(JSON.stringify({ data: "y".repeat(200_000) }).length);
      expect(JSON.stringify(results[0]).length).toBeLessThanOrEqual(50 * 1024);
    }),
  );

  // ---------------------------------------------------------------- RUN-023

  it.effect(
    "RUN-023: forwards raw cache splits per call and tracks the last call's input as the live-context estimate",
    () =>
      Effect.gen(function* () {
        const deltas = yield* Ref.make<ReadonlyArray<RunUsageDelta>>([]);
        const definition = Agent.make("live-context", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Search once, then answer.",
          toolkit: searchToolkit,
          policy: AgentPolicy.make({
            maxTurns: 10,
            maxToolCalls: 10,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 1_000,
          }),
        });
        const { model, requests } = scriptedModel([
          [
            { type: "tool-call", id: "s1", name: "search", params: {}, providerExecuted: false },
            {
              type: "finish",
              reason: "tool-calls",
              usage: {
                inputTokens: { uncached: 2, cacheRead: 3, cacheWrite: 4 },
                outputTokens: { total: 11 },
              },
            },
          ],
          finalParts('{"answer":"done"}', usageOf(150, 5)),
        ]);
        const toolLayer = searchToolkit.toLayer({ search: () => Effect.succeed("found") });

        yield* AgentRuntime.run(
          Agent.withModel(definition, model),
          { question: "count" },
          {
            budget: {
              guard: (effect) => effect,
              consume: (delta) => Ref.update(deltas, (all) => [...all, delta]),
            },
          },
        ).pipe(Effect.provide(toolLayer));

        const observed = yield* Ref.get(deltas);
        expect(observed).toHaveLength(2);
        expect(observed[0]?.inputTokens).toBe(9);
        expect(observed[0]?.usage.inputTokens.cacheRead).toBe(3);
        expect(observed[0]?.usage.inputTokens.cacheWrite).toBe(4);
        expect(observed[0]?.modelUsage).toMatchObject({
          provider: "scripted",
          model: "context-economics",
          inputTokens: { total: 9, uncached: 2, cacheRead: 3, cacheWrite: 4 },
        });
        expect(observed[1]?.inputTokens).toBe(150);
        expect(observed[1]?.modelUsage).toMatchObject({
          inputTokens: { total: 150, uncached: 150, cacheRead: 0, cacheWrite: 0 },
          outputTokens: { total: 5, text: 5, reasoning: 0 },
        });

        // The second request's status message reflects the FIRST call's input,
        // not the cumulative total.
        const second = requests[1];
        if (second === undefined) throw new Error("expected a second model request");
        expect(promptText(second.prompt)).toContain("last-context 9");
      }),
  );

  it.effect("RUN-023: separates cache writes already included in provider uncached input", () =>
    Effect.gen(function* () {
      const deltas = yield* Ref.make<ReadonlyArray<RunUsageDelta>>([]);
      const definition = Agent.make("overlapping-cache-write", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const rawUsage: FinishUsage = {
        inputTokens: { total: 100, uncached: 90, cacheRead: 10, cacheWrite: 40 },
        outputTokens: { total: 5, text: 5, reasoning: 0 },
      };
      const { model } = scriptedModel([finalParts('{"answer":"done"}', rawUsage)]);

      yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "q" },
        {
          budget: {
            guard: (effect) => effect,
            consume: (delta) => Ref.update(deltas, (all) => [...all, delta]),
          },
        },
      );

      const observed = (yield* Ref.get(deltas))[0];
      if (observed === undefined) throw new Error("expected one usage delta");
      expect(observed.inputTokens).toBe(100);
      expect(observed.usage).toEqual(rawUsage);
      if (observed.modelUsage === undefined) throw new Error("expected canonical model usage");
      expect(observed.modelUsage.inputTokens).toEqual({
        total: 100,
        uncached: 50,
        cacheRead: 10,
        cacheWrite: 40,
      });
    }),
  );

  it.effect("RUN-023: rejects malformed provider token usage instead of counting it as zero", () =>
    Effect.gen(function* () {
      const estimatorCalls = yield* Ref.make(0);
      const definition = Agent.make("invalid-provider-usage", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const malformed: ReadonlyArray<FinishUsage> = [
        { inputTokens: { total: -1 }, outputTokens: {} },
        { inputTokens: { uncached: -1 }, outputTokens: {} },
        { inputTokens: { cacheRead: -1 }, outputTokens: {} },
        { inputTokens: { cacheWrite: -1 }, outputTokens: {} },
        { inputTokens: {}, outputTokens: { total: -1 } },
        { inputTokens: {}, outputTokens: { text: -1 } },
        { inputTokens: {}, outputTokens: { reasoning: -1 } },
        // Effect AI rejects non-integers and unsafe integers before this seam.
        // This pair proves that individually valid fields cannot overflow a
        // derived total and escape accounting.
        {
          inputTokens: {
            uncached: Number.MAX_SAFE_INTEGER,
            cacheRead: Number.MAX_SAFE_INTEGER,
          },
          outputTokens: {},
        },
        // Reported totals cannot contradict explicitly reported components.
        { inputTokens: { total: 0, cacheRead: 100 }, outputTokens: {} },
        {
          inputTokens: { total: 200, uncached: 50, cacheRead: 50, cacheWrite: 0 },
          outputTokens: {},
        },
        { inputTokens: {}, outputTokens: { total: 0, text: 100 } },
        { inputTokens: {}, outputTokens: { total: 200, text: 50, reasoning: 50 } },
      ];

      for (const usage of malformed) {
        const { model, requests } = scriptedModel([finalParts('{"answer":"invalid"}', usage)]);
        const exit = yield* AgentRuntime.run(
          Agent.withModel(definition, model),
          { question: "q" },
          {
            estimateCostMicrousd: () =>
              Ref.update(estimatorCalls, (count) => count + 1).pipe(Effect.as(0)),
          },
        ).pipe(Effect.exit);

        expect(failureFrom(exit)).toBeInstanceOf(ModelProtocolError);
        expect(requests).toHaveLength(1);
      }
      expect(yield* Ref.get(estimatorCalls)).toBe(0);
    }),
  );

  it.effect("RUN-023: assigns aggregate remainders only to omitted provider components", () =>
    Effect.gen(function* () {
      const deltas = yield* Ref.make<ReadonlyArray<RunUsageDelta>>([]);
      const definition = Agent.make("provider-usage-remainder", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model } = scriptedModel([
        finalParts('{"answer":"done"}', {
          inputTokens: { total: 100, uncached: 20, cacheRead: 30 },
          outputTokens: { total: 20, text: 5 },
        }),
      ]);

      yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "q" },
        {
          budget: {
            guard: (effect) => effect,
            consume: (delta) => Ref.update(deltas, (all) => [...all, delta]),
          },
        },
      );

      expect((yield* Ref.get(deltas))[0]?.modelUsage).toMatchObject({
        inputTokens: { total: 100, uncached: 20, cacheRead: 30, cacheWrite: 50 },
        outputTokens: { total: 20, text: 5, reasoning: 15 },
      });
    }),
  );

  // ---------------------------------------------------------------- RUN-024

  it.effect(
    "RUN-024: appends one derived run-status message per outgoing request and never persists it",
    () =>
      Effect.gen(function* () {
        const histories: Array<Prompt.Prompt> = [];
        const definition = Agent.make("status-appended", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Search once, then answer.",
          toolkit: searchToolkit,
          policy: AgentPolicy.make({
            maxTurns: 10,
            maxToolCalls: 10,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 1_000,
          }),
        });
        const { model, requests } = scriptedModel([
          toolCallParts("s1", "search", {}, usageOf(100, 20)),
          finalParts('{"answer":"done"}', usageOf(140, 5)),
        ]);
        const toolLayer = searchToolkit.toLayer({ search: () => Effect.succeed("found") });

        yield* AgentRuntime.run(
          Agent.withModel(definition, model),
          { question: "status" },
          {
            onHistory: (history) =>
              Effect.sync(() => {
                histories.push(history);
              }),
          },
        ).pipe(Effect.provide(toolLayer));

        expect(requests).toHaveLength(2);
        const first = requests[0];
        const second = requests[1];
        if (first === undefined || second === undefined) throw new Error("expected two requests");

        const firstLast = first.prompt.content.at(-1);
        expect(firstLast?.role).toBe("user");
        expect(messageText(firstLast!)).toBe(
          "<run-status>turn 1/10 · tool-calls 0/10 · tokens 0/1000 · research-remaining 800 · completion-reserve 200 · last-context 0 · elapsed 0s/30s</run-status>",
        );

        const secondLast = second.prompt.content.at(-1);
        expect(messageText(secondLast!)).toBe(
          "<run-status>turn 2/10 · tool-calls 1/10 · tokens 120/1000 · research-remaining 680 · completion-reserve 200 · last-context 100 · elapsed 0s/30s</run-status>",
        );
        expect(occurrences(promptText(second.prompt), "<run-status>")).toBe(1);

        expect(histories.length).toBeGreaterThan(0);
        for (const history of histories) {
          expect(promptText(history)).not.toContain("<run-status>");
        }
      }),
  );

  it.effect("RUN-024: a tightened deadline does not fabricate elapsed time or a warning", () =>
    Effect.gen(function* () {
      const definition = Agent.make("status-tightened-deadline", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 10,
          maxToolCalls: 10,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model, requests } = scriptedModel([finalParts('{"answer":"done"}')]);
      const attemptStartedAt = yield* DateTime.now;
      const durationDeadline = DateTime.addDuration(attemptStartedAt, "5 seconds");

      yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "status" },
        { durationDeadline },
      );

      const request = requests[0];
      if (request === undefined) throw new Error("expected one request");
      expect(messageText(request.prompt.content.at(-1)!)).toBe(
        "<run-status>turn 1/10 · tool-calls 0/10 · tokens 0/unbounded · last-context 0 · elapsed 0s/30s</run-status>",
      );
    }),
  );

  it.effect("RUN-024: a resumed Run reports elapsed time from its supplied logical start", () =>
    Effect.gen(function* () {
      const definition = Agent.make("status-resumed-start", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer from the restored result.",
        toolkit: searchToolkit,
        policy: AgentPolicy.make({
          maxTurns: 10,
          maxToolCalls: 10,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model, requests } = scriptedModel([finalParts('{"answer":"done"}')]);
      const toolLayer = searchToolkit.toLayer({ search: () => Effect.succeed("unexpected") });
      const runStartedAt = yield* DateTime.now;
      const durationDeadline = DateTime.addDuration(runStartedAt, "30 seconds");
      const resumedTurnId = yield* Schema.decodeEffect(TurnId)("turn-resumed-status");
      const resume: RunTurnResume = {
        turn: 1,
        turnId: resumedTurnId,
        calls: [{ id: "search-1", name: "search", params: {} }],
        settled: [{ id: "search-1", result: "found", isFailure: false }],
      };
      yield* TestClock.adjust("12 seconds");

      yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "status" },
        {
          runStartedAt,
          durationDeadline,
          resume,
          resumeUsage: { ...emptyResumeUsage, committedTurns: 1, toolCalls: 1, modelCalls: 1 },
        },
      ).pipe(Effect.provide(toolLayer));

      const request = requests[0];
      if (request === undefined) throw new Error("expected one request");
      expect(messageText(request.prompt.content.at(-1)!)).toBe(
        "<run-status>turn 2/10 · tool-calls 1/10 · tokens 0/unbounded · last-context 0 · elapsed 12s/30s</run-status>",
      );
    }),
  );

  it.effect("RUN-024: omits the run-status message when policy runStatus is off", () =>
    Effect.gen(function* () {
      const definition = Agent.make("status-off", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          runStatus: "off",
        }),
      });
      const { model, requests } = scriptedModel([finalParts('{"answer":"quiet"}')]);

      yield* AgentRuntime.run(Agent.withModel(definition, model), { question: "quiet" });

      const first = requests[0];
      if (first === undefined) throw new Error("expected one request");
      expect(promptText(first.prompt)).not.toContain("<run-status>");
    }),
  );

  it.effect(
    "RUN-024: the run-status message carries the wrap-up warning at 80% of a dimension",
    () =>
      Effect.gen(function* () {
        const definition = Agent.make("status-warning", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Search once, then answer.",
          toolkit: searchToolkit,
          policy: AgentPolicy.make({
            maxTurns: 10,
            maxToolCalls: 10,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 10_000,
          }),
        });
        const { model, requests } = scriptedModel([
          toolCallParts("s1", "search", {}, usageOf(7_000, 1_500)),
          finalParts('{"answer":"done"}', usageOf(1, 1)),
        ]);
        const toolLayer = searchToolkit.toLayer({ search: () => Effect.succeed("found") });

        yield* AgentRuntime.run(Agent.withModel(definition, model), { question: "warn" }).pipe(
          Effect.provide(toolLayer),
        );

        const first = requests[0];
        const second = requests[1];
        if (first === undefined || second === undefined) throw new Error("expected two requests");
        expect(promptText(first.prompt)).not.toContain("WARNING:");
        expect(messageText(second.prompt.content.at(-1)!)).toBe(
          "<run-status>turn 2/10 · tool-calls 1/10 · tokens 8500/10000 · research-remaining 0 · completion-reserve 2000 · last-context 7000 · elapsed 0s/30s · WARNING: approaching limits — converge and deliver your final result now.</run-status>",
        );
      }),
  );

  it.effect("RUN-024: formatRunStatus renders unbounded token budgets", () =>
    Effect.sync(() => {
      expect(
        formatRunStatus({
          turn: 3,
          maxTurns: 16,
          toolCallsUsed: 5,
          maxToolCalls: 32,
          tokensConsumed: 1234,
          tokenBudget: undefined,
          lastInputTokens: 456,
          elapsedSeconds: 78,
          maxDurationSeconds: 360,
        }),
      ).toBe(
        "<run-status>turn 3/16 · tool-calls 5/32 · tokens 1234/unbounded · last-context 456 · elapsed 78s/360s</run-status>",
      );
    }),
  );

  it.effect("warns before the final reserve makes the next research call unaffordable", () =>
    Effect.sync(() => {
      const status = formatRunStatus({
        turn: 4,
        maxTurns: 8,
        toolCallsUsed: 30,
        maxToolCalls: 128,
        tokensConsumed: 180_000,
        tokenBudget: 416_000,
        completionReserveTokens: 160_000,
        lastInputTokens: 80_000,
        elapsedSeconds: 80,
        maxDurationSeconds: 300,
      });
      expect(status).toContain("tokens 180000/416000");
      expect(status).toContain("research-remaining 76000 · completion-reserve 160000");
      expect(status).toContain("WARNING:");
    }),
  );

  // ---------------------------------------------------------------- RUN-025

  it.effect(
    "RUN-025: emits BudgetWarning once when cumulative tokens cross 80% of the budget",
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const definition = Agent.make("token-warning", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Search twice, then answer.",
          toolkit: searchToolkit,
          policy: AgentPolicy.make({
            maxTurns: 10,
            maxToolCalls: 10,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 10_000,
          }),
        });
        const { model } = scriptedModel([
          toolCallParts("s1", "search", {}, usageOf(800, 200)),
          toolCallParts("s2", "search", {}, usageOf(7_500, 0)),
          finalParts('{"answer":"done"}', usageOf(500, 0)),
        ]);
        const toolLayer = searchToolkit.toLayer({ search: () => Effect.succeed("found") });

        yield* AgentRuntime.stream(Agent.withModel(definition, model), { question: "warn" }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
        );

        const warnings = (yield* Ref.get(events)).filter((event) => event._tag === "BudgetWarning");
        expect(warnings).toHaveLength(1);
        expect(warnings[0]).toMatchObject({
          limit: "tokens",
          consumed: 8_500,
          limitValue: 10_000,
        });
      }),
  );

  it.effect(
    "RUN-025: an ordinary completion carries neither budget-exhausted nor the exhausted marker",
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const definition = Agent.make("ordinary-completion", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Answer.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 4,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 100_000,
          }),
        });
        const { model } = scriptedModel([finalParts('{"answer":"done"}', usageOf(2, 2))]);

        const result = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "answer",
        }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.andThen(Ref.get(events)),
        );

        const completed = result.find(
          (event): event is RunCompleted => event._tag === "RunCompleted",
        );
        expect(completed).toBeDefined();
        // The exhausted marker pairs with budget-exhausted exactly: an
        // ordinary stop carries neither (core events JSDoc invariant).
        expect(completed).toMatchObject({ finishReason: "model-stop" });
        expect(completed?.exhausted).toBeUndefined();
      }),
  );

  it.effect(
    "RUN-011: token exhaustion on a stop response completes with the answer and the exhausted marker",
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const definition = Agent.make("token-exhausted-stop", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Answer.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 3,
          }),
        });
        const { model, requests } = scriptedModel([
          finalParts('{"answer":"overrun"}', usageOf(2, 2)),
        ]);

        const result = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "answer",
        }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.andThen(Ref.get(events)),
        );

        // A single model call: the breaching response already carries the
        // final answer, so no extra finalize call is spent.
        expect(requests).toHaveLength(1);
        const completed = result.find((event) => event._tag === "RunCompleted");
        expect(completed).toBeDefined();
        expect(completed).toMatchObject({
          output: { answer: "overrun" },
          finishReason: "budget-exhausted",
          exhausted: "tokens",
        });
      }),
  );

  it.effect(
    "RUN-025: token exhaustion on a Tool-declaring response settles the batch synthetically and grants one constrained grace Turn",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const commits = yield* Ref.make(0);
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const definition = Agent.make("token-exhausted-tools", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Search, then answer.",
          toolkit: searchToolkit,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 10_000,
          }),
        });
        const { model, requests } = scriptedModel([
          toolCallParts("s1", "search", {}, usageOf(9_000, 4_000)),
          finalParts('{"answer":"partial"}', usageOf(3_000, 1_000)),
        ]);
        const toolLayer = searchToolkit.toLayer({
          search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
        });
        const durability: RunDurabilityHook = {
          commitResponse: () => Ref.update(commits, (count) => count + 1),
          // Required by the durability protocol; this harness exercises neither seam.
          commitCompaction: () => Effect.void,
          noteTurnUsage: () => Effect.void,
          prepareToolCalls: () => Effect.void,
          step: {
            lookup: () => Effect.succeed(Option.none()),
            commit: () => Effect.void,
          },
        };

        const exit = yield* AgentRuntime.stream(
          Agent.withModel(definition, model),
          { question: "exhaust" },
          { durability },
        ).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
          Effect.exit,
        );

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(requests).toHaveLength(2);
        // The breaching batch never executed a handler and, per the RUN-018
        // synthetic-settlement path it joins, was never durably committed as
        // a response; the model sees the rejection as a failed tool result.
        expect(yield* Ref.get(handlerStarts)).toBe(0);
        expect(yield* Ref.get(commits)).toBe(0);
        const grace = requests[1];
        if (grace === undefined) throw new Error("expected the grace request");
        expect(grace.toolChoice).toBe("none");
        const rejectionResults = toolResultValues(grace.prompt);
        expect(rejectionResults).toHaveLength(1);
        expect(rejectionResults[0]).toMatchObject({
          limit: "tokens",
          message: expect.stringContaining("Token budget exhausted"),
        });

        const observed = yield* Ref.get(events);
        const rejected = observed.filter((event) => event._tag === "ToolCallFailed");
        expect(rejected).toHaveLength(1);
        expect(observed.some((event) => event._tag === "ToolCallStarted")).toBe(false);
        const completed = observed.find((event) => event._tag === "RunCompleted");
        expect(completed).toMatchObject({
          output: { answer: "partial" },
          finishReason: "budget-exhausted",
          exhausted: "tokens",
        });
      }),
  );

  it.effect(
    "RUN-032: required completion uses native required Tool choice until the completion Tool settles",
    () =>
      Effect.gen(function* () {
        const SearchThenPost = Toolkit.make(SearchTool, PostMessageTool);
        const definition = Agent.make("required-terminal-tool", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
          instructions: "Research, then deliver with post_message.",
          toolkit: SearchThenPost,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
          completion: {
            tool: "post_message",
            required: true,
            project: ({ parameters, result }) => ({
              message: parameters.message,
              messageId: result.messageId,
            }),
          },
        });
        const { model, requests } = scriptedModel([
          toolCallParts("research-required", "search", {}),
          toolCallParts("delivery-required", "post_message", { message: "delivered" }),
        ]);
        const toolLayer = SearchThenPost.toLayer({
          search: () => Effect.succeed("found"),
          post_message: () => Effect.succeed({ messageId: "message-required" }),
        });

        const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
          question: "deliver",
        }).pipe(Effect.provide(toolLayer));

        expect(requests.map((request) => request.toolChoice)).toEqual(["required", "required"]);
        expect(result.output).toEqual({
          message: "delivered",
          messageId: "message-required",
        });
      }),
  );

  it.effect(
    "RUN-032: required completion rejects an ordinary final text response without retrying",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const definition = Agent.make("required-terminal-tool-text-stop", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
          instructions: "Deliver with post_message.",
          toolkit: postMessageToolkit,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
          completion: {
            tool: "post_message",
            required: true,
            project: ({ parameters, result }) => ({
              message: parameters.message,
              messageId: result.messageId,
            }),
          },
        });
        const { model, requests } = scriptedModel([
          finalParts('{"message":"looks valid","messageId":"but is text"}'),
          toolCallParts("must-not-retry", "post_message", { message: "retry" }),
        ]);
        const toolLayer = postMessageToolkit.toLayer({
          post_message: () =>
            Ref.update(handlerStarts, (count) => count + 1).pipe(
              Effect.as({ messageId: "must-not-exist" }),
            ),
        });

        const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), {
          question: "deliver",
        }).pipe(Effect.provide(toolLayer), Effect.exit);
        const failure = failureFrom(exit);

        expect(failure).toBeInstanceOf(ModelProtocolError);
        expect(failure).toMatchObject({
          message: expect.stringContaining("required completion Tool post_message"),
        });
        expect(requests.map((request) => request.toolChoice)).toEqual(["required"]);
        expect(yield* Ref.get(handlerStarts)).toBe(0);
      }),
  );

  it.effect(
    "RUN-032: final-answer mode lets an authorized completion Tool settle when its response crosses the token budget",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const definition = Agent.make("token-exhausted-terminal-tool", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
          instructions: "Deliver the final answer with post_message.",
          toolkit: postMessageToolkit,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 10_000,
          }),
          completion: {
            tool: "post_message",
            project: ({ parameters, result }) => ({
              message: parameters.message,
              messageId: result.messageId,
            }),
          },
        });
        const { model, requests } = scriptedModel([
          toolCallParts(
            "delivery-1",
            "post_message",
            { message: "delivered" },
            usageOf(9_000, 4_000),
          ),
          finalParts('{"message":"private summary","messageId":"wrong"}'),
        ]);
        const toolLayer = postMessageToolkit.toLayer({
          post_message: () =>
            Ref.update(handlerStarts, (count) => count + 1).pipe(
              Effect.as({ messageId: "message-1" }),
            ),
        });

        const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
          question: "deliver",
        }).pipe(Effect.provide(toolLayer));

        expect(requests).toHaveLength(1);
        expect(yield* Ref.get(handlerStarts)).toBe(1);
        expect(result).toMatchObject({
          output: { message: "delivered", messageId: "message-1" },
          finishReason: "budget-exhausted",
          exhausted: "tokens",
        });
        const request = requests[0];
        if (request === undefined) throw new Error("Missing completion Tool request");
        const prompt = promptText(request.prompt);
        expect(prompt).toContain('without calling the "post_message" completion Tool');
        expect(prompt).toContain(
          'When calling the "post_message" completion Tool, never place this private Agent output JSON in any Tool argument; follow the Tool\'s parameter schema instead.',
        );
        expect(prompt).toContain(
          "The engine projects the successful completion Tool result into the Agent output.",
        );
      }),
  );

  it.effect(
    "RUN-032: fail mode rejects a token-breaching completion Tool before its handler starts",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const definition = Agent.make("token-exhausted-terminal-tool-fail", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
          instructions: "Deliver the final answer with post_message.",
          toolkit: postMessageToolkit,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 10_000,
            onExhaustion: "fail",
          }),
          completion: {
            tool: "post_message",
            project: ({ parameters, result }) => ({
              message: parameters.message,
              messageId: result.messageId,
            }),
          },
        });
        const { model, requests } = scriptedModel([
          toolCallParts(
            "delivery-fail",
            "post_message",
            { message: "must not deliver" },
            usageOf(9_000, 4_000),
          ),
        ]);
        const toolLayer = postMessageToolkit.toLayer({
          post_message: () =>
            Ref.update(handlerStarts, (count) => count + 1).pipe(
              Effect.as({ messageId: "must-not-exist" }),
            ),
        });

        const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), {
          question: "deliver",
        }).pipe(Effect.provide(toolLayer), Effect.exit);
        const failure = failureFrom(exit);

        expect(failure).toBeInstanceOf(AgentPolicyError);
        expect((failure as AgentPolicyError).limit).toBe("tokens");
        expect(requests).toHaveLength(1);
        expect(yield* Ref.get(handlerStarts)).toBe(0);
      }),
  );

  it.effect.each([2, 3])("resumes recorded completion only through the final Turn: %s", (turn) =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      const definition = Agent.make("resume-grace-completion", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
        instructions: "Deliver with post_message.",
        toolkit: postMessageToolkit,
        policy: AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          onExhaustion: "final-answer",
        }),
        completion: {
          tool: "post_message",
          required: true,
          project: ({ parameters, result }) => ({
            message: parameters.message,
            messageId: result.messageId,
          }),
        },
      });
      const { model, requests } = scriptedModel([finalParts("must not call model")]);
      const turnId = yield* Schema.decodeEffect(TurnId)("recorded-completion");
      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "resume" },
        {
          resumeUsage: {
            ...emptyResumeUsage,
            committedTurns: turn,
            modelCalls: turn,
            toolCalls: 1,
            finalizationUsed: true,
          },
          resume: {
            turn,
            turnId,
            calls: [{ id: "delivery", name: "post_message", params: { message: "delivered" } }],
            settled: [{ id: "delivery", result: { messageId: "message-1" }, isFailure: false }],
          },
        },
      ).pipe(
        Effect.provide(
          postMessageToolkit.toLayer({
            post_message: () =>
              Ref.update(handlerStarts, (count) => count + 1).pipe(
                Effect.as({ messageId: "must-not-repeat" }),
              ),
          }),
        ),
        Effect.exit,
      );
      expect(requests).toHaveLength(0);
      expect(yield* Ref.get(handlerStarts)).toBe(0);
      if (turn === 2) {
        expect(Exit.isSuccess(exit)).toBe(true);
        if (Exit.isSuccess(exit))
          expect(exit.value).toMatchObject({
            output: { message: "delivered", messageId: "message-1" },
            finishReason: "budget-exhausted",
            exhausted: "turns",
          });
      } else {
        expect(failureFrom(exit)).toMatchObject({ _tag: "AgentPolicyError", limit: "turns" });
      }
    }),
  );

  it.effect("does not retry a failed completion after the single final Turn", () =>
    Effect.gen(function* () {
      const deliveryStarts = yield* Ref.make(0);
      const Finish = Tool.make("finish", {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: Schema.String,
        failureMode: "return",
      });
      const toolkit = Toolkit.make(SearchTool, Finish);
      const definition = Agent.make("failed-grace-completion", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Search then finish.",
        toolkit,
        policy: AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          repeatedFailureLimit: 0,
          onExhaustion: "final-answer",
        }),
        completion: {
          tool: "finish",
          required: true,
          project: ({ result }) => ({ answer: result }),
        },
      });
      const { model, requests } = scriptedModel([
        toolCallParts("research", "search", {}),
        toolCallParts("delivery", "finish", {}),
        toolCallParts("must-not-retry", "finish", {}),
      ]);
      const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "deliver",
      }).pipe(
        Effect.provide(
          toolkit.toLayer({
            search: () => Effect.succeed("found"),
            finish: () =>
              Ref.update(deliveryStarts, (count) => count + 1).pipe(
                Effect.andThen(Effect.fail("delivery failed")),
              ),
          }),
        ),
        Effect.exit,
      );
      expect(failureFrom(exit)).toMatchObject({ _tag: "AgentPolicyError", limit: "turns" });
      expect(requests).toHaveLength(2);
      expect(requests[1]?.toolChoice).toEqual({ tool: "finish" });
      expect(yield* Ref.get(deliveryStarts)).toBe(1);
    }),
  );

  it.effect("RUN-032: fail mode constrains terminal delivery exactly at the Turn limit", () =>
    Effect.gen(function* () {
      const searchStarts = yield* Ref.make(0);
      const deliveryStarts = yield* Ref.make(0);
      const SearchThenPost = Toolkit.make(SearchTool, PostMessageTool);
      const definition = Agent.make("turn-exhausted-terminal-tool-fail", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
        instructions: "Deliver the final answer with post_message.",
        toolkit: SearchThenPost,
        policy: AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          onExhaustion: "fail",
        }),
        completion: {
          tool: "post_message",
          required: true,
          project: ({ parameters, result }) => ({
            message: parameters.message,
            messageId: result.messageId,
          }),
        },
      });
      const { model, requests } = scriptedModel([
        toolCallParts("delivery-turn-exact", "post_message", { message: "delivered" }),
        finalParts('{"message":"must not summarize","messageId":"wrong"}'),
      ]);
      const toolLayer = SearchThenPost.toLayer({
        search: () => Ref.update(searchStarts, (count) => count + 1).pipe(Effect.as("found")),
        post_message: () =>
          Ref.update(deliveryStarts, (count) => count + 1).pipe(
            Effect.as({ messageId: "message-exact" }),
          ),
      });

      const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "deliver",
      }).pipe(Effect.provide(toolLayer));

      expect(requests).toHaveLength(1);
      expect(requests[0]?.toolChoice).toEqual({
        tool: "post_message",
      });
      expect(yield* Ref.get(searchStarts)).toBe(0);
      expect(yield* Ref.get(deliveryStarts)).toBe(1);
      expect(result).toMatchObject({
        output: { message: "delivered", messageId: "message-exact" },
        turns: 1,
        finishReason: "completed",
      });

      const optionalDefinition = Agent.make("optional-terminal-tool-at-turn-limit", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
        instructions: "Deliver the final answer.",
        toolkit: SearchThenPost,
        policy: AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          onExhaustion: "fail",
        }),
        completion: {
          tool: "post_message",
          project: ({ parameters, result }) => ({
            message: parameters.message,
            messageId: result.messageId,
          }),
        },
      });
      const { model: optionalModel, requests: optionalRequests } = scriptedModel([
        finalParts('{"message":"text is still valid","messageId":"text-result"}'),
      ]);

      const optionalResult = yield* AgentRuntime.run(
        Agent.withModel(optionalDefinition, optionalModel),
        { question: "deliver" },
      ).pipe(Effect.provide(toolLayer));

      expect(optionalRequests).toHaveLength(1);
      expect(optionalRequests[0]?.toolChoice).toBe("auto");
      expect(optionalResult).toMatchObject({
        output: { message: "text is still valid", messageId: "text-result" },
        turns: 1,
        finishReason: "model-stop",
      });
      expect(yield* Ref.get(searchStarts)).toBe(0);
      expect(yield* Ref.get(deliveryStarts)).toBe(1);

      const { model: researchModel, requests: researchRequests } = scriptedModel([
        toolCallParts("research-must-not-run", "search", {}),
      ]);
      const researchExit = yield* AgentRuntime.run(Agent.withModel(definition, researchModel), {
        question: "research",
      }).pipe(Effect.provide(toolLayer), Effect.exit);

      expect(failureFrom(researchExit)).toMatchObject({
        _tag: "AgentPolicyError",
        limit: "turns",
      });
      expect(researchRequests[0]?.toolChoice).toEqual({
        tool: "post_message",
      });
      expect(yield* Ref.get(searchStarts)).toBe(0);
      expect(yield* Ref.get(deliveryStarts)).toBe(1);

      const { model: beyondModel, requests: beyondRequests } = scriptedModel([
        finalParts('{"message":"must not run","messageId":"wrong"}'),
      ]);
      const beyondExit = yield* AgentRuntime.run(
        Agent.withModel(definition, beyondModel),
        { question: "deliver" },
        {
          resume: {
            turn: 2,
            turnId: Schema.decodeSync(TurnId)("turn-resumed-beyond-limit"),
            calls: [
              {
                id: "delivery-beyond-turn-limit",
                name: "post_message",
                params: { message: "must not deliver" },
              },
            ],
            settled: [],
          },
          resumeUsage: { ...emptyResumeUsage, committedTurns: 2, toolCalls: 1, modelCalls: 2 },
        },
      ).pipe(Effect.provide(toolLayer), Effect.exit);
      const beyondFailure = failureFrom(beyondExit);

      expect(beyondFailure).toMatchObject({ _tag: "AgentPolicyError", limit: "turns" });
      expect(beyondRequests).toHaveLength(0);
      expect(yield* Ref.get(searchStarts)).toBe(0);
      expect(yield* Ref.get(deliveryStarts)).toBe(1);
    }),
  );

  it.effect(
    "RUN-032: failed returned completion at the Turn limit cannot start another model Turn",
    () =>
      Effect.gen(function* () {
        const FailedPostMessage = Tool.make("failed_post_message", {
          parameters: Schema.Struct({ message: Schema.String }),
          success: Schema.Struct({ messageId: Schema.String }),
          failure: Schema.Struct({ message: Schema.String }),
          failureMode: "return",
        });
        const failedToolkit = Toolkit.make(FailedPostMessage);
        const handlerStarts = yield* Ref.make(0);
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const definition = Agent.make("failed-terminal-tool-at-turn-limit", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
          instructions: "Deliver through failed_post_message.",
          toolkit: failedToolkit,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            onExhaustion: "fail",
          }),
          completion: {
            tool: "failed_post_message",
            required: true,
            project: ({ parameters, result }) => ({
              message: parameters.message,
              messageId: result.messageId,
            }),
          },
        });
        const { model, requests } = scriptedModel([
          toolCallParts("failed-delivery-exact", "failed_post_message", {
            message: "deliver",
          }),
          finalParts('{"message":"must not run","messageId":"wrong"}'),
        ]);
        const toolLayer = failedToolkit.toLayer({
          failed_post_message: () =>
            Ref.update(handlerStarts, (count) => count + 1).pipe(
              Effect.andThen(Effect.fail({ message: "delivery failed" })),
            ),
        });

        const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "deliver",
        }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
          Effect.exit,
        );
        const failure = failureFrom(exit);

        expect(failure).toMatchObject({ _tag: "AgentPolicyError", limit: "turns" });
        expect(requests).toHaveLength(1);
        expect(yield* Ref.get(handlerStarts)).toBe(1);
        expect(
          (yield* Ref.get(events)).filter((event) => event._tag === "ToolCallFailed"),
        ).toHaveLength(1);
      }),
  );

  it.effect("RUN-032: fail mode does not exempt a completion Tool from the Tool Call limit", () =>
    Effect.gen(function* () {
      const SearchThenPost = Toolkit.make(SearchTool, PostMessageTool);
      const searchStarts = yield* Ref.make(0);
      const deliveryStarts = yield* Ref.make(0);
      const definition = Agent.make("tool-exhausted-terminal-tool-fail", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
        instructions: "Research, then deliver the final answer with post_message.",
        toolkit: SearchThenPost,
        policy: AgentPolicy.make({
          maxTurns: 5,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          onExhaustion: "fail",
        }),
        completion: {
          tool: "post_message",
          project: ({ parameters, result }) => ({
            message: parameters.message,
            messageId: result.messageId,
          }),
        },
      });
      const { model, requests } = scriptedModel([
        toolCallParts("search-before-delivery", "search", {}),
        toolCallParts("delivery-tool-fail", "post_message", {
          message: "must not deliver",
        }),
      ]);
      const toolLayer = SearchThenPost.toLayer({
        search: () => Ref.update(searchStarts, (count) => count + 1).pipe(Effect.as("found")),
        post_message: () =>
          Ref.update(deliveryStarts, (count) => count + 1).pipe(
            Effect.as({ messageId: "must-not-exist" }),
          ),
      });

      const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "deliver",
      }).pipe(Effect.provide(toolLayer), Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect((failure as AgentPolicyError).limit).toBe("tool-calls");
      expect(requests).toHaveLength(2);
      expect(yield* Ref.get(searchStarts)).toBe(1);
      expect(yield* Ref.get(deliveryStarts)).toBe(0);
    }),
  );

  it.effect(
    "RUN-032: fail mode rejects an over-budget resumed completion Tool before its handler starts",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const definition = Agent.make("resume-token-exhausted-terminal-tool-fail", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
          instructions: "Deliver the final answer with post_message.",
          toolkit: postMessageToolkit,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 100,
            onExhaustion: "fail",
          }),
          completion: {
            tool: "post_message",
            required: true,
            project: ({ parameters, result }) => ({
              message: parameters.message,
              messageId: result.messageId,
            }),
          },
        });
        const { model, requests } = scriptedModel([finalParts('{"message":"never"}')]);
        const toolLayer = postMessageToolkit.toLayer({
          post_message: () =>
            Ref.update(handlerStarts, (count) => count + 1).pipe(
              Effect.as({ messageId: "must-not-exist" }),
            ),
        });
        const resume: RunTurnResume = {
          turn: 1,
          turnId: Schema.decodeSync(TurnId)("turn-resumed-delivery"),
          calls: [
            {
              id: "delivery-resumed-fail",
              name: "post_message",
              params: { message: "must not deliver" },
            },
          ],
          settled: [],
        };

        const exit = yield* AgentRuntime.run(
          Agent.withModel(definition, model),
          { question: "deliver" },
          {
            resume,
            resumeUsage: {
              ...emptyPolicyUsage,
              committedTurns: 1,
              toolCalls: 1,
              modelCalls: 1,
              inputTokens: 90,
              outputTokens: 20,
              lastInputTokens: 90,
              lastOutputTokens: 20,
              costMicrousd: 0,
            },
          },
        ).pipe(Effect.provide(toolLayer), Effect.exit);
        const failure = failureFrom(exit);

        expect(failure).toBeInstanceOf(AgentPolicyError);
        expect((failure as AgentPolicyError).limit).toBe("tokens");
        expect(requests).toHaveLength(0);
        expect(yield* Ref.get(handlerStarts)).toBe(0);
      }),
  );

  it.effect("RUN-032: a completion Tool must be the singleton declared batch", () =>
    Effect.gen(function* () {
      const mixedToolkit = Toolkit.make(PostMessageTool, SearchTool);
      const handlerStarts = yield* Ref.make(0);
      const definition = Agent.make("mixed-terminal-tool-batch", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
        instructions: "Deliver the final message.",
        toolkit: mixedToolkit,
        policy: AgentPolicy.make({
          maxTurns: 5,
          maxToolCalls: 5,
          maxDuration: "30 seconds",
          toolConcurrency: 2,
        }),
        completion: {
          tool: "post_message",
          project: ({ parameters, result }) => ({
            message: parameters.message,
            messageId: result.messageId,
          }),
        },
      });
      const { model, requests } = scriptedModel([
        [
          {
            type: "tool-call",
            id: "delivery-mixed",
            name: "post_message",
            params: { message: "must not send" },
            providerExecuted: false,
          },
          {
            type: "tool-call",
            id: "search-mixed",
            name: "search",
            params: {},
            providerExecuted: false,
          },
          { type: "finish", reason: "tool-calls", usage: usageOf(10, 5) },
        ],
      ]);
      const toolLayer = mixedToolkit.toLayer({
        post_message: () =>
          Ref.update(handlerStarts, (count) => count + 1).pipe(
            Effect.as({ messageId: "must-not-exist" }),
          ),
        search: () =>
          Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("must-not-run")),
      });

      const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "deliver",
      }).pipe(Effect.provide(toolLayer), Effect.exit);

      expect(failureFrom(exit)).toBeInstanceOf(ModelProtocolError);
      expect(requests).toHaveLength(1);
      expect(yield* Ref.get(handlerStarts)).toBe(0);
    }),
  );

  it.effect.each([false, true])(
    "RUN-034: completion reserve preserves terminal Tool choice with required=%s",
    (required) =>
      Effect.gen(function* () {
        const definition = Agent.make("completion-reserve-terminal-tool", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ message: Schema.String, messageId: Schema.String }),
          instructions: `Research only while delivery capacity remains. ${"context ".repeat(100)}`,
          toolkit: postMessageToolkit,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 10_000,
            completionReserveTokens: 9_800,
          }),
          completion: {
            tool: "post_message",
            ...(required ? { required: true } : {}),
            project: ({ parameters, result }) => ({
              message: parameters.message,
              messageId: result.messageId,
            }),
          },
        });
        const { model, requests } = scriptedModel([
          toolCallParts("delivery-1", "post_message", { message: "reserved" }, usageOf(50, 10)),
        ]);
        const toolLayer = postMessageToolkit.toLayer({
          post_message: () => Effect.succeed({ messageId: "message-reserved" }),
        });

        const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
          question: "deliver",
        }).pipe(Effect.provide(toolLayer));

        expect(requests).toHaveLength(1);
        expect(requests[0]?.toolChoice).toEqual(
          required ? { tool: "post_message" } : { mode: "auto", oneOf: ["post_message"] },
        );
        expect(result).toMatchObject({
          output: { message: "reserved", messageId: "message-reserved" },
          finishReason: "budget-exhausted",
          exhausted: "tokens",
        });
      }),
  );

  it.effect(
    "RUN-025: Tool Call exhaustion settles budget-exhausted with the exhausted marker",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const definition = Agent.make("tool-calls-exhausted", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Search until done.",
          toolkit: searchToolkit,
          policy: AgentPolicy.make({
            maxTurns: 10,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const { model, requests } = scriptedModel([
          toolCallParts("s1", "search", {}),
          toolCallParts("s2", "search", {}),
          finalParts('{"answer":"capped"}'),
        ]);
        const toolLayer = searchToolkit.toLayer({
          search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
        });

        yield* AgentRuntime.stream(Agent.withModel(definition, model), { question: "cap" }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
        );

        // The final permitted call executes; the exceeding batch settles
        // synthetically and the grace Turn is tool-choice constrained.
        expect(yield* Ref.get(handlerStarts)).toBe(1);
        expect(requests).toHaveLength(3);
        expect(requests[2]?.toolChoice).toBe("none");
        const completed = (yield* Ref.get(events)).find((event) => event._tag === "RunCompleted");
        expect(completed).toMatchObject({
          output: { answer: "capped" },
          finishReason: "budget-exhausted",
          exhausted: "tool-calls",
        });
      }),
  );

  it.effect(
    "RUN-025: Turn exhaustion executes the final permitted batch and settles on the grace Turn with the exhausted marker",
    () =>
      Effect.gen(function* () {
        const handlerStarts = yield* Ref.make(0);
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const definition = Agent.make("turns-exhausted", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Search until done.",
          toolkit: searchToolkit,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const { model, requests } = scriptedModel([
          toolCallParts("s1", "search", {}),
          finalParts('{"answer":"turn-capped"}'),
        ]);
        const toolLayer = searchToolkit.toLayer({
          search: () => Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("found")),
        });

        yield* AgentRuntime.stream(Agent.withModel(definition, model), { question: "cap" }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
        );

        // RUN-019: the pending batch at the final permitted Turn executes
        // normally; only the grace Turn is constrained.
        expect(yield* Ref.get(handlerStarts)).toBe(1);
        expect(requests).toHaveLength(2);
        expect(requests[1]?.toolChoice).toBe("none");
        const completed = (yield* Ref.get(events)).find((event) => event._tag === "RunCompleted");
        expect(completed).toMatchObject({
          output: { answer: "turn-capped" },
          finishReason: "budget-exhausted",
          exhausted: "turns",
        });
      }),
  );

  it.effect(
    "RUN-025: Turn exhaustion with a queued follow-up consumes it on the single grace Turn",
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const drained = yield* Ref.make(false);
        const definition = Agent.make("turns-follow-up", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Answer.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const { model, requests } = scriptedModel([
          finalParts('{"answer":"first"}'),
          finalParts('{"answer":"followed"}'),
        ]);

        yield* AgentRuntime.stream(
          Agent.withModel(definition, model),
          { question: "first" },
          {
            input: {
              drain: () =>
                Effect.gen(function* () {
                  const already = yield* Ref.getAndSet(drained, true);
                  return already ? [] : [{ kind: "follow-up" as const, input: "again" }];
                }),
            },
          },
        ).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
        );

        // RUN-019: exactly one grace Turn past maxTurns serves the follow-up;
        // its settlement is honest about the exhaustion.
        expect(requests).toHaveLength(2);
        const completed = (yield* Ref.get(events)).find((event) => event._tag === "RunCompleted");
        expect(completed).toMatchObject({
          output: { answer: "followed" },
          finishReason: "budget-exhausted",
          exhausted: "turns",
        });
      }),
  );

  it.effect(
    "RUN-025: a grace-Turn response that declares Tool calls fails typed (the RUN-020 fail-closed constraint)",
    () =>
      Effect.gen(function* () {
        const definition = Agent.make("finalize-declares-tools", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Search, then answer.",
          toolkit: searchToolkit,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 10_000,
          }),
        });
        const { model } = scriptedModel([
          toolCallParts("s1", "search", {}, usageOf(9, 4)),
          toolCallParts("s2", "search", {}, usageOf(1, 1)),
        ]);
        const toolLayer = searchToolkit.toLayer({ search: () => Effect.succeed("found") });

        const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), {
          question: "misbehave",
        }).pipe(Effect.provide(toolLayer), Effect.exit);

        const failure = failureFrom(exit);
        expect(failure).toBeInstanceOf(ModelProtocolError);
        expect(String((failure as ModelProtocolError).message)).toContain('toolChoice "none"');
      }),
  );

  it.effect(
    "RUN-025: a token-breaching stop response without decodable output fails as an ordinary decode failure",
    () =>
      Effect.gen(function* () {
        const definition = Agent.make("finalize-bad-output", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Search, then answer.",
          toolkit: searchToolkit,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 10_000,
          }),
        });
        const { model } = scriptedModel([
          toolCallParts("s1", "search", {}, usageOf(9_000, 4_000)),
          finalParts("not json", usageOf(1_000, 1_000)),
        ]);
        const toolLayer = searchToolkit.toLayer({ search: () => Effect.succeed("found") });

        const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), {
          question: "garble",
        }).pipe(Effect.provide(toolLayer), Effect.exit);

        // The soft landing never launders an unusable answer: the grace
        // Turn's undecodable output surfaces as the ordinary typed decode
        // failure, exactly as it would without the breach.
        const failure = failureFrom(exit);
        expect(failure).not.toBeInstanceOf(AgentPolicyError);
        expect((failure as { _tag?: string })._tag).toBe("AgentOutputError");
      }),
  );

  it.effect("RUN-025: run results surface the exhausted marker", () =>
    Effect.gen(function* () {
      const definition = Agent.make("result-exhausted", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 5,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          tokenBudget: 3,
        }),
      });
      const { model } = scriptedModel([finalParts('{"answer":"overrun"}', usageOf(2, 2))]);

      const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "answer",
      });

      expect(result.finishReason).toBe("budget-exhausted");
      expect(result.exhausted).toBe("tokens");
      expect(result.output).toEqual({ answer: "overrun" });
    }),
  );

  // ------------------------------------------------- round-2 review findings

  it.effect("RUN-025: a simultaneous token and cost breach fails typed on the cost rail", () =>
    Effect.gen(function* () {
      const estimatedInputTokens = yield* Ref.make<number | undefined>(undefined);
      const definition = Agent.make("both-breach", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          tokenBudget: 100,
          costBudgetMicrousd: 1_000,
        }),
      });
      const { model } = scriptedModel([finalParts('{"answer":"spent"}', usageOf(150, 10))]);
      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "q" },
        {
          estimateCostMicrousd: (usage) =>
            Ref.set(estimatedInputTokens, usage.inputTokens.total).pipe(Effect.as(2_000)),
        },
      ).pipe(Effect.exit);
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect((failure as AgentPolicyError).limit).toBe("cost");
      expect(yield* Ref.get(estimatedInputTokens)).toBe(150);
    }),
  );

  it.effect("RUN-035: cumulative model cost fails typed before safe-integer overflow", () =>
    Effect.gen(function* () {
      const estimatorCalls = yield* Ref.make(0);
      const definition = Agent.make("cost-overflow", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Search, then answer.",
        toolkit: searchToolkit,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model } = scriptedModel([
        toolCallParts("cost-search", "search", {}),
        finalParts('{"answer":"done"}'),
      ]);
      const toolLayer = searchToolkit.toLayer({ search: () => Effect.succeed("found") });

      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "q" },
        {
          estimateCostMicrousd: () =>
            Ref.modify(estimatorCalls, (count) => [
              count === 0 ? Number.MAX_SAFE_INTEGER : 1,
              count + 1,
            ]),
        },
      ).pipe(Effect.provide(toolLayer), Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect((failure as AgentPolicyError).limit).toBe("cost");
      expect(yield* Ref.get(estimatorCalls)).toBe(2);
    }),
  );

  it.effect("RUN-022: an unserializable Tool result becomes the fail-closed sentinel", () =>
    Effect.gen(function* () {
      const UnknownTool = Tool.make("emitUnknown", {
        parameters: Schema.Struct({}),
        success: Schema.Unknown,
      });
      const unknownToolkit = Toolkit.make(UnknownTool);
      const definition = Agent.make("bounds-unserializable", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Use the tool once, then answer.",
        toolkit: unknownToolkit,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model, requests } = scriptedModel([
        toolCallParts("u-1", "emitUnknown", {}),
        finalParts('{"answer":"done"}'),
      ]);
      const toolLayer = unknownToolkit.toLayer({
        emitUnknown: () =>
          Effect.succeed({
            toJSON: () => {
              throw new Error("cyclic tool payload");
            },
          }),
      });
      const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "u",
      }).pipe(Effect.provide(toolLayer));
      expect(result.output).toEqual({ answer: "done" });
      const second = requests[1];
      if (second === undefined) throw new Error("expected a second model request");
      const values = toolResultValues(second.prompt);
      expect(values).toHaveLength(1);
      const sentinel = Schema.decodeUnknownSync(UnserializableToolResult)(values[0]);
      expect(sentinel.reason).toContain("cyclic tool payload");
    }),
  );

  it.effect(
    "RUN-022: a within-bounds Tool result is canonicalized to its measured JSON projection",
    () =>
      Effect.gen(function* () {
        const UnknownTool = Tool.make("emitUnknown", {
          parameters: Schema.Struct({}),
          success: Schema.Unknown,
        });
        const unknownToolkit = Toolkit.make(UnknownTool);
        const definition = Agent.make("bounds-canonicalize", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Use the tool once, then answer.",
          toolkit: unknownToolkit,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const { model, requests } = scriptedModel([
          toolCallParts("c-1", "emitUnknown", {}),
          finalParts('{"answer":"done"}'),
        ]);
        // Passes the byte check via a small `toJSON` projection while the
        // object itself carries unbounded state and an `undefined` hole:
        // only the measured projection may be retained.
        const toolLayer = unknownToolkit.toLayer({
          emitUnknown: () =>
            Effect.succeed({
              state: "x".repeat(200_000),
              hole: undefined,
              toJSON: () => ({ ok: true }),
            }),
        });
        const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
          question: "c",
        }).pipe(Effect.provide(toolLayer));
        expect(result.output).toEqual({ answer: "done" });
        const second = requests[1];
        if (second === undefined) throw new Error("expected a second model request");
        const values = toolResultValues(second.prompt);
        expect(values).toHaveLength(1);
        expect(values[0]).toEqual({ ok: true });
      }),
  );

  it.effect("RUN-022: an unserializable FAILED Tool result becomes the sentinel too", () =>
    Effect.gen(function* () {
      const FragileTool = Tool.make("fragile", {
        parameters: Schema.Struct({}),
        success: Schema.Struct({ ok: Schema.Boolean }),
        failure: Schema.Unknown,
        failureMode: "return",
      });
      const fragileToolkit = Toolkit.make(FragileTool);
      const definition = Agent.make("bounds-failed-unserializable", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Use the tool once, then answer.",
        toolkit: fragileToolkit,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model, requests } = scriptedModel([
        toolCallParts("f-1", "fragile", {}),
        finalParts('{"answer":"recovered"}'),
      ]);
      const toolLayer = fragileToolkit.toLayer({
        fragile: () =>
          Effect.fail({
            toJSON: () => {
              throw new Error("unserializable failure payload");
            },
          }),
      });
      const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "f",
      }).pipe(Effect.provide(toolLayer));
      expect(result.output).toEqual({ answer: "recovered" });
      const second = requests[1];
      if (second === undefined) throw new Error("expected a second model request");
      const values = toolResultValues(second.prompt);
      expect(values).toHaveLength(1);
      const sentinel = Schema.decodeUnknownSync(UnserializableToolResult)(values[0]);
      expect(sentinel.reason).toContain("unserializable failure payload");
    }),
  );

  it.effect(
    "restores logical Run turn and combined Tool budgets before the next model request",
    () =>
      Effect.gen(function* () {
        for (const limit of ["turns", "tool-calls"] as const) {
          for (const onExhaustion of ["fail", "final-answer"] as const) {
            const definition = Agent.make("resume-policy", {
              input: Schema.String,
              output: answerOutput,
              instructions: "Answer.",
              toolkit: emitToolkit,
              policy: AgentPolicy.make({
                maxTurns: 3,
                maxToolCalls: 3,
                maxDuration: "30 seconds",
                toolConcurrency: 1,
                onExhaustion,
              }),
            });
            const { model, requests } = scriptedModel([finalParts('{"answer":"remaining"}')]);
            const reservations: Array<unknown> = [];
            const durability: RunDurabilityHook = {
              commitResponse: () => Effect.void,
              prepareToolCalls: () => Effect.void,
              commitCompaction: () => Effect.void,
              noteTurnUsage: () => Effect.void,
              step: { lookup: () => Effect.succeed(Option.none()), commit: () => Effect.void },
              reservePolicyUsage: (usage) =>
                Effect.sync(() => {
                  reservations.push(usage);
                }),
            };
            const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), "q", {
              durability,
              resumeUsage: {
                ...emptyResumeUsage,
                committedTurns: limit === "turns" ? 3 : 1,
                modelCalls: limit === "turns" ? 3 : 1,
                toolCalls: 2,
                programmaticToolCalls: limit === "tool-calls" ? 2 : 0,
              },
            }).pipe(
              Effect.provide(
                emitToolkit.toLayer({ emit: () => Effect.succeed({ data: "unused" }) }),
              ),
              Effect.exit,
            );
            if (onExhaustion === "fail") {
              expect(failureFrom(exit)).toMatchObject({ _tag: "AgentPolicyError", limit });
              expect(requests).toHaveLength(0);
              expect(reservations).toEqual([]);
            } else {
              expect(Exit.isSuccess(exit)).toBe(true);
              if (Exit.isFailure(exit)) throw new Error("expected final answer");
              expect(exit.value).toMatchObject({
                turns: limit === "turns" ? 4 : 2,
                finishReason: "budget-exhausted",
                exhausted: limit,
              });
              expect(requests).toHaveLength(1);
              expect(requests[0]?.toolChoice).toBe("none");
              expect(reservations).toEqual([
                { programmaticToolCalls: limit === "tool-calls" ? 2 : 0, finalizationUsed: true },
              ]);
            }
          }
        }
      }),
  );

  it.effect(
    "folds a pending batch once onto the restored failure streak without recounting calls",
    () =>
      Effect.gen(function* () {
        for (const { repeatedFailureLimit, budgetRejected } of [
          { repeatedFailureLimit: 2, budgetRejected: undefined },
          { repeatedFailureLimit: 3, budgetRejected: undefined },
          { repeatedFailureLimit: 2, budgetRejected: true as const },
        ]) {
          const definition = Agent.make("resume-failures", {
            input: Schema.String,
            output: answerOutput,
            instructions: "Answer.",
            toolkit: emitToolkit,
            policy: AgentPolicy.make({
              maxTurns: 4,
              maxToolCalls: 4,
              maxDuration: "30 seconds",
              toolConcurrency: 1,
              repeatedFailureLimit,
              onExhaustion: "fail",
            }),
          });
          const { model, requests } = scriptedModel([finalParts('{"answer":"done"}')]);
          let starts = 0;
          const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), "q", {
            resumeUsage: {
              ...emptyResumeUsage,
              committedTurns: 2,
              modelCalls: 2,
              toolCalls: 2,
              programmaticToolCalls: 2,
              consecutiveToolFailures: 1,
            },
            resume: {
              turn: 2,
              turnId: Schema.decodeSync(TurnId)("pending-turn"),
              calls: [{ id: "pending", name: "emit", params: {} }],
              settled: [
                {
                  id: "pending",
                  result: { _tag: "PriorFailure" },
                  isFailure: true,
                  ...(budgetRejected === undefined ? {} : { budgetRejected }),
                },
              ],
            },
          }).pipe(
            Effect.provide(
              emitToolkit.toLayer({
                emit: () =>
                  Effect.sync(() => {
                    starts += 1;
                    return { data: "never" };
                  }),
              }),
            ),
            Effect.exit,
          );
          expect(starts).toBe(0);
          if (repeatedFailureLimit === 2 && budgetRejected !== true) {
            expect(failureFrom(exit)).toMatchObject({
              _tag: "AgentPolicyError",
              limit: "repeated-failures",
            });
            expect(requests).toHaveLength(0);
          } else {
            expect(Exit.isSuccess(exit)).toBe(true);
            if (Exit.isFailure(exit)) throw new Error("expected continuation");
            expect(exit.value.turns).toBe(3);
            expect(requests).toHaveLength(1);
          }
        }
      }),
  );

  it.effect(
    "reserves grace before provider execution and never grants it again after interruption",
    () =>
      Effect.gen(function* () {
        const definition = Agent.make("resume-grace", {
          input: Schema.String,
          output: answerOutput,
          instructions: "Answer.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const { model, requests } = scriptedModel([finalParts('{"answer":"never"}')]);
        let reserved = false;
        const durability: RunDurabilityHook = {
          commitResponse: () => Effect.void,
          prepareToolCalls: () => Effect.void,
          commitCompaction: () => Effect.void,
          noteTurnUsage: () => Effect.void,
          step: { lookup: () => Effect.succeed(Option.none()), commit: () => Effect.void },
          reservePolicyUsage: (usage) =>
            Effect.sync(() => {
              reserved = usage.finalizationUsed;
            }).pipe(Effect.andThen(Effect.interrupt)),
        };
        const seed = { ...emptyResumeUsage, committedTurns: 1, modelCalls: 1 };
        const interrupted = yield* AgentRuntime.run(Agent.withModel(definition, model), "q", {
          resumeUsage: seed,
          durability,
        }).pipe(Effect.exit);
        expect(Exit.isFailure(interrupted) && Cause.hasInterrupts(interrupted.cause)).toBe(true);
        expect(reserved).toBe(true);
        const replacement = yield* AgentRuntime.run(Agent.withModel(definition, model), "q", {
          resumeUsage: { ...seed, finalizationUsed: reserved },
        }).pipe(Effect.exit);
        expect(failureFrom(replacement)).toMatchObject({
          _tag: "AgentPolicyError",
          limit: "turns",
        });
        expect(requests).toHaveLength(0);
      }),
  );

  it.effect("rejects missing or contradictory pending-batch accounting before execution", () =>
    Effect.gen(function* () {
      const definition = Agent.make("invalid-pending-accounting", {
        input: Schema.String,
        output: answerOutput,
        instructions: "Answer.",
        toolkit: emitToolkit,
        policy: AgentPolicy.make({
          maxTurns: 4,
          maxToolCalls: 4,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model, requests } = scriptedModel([finalParts('{"answer":"must not run"}')]);
      const seed = {
        ...emptyResumeUsage,
        modelCalls: 2,
        committedTurns: 2,
        toolCalls: 3,
        consecutiveToolFailures: 1,
      };
      let inputStarts = 0;
      let handlerStarts = 0;
      for (const resumeUsage of [
        undefined,
        { ...seed, modelCalls: 1 },
        { ...seed, committedTurns: 1 },
        { ...seed, committedTurns: 3 },
        { ...seed, toolCalls: 1 },
        { ...seed, consecutiveToolFailures: 2 },
      ]) {
        const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), "q", {
          resumeUsage,
          resume: {
            turn: 2,
            turnId: Schema.decodeSync(TurnId)("invalid-pending-turn"),
            calls: [
              { id: "pending-a", name: "emit", params: {} },
              { id: "pending-b", name: "emit", params: {} },
            ],
            settled: [],
          },
          input: {
            start: () =>
              Effect.sync(() => {
                inputStarts += 1;
              }),
            drain: () => Effect.succeed([]),
          },
        }).pipe(
          Effect.provide(
            emitToolkit.toLayer({
              emit: () =>
                Effect.sync(() => {
                  handlerStarts += 1;
                  return { data: "unexpected" };
                }),
            }),
          ),
          Effect.exit,
        );
        expect(failureFrom(exit)).toBeInstanceOf(ModelProtocolError);
      }
      expect(inputStarts).toBe(0);
      expect(handlerStarts).toBe(0);
      expect(requests).toEqual([]);
    }),
  );

  it.effect("RUN-023: invalid restored usage fails before Run input or model execution", () =>
    Effect.gen(function* () {
      const definition = Agent.make("invalid-resume-usage", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const { model, requests } = scriptedModel([finalParts('{"answer":"never"}')]);
      let inputStarts = 0;
      const invalidSeeds = [
        { ...emptyResumeUsage, committedTurns: -1 },
        { ...emptyResumeUsage, toolCalls: Number.MAX_SAFE_INTEGER + 1 },
        { ...emptyResumeUsage, programmaticToolCalls: -1 },
        { ...emptyResumeUsage, consecutiveToolFailures: 0.5 },
        { ...emptyResumeUsage, consecutiveToolFailures: 1 },
        { ...emptyResumeUsage, committedTurns: 1 },
        {
          ...emptyPolicyUsage,
          modelCalls: 1,
          inputTokens: -1,
          outputTokens: 0,
          lastInputTokens: 0,
          lastOutputTokens: 0,
          costMicrousd: 0,
        },
        {
          ...emptyPolicyUsage,
          modelCalls: 1,
          inputTokens: Number.NaN,
          outputTokens: 0,
          lastInputTokens: 0,
          lastOutputTokens: 0,
          costMicrousd: 0,
        },
        {
          ...emptyPolicyUsage,
          modelCalls: 1,
          inputTokens: 1,
          outputTokens: 0,
          lastInputTokens: 2,
          lastOutputTokens: 0,
          costMicrousd: 0,
        },
      ];

      for (const resumeUsage of invalidSeeds) {
        const exit = yield* AgentRuntime.run(
          Agent.withModel(definition, model),
          { question: "q" },
          {
            input: {
              start: () =>
                Effect.sync(() => {
                  inputStarts += 1;
                }),
              drain: () => Effect.succeed([]),
            },
            resumeUsage,
          },
        ).pipe(Effect.exit);
        const failure = failureFrom(exit);
        expect(failure).toBeInstanceOf(ModelProtocolError);
      }

      let accessorReads = 0;
      const accessorUsage = {
        ...emptyPolicyUsage,
        modelCalls: 1,
        inputTokens: 1,
        outputTokens: 0,
        lastInputTokens: 1,
        lastOutputTokens: 0,
        costMicrousd: 0,
      };
      Object.defineProperty(accessorUsage, "inputTokens", {
        enumerable: true,
        get: () => {
          accessorReads += 1;
          throw new Error("resume usage accessor must not run");
        },
      });
      const accessorExit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "q" },
        {
          input: {
            start: () =>
              Effect.sync(() => {
                inputStarts += 1;
              }),
            drain: () => Effect.succeed([]),
          },
          resumeUsage: accessorUsage,
        },
      ).pipe(Effect.exit);
      expect(failureFrom(accessorExit)).toBeInstanceOf(ModelProtocolError);
      expect(accessorReads).toBe(0);

      expect(inputStarts).toBe(0);
      expect(requests).toHaveLength(0);
    }),
  );

  it.effect(
    "RUN-025: restored totals that already breach fail before any model call under fail mode",
    () =>
      Effect.gen(function* () {
        const definition = Agent.make("resume-breach-fail", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Answer.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 100,
            onExhaustion: "fail",
          }),
        });
        const { model, requests } = scriptedModel([finalParts('{"answer":"never"}')]);
        const exit = yield* AgentRuntime.run(
          Agent.withModel(definition, model),
          { question: "q" },
          {
            resumeUsage: {
              ...emptyPolicyUsage,
              modelCalls: 2,
              inputTokens: 90,
              outputTokens: 20,
              lastInputTokens: 90,
              lastOutputTokens: 20,
              costMicrousd: 0,
            },
          },
        ).pipe(Effect.exit);
        const failure = failureFrom(exit);
        expect(failure).toBeInstanceOf(AgentPolicyError);
        expect((failure as AgentPolicyError).limit).toBe("tokens");
        expect(requests).toHaveLength(0);
      }),
  );

  it.effect("RUN-025: restored totals that already breach start final-answer constrained", () =>
    Effect.gen(function* () {
      const definition = Agent.make("resume-breach-final", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        // A real toolkit: `toolChoice: "none"` on the first request can only
        // come from the derived exhaustion constraint, never from an empty
        // tool list.
        toolkit: emitToolkit,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          tokenBudget: 100,
        }),
      });
      const { model, requests } = scriptedModel([
        finalParts('{"answer":"partial"}', usageOf(10, 5)),
      ]);
      const result = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "q" },
        {
          resumeUsage: {
            ...emptyPolicyUsage,
            modelCalls: 2,
            inputTokens: 90,
            outputTokens: 20,
            lastInputTokens: 90,
            lastOutputTokens: 20,
            costMicrousd: 0,
          },
        },
      ).pipe(
        Effect.provide(emitToolkit.toLayer({ emit: () => Effect.succeed({ data: "unused" }) })),
      );
      expect(result.finishReason).toBe("budget-exhausted");
      expect(result.exhausted).toBe("tokens");
      expect(requests).toHaveLength(1);
      expect(requests[0]?.toolChoice).toBe("none");
    }),
  );

  it.effect("RUN-023: restored cost accumulates into the cost budget across resume", () =>
    Effect.gen(function* () {
      const definition = Agent.make("resume-cost", {
        input: Schema.Struct({ question: Schema.String }),
        output: answerOutput,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          costBudgetMicrousd: 1_000,
        }),
      });
      const { model } = scriptedModel([finalParts('{"answer":"cheap"}', usageOf(10, 5))]);
      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "q" },
        {
          estimateCostMicrousd: () => Effect.succeed(200),
          resumeUsage: {
            ...emptyPolicyUsage,
            modelCalls: 1,
            inputTokens: 10,
            outputTokens: 5,
            lastInputTokens: 10,
            lastOutputTokens: 5,
            costMicrousd: 900,
          },
        },
      ).pipe(Effect.exit);
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect((failure as AgentPolicyError).limit).toBe("cost");
    }),
  );

  it.effect(
    "RUN-023: an already-over-cost resume rejects before any model call in both exhaustion modes",
    () =>
      Effect.gen(function* () {
        // Cost is an unconditional hard rail: unlike tokens, no mode grants a
        // grace call, so the seeded breach must reject before any external
        // model execution.
        for (const onExhaustion of ["final-answer", "fail"] as const) {
          const definition = Agent.make(`resume-over-cost-${onExhaustion}`, {
            input: Schema.Struct({ question: Schema.String }),
            output: answerOutput,
            instructions: "Answer.",
            toolkit: Toolkit.empty,
            policy: AgentPolicy.make({
              maxTurns: 3,
              maxToolCalls: 2,
              maxDuration: "30 seconds",
              toolConcurrency: 1,
              costBudgetMicrousd: 1_000,
              onExhaustion,
            }),
          });
          const { model, requests } = scriptedModel([
            finalParts('{"answer":"never"}', usageOf(10, 5)),
          ]);
          const exit = yield* AgentRuntime.run(
            Agent.withModel(definition, model),
            { question: "q" },
            {
              estimateCostMicrousd: () => Effect.succeed(200),
              resumeUsage: {
                ...emptyPolicyUsage,
                modelCalls: 1,
                inputTokens: 10,
                outputTokens: 5,
                lastInputTokens: 10,
                lastOutputTokens: 5,
                costMicrousd: 1_100,
              },
            },
          ).pipe(Effect.exit);
          const failure = failureFrom(exit);
          expect(failure).toBeInstanceOf(AgentPolicyError);
          expect((failure as AgentPolicyError).limit).toBe("cost");
          expect(requests).toHaveLength(0);
        }
      }),
  );

  it.effect.each(["single-turn", "token-breach", "required-token-breach"] as const)(
    "provider-only final text honors completion and budget policy: %s",
    (scenario) =>
      Effect.gen(function* () {
        const required = scenario === "required-token-breach";
        const singleTurn = scenario === "single-turn";
        const deliveries: Array<string> = [];
        const HostedSearch = Tool.providerDefined({
          id: "test.web_search",
          customName: "HostedSearch",
          providerName: "web_search",
          parameters: Schema.Struct({ query: Schema.String }),
          success: Schema.Struct({ status: Schema.String }),
        })(undefined);
        const hostedToolkit = Toolkit.make(HostedSearch, PostMessageTool);
        const definition = Agent.make("provider-only-breach", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Answer.",
          toolkit: hostedToolkit,
          policy: AgentPolicy.make({
            maxTurns: singleTurn ? 1 : 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 10_000,
            onExhaustion: singleTurn ? "fail" : "final-answer",
          }),
          ...(required
            ? {
                completion: {
                  tool: "post_message" as const,
                  required: true,
                  project: ({ parameters }: { parameters: { message: string } }) => ({
                    answer: parameters.message,
                  }),
                },
              }
            : {}),
        });
        const { model, requests } = scriptedModel([
          [
            {
              type: "tool-call",
              id: "hosted-1",
              name: "HostedSearch",
              params: { query: "sea" },
              providerExecuted: true,
            },
            {
              type: "tool-result",
              id: "hosted-1",
              name: "HostedSearch",
              result: { status: "completed" },
              isFailure: false,
              providerExecuted: true,
            },
            { type: "text-start", id: "answer" },
            { type: "text-delta", id: "answer", delta: '{"answer":"hosted"}' },
            { type: "text-end", id: "answer" },
            {
              type: "finish",
              reason: "stop",
              usage: singleTurn ? usageOf(100, 10) : usageOf(15_000, 1_000),
            },
          ],
          toolCallParts("delivery", "post_message", { message: "delivered" }),
        ]);
        const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
          question: "q",
        }).pipe(
          Effect.provide(
            hostedToolkit.toLayer({
              post_message: ({ message }) =>
                Effect.sync(() => {
                  deliveries.push(message);
                  return { messageId: "message-1" };
                }),
            }),
          ),
        );
        expect(result.output).toEqual({ answer: required ? "delivered" : "hosted" });
        expect(result.finishReason).toBe(singleTurn ? "model-stop" : "budget-exhausted");
        expect(result.exhausted).toBe(singleTurn ? undefined : "tokens");
        expect(requests.map((request) => request.toolChoice)).toEqual(
          required ? ["required", { tool: "post_message" }] : ["auto"],
        );
        expect(deliveries).toEqual(required ? ["delivered"] : []);
      }),
  );
});
