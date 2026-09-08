import * as Agent from "@effect-agent/core/Agent";
import {
  AgentPolicyError,
  ContextBudgetError,
  ModelProtocolError,
} from "@effect-agent/core/AgentError";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId, RunId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { type RunCompleted, type RunEvent } from "@effect-agent/core/RunEvent";
import { SubagentGrant } from "@effect-agent/core/SubagentContract";
import {
  ToolResultBounds,
  TruncatedToolResult,
  UnserializableToolResult,
} from "@effect-agent/core/ToolResult";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ModelCallContext } from "@effect-agent/engine/ContextWindow";
import {
  type RunDurabilityHook,
  type RunTurnResume,
  type RunUsageDelta,
  type RunContextHook,
} from "@effect-agent/engine/RunOptions";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
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
import { toCodecAnthropic } from "effect/unstable/ai/AnthropicStructuredOutput";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";
import { HttpClient, HttpClientResponse, HttpServerResponse } from "effect/unstable/http";

import { formatRunStatus } from "../src/internal/agent-runtime.ts";
import { RunContextPreparationPassthrough } from "../src/RunOptions.ts";
import { ThreadHistory } from "../src/ThreadHistory.ts";

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("thread-1")),
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

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layerTransient,
  RunContextPreparationPassthrough,
);

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

  // https://linear.app/reve-ai/issue/KOM-144
  it.effect.each(["committed", "pending", "failure"] as const)(
    "action completion settles only a committed whole-request result: %s",
    (outcome) =>
      Effect.gen(function* () {
        class ActionFailure extends Schema.TaggedError<ActionFailure>()("ActionFailure", {}) {}

        const Create = Tool.make("complete_action", {
          parameters: Schema.Struct({ name: Schema.String, wholeRequestSatisfied: Schema.Boolean }),
          success: Schema.Struct({
            status: Schema.Literals(["committed", "pending"]),
            href: Schema.String,
          }),
          failure: ActionFailure,
          failureMode: "return",
        });

        const tools = Toolkit.make(Create, PostMessageTool);

        const definition = Agent.make("action-completion", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Complete one requested action or explain what remains.",
          toolkit: tools,
          policy: { maxTurns: 3, maxToolCalls: 3 },
          completion: {
            tool: "post_message",
            required: true,
            project: ({ parameters }) => parameters.message,
          },
          completionFromTools: [
            {
              tool: "complete_action",
              project: ({ parameters, result }) =>
                parameters.wholeRequestSatisfied && result.status === "committed"
                  ? Option.some(`Created [${parameters.name}](${result.href}).`)
                  : Option.none(),
            },
          ],
        });

        const { model, requests } = scriptedModel([
          toolCallParts("create-1", "complete_action", {
            name: "Project",
            wholeRequestSatisfied: true,
          }),
          toolCallParts("reply-1", "post_message", { message: "Action needs attention." }),
        ]);

        const handlerStarts = yield* Ref.make(0);

        const result = yield* AgentRuntime.run(
          Agent.withModel(definition, model),
          "Create Project",
        ).pipe(
          Effect.provide(
            tools.toLayer({
              complete_action: () =>
                Ref.update(handlerStarts, (count) => count + 1).pipe(
                  Effect.andThen(
                    outcome === "failure"
                      ? ActionFailure.make({})
                      : Effect.succeed({ status: outcome, href: "/project/1" }),
                  ),
                ),
              post_message: () => Effect.succeed({ messageId: "reply-1" }),
            }),
          ),
        );

        expect(result.output).toBe(
          outcome === "committed" ? "Created [Project](/project/1)." : "Action needs attention.",
        );
        expect(requests).toHaveLength(outcome === "committed" ? 1 : 2);
        expect(yield* Ref.get(handlerStarts)).toBe(1);
        expect(promptText(requests[0]!.prompt)).toContain("satisfies the whole request");
      }),
  );

  it.effect.each(["mixed", "tokens", "tool-calls", "turns"] as const)(
    "action completion cannot bypass ordinary admission: %s",
    (boundary) =>
      Effect.gen(function* () {
        const tools = Toolkit.make(PostMessageTool, SearchTool);

        const definition = Agent.make("bounded-action-completion", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Use search for the action and post_message to finish.",
          toolkit: tools,
          policy: {
            maxTurns: boundary === "turns" ? 1 : 4,
            maxToolCalls: boundary === "tool-calls" ? 1 : 4,
            tokenBudget: 10_000,
          },
          completion: {
            tool: "post_message",
            required: true,
            project: ({ parameters }) => parameters.message,
          },
          completionFromTools: [{ tool: "search", project: ({ result }) => Option.some(result) }],
        });

        const action = toolCallParts(
          "action-1",
          "search",
          {},
          boundary === "tokens" ? usageOf(9_000, 4_000) : emptyUsage,
        );

        const first =
          boundary === "mixed"
            ? [
                ...action.slice(0, -1),
                {
                  type: "tool-call" as const,
                  id: "reply-mixed",
                  name: "post_message",
                  params: { message: "invalid" },
                  providerExecuted: false,
                },
                { type: "finish" as const, reason: "tool-calls" as const, usage: emptyUsage },
              ]
            : action;

        const { model, requests } = scriptedModel([
          ...(boundary === "tool-calls" || boundary === "turns"
            ? [toolCallParts("ordinary-1", "post_message", { message: "ordinary failure" })]
            : []),
          first,
          toolCallParts("reply-final", "post_message", { message: "No action performed." }),
        ]);

        // A failed required completion consumes the first turn/call without ending the Run.
        class DeliveryFailure extends Schema.TaggedError<DeliveryFailure>()(
          "DeliveryFailure",
          {},
        ) {}

        const failingPost = Tool.make("post_message", {
          parameters: PostMessageTool.parametersSchema,
          success: PostMessageTool.successSchema,
          failure: DeliveryFailure,
          failureMode: "return",
        });

        const executable = Toolkit.make(failingPost, SearchTool);
        const starts = yield* Ref.make(0);
        const posts = yield* Ref.make(0);

        const result = yield* AgentRuntime.run(
          Agent.withModel({ ...definition, toolkit: executable }, model),
          "act",
        ).pipe(
          Effect.provide(
            executable.toLayer({
              search: () =>
                Ref.update(starts, (count) => count + 1).pipe(Effect.as("must not happen")),
              post_message: () =>
                Ref.updateAndGet(posts, (count) => count + 1).pipe(
                  Effect.flatMap((count) =>
                    count === 1 && (boundary === "tool-calls" || boundary === "turns")
                      ? DeliveryFailure.make({})
                      : Effect.succeed({ messageId: "reply" }),
                  ),
                ),
            }),
          ),
          Effect.exit,
        );

        expect(yield* Ref.get(starts)).toBe(0);
        if (boundary === "mixed" || boundary === "turns")
          expect(failureFrom(result)).toBeInstanceOf(ModelProtocolError);
        else {
          expect(Exit.isSuccess(result)).toBe(true);
          if (Exit.isSuccess(result)) expect(result.value.output).toBe("No action performed.");
        }
        expect(requests.length).toBeLessThanOrEqual(3);
      }),
  );

  it.effect.each(["completion", "completionFromTools"] as const)(
    "completion projectors encode decoded output before canonical validation: %s",
    (kind) =>
      Effect.gen(function* () {
        const definition = Agent.make("transformed-completion", {
          input: Schema.String,
          output: Schema.NumberFromString,
          instructions: "Return the committed numeric result.",
          toolkit: searchToolkit,
          ...(kind === "completion"
            ? { completion: { tool: "search" as const, project: () => 42 } }
            : {
                completionFromTools: [{ tool: "search" as const, project: () => Option.some(42) }],
              }),
        });

        const { model, requests } = scriptedModel([
          toolCallParts("transformed-action", "search", {}),
        ]);

        const result = yield* AgentRuntime.run(Agent.withModel(definition, model), "act").pipe(
          Effect.provide(searchToolkit.toLayer({ search: () => Effect.succeed("committed") })),
        );

        expect(result.output).toBe(42);
        expect(requests).toHaveLength(1);
      }),
  );

  it.effect.each(["throws", "invalid-output"] as const)(
    "action completion rejects an invalid projector after the action: %s",
    (mode) =>
      Effect.gen(function* () {
        const definition = Agent.make("invalid-action-completion", {
          input: Schema.String,
          output: Schema.String.check(Schema.isMinLength(1)),
          instructions: "Act once.",
          toolkit: searchToolkit,
          completionFromTools: [
            {
              tool: "search",
              project: () => {
                if (mode === "throws") throw new Error("projection failed");

                return Option.some("");
              },
            },
          ],
        });

        const { model, requests } = scriptedModel([toolCallParts("action-invalid", "search", {})]);
        const starts = yield* Ref.make(0);

        const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), "act").pipe(
          Effect.provide(
            searchToolkit.toLayer({
              search: () => Ref.update(starts, (n) => n + 1).pipe(Effect.as("done")),
            }),
          ),
          Effect.exit,
        );

        expect(failureFrom(exit)).toMatchObject({ _tag: "AgentOutputError" });
        expect(yield* Ref.get(starts)).toBe(1);
        expect(requests).toHaveLength(1);
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
  // Regression seam: https://linear.app/reve/issue/KOM-125
  for (const [provider, transformer] of [
    ["OpenAI", toCodecOpenAI],
    ["Anthropic", toCodecAnthropic],
  ] as const) {
    it.effect(`accounts for ${provider} native Tool schema expansion before dispatch`, () =>
      Effect.gen(function* () {
        const lookup = Tool.make("expanded_lookup", {
          parameters: Schema.Struct(
            Object.fromEntries(
              Array.from({ length: 10 }, (_, index) => [
                `record${index}`,
                Schema.Record(Schema.String, Schema.String),
              ]),
            ),
          ),
          success: Schema.String,
        });

        const tools = Toolkit.make(lookup);

        const definition = Agent.make("provider-schema-expansion", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Return a JSON string.",
          toolkit: tools,
          policy: { maxTurns: 2, maxToolCalls: 2, maxDuration: "30 seconds", runStatus: "off" },
        });

        const run = (toolSchemaTransformer?: LanguageModel.CodecTransformer) =>
          Effect.gen(function* () {
            const { model, requests } = scriptedModel([finalParts('"done"')]);

            const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), "input", {
              context: {
                prepare: (request) =>
                  Effect.succeed({
                    prompt: request.source,
                    modelCall: {
                      model,
                      toolSchemaTransformer,
                      context: ModelCallContext.make({
                        contextCapacity: 1_200,
                        outputReserveTokens: 100,
                        uncountedOverheadTokens: 0,
                      }),
                    },
                  }),
              },
            }).pipe(
              Effect.provide(tools.toLayer({ expanded_lookup: () => Effect.succeed("done") })),
              Effect.exit,
            );

            return { exit, requests };
          });

        const generic = yield* run();
        const native = yield* run(transformer);

        expect(Exit.isSuccess(generic.exit)).toBe(true);
        expect(generic.requests).toHaveLength(1);
        expect(failureFrom(native.exit)).toBeInstanceOf(ContextBudgetError);
        expect(native.requests).toHaveLength(0);
      }),
    );
  }

  it.effect("counts only tools exposed by the inherited grant for resolved context admission", () =>
    Effect.gen(function* () {
      const search = Tool.make("search", {
        parameters: Schema.Struct({ query: Schema.String }),
        success: Schema.String,
      });

      const forbidden = Tool.make("forbidden", {
        description: "Unavailable tool schema detail. ".repeat(220),
        parameters: Schema.Struct({ query: Schema.String }),
        success: Schema.String,
      });

      const tools = Toolkit.make(search, forbidden);

      const definition = Agent.make("granted-tool-context", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Return a JSON string.",
        toolkit: tools,
        policy: { maxTurns: 2, maxToolCalls: 2, maxDuration: "30 seconds", runStatus: "off" },
      });

      const run = Effect.fn(function* (restricted: boolean) {
        const { model, requests } = scriptedModel([finalParts('"done"')]);

        const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), "input", {
          delegationDepth: 1,
          ...(restricted
            ? { subagentGrant: SubagentGrant.make({ allowedToolNames: ["search"], maxDepth: 1 }) }
            : {}),
          context: {
            prepare: (request) =>
              Effect.succeed({
                prompt: request.source,
                modelCall: {
                  model,
                  toolSchemaTransformer: toCodecOpenAI,
                  context: ModelCallContext.make({
                    contextCapacity: 900,
                    outputReserveTokens: 100,
                    uncountedOverheadTokens: 0,
                  }),
                },
              }),
          },
        }).pipe(
          Effect.provide(
            tools.toLayer({
              search: () => Effect.succeed("unused"),
              forbidden: () => Effect.die("The hidden tool must not run"),
            }),
          ),
          Effect.exit,
        );

        return { exit, requests };
      });

      const unrestricted = yield* run(false);

      expect(failureFrom(unrestricted.exit)).toBeInstanceOf(ContextBudgetError);
      expect(unrestricted.requests).toHaveLength(0);

      const restricted = yield* run(true);

      expect(Exit.isSuccess(restricted.exit)).toBe(true);
      expect(restricted.requests).toHaveLength(1);
      expect(restricted.requests[0]?.toolCount).toBe(1);
    }),
  );

  // Regression: https://github.com/danieljvdm/effect-agent/commit/2259fc0
  // KOM-125: a transformed Class retains provider definitions that Schema.toEncoded removes.
  it.effect("admits original Tool schemas exactly as native OpenAI serializes them", () =>
    Effect.gen(function* () {
      class CredentialLookup extends Schema.Class<CredentialLookup>("CredentialLookupEncoded")({
        bindingId: Schema.String.annotate({
          description: "Reserved credential metadata. ".repeat(200),
        }),
      }) {}

      const lookup = Tool.make("credential_lookup", {
        parameters: CredentialLookup,
        success: Schema.String,
      });

      const toolkit = Toolkit.make(lookup);

      const definition = Agent.make("original-tool-provider-schema", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Return a JSON string.",
        toolkit,
        policy: { maxTurns: 2, maxToolCalls: 2, maxDuration: "30 seconds", runStatus: "off" },
      });

      const run = Effect.fn(function* (capacity: number) {
        const admittedSchemas: Array<unknown> = [];
        const wireSchemas: Array<unknown> = [];

        const client = HttpClient.make((request) =>
          Effect.gen(function* () {
            if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request body");

            const body = yield* HttpClientResponse.fromWeb(
              request,
              HttpServerResponse.toWeb(HttpServerResponse.uint8Array(request.body.body)),
            ).json.pipe(
              Effect.flatMap(
                Schema.decodeUnknownEffect(
                  Schema.Struct({
                    tools: Schema.Array(
                      Schema.Struct({ name: Schema.String, parameters: Schema.Json }),
                    ),
                  }),
                ),
              ),
              Effect.orDie,
            );

            expect(body.tools.map(({ name }) => name)).toEqual(["credential_lookup"]);
            wireSchemas.push(body.tools[0]?.parameters);

            const message = {
              type: "message",
              id: "message-schema",
              role: "assistant",
              status: "completed",
              content: [{ type: "output_text", text: '"done"', annotations: [] }],
            };

            const response = {
              id: "response-schema",
              object: "response",
              model: "gpt-5.6-terra",
              created_at: 0,
              output: [message],
            };

            const events = [
              { type: "response.created", response: { ...response, output: [] } },
              {
                type: "response.output_item.added",
                output_index: 0,
                item: { ...message, status: "in_progress", content: [] },
              },
              {
                type: "response.output_text.delta",
                item_id: message.id,
                output_index: 0,
                content_index: 0,
                delta: '"done"',
              },
              { type: "response.output_item.done", output_index: 0, item: message },
              { type: "response.completed", response },
            ];

            return HttpClientResponse.fromWeb(
              request,
              HttpServerResponse.toWeb(
                HttpServerResponse.text(
                  events
                    .map(
                      (event, sequence_number) =>
                        `event: ${event.type}\ndata: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
                    )
                    .join(""),
                  { contentType: "text/event-stream" },
                ),
              ),
            );
          }),
        );

        const provider = yield* OpenAiClient.make({ apiUrl: "https://provider.invalid/v1" }).pipe(
          Effect.provideService(HttpClient.HttpClient, client),
        );

        const model = OpenAiLanguageModel.model("gpt-5.6-terra").pipe(
          Layer.provide(Layer.succeed(OpenAiClient.OpenAiClient, provider)),
        );

        const transformer: LanguageModel.CodecTransformer = (schema) => {
          const transformed = toCodecOpenAI(schema);

          admittedSchemas.push(transformed.jsonSchema);

          return transformed;
        };

        const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), "input", {
          context: {
            prepare: (request) =>
              Effect.succeed({
                prompt: request.source,
                modelCall: {
                  model,
                  toolSchemaTransformer: transformer,
                  context: ModelCallContext.make({
                    contextCapacity: capacity,
                    outputReserveTokens: 100,
                    uncountedOverheadTokens: 0,
                  }),
                },
              }),
          },
        }).pipe(
          Effect.provide(toolkit.toLayer({ credential_lookup: () => Effect.succeed("unused") })),
          Effect.exit,
        );

        return { exit, admittedSchemas, wireSchemas };
      });

      const fitting = yield* run(20_000);

      expect(Exit.isSuccess(fitting.exit)).toBe(true);
      expect(fitting.wireSchemas).toHaveLength(1);
      expect(fitting.admittedSchemas).toEqual(fitting.wireSchemas);
      const tooSmall = yield* run(2_400);

      expect(failureFrom(tooSmall.exit)).toBeInstanceOf(ContextBudgetError);
      expect(tooSmall.wireSchemas).toHaveLength(0);
    }),
  );

  for (const timing of ["initial", "canonical-late", "transient-late"] as const) {
    it.effect(`accounts only selected completion tools after ${timing} finalization`, () =>
      Effect.gen(function* () {
        const research = Tool.make("expensive_research", {
          description: "Research-only schema detail. ".repeat(220),
          parameters: Schema.Struct({ query: Schema.String }),
          success: Schema.String,
        });

        const finish = Tool.make("finish", {
          parameters: Schema.Struct({ reason: Schema.String }),
          success: Schema.String,
        });

        const tools = Toolkit.make(research, finish);

        const definition = Agent.make("selected-completion-schema", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Return a JSON string or finish.",
          toolkit: tools,
          policy: {
            maxTurns: 4,
            maxToolCalls: 4,
            maxDuration: "30 seconds",
            runStatus: "off",
            tokenBudget: timing === "transient-late" ? 3_000 : 500,
            completionReserveTokens: timing === "canonical-late" ? 400 : 100,
          },
          completion: { tool: "finish", project: ({ result }) => result },
        });

        const { model, requests } = scriptedModel([finalParts('"done"')]);

        const context: RunContextHook = {
          prepare: (request) =>
            Effect.succeed({
              prompt: request.source,
              modelCall: {
                model,
                toolSchemaTransformer: toCodecOpenAI,
                context: ModelCallContext.make({
                  contextCapacity: timing === "transient-late" ? 2_200 : 900,
                  outputReserveTokens: 100,
                  uncountedOverheadTokens: 0,
                }),
              },
            }),
        };

        const result = yield* AgentRuntime.run(Agent.withModel(definition, model), "input", {
          context,
          ...(timing === "initial"
            ? { resumeUsage: { ...emptyResumeUsage, inputTokens: 501 } }
            : {}),
          ...(timing === "transient-late"
            ? { transientContext: { load: () => Effect.succeed("fresh reference ".repeat(300)) } }
            : {}),
        }).pipe(
          Effect.provide(
            tools.toLayer({
              expensive_research: () => Effect.die("Research must not run during finalization"),
              finish: () => Effect.succeed("done"),
            }),
          ),
        );

        expect(result.output).toBe("done");
        expect(result.finishReason).toBe("budget-exhausted");
        expect(requests).toHaveLength(1);
        expect(requests[0]?.toolCount).toBe(1);
        expect(requests[0]?.toolChoice).toEqual({ mode: "auto", oneOf: ["finish"] });
      }),
    );
  }
  // Regression seam: https://linear.app/reve/issue/KOM-125
  for (const [kind, parameters] of [
    ["unknown", Schema.Unknown],
    ["void", Schema.Void],
  ] as const) {
    it.effect(`counts native provider Tool configuration with ${kind} call parameters`, () =>
      Effect.gen(function* () {
        const builtin = Tool.providerDefined({
          id: "provider.builtin",
          customName: "Builtin",
          providerName: "builtin",
          args: Schema.Struct({ configuration: Schema.String }),
          parameters,
          success: Schema.String,
        });

        const user = Tool.make("user_tool", {
          parameters: Schema.Struct({ query: Schema.String }),
          success: Schema.String,
        });

        const dynamic = Tool.dynamic("dynamic_json", {
          parameters: {
            type: "object",
            properties: { query: { type: "string" } },
            required: ["query"],
          },
          success: Schema.String,
        });

        const run = (configuration: string) =>
          Effect.gen(function* () {
            const tools = Toolkit.make(builtin({ configuration }), user, dynamic);

            const definition = Agent.make("provider-tool-configuration", {
              input: Schema.String,
              output: Schema.String,
              instructions: "Return a JSON string.",
              toolkit: tools,
              policy: { maxTurns: 2, maxToolCalls: 2, maxDuration: "30 seconds", runStatus: "off" },
            });

            const { model, requests } = scriptedModel([finalParts('"done"')]);
            let transformations = 0;

            const transformer: LanguageModel.CodecTransformer = (schema) => {
              transformations += 1;

              return toCodecOpenAI(schema);
            };

            const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), "input", {
              context: {
                prepare: (request) =>
                  Effect.succeed({
                    prompt: request.source,
                    modelCall: {
                      model,
                      toolSchemaTransformer: transformer,
                      context: ModelCallContext.make({
                        contextCapacity: 1_200,
                        outputReserveTokens: 100,
                        uncountedOverheadTokens: 0,
                      }),
                    },
                  }),
              },
            }).pipe(
              Effect.provide(
                tools.toLayer({
                  user_tool: () => Effect.succeed("done"),
                  dynamic_json: () => Effect.succeed("done"),
                }),
              ),
              Effect.exit,
            );

            return { exit, requests, transformations };
          });

        const bounded = yield* run("small provider configuration");

        expect(Exit.isSuccess(bounded.exit)).toBe(true);
        expect(bounded.requests).toHaveLength(1);
        expect(bounded.requests[0]?.toolCount).toBe(3);
        // Raw dynamic JSON and provider-generated call parameters bypass function transformers.
        expect(bounded.transformations).toBe(1);
        const oversized = yield* run("provider configuration ".repeat(500));

        expect(failureFrom(oversized.exit)).toBeInstanceOf(ContextBudgetError);
        expect(oversized.requests).toHaveLength(0);
      }),
    );
  }
});
