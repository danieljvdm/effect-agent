import { expect, layer } from "@effect/vitest";
import { Cause, Context, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy, CompactionPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import {
  CompactionError,
  ContextCompactor,
  type CompactionDecision,
} from "effect-agent/context-compactor";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import { type RunEvent } from "effect-agent/run-event";
import {
  type RunCompactionCommit,
  type RunContextHook,
  type RunDurabilityHook,
} from "effect-agent/run-options";
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
  CLEARED_TOOL_RESULT,
  estimatePromptTokens,
  initialCompactionState,
} from "../../src/engine/internal/compaction.ts";
import { RunContextPreparationPassthrough } from "../../src/engine/RunOptions.ts";
import { ThreadHistory } from "../../src/engine/ThreadHistory.ts";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => Schema.decodeSync(ThreadId)(`thread-1-${++threadSequence}`)),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-1")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-1")),
});

const emptyUsage = { inputTokens: {}, outputTokens: {} };

const usageOf = (input: number, output: number) => ({
  inputTokens: { total: input },
  outputTokens: { total: output },
});

const finalParts = (
  text: string,
  usage: typeof emptyUsage | ReturnType<typeof usageOf> = emptyUsage,
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
  usage: typeof emptyUsage | ReturnType<typeof usageOf> = emptyUsage,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "tool-call", id, name, params, providerExecuted: false },
  { type: "finish", reason: "tool-calls", usage },
];

interface CapturedRequest {
  readonly prompt: Prompt.Prompt;
  readonly toolCount: number;
  readonly toolChoice: unknown;
}

type ScriptEntry = ReadonlyArray<Response.StreamPartEncoded> | { readonly fail: string };

const overflowFailure = (description: string): AiError.AiError =>
  AiError.AiError.make({
    module: "test",
    method: "streamText",
    reason: AiError.UnknownError.make({ description }),
  });

/** Scripted multi-call model; an entry may fail the whole request typed. */
const scriptedModel = (script: ReadonlyArray<ScriptEntry>, name = "compaction") => {
  const requests: Array<CapturedRequest> = [];

  const model = Model.make(
    "scripted",
    name,
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
          const entry = script[index];

          if (entry === undefined) return Stream.empty;
          if ("fail" in entry) return Stream.fail(overflowFailure(entry.fail));

          return Stream.fromIterable(entry);
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

const SearchTool = Tool.make("search", {
  parameters: Schema.Struct({}),
  success: Schema.String,
});

const searchToolkit = Toolkit.make(SearchTool);

const answerOutput = Schema.Struct({ answer: Schema.String });

const compactionEvents = (events: ReadonlyArray<RunEvent>) =>
  events.flatMap((event) => (event._tag === "CompactionPerformed" ? [event] : []));

interface RunSetup {
  readonly policy: AgentPolicy;
  readonly script: ReadonlyArray<ScriptEntry>;
  readonly results: ReadonlyArray<string>;
  readonly commitCompaction?: (commit: RunCompactionCommit) => Effect.Effect<void>;
  readonly context?: RunContextHook | undefined;
}

const basePolicy = {
  maxTurns: 6,
  maxToolCalls: 6,
  maxDuration: "1 minute",
  toolConcurrency: 1,
} as const;

/** Capture model requests, published history and Run outcomes. */
const driveRun = (setup: RunSetup) =>
  Effect.gen(function* () {
    const { policy, script, results, commitCompaction } = setup;

    const definition = Agent.make("compaction-agent", {
      input: Schema.Struct({ question: Schema.String }),
      output: answerOutput,
      instructions: "Research the question with the search tool, then answer.",
      toolkit: searchToolkit,
      policy,
    });

    const { model, requests } = scriptedModel(script);
    const callCount = yield* Ref.make(0);

    const toolLayer = searchToolkit.toLayer({
      search: () =>
        Ref.getAndUpdate(callCount, (count) => count + 1).pipe(
          Effect.map((count) => results[count] ?? "found"),
        ),
    });

    const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
    const histories: Array<Prompt.Prompt> = [];

    const durability: RunDurabilityHook | undefined =
      commitCompaction === undefined
        ? undefined
        : {
            commitResponse: () => Effect.void,
            prepareToolCalls: () => Effect.void,
            step: {
              lookup: () => Effect.succeed(Option.none()),
              commit: () => Effect.void,
            },
            commitCompaction,
            noteTurnUsage: () => Effect.void,
          };

    const exit = yield* AgentRuntime.stream(
      Agent.withModel(definition, model),
      { question: "compact?" },
      {
        context: setup.context,
        onHistory: (history) => Effect.sync(() => void histories.push(history)),
        ...(durability === undefined ? {} : { durability }),
      },
    ).pipe(
      Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
      Stream.runDrain,
      Effect.provide(toolLayer),
      Effect.exit,
    );

    return { exit, requests, histories, events: yield* Ref.get(events) };
  });

const compactionTestLayer = Layer.merge(identifiers, ContextCompactor.layer);

const testLayer = Layer.mergeAll(
  compactionTestLayer,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer)("engine compaction and overflow recovery", (it) => {
  for (const change of ["equivalent", "replace"] as const) {
    it.effect(`preserves protected prepared history across compaction: ${change}`, () =>
      Effect.gen(function* () {
        const result = yield* driveRun({
          policy: AgentPolicy.make({
            ...basePolicy,
            contextTokenLimit: 2_000,
            compaction: CompactionPolicy.make({ mode: "summarize", keepRecentTokens: 300 }),
          }),
          context: {
            prepare: ({ source, turn }) => {
              const messages = [...source.content];

              if (turn === 4) {
                if (change === "replace") {
                  messages[2] = Prompt.assistantMessage({
                    content: [Prompt.textPart({ text: "UNRELATED" })],
                  });
                }
              }

              return Effect.succeed({
                prompt: Schema.decodeUnknownSync(Prompt.Prompt)(
                  JSON.parse(JSON.stringify({ content: messages })),
                ),
              });
            },
          },
          script: [
            toolCallParts("s1", "search", {}, usageOf(100, 5)),
            toolCallParts("s2", "search", {}, usageOf(1_300, 5)),
            toolCallParts("s3", "search", {}, usageOf(50, 5)),
            finalParts('{"answer":"done"}'),
          ],
          results: ["a".repeat(4_000), "b".repeat(4_000), "small"],
        }).pipe(
          Effect.provideService(ContextCompactor, {
            estimate: estimatePromptTokens,
            compact: () =>
              Stream.succeed({ kind: "summarize", through: 4, summary: "First result covered" }),
          }),
        );

        expect(promptText(result.requests[2]?.prompt ?? Prompt.empty)).toContain("compact?");
        if (change === "equivalent") {
          if (Exit.isFailure(result.exit)) return yield* Effect.failCause(result.exit.cause);
          expect(result.requests).toHaveLength(4);
          expect(toolResultValues(result.requests[3]?.prompt ?? Prompt.empty)).toEqual([
            "b".repeat(4_000),
            "small",
          ]);
        } else {
          expect(failureFrom(result.exit)).toBeInstanceOf(CompactionError);
          expect(result.requests).toHaveLength(3);
        }
        expect(JSON.stringify(result.histories)).toContain("a".repeat(4_000));
        expect(JSON.stringify(result.histories)).not.toContain("UNRELATED");
      }),
    );
  }

  it.effect("RUN-034: summarizer usage is charged before admitting the post-compaction call", () =>
    Effect.gen(function* () {
      const policy = AgentPolicy.make({
        ...basePolicy,
        tokenBudget: 12_000,
        completionReserveTokens: 1_000,
        contextTokenLimit: 2_000,
        compaction: CompactionPolicy.make({ keepRecentTokens: 300, mode: "summarize" }),
      });

      const { exit, requests, events } = yield* driveRun({
        policy,
        script: [
          toolCallParts("s1", "search", {}, usageOf(100, 5)),
          toolCallParts("s2", "search", {}, usageOf(1_300, 5)),
          finalParts("Goal: preserve delivery capacity", usageOf(9_600, 100)),
          finalParts('{"answer":"delivered"}', usageOf(50, 10)),
        ],
        results: ["a".repeat(4_000), "b".repeat(4_000)],
        commitCompaction: () => Effect.void,
      });

      expect(requests).toHaveLength(4);
      expect(Exit.isSuccess(exit)).toBe(true);
      expect(requests[3]?.toolChoice).toBe("none");
      expect(
        events.some(
          (event) =>
            event._tag === "RunCompleted" &&
            event.finishReason === "budget-exhausted" &&
            event.exhausted === "tokens",
        ),
      ).toBe(true);
    }),
  );

  // ------------------------------------------------------------ RUN-027 flows

  {
    it.effect("shares the Turn's summary allowance with overflow recovery", () =>
      Effect.gen(function* () {
        const commits: Array<RunCompactionCommit> = [];
        const passes: Array<boolean> = [];

        const result = yield* driveRun({
          policy: AgentPolicy.make({ ...basePolicy, contextTokenLimit: 800, runStatus: "off" }),
          script: [
            toolCallParts("s1", "search", {}),
            toolCallParts("s2", "search", {}),
            toolCallParts("s3", "search", {}),
            finalParts("summary-first"),
            { fail: "context_length_exceeded" },
            finalParts("summary-retry"),
            finalParts('{"answer":"done"}'),
          ],
          results: ["first result", "second result", "third result"],
          commitCompaction: (commit) =>
            Effect.sync(() => {
              commits.push(commit);
            }),
        }).pipe(
          Effect.provideService(ContextCompactor, {
            // Three active results trigger pressure; clearing or summarizing one fits the target.
            estimate: (messages) =>
              300 *
              messages.filter(
                (message) =>
                  message.role === "tool" &&
                  message.content.some(
                    (part) => part.type === "tool-result" && part.result !== CLEARED_TOOL_RESULT,
                  ),
              ).length,
            compact: (request) =>
              Stream.suspend(() => {
                passes.push(request.trigger === "overflow");
                const kind = "summarize";
                const through = request.trigger === "overflow" ? 6 : 4;

                return Stream.fromEffect(
                  request.summarize(
                    Prompt.fromMessages([
                      Prompt.userMessage({
                        content: [Prompt.textPart({ text: "Summarize requested coverage." })],
                      }),
                    ]),
                  ),
                ).pipe(Stream.map((summary): CompactionDecision => ({ kind, through, summary })));
              }),
          }),
        );

        expect(commits.map(({ turn, kind }) => [turn, kind])).toEqual([[4, "summarize"]]);
        expect(compactionEvents(result.events).map((event) => event.kind)).toEqual(["summarize"]);
        expect(passes).toEqual([false]);
        expect(
          result.requests.filter(({ prompt }) =>
            promptText(prompt).includes("Summarize requested coverage."),
          ),
        ).toHaveLength(1);

        expect(failureFrom(result.exit)).toBeInstanceOf(CompactionError);
        expect(result.requests).toHaveLength(5);
      }),
    );
  }
});

export const verifyCompactionCallbackAndModelRequirements = () =>
  Effect.gen(function* () {
    class SummaryConfig extends Context.Service<SummaryConfig, { readonly text: string }>()(
      "test/SummaryConfig",
    ) {}
    class SummaryFailure extends Schema.TaggedError<SummaryFailure>()("SummaryFailure", {}) {}

    const source = Prompt.fromMessages([
      Prompt.assistantMessage({ content: [Prompt.textPart({ text: "older history" })] }),
      Prompt.userMessage({ content: [Prompt.textPart({ text: "latest input" })] }),
    ]);

    const compactor = yield* ContextCompactor;

    const summarize = () =>
      Effect.gen(function* () {
        const config = yield* SummaryConfig;

        if (config.text === "") return yield* SummaryFailure.make({});

        return config.text;
      });

    const program = compactor
      .compact({
        source,
        state: initialCompactionState(),
        policy: CompactionPolicy.make({ keepRecentTokens: 1, mode: "summarize" }),
        targetTokens: 10,
        threadId: ThreadId.make("thread-compact"),
        runId: RunId.make("run-compact"),
        turn: 1,
        trigger: "pressure",
        modelCallAllowed: true,
        summarize,
      })
      .pipe(Stream.runCollect);

    const errorProof: SummaryFailure extends Effect.Error<typeof program> ? true : false = true;

    const requirementProof: SummaryConfig extends Effect.Services<typeof program> ? true : false =
      true;

    const model = Model.make(
      "test",
      "configured",
      Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          const config = yield* SummaryConfig;

          return yield* LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => Stream.fromIterable(finalParts(config.text)),
          });
        }),
      ),
    );

    const configured = ContextCompactor.layerWithModel(model);

    const modelRequirements: SummaryConfig extends Layer.Services<typeof configured>
      ? true
      : false = true;

    void [errorProof, requirementProof, modelRequirements];
  });
