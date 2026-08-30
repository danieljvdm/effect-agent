import {
  Agent,
  AgentPolicy,
  CompactionPolicy,
  ContextBudgetError,
  ContextOverflowError,
  ConversationId,
  IdGenerator,
  ModelProtocolError,
  RunId,
  TurnId,
  type RunEvent,
} from "@effect-agent/core";
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

import {
  CLEARED_TOOL_RESULT,
  COMPACTION_INSTRUCTION,
  COMPACTION_SUMMARY_PREFIX,
  estimateMessageTokens,
  estimatePromptTokens,
  isContextOverflowMessage,
  renderForSummary,
  SUMMARY_INPUT_BUDGET,
} from "../src/compaction.ts";
import {
  AgentRuntime,
  type RunCompactionCommit,
  type RunDurabilityHook,
  type RunTurnUsage,
} from "../src/index.ts";

const identifiers = Layer.succeed(IdGenerator, {
  nextConversationId: Effect.succeed(Schema.decodeSync(ConversationId)("conversation-1")),
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
const scriptedModel = (script: ReadonlyArray<ScriptEntry>) => {
  const requests: Array<CapturedRequest> = [];
  const model = Model.make(
    "scripted",
    "compaction",
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
  readonly noteTurnUsage?: (usage: RunTurnUsage) => Effect.Effect<void>;
}

const basePolicy = {
  maxTurns: 6,
  maxToolCalls: 6,
  maxDuration: "1 minute",
  toolConcurrency: 1,
} as const;

/** Drive one scripted run under an explicit output Schema (RUN-028 reservation cases). */
const driveRunWith = <Output extends Schema.Top>(output: Output, setup: RunSetup) =>
  Effect.gen(function* () {
    const { policy, script, results, commitCompaction } = setup;
    const definition = Agent.define("compaction-agent", {
      input: Schema.Struct({ question: Schema.String }),
      output,
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
            // Required by the durability protocol; usage staging is not under test here.
            noteTurnUsage: setup.noteTurnUsage ?? (() => Effect.void),
          };
    const exit = yield* AgentRuntime.stream(
      Agent.withModel(definition, model),
      { question: "compact?" },
      durability === undefined ? {} : { durability },
    ).pipe(
      Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
      Stream.runDrain,
      Effect.provide(toolLayer),
      Effect.exit,
    );
    return { exit, requests, events: yield* Ref.get(events) };
  });

/** Drive one scripted run and capture requests, events, and the exit. */
const driveRun = (setup: RunSetup) => driveRunWith(answerOutput, setup);

layer(identifiers)("engine compaction and overflow recovery", (it) => {
  // ------------------------------------------------------------ pure helpers

  it.effect("RUN-027: classifies provider overflow messages and nothing else", () =>
    Effect.sync(() => {
      const positives = [
        "context_length_exceeded",
        "Prompt is too long: 210000 tokens",
        "This request exceeds the maximum context length of the model",
        "the input is far too long for this model",
        "request exceeds the available context",
        "too many tokens in the request",
        "maximum context window reached",
      ];
      for (const text of positives) {
        expect(isContextOverflowMessage(text)).toBe(true);
      }
      const negatives = ["rate limit exceeded", "invalid api key", "content filtered", ""];
      for (const text of negatives) {
        expect(isContextOverflowMessage(text)).toBe(false);
      }
    }),
  );

  it.effect("RUN-026: summarizer input clips every message and stays under the total budget", () =>
    Effect.sync(() => {
      const userMessage = (text: string) =>
        Prompt.makeMessage("user", {
          content: [Prompt.makePart("text", { text })],
        });

      // Per-message clip applies to plain text, not only tool results.
      const oversized = renderForSummary([userMessage(`start${"x".repeat(10_000)}end`)], undefined);
      expect(oversized.length).toBeLessThan(3_000);

      // Total budget: many clipped messages exceed it; middle-out retention
      // keeps head and tail content with one deterministic elision marker.
      const many = Array.from({ length: 80 }, (_, index) =>
        userMessage(`marker-${index} ${"y".repeat(1_900)}`),
      );
      const rendered = renderForSummary(many, "previous summary text");
      expect(rendered.length + COMPACTION_INSTRUCTION.length).toBeLessThanOrEqual(
        SUMMARY_INPUT_BUDGET,
      );
      expect(rendered.includes("previous summary text")).toBe(true);
      expect(rendered.includes("marker-0 ")).toBe(true);
      expect(rendered.includes("marker-79 ")).toBe(true);
      expect(rendered.includes("omitted from summary input")).toBe(true);
      expect(rendered.includes("marker-40 ")).toBe(false);
      // Determinism: identical input renders identically.
      expect(renderForSummary(many, "previous summary text")).toBe(rendered);
    }),
  );

  it.effect("RUN-026: token estimates are deterministic and additive", () =>
    Effect.sync(() => {
      const message = {
        role: "user",
        content: [{ type: "text", text: "hello 🌍" }],
      } as unknown as Prompt.Message;
      const one = estimateMessageTokens(message);
      expect(one).toBeGreaterThan(0);
      expect(estimateMessageTokens(message)).toBe(one);
      expect(estimatePromptTokens([message, message])).toBe(one * 2);
    }),
  );

  // ------------------------------------------------------------ RUN-026 prune

  it.effect(
    "RUN-026: clears old tool results at the pre-turn threshold and keeps the protected tail",
    () =>
      Effect.gen(function* () {
        const policy = AgentPolicy.make({
          ...basePolicy,
          contextTokenLimit: 2_000,
          compaction: CompactionPolicy.make({ keepRecentTokens: 1_200 }),
        });
        const { exit, requests, events } = yield* driveRun({
          policy,
          script: [
            toolCallParts("s1", "search", {}, usageOf(100, 5)),
            toolCallParts("s2", "search", {}, usageOf(1_200, 5)),
            finalParts('{"answer":"done"}', usageOf(1_100, 5)),
          ],
          results: ["a".repeat(4_000), "b".repeat(4_000)],
        });

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(requests).toHaveLength(3);
        const third = requests[2];
        if (third === undefined) throw new Error("expected a third model request");
        const results = toolResultValues(third.prompt);
        expect(results).toHaveLength(2);
        expect(results[0]).toBe(CLEARED_TOOL_RESULT);
        expect(results[1]).toBe("b".repeat(4_000));

        const performed = compactionEvents(events);
        expect(performed).toHaveLength(1);
        expect(performed[0]?.kind).toBe("clear-tool-results");
        expect(performed[0]?.tokensAfterEstimate).toBeLessThan(
          performed[0]?.tokensBeforeEstimate ?? 0,
        );
      }),
  );

  it.effect(
    "RUN-034: cumulative token pressure compacts before it spends the completion reserve",
    () =>
      Effect.gen(function* () {
        const policy = AgentPolicy.make({
          ...basePolicy,
          tokenBudget: 2_000,
          completionReserveTokens: 500,
          compaction: CompactionPolicy.make({ keepRecentTokens: 200, mode: "prune" }),
        });
        const { exit, requests, events } = yield* driveRun({
          policy,
          script: [
            toolCallParts("s1", "search", {}, usageOf(100, 5)),
            toolCallParts("s2", "search", {}, usageOf(700, 5)),
            finalParts('{"answer":"done"}', usageOf(200, 5)),
          ],
          results: ["old".repeat(1_000), "recent"],
        });

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(requests).toHaveLength(3);
        const finalRequest = requests[2];
        if (finalRequest === undefined) throw new Error("expected a final request");
        expect(toolResultValues(finalRequest.prompt)).toEqual([CLEARED_TOOL_RESULT, "recent"]);
        expect(compactionEvents(events).map((event) => event.kind)).toContain("clear-tool-results");
      }),
  );

  it.effect("RUN-034: summarizer usage is charged before admitting the post-compaction call", () =>
    Effect.gen(function* () {
      const policy = AgentPolicy.make({
        ...basePolicy,
        tokenBudget: 12_000,
        completionReserveTokens: 1_000,
        contextTokenLimit: 1_500,
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

  // -------------------------------------------------------- RUN-026 summarize

  const invalidSummaries: ReadonlyArray<{
    readonly name: string;
    readonly parts: ReadonlyArray<Response.StreamPartEncoded>;
  }> = [
    { name: "finish-only", parts: [{ type: "finish", reason: "stop", usage: usageOf(50, 20) }] },
    { name: "whitespace-only", parts: finalParts(" \n\t ", usageOf(50, 20)) },
    {
      name: "truncated",
      parts: [
        ...finalParts("partial summary").slice(0, -1),
        { type: "finish", reason: "length", usage: usageOf(50, 20) },
      ],
    },
    { name: "missing finish", parts: finalParts("unfinished summary").slice(0, -1) },
    {
      name: "unfinished text",
      parts: [
        { type: "text-start", id: "summary" },
        { type: "text-delta", id: "summary", delta: "partial summary" },
        { type: "finish", reason: "stop", usage: usageOf(50, 20) },
      ],
    },
  ];

  for (const invalid of invalidSummaries) {
    it.effect(
      `rejects ${invalid.name} compaction without replacing prior coverage, and charges usage`,
      () =>
        Effect.gen(function* () {
          const commits: Array<RunCompactionCommit> = [];
          const usage: Array<RunTurnUsage> = [];
          const { exit, requests, events } = yield* driveRun({
            policy: AgentPolicy.make({
              ...basePolicy,
              contextTokenLimit: 1_500,
              compaction: CompactionPolicy.make({ keepRecentTokens: 300, mode: "summarize" }),
            }),
            script: [
              toolCallParts("s1", "search", {}, usageOf(100, 5)),
              toolCallParts("s2", "search", {}, usageOf(1_300, 5)),
              finalParts("Goal: preserved summary", usageOf(50, 20)),
              toolCallParts("s3", "search", {}, usageOf(1_300, 5)),
              invalid.parts,
              finalParts('{"answer":"must not run"}'),
            ],
            results: ["alpha".repeat(800), "bravo".repeat(800), "charl".repeat(800)],
            commitCompaction: (commit) =>
              Effect.sync(() => {
                commits.push(commit);
              }),
            noteTurnUsage: (call) =>
              Effect.sync(() => {
                usage.push(call);
              }),
          });
          expect(failureFrom(exit)).toBeInstanceOf(ModelProtocolError);
          expect(requests).toHaveLength(5);
          expect(compactionEvents(events).map((event) => event.kind)).toEqual(["summarize"]);
          expect(commits.map((commit) => commit.summary)).toEqual(["Goal: preserved summary"]);
          expect(usage).toHaveLength(invalid.name === "missing finish" ? 4 : 5);
          if (invalid.name !== "missing finish") {
            expect(usage[4]?.usage.inputTokens.total).toBe(50);
            expect(usage[4]?.usage.outputTokens.total).toBe(20);
          }
          const lastRequest = requests[4];
          if (lastRequest === undefined) throw new Error("expected rejected summarizer request");
          expect(promptText(lastRequest.prompt)).toContain(
            "[Previous summary]\nGoal: preserved summary",
          );
        }),
    );
  }

  it.effect("RUN-026: summarizes when configured, rebuilding instructions + summary + tail", () =>
    Effect.gen(function* () {
      const commits = yield* Ref.make<ReadonlyArray<RunCompactionCommit>>([]);
      const policy = AgentPolicy.make({
        ...basePolicy,
        contextTokenLimit: 1_500,
        compaction: CompactionPolicy.make({ keepRecentTokens: 300, mode: "summarize" }),
      });
      const { exit, requests, events } = yield* driveRun({
        policy,
        script: [
          toolCallParts("s1", "search", {}, usageOf(100, 5)),
          toolCallParts("s2", "search", {}, usageOf(1_300, 5)),
          finalParts("Goal: find data", usageOf(50, 20)),
          finalParts('{"answer":"done"}', usageOf(400, 5)),
        ],
        results: ["a".repeat(4_000), "b".repeat(4_000)],
        commitCompaction: (commit) => Ref.update(commits, (all) => [...all, commit]),
      });

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(requests).toHaveLength(4);

      // Request 3 is the summarizer call: instruction + rendered transcript.
      const summarizer = requests[2];
      if (summarizer === undefined) throw new Error("expected a summarizer request");
      const summarizerText = promptText(summarizer.prompt);
      expect(summarizerText).toContain(COMPACTION_INSTRUCTION);
      expect(summarizerText).toContain("[tool result search:");
      expect(summarizer.toolCount).toBe(0);

      // Request 4 is the compacted turn: instructions survive, the summary
      // replaces the covered span, the recent tail stays verbatim.
      const compacted = requests[3];
      if (compacted === undefined) throw new Error("expected a compacted request");
      const compactedText = promptText(compacted.prompt);
      expect(compactedText).toContain("Research the question with the search tool");
      expect(compactedText).toContain(`${COMPACTION_SUMMARY_PREFIX}Goal: find data`);
      const results = toolResultValues(compacted.prompt);
      expect(results).toEqual(["b".repeat(4_000)]);

      const performed = compactionEvents(events);
      expect(performed).toHaveLength(1);
      expect(performed[0]?.kind).toBe("summarize");

      const observedCommits = yield* Ref.get(commits);
      expect(observedCommits).toHaveLength(1);
      expect(observedCommits[0]?.kind).toBe("summarize");
      expect(observedCommits[0]?.turn).toBe(3);
      expect(observedCommits[0]?.summary).toBe("Goal: find data");
    }),
  );

  it.effect(
    "RUN-026: a repeated summarize covers only the new interval plus the previous summary",
    () =>
      Effect.gen(function* () {
        const policy = AgentPolicy.make({
          ...basePolicy,
          maxTurns: 8,
          maxToolCalls: 8,
          contextTokenLimit: 1_500,
          compaction: CompactionPolicy.make({ keepRecentTokens: 300, mode: "summarize" }),
        });
        const { exit, requests } = yield* driveRun({
          policy,
          script: [
            toolCallParts("s1", "search", {}, usageOf(100, 5)),
            toolCallParts("s2", "search", {}, usageOf(1_300, 5)),
            finalParts("Goal: first summary", usageOf(50, 20)),
            toolCallParts("s3", "search", {}, usageOf(1_300, 5)),
            finalParts("Goal: second summary", usageOf(50, 20)),
            finalParts('{"answer":"done"}', usageOf(400, 5)),
          ],
          results: ["alpha".repeat(800), "bravo".repeat(800), "charl".repeat(800)],
          commitCompaction: () => Effect.void,
        });

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(requests).toHaveLength(6);

        const second = requests[4];
        if (second === undefined) throw new Error("expected a second summarizer request");
        const secondText = promptText(second.prompt);
        // The second summarize carries the previous summary and the newly
        // covered span — never the span the first summarize already folded.
        expect(secondText).toContain("[Previous summary]");
        expect(secondText).toContain("Goal: first summary");
        expect(secondText.includes("bravo")).toBe(true);
        expect(secondText.includes("alpha")).toBe(false);
      }),
  );

  it.effect("RUN-034: mode prune fails typed when protected context cannot reach the target", () =>
    Effect.gen(function* () {
      const policy = AgentPolicy.make({
        ...basePolicy,
        contextTokenLimit: 500,
        compaction: CompactionPolicy.make({ keepRecentTokens: 1_000, mode: "prune" }),
      });
      const { exit, requests, events } = yield* driveRun({
        policy,
        script: [
          toolCallParts("s1", "search", {}, usageOf(600, 5)),
          finalParts('{"answer":"done"}', usageOf(650, 5)),
        ],
        results: ["small"],
      });

      expect(Exit.isFailure(exit)).toBe(true);
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(ContextBudgetError);
      expect(requests).toHaveLength(1);
      expect(compactionEvents(events)).toHaveLength(0);
    }),
  );

  it.effect(
    "RUN-028: the context target includes the output contract and fails before provider I/O when the protected tail cannot fit",
    () =>
      Effect.gen(function* () {
        // The contract is appended to every outgoing request AFTER the window
        // check, so the estimate must reserve its size: a view comfortably
        // under the limit still compacts when view + contract crosses it.
        const paddedOutput = Schema.Struct({ answer: Schema.String }).annotate({
          description: "p".repeat(12_000),
        });
        const policy = AgentPolicy.make({
          ...basePolicy,
          contextTokenLimit: 5_000,
          compaction: CompactionPolicy.make({ keepRecentTokens: 200, mode: "prune" }),
        });
        const bigResult = "r".repeat(6_000);
        const recentResult = "s".repeat(8_000);

        const reserved = yield* driveRunWith(paddedOutput, {
          policy,
          script: [
            toolCallParts("s1", "search", {}, usageOf(100, 5)),
            toolCallParts("s2", "search", {}, usageOf(200, 5)),
            finalParts('{"answer":"done"}', usageOf(300, 5)),
          ],
          results: [bigResult, recentResult],
        });
        expect(Exit.isFailure(reserved.exit)).toBe(true);
        expect(failureFrom(reserved.exit)).toBeInstanceOf(ContextBudgetError);
        // The contract plus the protected newest result cannot reach the
        // target, so the next provider request never starts.
        expect(reserved.requests.length).toBeLessThan(3);

        // Control: an unrenderable output Schema produces no contract, so the
        // identical view under the identical limit never compacts.
        const control = yield* driveRunWith(
          Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Int, Schema.String]),
          {
            policy,
            script: [
              toolCallParts("s1", "search", {}, usageOf(100, 5)),
              toolCallParts("s2", "search", {}, usageOf(200, 5)),
              finalParts('["first",1,"last"]', usageOf(300, 5)),
            ],
            results: [bigResult, recentResult],
          },
        );
        expect(Exit.isSuccess(control.exit)).toBe(true);
        expect(compactionEvents(control.events)).toHaveLength(0);
        const controlFinal = control.requests.at(-1);
        if (controlFinal === undefined) throw new Error("expected a final control request");
        expect(toolResultValues(controlFinal.prompt)).toContain(bigResult);
      }),
  );

  it.effect("RUN-026: never compacts without a contextTokenLimit", () =>
    Effect.gen(function* () {
      const policy = AgentPolicy.make({ ...basePolicy });
      const { exit, requests, events } = yield* driveRun({
        policy,
        script: [
          toolCallParts("s1", "search", {}, usageOf(100, 5)),
          toolCallParts("s2", "search", {}, usageOf(1_200, 5)),
          finalParts('{"answer":"done"}', usageOf(2_000, 5)),
        ],
        results: ["a".repeat(4_000), "b".repeat(4_000)],
      });

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(requests).toHaveLength(3);
      const third = requests[2];
      if (third === undefined) throw new Error("expected a third model request");
      expect(toolResultValues(third.prompt)).toEqual(["a".repeat(4_000), "b".repeat(4_000)]);
      expect(compactionEvents(events)).toHaveLength(0);
    }),
  );

  // ------------------------------------------------------------ RUN-027 flows

  it.effect("RUN-027: a classified overflow compacts and retries exactly once", () =>
    Effect.gen(function* () {
      const policy = AgentPolicy.make({
        ...basePolicy,
        contextTokenLimit: 100_000,
        compaction: CompactionPolicy.make({ keepRecentTokens: 300 }),
      });
      const { exit, requests, events } = yield* driveRun({
        policy,
        script: [
          toolCallParts("s1", "search", {}, usageOf(100, 5)),
          toolCallParts("s2", "search", {}, usageOf(200, 5)),
          { fail: "context_length_exceeded: prompt is too long" },
          finalParts("Goal: recover", usageOf(20, 10)),
          finalParts('{"answer":"recovered"}', usageOf(60, 5)),
        ],
        results: ["a".repeat(4_000), "b".repeat(4_000)],
      });

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(requests).toHaveLength(5);
      const performed = compactionEvents(events);
      expect(performed).toHaveLength(1);
      expect(performed[0]?.kind).toBe("summarize");
      const retried = requests[4];
      if (retried === undefined) throw new Error("expected a retried model request");
      expect(promptText(retried.prompt)).toContain(COMPACTION_SUMMARY_PREFIX);
      expect(toolResultValues(retried.prompt)).toEqual(["b".repeat(4_000)]);
    }),
  );

  it.effect("RUN-027: a second overflow on the same Turn fails typed with retried true", () =>
    Effect.gen(function* () {
      const policy = AgentPolicy.make({
        ...basePolicy,
        contextTokenLimit: 100_000,
        compaction: CompactionPolicy.make({ keepRecentTokens: 300 }),
      });
      const { exit, requests } = yield* driveRun({
        policy,
        script: [
          toolCallParts("s1", "search", {}, usageOf(100, 5)),
          toolCallParts("s2", "search", {}, usageOf(200, 5)),
          { fail: "maximum context length exceeded" },
          finalParts("Goal: recover", usageOf(20, 10)),
          { fail: "maximum context length exceeded" },
        ],
        results: ["a".repeat(4_000), "b".repeat(4_000)],
      });

      expect(requests).toHaveLength(5);
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(ContextOverflowError);
      if (failure instanceof ContextOverflowError) {
        expect(failure.retried).toBe(true);
      }
    }),
  );

  it.effect("RUN-034: overflow compaction fails locally when its summary cannot reach target", () =>
    Effect.gen(function* () {
      const policy = AgentPolicy.make({
        ...basePolicy,
        contextTokenLimit: 20_000,
        compaction: CompactionPolicy.make({ keepRecentTokens: 300, mode: "summarize" }),
      });
      const { exit, requests } = yield* driveRun({
        policy,
        script: [
          toolCallParts("s1", "search", {}, usageOf(100, 5)),
          toolCallParts("s2", "search", {}, usageOf(200, 5)),
          { fail: "maximum context length exceeded" },
          finalParts(`Goal: ${"summary".repeat(20_000)}`, usageOf(20, 10)),
        ],
        results: ["a".repeat(4_000), "b".repeat(4_000)],
      });

      expect(failureFrom(exit)).toBeInstanceOf(ContextBudgetError);
      // Two research calls, the rejected call, and one summarizer call. The
      // engine never sends a retry it already knows exceeds the target.
      expect(requests).toHaveLength(4);
    }),
  );

  it.effect(
    "RUN-027: overflow without compaction configured fails typed immediately with retried false",
    () =>
      Effect.gen(function* () {
        const policy = AgentPolicy.make({ ...basePolicy });
        const { exit, requests } = yield* driveRun({
          policy,
          script: [{ fail: "This request exceeds the maximum context length" }],
          results: [],
        });

        expect(requests).toHaveLength(1);
        const failure = failureFrom(exit);
        expect(failure).toBeInstanceOf(ContextOverflowError);
        if (failure instanceof ContextOverflowError) {
          expect(failure.retried).toBe(false);
        }
      }),
  );

  it.effect("RUN-027: a non-overflow provider error passes through unchanged", () =>
    Effect.gen(function* () {
      const policy = AgentPolicy.make({
        ...basePolicy,
        contextTokenLimit: 100_000,
      });
      const { exit, requests } = yield* driveRun({
        policy,
        script: [{ fail: "rate limited, slow down" }],
        results: [],
      });

      expect(requests).toHaveLength(1);
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(AiError.AiError);
    }),
  );
});
