import {
  Agent,
  AgentApprovalDenied,
  AgentPolicy,
  ConversationId,
  IdGenerator,
  ModelProtocolError,
  RunId,
  TurnId,
  type RunEvent,
} from "@effect-agent/core";
import { expect, layer } from "@effect/vitest";
import { Cause, Context, DateTime, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Prompt, type Response, Tool, Toolkit } from "effect/unstable/ai";

import {
  AgentRuntime,
  DurableStep,
  DurableStepError,
  ToolExecutionClass,
  type RunDurabilityHook,
  type RunStepHook,
  type RunTurnResponseCommit,
  type RunTurnResume,
} from "../src/index.ts";

class HookFailure extends Schema.TaggedError<HookFailure>()("HookFailure", {
  message: Schema.String,
}) {}

class FlakyFailure extends Schema.TaggedError<FlakyFailure>()("FlakyFailure", {
  message: Schema.String,
}) {}

class TypedHookService extends Context.Service<TypedHookService, { readonly enabled: true }>()(
  "@effect-agent/engine/test/TypedHookService",
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

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

/** Scripted two-turn model: a fresh turn counter per Layer build (per Run). */
const scriptedModel = (firstTurn: ReadonlyArray<Response.StreamPartEncoded>, finalText: string) =>
  Model.make(
    "scripted",
    "durable-seam",
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
                    value === 0 ? firstTurn : finalParts(finalText),
                  ),
                ),
              ),
            ),
        });
      }),
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

const resumeTurnId = Schema.decodeSync(TurnId)("turn-resume");

const inertStepHook: RunStepHook = {
  lookup: () => Effect.succeed(Option.none()),
  commit: () => Effect.void,
};

const markingDurability = (
  marks: Ref.Ref<ReadonlyArray<string>>,
  commits?: Ref.Ref<ReadonlyArray<RunTurnResponseCommit>>,
): RunDurabilityHook => ({
  commitResponse: (commit) =>
    Ref.update(marks, (all) => [...all, "commit-response"]).pipe(
      Effect.andThen(
        commits === undefined ? Effect.void : Ref.update(commits, (all) => [...all, commit]),
      ),
    ),
  prepareToolCalls: (calls) =>
    Ref.update(marks, (all) => [
      ...all,
      `prepare:${calls.map((call) => call.toolCallId).join(",")}`,
    ]),
  step: inertStepHook,
});

const policy = (overrides?: Partial<Parameters<typeof AgentPolicy.make>[0]>) =>
  AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
    ...overrides,
  });

layer(identifiers)("P5 WP1 durable Tool seams", (it) => {
  it.effect("commitResponse fires after the finish part and before approval preflight", () =>
    Effect.gen(function* () {
      const marks = yield* Ref.make<ReadonlyArray<string>>([]);
      const commits = yield* Ref.make<ReadonlyArray<RunTurnResponseCommit>>([]);
      const Book = Tool.make("book", {
        parameters: Schema.Struct({ ref: Schema.String }),
        success: Schema.String,
        needsApproval: true,
      });
      const tools = Toolkit.make(Book);
      const definition = Agent.define("commit-response-ordering", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Book after approval.",
        toolkit: tools,
        policy: policy(),
      });
      const model = scriptedModel(
        [
          {
            type: "tool-call",
            id: "book-1",
            name: "book",
            params: { ref: "r-1" },
            providerExecuted: false,
          },
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"booked"}',
      );
      const toolLayer = tools.toLayer({
        book: () => Ref.update(marks, (all) => [...all, "handler"]).pipe(Effect.as("booked")),
      });

      const result = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "book" },
        {
          approval: {
            request: (request) =>
              Ref.update(marks, (all) => [...all, `approval:${request.toolCallId}`]).pipe(
                Effect.as({ _tag: "approved" as const }),
              ),
          },
          durability: markingDurability(marks, commits),
        },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);

      expect(yield* Ref.get(marks)).toEqual([
        "commit-response",
        "approval:book-1",
        "prepare:book-1",
        "handler",
      ]);
      const committed = yield* Ref.get(commits);
      expect(committed).toHaveLength(1);
      expect(committed[0]).toMatchObject({
        turn: 1,
        calls: [
          {
            toolCallId: "book-1",
            toolName: "book",
            parameters: { ref: "r-1" },
            executionClass: "uncertain",
          },
        ],
      });
      const assistantCall = committed[0].responseMessages.content
        .filter((message) => message.role === "assistant")
        .flatMap((message) => message.content)
        .find((part) => part.type === "tool-call");
      expect(assistantCall).toBeDefined();
      if (assistantCall === undefined || assistantCall.type !== "tool-call") {
        throw new Error("Expected the committed response to carry the tool-call part");
      }
      expect(Option.isSome(Schema.decodeUnknownOption(Schema.Json)(assistantCall.params))).toBe(
        true,
      );
      expect(result.output).toEqual({ answer: "booked" });
    }),
  );

  it.effect(
    "emits every staged provider event before usage and response persistence mutations",
    () =>
      Effect.gen(function* () {
        const marks = yield* Ref.make<ReadonlyArray<string>>([]);
        const observed = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        const mutationChecks = yield* Ref.make<ReadonlyArray<string>>([]);
        const HostedLookup = Tool.providerDefined({
          id: "test.hosted_lookup",
          customName: "HostedLookup",
          providerName: "hosted_lookup",
          parameters: Schema.Struct({ query: Schema.String }),
          success: Schema.Struct({ status: Schema.String }),
        })(undefined);
        const Read = Tool.make("read", {
          parameters: Schema.Struct({ path: Schema.String }),
          success: Schema.String,
        });
        const tools = Toolkit.make(HostedLookup, Read);
        const definition = Agent.define("staged-provider-before-mutations", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Use both tools.",
          toolkit: tools,
          policy: policy({ maxToolCalls: 2 }),
        });
        const model = scriptedModel(
          [
            {
              type: "tool-call",
              id: "hosted-before-mutation",
              name: "HostedLookup",
              params: { query: "safe ordering" },
              providerExecuted: true,
            },
            {
              type: "tool-result",
              id: "hosted-before-mutation",
              name: "HostedLookup",
              result: { status: "complete" },
              isFailure: false,
              providerExecuted: true,
            },
            {
              type: "tool-call",
              id: "read-after-provider",
              name: "read",
              params: { path: "/safe" },
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage },
          ],
          '{"answer":"complete"}',
        );
        const assertStagedEventsObserved = (mutation: string) =>
          Effect.gen(function* () {
            const events = yield* Ref.get(observed);
            const providerSequence = events.filter(
              (event) =>
                ("toolCallId" in event && event.toolCallId === "hosted-before-mutation") ||
                (event._tag === "TurnCompleted" && event.turn === 1),
            );
            expect(providerSequence).toMatchObject([
              {
                _tag: "ToolCallDeclared",
                toolCallId: "hosted-before-mutation",
                toolName: "HostedLookup",
                providerExecuted: true,
              },
              {
                _tag: "ToolCallSucceeded",
                toolCallId: "hosted-before-mutation",
                toolName: "HostedLookup",
                result: { status: "complete" },
                providerExecuted: true,
              },
              { _tag: "TurnCompleted", turn: 1, finishReason: "tool-calls" },
            ]);
            const providerSequences = providerSequence.map(({ sequence }) => sequence);
            expect(providerSequences).toEqual(
              [...providerSequences].sort((left, right) => left - right),
            );
            yield* Ref.update(mutationChecks, (all) => [...all, mutation]);
          });
        const durability: RunDurabilityHook = {
          commitResponse: () =>
            assertStagedEventsObserved("commit-response").pipe(
              Effect.andThen(Ref.update(marks, (all) => [...all, "commit-response"])),
            ),
          prepareToolCalls: () =>
            assertStagedEventsObserved("prepare").pipe(
              Effect.andThen(Ref.update(marks, (all) => [...all, "prepare"])),
            ),
          step: inertStepHook,
        };
        let modelConsumptions = 0;

        const events = yield* AgentRuntime.stream(
          Agent.withModel(definition, model),
          { question: "order the mixed response" },
          {
            durability,
            budget: {
              guard: (effect) => effect,
              consume: () => {
                modelConsumptions += 1;
                return assertStagedEventsObserved(`consume:${modelConsumptions}`).pipe(
                  Effect.andThen(Ref.update(marks, (all) => [...all, "consume"])),
                );
              },
            },
          },
        ).pipe(
          Stream.tap((event) => Ref.update(observed, (all) => [...all, event])),
          Stream.runCollect,
          Effect.provide(
            tools.toLayer({
              read: () =>
                Ref.update(marks, (all) => [...all, "handler"]).pipe(Effect.as("read-complete")),
            }),
          ),
        );

        expect(yield* Ref.get(marks)).toEqual([
          "consume",
          "commit-response",
          "prepare",
          "handler",
          "consume",
        ]);
        expect(events.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(2);
        expect(yield* Ref.get(mutationChecks)).toEqual([
          "consume:1",
          "commit-response",
          "prepare",
          "consume:2",
        ]);
      }),
  );

  it.effect("prepareToolCalls fires after every approval and before any handler permit", () =>
    Effect.gen(function* () {
      const marks = yield* Ref.make<ReadonlyArray<string>>([]);
      const Book = Tool.make("book", {
        parameters: Schema.Struct({ ref: Schema.String }),
        success: Schema.String,
        needsApproval: true,
      });
      const tools = Toolkit.make(Book);
      const definition = Agent.define("prepare-ordering", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Book both after approval.",
        toolkit: tools,
        policy: policy(),
      });
      const model = scriptedModel(
        [
          {
            type: "tool-call",
            id: "book-1",
            name: "book",
            params: { ref: "r-1" },
            providerExecuted: false,
          },
          {
            type: "tool-call",
            id: "book-2",
            name: "book",
            params: { ref: "r-2" },
            providerExecuted: false,
          },
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"booked"}',
      );
      const toolLayer = tools.toLayer({
        book: (params, context) =>
          Ref.update(marks, (all) => [...all, `handler:${context.toolCallId}`]).pipe(
            Effect.as(`booked-${params.ref}`),
          ),
      });

      yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "book" },
        {
          approval: {
            request: (request) =>
              Ref.update(marks, (all) => [...all, `approval:${request.toolCallId}`]).pipe(
                Effect.as({ _tag: "approved" as const }),
              ),
          },
          durability: markingDurability(marks),
        },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);

      const observed = yield* Ref.get(marks);
      expect(observed.slice(0, 4)).toEqual([
        "commit-response",
        "approval:book-1",
        "approval:book-2",
        "prepare:book-1,book-2",
      ]);
      expect(observed.slice(4).sort()).toEqual(["handler:book-1", "handler:book-2"]);
    }),
  );

  it.effect("a denied approval prevents prepareToolCalls entirely", () =>
    Effect.gen(function* () {
      const marks = yield* Ref.make<ReadonlyArray<string>>([]);
      const Book = Tool.make("book", {
        parameters: Schema.Struct({ ref: Schema.String }),
        success: Schema.String,
        needsApproval: true,
      });
      const tools = Toolkit.make(Book);
      const definition = Agent.define("denied-no-prepare", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Book after approval.",
        toolkit: tools,
        policy: policy(),
      });
      const model = scriptedModel(
        [
          {
            type: "tool-call",
            id: "book-1",
            name: "book",
            params: { ref: "r-1" },
            providerExecuted: false,
          },
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"unreachable"}',
      );
      const toolLayer = tools.toLayer({
        book: () => Ref.update(marks, (all) => [...all, "handler"]).pipe(Effect.as("booked")),
      });

      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "book" },
        {
          approval: {
            request: () => Effect.succeed({ _tag: "denied" as const, reason: "operator declined" }),
          },
          durability: markingDurability(marks),
        },
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);

      expect(failureFrom(exit)).toBeInstanceOf(AgentApprovalDenied);
      const observed = yield* Ref.get(marks);
      expect(observed).toEqual(["commit-response"]);
    }),
  );

  it.effect("readonly-annotated tools skip preparation", () =>
    Effect.gen(function* () {
      const marks = yield* Ref.make<ReadonlyArray<string>>([]);
      const commits = yield* Ref.make<ReadonlyArray<RunTurnResponseCommit>>([]);
      const Search = Tool.make("search", {
        parameters: Schema.Struct({ query: Schema.String }),
        success: Schema.String,
      }).annotate(ToolExecutionClass, "readonly");
      const Book = Tool.make("book", {
        parameters: Schema.Struct({ ref: Schema.String }),
        success: Schema.String,
      });
      const tools = Toolkit.make(Search, Book);
      const definition = Agent.define("readonly-skip", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Search, then book.",
        toolkit: tools,
        policy: policy({ maxTurns: 3, maxToolCalls: 3 }),
      });
      const toolLayer = tools.toLayer({
        search: () => Effect.succeed("found"),
        book: () => Effect.succeed("booked"),
      });
      const durability = markingDurability(marks, commits);

      // An all-readonly batch commits its response early but skips the
      // prepared batch entirely.
      yield* AgentRuntime.run(
        Agent.withModel(
          definition,
          scriptedModel(
            [
              {
                type: "tool-call",
                id: "search-1",
                name: "search",
                params: { query: "q" },
                providerExecuted: false,
              },
              { type: "finish", reason: "tool-calls", usage },
            ],
            '{"answer":"searched"}',
          ),
        ),
        { question: "search" },
        { durability },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);
      expect(yield* Ref.get(marks)).toEqual(["commit-response"]);
      const readonlyCommit = (yield* Ref.get(commits))[0];
      expect(readonlyCommit.calls.map((call) => call.executionClass)).toEqual(["readonly"]);

      // A mixed batch prepares only the non-readonly calls.
      yield* Ref.set(marks, []);
      yield* Ref.set(commits, []);
      yield* AgentRuntime.run(
        Agent.withModel(
          definition,
          scriptedModel(
            [
              {
                type: "tool-call",
                id: "search-2",
                name: "search",
                params: { query: "q" },
                providerExecuted: false,
              },
              {
                type: "tool-call",
                id: "book-1",
                name: "book",
                params: { ref: "r-1" },
                providerExecuted: false,
              },
              { type: "finish", reason: "tool-calls", usage },
            ],
            '{"answer":"booked"}',
          ),
        ),
        { question: "book" },
        { durability },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);
      expect(yield* Ref.get(marks)).toEqual(["commit-response", "prepare:book-1"]);
      const mixedCommit = (yield* Ref.get(commits))[0];
      expect(mixedCommit.calls.map((call) => [call.toolCallId, call.executionClass])).toEqual([
        ["search-2", "readonly"],
        ["book-1", "uncertain"],
      ]);
    }),
  );

  it.effect(
    "resume executes only unsettled calls and injects recorded results without execution",
    () =>
      Effect.gen(function* () {
        const marks = yield* Ref.make<ReadonlyArray<string>>([]);
        const handled = yield* Ref.make<ReadonlyArray<string>>([]);
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
        let modelCalls = 0;
        let secondRequestPrompt: Prompt.Prompt | undefined;
        const Lookup = Tool.make("lookup", {
          parameters: Schema.Struct({ key: Schema.String }),
          success: Schema.String,
        });
        const tools = Toolkit.make(Lookup);
        const definition = Agent.define("resume-open-calls", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Look everything up.",
          toolkit: tools,
          policy: policy(),
        });
        const model = Model.make(
          "scripted",
          "resume-final",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (request) => {
                modelCalls += 1;
                secondRequestPrompt = request.prompt;
                return Stream.fromIterable(finalParts('{"answer":"resumed"}'));
              },
            }),
          ),
        );
        const toolLayer = tools.toLayer({
          lookup: ({ key }) =>
            Ref.update(handled, (all) => [...all, key]).pipe(Effect.as(`handled-${key}`)),
        });
        const resume: RunTurnResume = {
          turn: 1,
          turnId: resumeTurnId,
          calls: [
            { id: "call-a", name: "lookup", params: { key: "a" } },
            { id: "call-b", name: "lookup", params: { key: "b" } },
          ],
          settled: [{ id: "call-a", result: "recorded-a", isFailure: false }],
        };

        const result = yield* AgentRuntime.stream(
          Agent.withModel(definition, model),
          { question: "resume" },
          { resume, durability: markingDurability(marks) },
        ).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runCollect,
          Effect.provide(toolLayer),
        );

        // No model re-invocation for the resumed Turn; exactly one call for the
        // following Turn.
        expect(modelCalls).toBe(1);
        expect(result.map((event) => event._tag)).toEqual([
          "RunStarted",
          "TurnStarted",
          "TurnCompleted",
          "ToolCallStarted",
          "ToolCallSucceeded",
          "TurnStarted",
          "ModelStarted",
          "TextDelta",
          "TurnCompleted",
          "BudgetWarning",
          "BudgetWarning",
          "RunCompleted",
        ]);
        // Only the open call executed; the settled call was injected.
        expect(yield* Ref.get(handled)).toEqual(["b"]);
        const started = (yield* Ref.get(events)).filter(
          (event) => event._tag === "ToolCallStarted",
        );
        expect(started.map((event) => event.toolCallId)).toEqual(["call-b"]);
        // The prepared batch replays with the identical full descriptor list;
        // the resumed Turn's response never re-commits.
        expect(yield* Ref.get(marks)).toEqual(["prepare:call-a,call-b"]);
        // The next model request carries the rebuilt assistant response and the
        // Tool message in declaration order: injected first, executed second.
        expect(secondRequestPrompt).toBeDefined();
        if (secondRequestPrompt === undefined) {
          throw new Error("Expected the follow-up model request to be captured");
        }
        const assistantCalls = secondRequestPrompt.content
          .filter((message) => message.role === "assistant")
          .flatMap((message) => message.content)
          .filter((part) => part.type === "tool-call");
        expect(assistantCalls.map((part) => part.id)).toEqual(["call-a", "call-b"]);
        const toolResults = secondRequestPrompt.content
          .filter((message) => message.role === "tool")
          .flatMap((message) => message.content)
          .filter((part) => part.type === "tool-result");
        expect(toolResults.map((part) => [part.id, part.result])).toEqual([
          ["call-a", "recorded-a"],
          ["call-b", "handled-b"],
        ]);
      }),
  );

  it.effect("truncated tool arguments never execute after resume", () =>
    Effect.gen(function* () {
      const marks = yield* Ref.make<ReadonlyArray<string>>([]);
      let handlerStarted = false;
      let modelCalls = 0;
      const Lookup = Tool.make("lookup", {
        parameters: Schema.Struct({ key: Schema.String }),
        success: Schema.String,
      });
      const tools = Toolkit.make(Lookup);
      const definition = Agent.define("resume-truncated-arguments", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Look everything up.",
        toolkit: tools,
        policy: policy(),
      });
      const model = Model.make(
        "scripted",
        "resume-truncated",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              modelCalls += 1;
              return Stream.fromIterable(finalParts('{"answer":"unreachable"}'));
            },
          }),
        ),
      );
      const toolLayer = tools.toLayer({
        lookup: () =>
          Effect.sync(() => {
            handlerStarted = true;
            return "unexpected";
          }),
      });
      const resume: RunTurnResume = {
        turn: 1,
        turnId: resumeTurnId,
        calls: [{ id: "call-x", name: "lookup", params: { key: 42 } }],
        settled: [],
      };

      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "resume" },
        { resume, durability: markingDurability(marks) },
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect((failure as ModelProtocolError).message).toContain("failed validation on resume");
      expect(handlerStarted).toBe(false);
      expect(modelCalls).toBe(0);
      expect(yield* Ref.get(marks)).toEqual([]);
    }),
  );

  it.effect("DurableStep pass-through executes once without a hook", () =>
    Effect.gen(function* () {
      let bodyRuns = 0;
      const Work = Tool.make("work", {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: DurableStepError,
        dependencies: [DurableStep],
      });
      const tools = Toolkit.make(Work);
      const definition = Agent.define("step-pass-through", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Do the work.",
        toolkit: tools,
        policy: policy(),
      });
      const model = scriptedModel(
        [
          { type: "tool-call", id: "work-1", name: "work", params: {}, providerExecuted: false },
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"worked"}',
      );
      const toolLayer = tools.toLayer({
        work: () =>
          Effect.gen(function* () {
            const step = yield* DurableStep;
            return yield* step.do(
              "compute",
              Schema.String,
              Effect.sync(() => {
                bodyRuns += 1;
                return "computed";
              }),
            );
          }),
      });

      const result = yield* AgentRuntime.run(Agent.withModel(definition, model), {
        question: "work",
      }).pipe(Effect.provide(toolLayer), Effect.scoped);

      expect(bodyRuns).toBe(1);
      expect(result.output).toEqual({ answer: "worked" });
    }),
  );

  it.effect("DurableStep replays a hook-recorded result without executing", () =>
    Effect.gen(function* () {
      const store = new Map<string, unknown>();
      let bodyRuns = 0;
      let commits = 0;
      const recordingStepHook: RunStepHook = {
        lookup: (key) =>
          Effect.sync(() => {
            const hit = store.get(`${key.toolCallId}:${key.stepName}`);
            return hit === undefined ? Option.none() : Option.some({ encodedOutput: hit });
          }),
        commit: (key, encodedOutput) =>
          Effect.sync(() => {
            commits += 1;
            store.set(`${key.toolCallId}:${key.stepName}`, encodedOutput);
          }),
      };
      const durability: RunDurabilityHook = {
        commitResponse: () => Effect.void,
        prepareToolCalls: () => Effect.void,
        step: recordingStepHook,
      };
      const Stamp = Tool.make("stamp", {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: DurableStepError,
        dependencies: [DurableStep],
      });
      const tools = Toolkit.make(Stamp);
      const definition = Agent.define("step-replay", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Issue the stamp.",
        toolkit: tools,
        policy: policy(),
      });
      const toolLayer = tools.toLayer({
        stamp: () =>
          Effect.gen(function* () {
            const step = yield* DurableStep;
            // The output Schema is the canonical codec: the recorded value is
            // the encoded ISO string, the replayed value a decoded DateTime.
            const at = yield* step.do(
              "issue",
              Schema.DateTimeUtcFromString,
              Effect.sync(() => {
                bodyRuns += 1;
                return DateTime.makeUnsafe(1755000000000);
              }),
            );
            return DateTime.formatIso(at);
          }),
      });
      const makeModel = () =>
        scriptedModel(
          [
            {
              type: "tool-call",
              id: "stamp-1",
              name: "stamp",
              params: {},
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage },
          ],
          '{"answer":"stamped"}',
        );

      const first = yield* AgentRuntime.run(
        Agent.withModel(definition, makeModel()),
        { question: "stamp" },
        { durability },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);
      expect(bodyRuns).toBe(1);
      expect(commits).toBe(1);
      expect(typeof store.get("stamp-1:issue")).toBe("string");

      const second = yield* AgentRuntime.run(
        Agent.withModel(definition, makeModel()),
        { question: "stamp" },
        { durability },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);

      // The recorded Step result replays without executing the body and
      // without a second commit.
      expect(bodyRuns).toBe(1);
      expect(commits).toBe(1);
      expect(second.output).toEqual(first.output);
    }),
  );

  it.effect("fails a duplicate Step name within one Tool Call as a typed identity conflict", () =>
    Effect.gen(function* () {
      let bodyRuns = 0;
      const Work = Tool.make("work", {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: DurableStepError,
        dependencies: [DurableStep],
      });
      const tools = Toolkit.make(Work);
      const definition = Agent.define("step-duplicate-name", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Do the work.",
        toolkit: tools,
        policy: policy(),
      });
      const model = scriptedModel(
        [
          { type: "tool-call", id: "work-1", name: "work", params: {}, providerExecuted: false },
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"unreachable"}',
      );
      const durability: RunDurabilityHook = {
        commitResponse: () => Effect.void,
        prepareToolCalls: () => Effect.void,
        step: inertStepHook,
      };
      const toolLayer = tools.toLayer({
        work: () =>
          Effect.gen(function* () {
            const step = yield* DurableStep;
            const run = Effect.sync(() => {
              bodyRuns += 1;
              return "computed";
            });
            yield* step.do("same", Schema.String, run);
            return yield* step.do("same", Schema.String, run);
          }),
      });

      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "work" },
        { durability },
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(DurableStepError);
      expect((failure as DurableStepError).reason).toBe("duplicate-step-name");
      expect(bodyRuns).toBe(1);
    }),
  );

  it.effect("records only Step success and re-executes after a failed body", () =>
    Effect.gen(function* () {
      const store = new Map<string, unknown>();
      let bodyRuns = 0;
      let commits = 0;
      const recordingStepHook: RunStepHook = {
        lookup: (key) =>
          Effect.sync(() => {
            const hit = store.get(`${key.toolCallId}:${key.stepName}`);
            return hit === undefined ? Option.none() : Option.some({ encodedOutput: hit });
          }),
        commit: (key, encodedOutput) =>
          Effect.sync(() => {
            commits += 1;
            store.set(`${key.toolCallId}:${key.stepName}`, encodedOutput);
          }),
      };
      const durability: RunDurabilityHook = {
        commitResponse: () => Effect.void,
        prepareToolCalls: () => Effect.void,
        step: recordingStepHook,
      };
      const Flaky = Tool.make("flaky", {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: Schema.Union([DurableStepError, FlakyFailure]),
        dependencies: [DurableStep],
      });
      const tools = Toolkit.make(Flaky);
      const definition = Agent.define("step-success-only", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Attempt the work.",
        toolkit: tools,
        policy: policy(),
      });
      const toolLayer = tools.toLayer({
        flaky: () =>
          Effect.gen(function* () {
            const step = yield* DurableStep;
            return yield* step.do(
              "attempt",
              Schema.String,
              Effect.suspend(() => {
                bodyRuns += 1;
                return bodyRuns === 1
                  ? Effect.fail(FlakyFailure.make({ message: "first attempt fails" }))
                  : Effect.succeed("second attempt");
              }),
            );
          }),
      });
      const makeModel = () =>
        scriptedModel(
          [
            {
              type: "tool-call",
              id: "flaky-1",
              name: "flaky",
              params: {},
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage },
          ],
          '{"answer":"attempted"}',
        );

      const firstExit = yield* AgentRuntime.run(
        Agent.withModel(definition, makeModel()),
        { question: "attempt" },
        { durability },
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);

      // A failing body is never recorded: the failure reaches the handler's
      // error channel and nothing was committed.
      expect(failureFrom(firstExit)).toBeInstanceOf(FlakyFailure);
      expect(bodyRuns).toBe(1);
      expect(commits).toBe(0);
      expect(store.size).toBe(0);

      const second = yield* AgentRuntime.run(
        Agent.withModel(definition, makeModel()),
        { question: "attempt" },
        { durability },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);

      // Re-entry re-executes the body; only the success commits.
      expect(bodyRuns).toBe(2);
      expect(commits).toBe(1);
      expect(second.output).toEqual({ answer: "attempted" });
    }),
  );

  it.effect("surfaces step hook failures as typed DurableStepError", () =>
    Effect.gen(function* () {
      let bodyRuns = 0;
      const failingStepHook: RunStepHook<HookFailure> = {
        lookup: () => Effect.fail(HookFailure.make({ message: "lookup boom" })),
        commit: () => Effect.void,
      };
      const durability: RunDurabilityHook<HookFailure> = {
        commitResponse: () => Effect.void,
        prepareToolCalls: () => Effect.void,
        step: failingStepHook,
      };
      const Work = Tool.make("work", {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: DurableStepError,
        dependencies: [DurableStep],
      });
      const tools = Toolkit.make(Work);
      const definition = Agent.define("step-hook-failure", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Do the work.",
        toolkit: tools,
        policy: policy(),
      });
      const model = scriptedModel(
        [
          { type: "tool-call", id: "work-1", name: "work", params: {}, providerExecuted: false },
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"unreachable"}',
      );
      const toolLayer = tools.toLayer({
        work: () =>
          Effect.gen(function* () {
            const step = yield* DurableStep;
            return yield* step.do(
              "compute",
              Schema.String,
              Effect.sync(() => {
                bodyRuns += 1;
                return "computed";
              }),
            );
          }),
      });

      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "work" },
        { durability },
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toBeInstanceOf(DurableStepError);
      expect((failure as DurableStepError).reason).toBe("lookup-failed");
      expect((failure as DurableStepError).message).toContain("lookup boom");
      expect(bodyRuns).toBe(0);
    }),
  );

  it.effect("keeps durability hook failures and requirements visible in Effect E and R", () => {
    const Empty = Tool.make("noop", {
      parameters: Schema.Struct({}),
      success: Schema.String,
    });
    const tools = Toolkit.make(Empty);
    const definition = Agent.define("durability-hook-types", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Answer.",
      toolkit: tools,
      policy: policy(),
    });
    const durability: RunDurabilityHook<HookFailure, TypedHookService> = {
      commitResponse: () =>
        Effect.gen(function* () {
          yield* TypedHookService;
          return yield* HookFailure.make({ message: "commit failed" });
        }),
      prepareToolCalls: () => Effect.void,
      step: {
        lookup: () => Effect.andThen(TypedHookService, Effect.succeed(Option.none())),
        commit: () => Effect.void,
      },
    };
    const program = AgentRuntime.run(
      Agent.withModel(definition, scriptedModel([], '{"answer":"typed"}')),
      { question: "typed" },
      { durability },
    );
    type ErrorProof = HookFailure extends Effect.Error<typeof program> ? true : false;
    type RequirementsProof =
      TypedHookService extends Effect.Services<typeof program> ? true : false;
    const errorProof: ErrorProof = true;
    const requirementsProof: RequirementsProof = true;

    expect(errorProof).toBe(true);
    expect(requirementsProof).toBe(true);
    return Effect.void;
  });

  it.effect("keeps DurableStep body failures and services visible in E and R", () => {
    const body: Effect.Effect<DateTime.Utc, FlakyFailure, TypedHookService> = Effect.gen(
      function* () {
        yield* TypedHookService;
        return yield* FlakyFailure.make({ message: "body" });
      },
    );
    const program = Effect.gen(function* () {
      const step = yield* DurableStep;
      return yield* step.do("typed", Schema.DateTimeUtcFromString, body);
    });
    type StepErrorProof = DurableStepError extends Effect.Error<typeof program> ? true : false;
    type BodyErrorProof = FlakyFailure extends Effect.Error<typeof program> ? true : false;
    type BodyServicesProof =
      TypedHookService extends Effect.Services<typeof program> ? true : false;
    type StepServiceProof = DurableStep extends Effect.Services<typeof program> ? true : false;
    const stepErrorProof: StepErrorProof = true;
    const bodyErrorProof: BodyErrorProof = true;
    const bodyServicesProof: BodyServicesProof = true;
    const stepServiceProof: StepServiceProof = true;

    expect(stepErrorProof).toBe(true);
    expect(bodyErrorProof).toBe(true);
    expect(bodyServicesProof).toBe(true);
    expect(stepServiceProof).toBe(true);
    return Effect.void;
  });
});
