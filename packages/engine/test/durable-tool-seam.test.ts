import {
  Agent,
  AgentApprovalDenied,
  AgentPolicy,
  AgentPolicyError,
  ThreadId,
  DelegationTool,
  IdGenerator,
  MemoryRecallError,
  ModelProtocolError,
  RunId,
  TurnId,
  type RunEvent,
} from "@effect-agent/core";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Clock,
  Context,
  DateTime,
  Deferred,
  Effect,
  ErrorReporter,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { Prompt, LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";

import {
  AgentRuntime,
  RunContextPreparation,
  type RunContextPreparationError,
  RunContextPreparationPassthrough,
  RunToolAuthorization,
  DurableStep,
  DurableStepError,
  ToolExecutionClass,
  type RunDurabilityHook,
  type RunStepHook,
  type RunToolAuthorizationRequest,
  type RunTurnResponseCommit,
  type RunTurnResume,
} from "../src/index.ts";
import { ThreadHistory } from "../src/thread-history.ts";

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

const oneCallResumeUsage = {
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
};

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("thread-1")),
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
  // Required by the durability protocol; this harness exercises neither seam.
  commitCompaction: () => Effect.void,
  noteTurnUsage: () => Effect.void,
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

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layerTransient,
  RunContextPreparationPassthrough,
);

layer(testLayer)("P5 WP1 durable Tool seams", (it) => {
  for (const outcome of ["allowed", "denied", "preparation-failed"] as const) {
    it.effect(`composes independent host services through ephemeral hooks: ${outcome}`, () => {
      const seen: Array<string> = [];

      const tools = Toolkit.make(
        Tool.make("book", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        }),
      );

      const model = scriptedModel(
        [
          { type: "tool-call", id: "book-1", name: "book", params: {}, providerExecuted: false },
          { type: "finish", reason: "tool-calls", usage },
        ],
        '"done"',
      );

      const agent = Agent.withModel(
        Agent.make("independent-services", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Book it.",
          toolkit: tools,
          policy: policy(),
        }),
        model,
      );

      const program = Effect.gen(function* () {
        const authorization = yield* RunToolAuthorization;

        return yield* AgentRuntime.run(agent, "book", {
          toolAuthorization: authorization,
        });
      });

      const preparationRequired: RunContextPreparation extends Effect.Services<typeof program>
        ? true
        : false = false;

      const authorizationRequired: RunToolAuthorization extends Effect.Services<typeof program>
        ? true
        : false = true;

      const preparationError: RunContextPreparationError extends Effect.Error<typeof program>
        ? true
        : false = true;

      return Effect.gen(function* () {
        const result = yield* program.pipe(Effect.exit);

        if (outcome === "allowed") {
          expect(Exit.isSuccess(result)).toBe(true);
          expect(seen).toEqual(["prepare", "authorize", "handler", "prepare"]);
        } else {
          expect(Exit.isFailure(result)).toBe(true);
          if (Exit.isFailure(result)) {
            expect(Cause.findErrorOption(result.cause)).toMatchObject({
              _tag: "Some",
              value: {
                _tag: outcome === "denied" ? "AgentToolAuthorizationDenied" : "MemoryRecallError",
              },
            });
          }
          expect(seen).toEqual(outcome === "denied" ? ["prepare", "authorize"] : ["prepare"]);
        }
        expect(preparationRequired).toBe(false);
        expect(authorizationRequired && preparationError).toBe(true);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(RunContextPreparation, {
              hook: {
                prepare: ({ source }) =>
                  Effect.gen(function* () {
                    seen.push("prepare");
                    if (outcome === "preparation-failed") {
                      return yield* MemoryRecallError.make({
                        reason: "unavailable",
                        sourceId: "host",
                        message: "unavailable",
                      });
                    }

                    return { prompt: Prompt.fromMessages(source.content) };
                  }),
              },
            }),
            Layer.succeed(RunToolAuthorization, {
              authorize: () =>
                Effect.sync(() => {
                  seen.push("authorize");

                  return outcome === "denied"
                    ? { _tag: "denied" as const, reason: "revoked" }
                    : { _tag: "allowed" as const };
                }),
            }),
            tools.toLayer({
              book: () =>
                Effect.sync(() => {
                  seen.push("handler");

                  return "booked";
                }),
            }),
          ),
        ),
        Effect.scoped,
      );
    });
  }

  it.effect(
    "classifies fresh and resumed calls from definition annotations, never name prefixes",
    () =>
      Effect.gen(function* () {
        const Ordinary = Tool.make("delegate_fake", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        });

        const Delegated = Tool.make("research", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        })
          .annotate(DelegationTool, true)
          .annotate(ToolExecutionClass, "readonly");

        const toolkit = Toolkit.make(Ordinary, Delegated);

        const definition = Agent.make("classification", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Answer.",
          toolkit,
          policy: policy({ maxTurns: 3 }),
        });

        const calls = [
          { id: "ordinary", name: "delegate_fake", params: {} },
          { id: "delegated", name: "research", params: {} },
        ];

        for (const resumed of [false, true]) {
          const classifications: Array<string> = [];

          const model = scriptedModel(
            resumed
              ? finalParts('"done"')
              : [
                  ...calls.map((call) => ({
                    type: "tool-call" as const,
                    ...call,
                    providerExecuted: false,
                  })),
                  { type: "finish", reason: "tool-calls", usage },
                ],
            '"done"',
          );

          yield* AgentRuntime.run(Agent.withModel(definition, model), "q", {
            ...(resumed
              ? {
                  resume: { turn: 1, turnId: resumeTurnId, calls, settled: [] },
                  resumeUsage: { ...oneCallResumeUsage, toolCalls: 2 },
                }
              : {}),
            durability: {
              commitResponse: () => Effect.void,
              prepareToolCalls: (descriptors) =>
                Effect.sync(() => {
                  classifications.push(
                    ...descriptors.map((call) => `${call.toolName}:${call.executionKind}`),
                  );
                }),
              commitCompaction: () => Effect.void,
              noteTurnUsage: () => Effect.void,
              step: inertStepHook,
            },
          }).pipe(
            Effect.provide(
              toolkit.toLayer({
                delegate_fake: () => Effect.succeed("ok"),
                research: () => Effect.succeed("ok"),
              }),
            ),
          );
          expect(classifications).toEqual(["delegate_fake:ordinary", "research:delegation"]);
        }
      }),
  );

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

      const definition = Agent.make("commit-response-ordering", {
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
            executionKind: "ordinary",
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

        const definition = Agent.make("staged-provider-before-mutations", {
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
          // Required by the durability protocol; this harness exercises neither seam.
          commitCompaction: () => Effect.void,
          noteTurnUsage: () => Effect.void,
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

  it.effect("RUN-031 authorizes every fresh call before preparation and any handler permit", () =>
    Effect.gen(function* () {
      const marks = yield* Ref.make<ReadonlyArray<string>>([]);
      const authorizationRequests = yield* Ref.make<ReadonlyArray<RunToolAuthorizationRequest>>([]);

      const Book = Tool.make("book", {
        parameters: Schema.Struct({ ref: Schema.String }),
        success: Schema.String,
        needsApproval: true,
      });

      const tools = Toolkit.make(Book);

      const definition = Agent.make("prepare-ordering", {
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
          toolAuthorization: {
            authorize: (request) =>
              Ref.update(authorizationRequests, (all) => [...all, request]).pipe(
                Effect.andThen(
                  Ref.update(marks, (all) => [...all, `authorize:${request.call.toolCallId}`]),
                ),
                Effect.as({ _tag: "allowed" as const }),
              ),
          },
          durability: markingDurability(marks),
        },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);

      const observed = yield* Ref.get(marks);

      expect(observed.slice(0, 6)).toEqual([
        "commit-response",
        "approval:book-1",
        "approval:book-2",
        "authorize:book-1",
        "authorize:book-2",
        "prepare:book-1,book-2",
      ]);
      expect(observed.slice(6).sort()).toEqual(["handler:book-1", "handler:book-2"]);

      const requests = yield* Ref.get(authorizationRequests);

      expect(requests).toHaveLength(2);
      expect(
        requests.map(({ threadId, runId, turnId, turn, input, call }) => ({
          threadId,
          runId,
          turnId,
          turn,
          input,
          call,
        })),
      ).toEqual([
        {
          threadId: "thread-1",
          runId: "run-1",
          turnId: "turn-1",
          turn: 1,
          input: { question: "book" },
          call: {
            toolCallId: "book-1",
            toolName: "book",
            parameters: { ref: "r-1" },
            executionClass: "uncertain",
            executionKind: "ordinary",
          },
        },
        {
          threadId: "thread-1",
          runId: "run-1",
          turnId: "turn-1",
          turn: 1,
          input: { question: "book" },
          call: {
            toolCallId: "book-2",
            toolName: "book",
            parameters: { ref: "r-2" },
            executionClass: "uncertain",
            executionKind: "ordinary",
          },
        },
      ]);
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

      const definition = Agent.make("denied-no-prepare", {
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

      const definition = Agent.make("readonly-skip", {
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

        const definition = Agent.make("resume-open-calls", {
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

        let settledIteratorReads = 0;

        const settled: RunTurnResume["settled"] = [
          { id: "call-a", result: "recorded-a", isFailure: false },
        ];

        Object.defineProperty(settled, Symbol.iterator, {
          get: () => {
            settledIteratorReads += 1;
            throw new Error("settled iterator must not run");
          },
        });

        const resume: RunTurnResume = {
          turn: 1,
          turnId: resumeTurnId,
          calls: [
            { id: "call-a", name: "lookup", params: { key: "a" } },
            { id: "call-b", name: "lookup", params: { key: "b" } },
          ],
          settled,
        };

        const result = yield* AgentRuntime.stream(
          Agent.withModel(definition, model),
          { question: "resume" },
          {
            resume,
            resumeUsage: { ...oneCallResumeUsage, toolCalls: 2 },
            durability: markingDurability(marks),
          },
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
        expect(settledIteratorReads).toBe(0);

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

  it.effect("resume rejects non-canonical settled results before any external execution", () =>
    Effect.gen(function* () {
      const marks = yield* Ref.make<ReadonlyArray<string>>([]);
      let handlerStarts = 0;
      let modelCalls = 0;

      const Lookup = Tool.make("lookup", {
        parameters: Schema.Struct({ key: Schema.String }),
        success: Schema.String,
      });

      const tools = Toolkit.make(Lookup);

      const definition = Agent.make("resume-invalid-result", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Look everything up.",
        toolkit: tools,
        policy: policy(),
      });

      const model = Model.make(
        "scripted",
        "resume-invalid-result",
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
            handlerStarts += 1;

            return "unexpected";
          }),
      });

      const cyclic: Record<string, Schema.Json> = {};

      cyclic.self = cyclic;
      const invalidResults: ReadonlyArray<Schema.Json> = [Number.NaN, cyclic];

      for (const result of invalidResults) {
        const resume: RunTurnResume = {
          turn: 1,
          turnId: resumeTurnId,
          calls: [
            { id: "call-settled", name: "lookup", params: { key: "a" } },
            { id: "call-open", name: "lookup", params: { key: "b" } },
          ],
          settled: [{ id: "call-settled", result, isFailure: false }],
        };

        const exit = yield* AgentRuntime.run(
          Agent.withModel(definition, model),
          { question: "resume" },
          {
            resume,
            resumeUsage: { ...oneCallResumeUsage, toolCalls: 2 },
            durability: markingDurability(marks),
          },
        ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);

        const failure = failureFrom(exit);

        expect(failure).toBeInstanceOf(ModelProtocolError);
        expect((failure as ModelProtocolError).message).toContain("bounded canonical JSON");
      }

      let accessorReads = 0;

      const accessorSettled: RunTurnResume["settled"][number] = {
        id: "call-settled",
        result: "small",
        isFailure: false,
      };

      Object.defineProperty(accessorSettled, "result", {
        enumerable: true,
        get: () => {
          accessorReads += 1;

          return "small";
        },
      });

      const accessorResume: RunTurnResume = {
        turn: 1,
        turnId: resumeTurnId,
        calls: [
          { id: "call-settled", name: "lookup", params: { key: "a" } },
          { id: "call-open", name: "lookup", params: { key: "b" } },
        ],
        settled: [accessorSettled],
      };

      const accessorExit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "resume" },
        {
          resume: accessorResume,
          resumeUsage: { ...oneCallResumeUsage, toolCalls: 2 },
          durability: markingDurability(marks),
        },
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);

      const accessorFailure = failureFrom(accessorExit);

      expect(accessorFailure).toBeInstanceOf(ModelProtocolError);
      if (accessorFailure instanceof ModelProtocolError) {
        expect(accessorFailure.message).toContain("invalid settled Tool Call");
      }
      expect(accessorReads).toBe(0);

      let collectionAccessorReads = 0;

      const collectionAccessorResume: RunTurnResume = {
        turn: 1,
        turnId: resumeTurnId,
        calls: [{ id: "call-open", name: "lookup", params: { key: "b" } }],
        settled: [],
      };

      Object.defineProperty(collectionAccessorResume, "settled", {
        get: () => {
          collectionAccessorReads += 1;
          throw new Error("settled collection accessor must not run");
        },
      });

      const collectionAccessorExit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "resume" },
        {
          resume: collectionAccessorResume,
          resumeUsage: { ...oneCallResumeUsage, toolCalls: 2 },
          durability: markingDurability(marks),
        },
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);

      const collectionAccessorFailure = failureFrom(collectionAccessorExit);

      expect(collectionAccessorFailure).toBeInstanceOf(ModelProtocolError);
      if (collectionAccessorFailure instanceof ModelProtocolError) {
        expect(collectionAccessorFailure.message).toContain("invalid settled Tool Call collection");
      }
      expect(collectionAccessorReads).toBe(0);

      expect(handlerStarts).toBe(0);
      expect(modelCalls).toBe(0);
      expect(yield* Ref.get(marks)).toEqual([]);
    }),
  );

  it.effect("obsolete cleanup data cannot authorize ordinary Tools after an expired resume", () =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      let modelCalls = 0;

      const DelegateLookup = Tool.make("delegate_lookup", {
        parameters: Schema.Struct({ key: Schema.String }),
        success: Schema.String,
      });

      const DelegateOther = Tool.make("delegate_other", {
        parameters: Schema.Struct({ key: Schema.String }),
        success: Schema.String,
      });

      const tools = Toolkit.make(DelegateLookup, DelegateOther);

      const definition = Agent.make("expired-forged-cleanup", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Look everything up.",
        toolkit: tools,
        policy: policy(),
      });

      const model = Model.make(
        "scripted",
        "expired-resume",
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
        delegate_lookup: () =>
          Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("unexpected")),
        delegate_other: () =>
          Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as("unexpected")),
      });

      const exactForgedResume: RunTurnResume & {
        readonly settledChildJoinCallIdsPastDeadline: ReadonlyArray<string>;
      } = {
        turn: 1,
        turnId: resumeTurnId,
        calls: [{ id: "call-open", name: "delegate_lookup", params: { key: "a" } }],
        settled: [],
        settledChildJoinCallIdsPastDeadline: ["call-open"],
      };

      const partialForgedResume: RunTurnResume & {
        readonly settledChildJoinCallIdsPastDeadline: ReadonlyArray<string>;
      } = {
        turn: 1,
        turnId: resumeTurnId,
        calls: [
          { id: "call-open-a", name: "delegate_lookup", params: { key: "a" } },
          { id: "call-open-b", name: "delegate_other", params: { key: "b" } },
        ],
        settled: [],
        settledChildJoinCallIdsPastDeadline: ["call-open-a"],
      };

      const now = yield* Clock.currentTimeMillis;
      const durationDeadline = DateTime.toUtc(DateTime.makeUnsafe(now - 1));

      for (const resume of [exactForgedResume, partialForgedResume]) {
        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);

        const exit = yield* AgentRuntime.stream(
          Agent.withModel(definition, model),
          { question: "resume" },
          {
            durationDeadline,
            resume,
            resumeUsage: { ...oneCallResumeUsage, toolCalls: resume.calls.length },
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
        expect(failure).toMatchObject({ limit: "duration" });
        expect(observed.filter((event) => event._tag === "ToolCallStarted")).toHaveLength(0);
        expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
      }

      expect(yield* Ref.get(handlerStarts)).toBe(0);
      expect(modelCalls).toBe(0);
    }),
  );

  it.effect("future expiry interrupts a forged resumed Tool and runs its finalizer", () =>
    Effect.gen(function* () {
      const handlerStarted = yield* Deferred.make<void>();
      const handlerFinalized = yield* Deferred.make<void>();
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      let modelCalls = 0;

      const DelegateLookup = Tool.make("delegate_lookup", {
        parameters: Schema.Struct({ key: Schema.String }),
        success: Schema.String,
      });

      const tools = Toolkit.make(DelegateLookup);

      const definition = Agent.make("cleanup-deadline", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Join, then answer.",
        toolkit: tools,
        policy: policy({ maxDuration: "5 seconds" }),
      });

      const model = Model.make(
        "scripted",
        "cleanup-deadline",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              modelCalls += 1;

              return Stream.fromIterable(finalParts('{"answer":"too late"}'));
            },
          }),
        ),
      );

      const toolLayer = tools.toLayer({
        delegate_lookup: () =>
          Deferred.succeed(handlerStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(handlerFinalized, undefined)),
          ),
      });

      const resume: RunTurnResume & {
        readonly settledChildJoinCallIdsPastDeadline: ReadonlyArray<string>;
      } = {
        turn: 1,
        turnId: resumeTurnId,
        calls: [{ id: "call-child", name: "delegate_lookup", params: { key: "a" } }],
        settled: [],
        settledChildJoinCallIdsPastDeadline: ["call-child"],
      };

      const now = yield* Clock.currentTimeMillis;

      const durationDeadline = DateTime.addDuration(
        DateTime.toUtc(DateTime.makeUnsafe(now)),
        "5 seconds",
      );

      const fiber = yield* AgentRuntime.stream(
        Agent.withModel(definition, model),
        { question: "resume" },
        { durationDeadline, resume, resumeUsage: oneCallResumeUsage },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.forkChild,
      );

      yield* Deferred.await(handlerStarted);
      yield* TestClock.adjust("5 seconds");
      const exit = yield* Fiber.await(fiber);
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect(failure).toMatchObject({ limit: "duration" });
      expect(yield* Deferred.isDone(handlerFinalized)).toBe(true);
      expect(modelCalls).toBe(0);
      expect(observed.filter((event) => event._tag === "ToolCallStarted")).toHaveLength(1);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
      expect(observed.filter((event) => event._tag === "RunCompleted")).toHaveLength(0);
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

      const definition = Agent.make("resume-truncated-arguments", {
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
        { resume, resumeUsage: oneCallResumeUsage, durability: markingDurability(marks) },
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

      const definition = Agent.make("step-pass-through", {
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
        // Required by the durability protocol; this harness exercises neither seam.
        commitCompaction: () => Effect.void,
        noteTurnUsage: () => Effect.void,
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

      const definition = Agent.make("step-replay", {
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

      const definition = Agent.make("step-duplicate-name", {
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
        // Required by the durability protocol; this harness exercises neither seam.
        commitCompaction: () => Effect.void,
        noteTurnUsage: () => Effect.void,
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
        // Required by the durability protocol; this harness exercises neither seam.
        commitCompaction: () => Effect.void,
        noteTurnUsage: () => Effect.void,
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

      const definition = Agent.make("step-success-only", {
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
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const reported: Array<string> = [];

      const reporter = ErrorReporter.make(({ error }) => {
        reported.push(error.message);
      });

      const hookFailure = HookFailure.make({ message: "lookup boom" });

      const failingStepHook: RunStepHook<HookFailure> = {
        lookup: () => Effect.fail(hookFailure),
        commit: () => Effect.void,
      };

      const durability: RunDurabilityHook<HookFailure> = {
        commitResponse: () => Effect.void,
        // Required by the durability protocol; this harness exercises neither seam.
        commitCompaction: () => Effect.void,
        noteTurnUsage: () => Effect.void,
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

      const definition = Agent.make("step-hook-failure", {
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

      const exit = yield* AgentRuntime.stream(
        Agent.withModel(definition, model),
        { question: "work" },
        { durability },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(
          Layer.merge(toolLayer, ErrorReporter.layer([reporter], { mergeWithExisting: true })),
        ),
        Effect.exit,
      );

      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(DurableStepError);
      expect((failure as DurableStepError).reason).toBe("lookup-failed");
      expect((failure as DurableStepError).message).toBe("Durable Step lookup failed");
      expect((failure as DurableStepError).cause).toBe(hookFailure);
      expect(JSON.stringify(observed)).not.toContain("lookup boom");
      expect(reported).toEqual([]);
      expect(bodyRuns).toBe(0);
    }),
  );

  it.effect("keeps durability and authorization hook E/R visible", () => {
    const Empty = Tool.make("noop", {
      parameters: Schema.Struct({}),
      success: Schema.String,
    });

    const tools = Toolkit.make(Empty);

    const definition = Agent.make("durability-hook-types", {
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
      // Required by the durability protocol; this harness exercises neither seam.
      commitCompaction: () => Effect.void,
      noteTurnUsage: () => Effect.void,
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

    const authorizationProgram = AgentRuntime.run(
      Agent.withModel(definition, scriptedModel([], '{"answer":"typed"}')),
      { question: "typed" },
      {
        toolAuthorization: {
          authorize: () =>
            Effect.gen(function* () {
              yield* TypedHookService;

              return yield* HookFailure.make({ message: "authorization failed" });
            }),
        },
      },
    );

    const reservationProgram = AgentRuntime.run(
      Agent.withModel(definition, scriptedModel([], '{"answer":"typed"}')),
      { question: "typed" },
      {
        durability: {
          commitResponse: () => Effect.void,
          prepareToolCalls: () => Effect.void,
          commitCompaction: () => Effect.void,
          noteTurnUsage: () => Effect.void,
          step: inertStepHook,
          reservePolicyUsage: () =>
            Effect.gen(function* () {
              yield* TypedHookService;

              return yield* HookFailure.make({ message: "reservation failed" });
            }),
        },
      },
    );

    type ErrorProof = HookFailure extends Effect.Error<typeof program> ? true : false;
    type RequirementsProof =
      TypedHookService extends Effect.Services<typeof program> ? true : false;
    type AuthorizationErrorProof =
      HookFailure extends Effect.Error<typeof authorizationProgram> ? true : false;
    type AuthorizationRequirementsProof =
      TypedHookService extends Effect.Services<typeof authorizationProgram> ? true : false;
    const errorProof: ErrorProof = true;
    const requirementsProof: RequirementsProof = true;

    const reservationErrorProof: HookFailure extends Effect.Error<typeof reservationProgram>
      ? true
      : false = true;

    const reservationRequirementsProof: TypedHookService extends Effect.Services<
      typeof reservationProgram
    >
      ? true
      : false = true;

    const authorizationErrorProof: AuthorizationErrorProof = true;
    const authorizationRequirementsProof: AuthorizationRequirementsProof = true;

    expect(errorProof).toBe(true);
    expect(requirementsProof).toBe(true);
    expect(reservationErrorProof).toBe(true);
    expect(reservationRequirementsProof).toBe(true);
    expect(authorizationErrorProof).toBe(true);
    expect(authorizationRequirementsProof).toBe(true);

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
