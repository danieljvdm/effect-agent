import * as Agent from "@effect-agent/core/Agent";
import {
  AgentInputError,
  AgentPolicyError,
  ContextBudgetError,
} from "@effect-agent/core/AgentError";
import { AgentPolicy, CompactionPolicy } from "@effect-agent/core/AgentPolicy";
import { RunId, ThreadId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { MemoryRecallError } from "@effect-agent/core/MemoryReference";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { CompactionError, ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import { RunContextPreparation } from "@effect-agent/engine/RunOptions";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Context,
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
  AiError,
  LanguageModel,
  Model,
  Prompt,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";

import { CLEARED_TOOL_RESULT } from "../src/internal/compaction.ts";
import { ThreadHistory } from "../src/ThreadHistory.ts";

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("transient-thread")),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("transient-run")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("transient-turn")),
});

const testLayer = Layer.mergeAll(identifiers, ThreadHistory.layerTransient, ContextCompactor.layer);

const finalParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const makeModel = (requests: Array<Prompt.Prompt>) =>
  Model.make(
    "scripted",
    "transient-context",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          requests.push(request.prompt);

          return Stream.fromIterable(finalParts);
        },
      }),
    ),
  );

const makeAgent = (requests: Array<Prompt.Prompt>, contextTokenLimit?: number) =>
  Agent.withModel(
    Agent.make("transient-context", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Answer the question.",
      toolkit: Toolkit.empty,
      policy: AgentPolicy.make({
        maxTurns: 1,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
        ...(contextTokenLimit === undefined ? {} : { contextTokenLimit }),
      }),
    }),
    makeModel(requests),
  );

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected failure");
  const failure = Cause.findErrorOption(exit.cause);

  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");

  return failure.value;
};

class TransientContextFailure extends Schema.TaggedError<TransientContextFailure>()(
  "TransientContextFailure",
  { message: Schema.String },
) {}

class TransientContextDependency extends Context.Service<
  TransientContextDependency,
  { readonly value: string }
>()("@effect-agent/engine/test/TransientContextDependency") {}

layer(testLayer)("transient model context", (it) => {
  it.effect("runs without installing a context preparation service", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];
      const result = yield* AgentRuntime.run(makeAgent(requests), "question");

      expect(result.output).toBe("done");
      expect(requests).toHaveLength(1);
    }),
  );
  for (const entrypoint of ["run", "stream", "start"]) {
    it.effect(`loads the provided context service through ${entrypoint}`, () =>
      Effect.gen(function* () {
        const requests: Array<Prompt.Prompt> = [];
        const agent = makeAgent(requests);

        const contextLive = Layer.succeed(RunContextPreparation, {
          transientContext: { load: () => Effect.succeed("project memory") },
        });

        const program = Effect.gen(function* () {
          if (entrypoint === "run") yield* AgentRuntime.run(agent, "question");
          else if (entrypoint === "stream") {
            yield* AgentRuntime.stream(agent, "question").pipe(Stream.runDrain);
          } else {
            const handle = yield* AgentRuntime.start(agent, "question");

            yield* handle.await;
          }
        });

        const contextRequired: RunContextPreparation extends Effect.Services<typeof program>
          ? true
          : false = false;

        yield* program.pipe(Effect.provide(contextLive));

        expect(contextRequired).toBe(false);
        expect(requests).toHaveLength(1);
        expect(requests[0]?.content).toContainEqual(Prompt.make("project memory").content[0]);
      }),
    );
  }

  for (const phase of ["acquisition", "load"]) {
    it.effect(`preserves tagged ${phase} errors and closes the context Layer`, () =>
      Effect.gen(function* () {
        const requests: Array<Prompt.Prompt> = [];
        const finalized = yield* Ref.make(0);

        const contextLive = Layer.effect(
          RunContextPreparation,
          Effect.gen(function* () {
            const dependency = yield* TransientContextDependency;

            yield* Effect.acquireRelease(Effect.void, () =>
              Ref.update(finalized, (count) => count + 1),
            );
            if (phase === "acquisition") {
              return yield* TransientContextFailure.make({ message: dependency.value });
            }

            return RunContextPreparation.of({
              transientContext: {
                load: () =>
                  Effect.fail(
                    MemoryRecallError.make({
                      reason: "unavailable",
                      sourceId: "project-notes",
                      message: dependency.value,
                    }),
                  ),
              },
            });
          }),
        );

        const program = AgentRuntime.run(makeAgent(requests), "question").pipe(
          Effect.provide(contextLive),
        );

        const dependencyRequired: TransientContextDependency extends Effect.Services<typeof program>
          ? true
          : false = true;

        const acquisitionErrorPreserved: TransientContextFailure extends Effect.Error<
          typeof program
        >
          ? true
          : false = true;

        const methodErrorPreserved: MemoryRecallError extends Effect.Error<typeof program>
          ? true
          : false = true;

        const result = yield* program.pipe(
          Effect.catchTags({
            TransientContextFailure: (error) => Effect.succeed(error),
            MemoryRecallError: (error) => Effect.succeed(error),
          }),
          Effect.provideService(TransientContextDependency, { value: "source unavailable" }),
        );

        expect(result).toEqual(
          phase === "acquisition"
            ? TransientContextFailure.make({ message: "source unavailable" })
            : MemoryRecallError.make({
                reason: "unavailable",
                sourceId: "project-notes",
                message: "source unavailable",
              }),
        );
        expect(dependencyRequired && acquisitionErrorPreserved && methodErrorPreserved).toBe(true);
        expect(yield* Ref.get(finalized)).toBe(1);
        expect(requests).toHaveLength(0);
      }),
    );
  }

  it.effect("fails malformed transient messages before provider I/O", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const malformedMessage = new Proxy(
        { role: "user" as const, content: "valid until read" },
        {
          get: (target, property, receiver) =>
            property === "content" ? 42 : Reflect.get(target, property, receiver),
        },
      );

      const exit = yield* AgentRuntime.run(makeAgent(requests), "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: { load: () => Effect.succeed([malformedMessage]) },
        }),
        Effect.exit,
      );

      const failure = failureFrom(exit);

      expect(Schema.is(AgentInputError)(failure)).toBe(true);
      expect(requests).toHaveLength(0);
    }),
  );

  it.effect("budgets oversized transient context before provider I/O", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const exit = yield* AgentRuntime.run(makeAgent(requests, 2_000), "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: { load: () => Effect.succeed("reference".repeat(4_000)) },
        }),
        Effect.exit,
      );

      const failure = failureFrom(exit);

      expect(Schema.is(ContextBudgetError)(failure)).toBe(true);
      expect(requests).toHaveLength(0);
    }),
  );

  it.effect("loads transient context only after threshold compaction succeeds", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const Search = Tool.make("search", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });

      const tools = Toolkit.make(Search);

      const model = Model.make(
        "scripted",
        "transient-compaction-order",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              requests.push(request.prompt);

              return Stream.fromIterable<Response.StreamPartEncoded>([
                {
                  type: "tool-call",
                  id: "search-1",
                  name: "search",
                  params: {},
                  providerExecuted: false,
                },
                {
                  type: "finish",
                  reason: "tool-calls",
                  usage: { inputTokens: {}, outputTokens: {} },
                },
              ]);
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("transient-compaction-order", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Search, then answer.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 2_500,
            compaction: CompactionPolicy.make({ keepRecentTokens: 300, mode: "prune" }),
          }),
        }),
        model,
      );

      const loads = yield* Ref.make(0);
      const compactionFailure = CompactionError.make({ message: "compaction unavailable" });

      const exit = yield* AgentRuntime.run(agent, "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: {
            load: () => Ref.update(loads, (count) => count + 1).pipe(Effect.as(Prompt.empty)),
          },
        }),
        Effect.provideService(ContextCompactor, {
          estimate: (messages) =>
            messages.some((message) => message.role === "tool") ? 3_000 : messages.length * 100,
          compact: () => Stream.fail(compactionFailure),
        }),
        Effect.provide(tools.toLayer({ search: () => Effect.succeed("found") })),
        Effect.exit,
      );

      expect(failureFrom(exit)).toEqual(compactionFailure);
      expect(yield* Ref.get(loads)).toBe(1);
      expect(requests).toHaveLength(1);
    }),
  );

  it.effect("compacts the canonical basis when the loaded snapshot exceeds admission", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const Search = Tool.make("search", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });

      const tools = Toolkit.make(Search);

      const model = Model.make(
        "scripted",
        "transient-post-load-compaction",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              requests.push(request.prompt);
              if (requests.length <= 2) {
                return Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: "tool-call",
                    id: `search-${requests.length}`,
                    name: "search",
                    params: {},
                    providerExecuted: false,
                  },
                  {
                    type: "finish",
                    reason: "tool-calls",
                    usage: { inputTokens: {}, outputTokens: {} },
                  },
                ]);
              }

              return Stream.fromIterable(finalParts);
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("transient-post-load-compaction", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Search twice, then answer.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 2_500,
            compaction: CompactionPolicy.make({ keepRecentTokens: 300, mode: "prune" }),
          }),
        }),
        model,
      );

      const loads = yield* Ref.make(0);
      const compactionTargets: Array<number | undefined> = [];
      const toolCalls = yield* Ref.make(0);

      const result = yield* AgentRuntime.run(agent, "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: {
            load: () => Ref.update(loads, (count) => count + 1).pipe(Effect.as("snapshot")),
          },
        }),
        Effect.provideService(ContextCompactor, {
          estimate: (messages) => {
            const activeToolResults = messages.reduce(
              (count, message) =>
                count +
                (message.role === "tool" &&
                message.content.some(
                  (part) => part.type === "tool-result" && part.result !== CLEARED_TOOL_RESULT,
                )
                  ? 1
                  : 0),
              0,
            );

            return (
              messages.length * 50 +
              activeToolResults * 900 +
              (JSON.stringify(messages).includes("snapshot") ? 600 : 0)
            );
          },
          compact: (request) => {
            compactionTargets.push(request.targetTokens);

            return Stream.succeed({ kind: "clear-tool-results" as const, through: 4 });
          },
        }),
        Effect.provide(
          tools.toLayer({
            search: () =>
              Ref.getAndUpdate(toolCalls, (count) => count + 1).pipe(
                Effect.map((count) => (count === 0 ? "first-result" : "second-result")),
              ),
          }),
        ),
      );

      expect(result.output).toBe("done");
      expect(yield* Ref.get(loads)).toBe(3);
      expect(compactionTargets).toEqual([1_750]);
      expect(requests).toHaveLength(3);
      expect(JSON.stringify(requests[2]?.content)).not.toContain("first-result");
      expect(JSON.stringify(requests[2]?.content)).toContain("second-result");
      expect(JSON.stringify(requests[2]?.content)).toContain("snapshot");
    }),
  );

  it.effect("does not carry a prior Turn summary allowance into post-load compaction", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const Search = Tool.make("search", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });

      const tools = Toolkit.make(Search);

      const model = Model.make(
        "scripted",
        "transient-prior-summary",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              requests.push(request.prompt);
              if (requests.length <= 3) {
                return Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: "tool-call",
                    id: `search-${requests.length}`,
                    name: "search",
                    params: {},
                    providerExecuted: false,
                  },
                  {
                    type: "finish",
                    reason: "tool-calls",
                    usage: { inputTokens: {}, outputTokens: {} },
                  },
                ]);
              }

              return Stream.fromIterable(finalParts);
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("transient-prior-summary", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Search three times, then answer.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 4,
            maxToolCalls: 3,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 2_100,
            compaction: CompactionPolicy.make({ keepRecentTokens: 300, mode: "summarize" }),
          }),
        }),
        model,
      );

      const loads = yield* Ref.make(0);

      const compactionPasses: Array<{
        readonly sourceLength: number;
        readonly targetTokens: number | undefined;
        readonly trigger: string;
      }> = [];

      const toolCalls = yield* Ref.make(0);
      const results = ["first-result", "second-result", "third-result"];

      const result = yield* AgentRuntime.run(agent, "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: {
            load: () => Ref.update(loads, (count) => count + 1).pipe(Effect.as("snapshot")),
          },
        }),
        Effect.provideService(ContextCompactor, {
          estimate: (messages) => {
            const activeToolResults = messages.reduce(
              (count, message) =>
                count +
                (message.role === "tool" &&
                message.content.some(
                  (part) => part.type === "tool-result" && part.result !== CLEARED_TOOL_RESULT,
                )
                  ? 1
                  : 0),
              0,
            );

            return (
              messages.length * 50 +
              activeToolResults * 700 +
              (JSON.stringify(messages).includes("snapshot") ? 500 : 0)
            );
          },
          compact: (request) => {
            compactionPasses.push({
              sourceLength: request.source.content.length,
              targetTokens: request.targetTokens,
              trigger: request.trigger,
            });

            return request.source.content.length === 6
              ? Stream.succeed({
                  kind: "summarize" as const,
                  through: 4,
                  summary: "first search summarized",
                })
              : Stream.succeed({ kind: "clear-tool-results" as const, through: 6 });
          },
        }),
        Effect.provide(
          tools.toLayer({
            search: () =>
              Ref.getAndUpdate(toolCalls, (count) => count + 1).pipe(
                Effect.map((count) => results[count] ?? "unexpected-result"),
              ),
          }),
        ),
      );

      expect(result.output).toBe("done");
      expect(yield* Ref.get(loads)).toBe(4);
      expect(compactionPasses).toEqual([
        { sourceLength: 6, targetTokens: 1_450, trigger: "pressure" },
        { sourceLength: 8, targetTokens: 1_450, trigger: "pressure" },
      ]);
      expect(requests).toHaveLength(4);
      expect(JSON.stringify(requests[3]?.content)).toContain("first search summarized");
      expect(JSON.stringify(requests[3]?.content)).not.toContain("second-result");
      expect(JSON.stringify(requests[3]?.content)).toContain("third-result");
      expect(JSON.stringify(requests[3]?.content)).toContain("snapshot");
    }),
  );

  it.effect("reuses one transient snapshot for a same-Turn overflow retry", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const model = Model.make(
        "scripted",
        "transient-overflow-retry",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              requests.push(request.prompt);

              return requests.length === 1
                ? Stream.fail(
                    AiError.AiError.make({
                      module: "test",
                      method: "streamText",
                      reason: AiError.UnknownError.make({
                        description: "context_length_exceeded",
                      }),
                    }),
                  )
                : Stream.fromIterable(finalParts);
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("transient-overflow-retry", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Answer the question.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 100_000,
            compaction: CompactionPolicy.make({ keepRecentTokens: 300 }),
          }),
        }),
        model,
      );

      const loads = yield* Ref.make(0);

      const result = yield* AgentRuntime.run(agent, "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: {
            load: () =>
              Ref.getAndUpdate(loads, (count) => count + 1).pipe(
                Effect.map((count) => Prompt.make(`snapshot ${count + 1}`)),
              ),
          },
        }),
      );

      expect(result.output).toBe("done");
      expect(yield* Ref.get(loads)).toBe(1);
      expect(requests).toHaveLength(2);
      for (const request of requests) {
        expect(JSON.stringify(request.content)).toContain("snapshot 1");
        expect(JSON.stringify(request.content)).not.toContain("snapshot 2");
      }
    }),
  );

  it.effect("rejects an oversized transient reload before a grace provider call", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const Search = Tool.make("search", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });

      const tools = Toolkit.make(Search);

      const model = Model.make(
        "scripted",
        "transient-grace",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              requests.push(request.prompt);

              return Stream.fromIterable<Response.StreamPartEncoded>(
                requests.length === 1
                  ? [
                      {
                        type: "tool-call",
                        id: "search-1",
                        name: "search",
                        params: {},
                        providerExecuted: false,
                      },
                      {
                        type: "finish",
                        reason: "tool-calls",
                        usage: { inputTokens: {}, outputTokens: {} },
                      },
                    ]
                  : finalParts,
              );
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("transient-grace", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Search, then answer.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 2_000,
            onExhaustion: "final-answer",
          }),
        }),
        model,
      );

      const loads = yield* Ref.make(0);

      const exit = yield* AgentRuntime.run(agent, "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: {
            load: () =>
              Ref.getAndUpdate(loads, (count) => count + 1).pipe(
                Effect.map((count) =>
                  count === 0 ? Prompt.empty : Prompt.make("new reference".repeat(4_000)),
                ),
              ),
          },
        }),
        Effect.provide(tools.toLayer({ search: () => Effect.succeed("found") })),
        Effect.exit,
      );

      const failure = failureFrom(exit);

      expect(Schema.is(ContextBudgetError)(failure)).toBe(true);
      expect(yield* Ref.get(loads)).toBe(2);
      expect(requests).toHaveLength(1);
    }),
  );

  it.effect("preserves a per-Run override's typed error and service requirement", () => {
    const requests: Array<Prompt.Prompt> = [];

    const program = AgentRuntime.run(makeAgent(requests), "question", {
      transientContext: {
        load: () =>
          Effect.gen(function* () {
            const dependency = yield* TransientContextDependency;

            return yield* TransientContextFailure.make({ message: dependency.value });
          }),
      },
    });

    const dependencyRequired: TransientContextDependency extends Effect.Services<typeof program>
      ? true
      : false = true;

    const failurePreserved: TransientContextFailure extends Effect.Error<typeof program>
      ? true
      : false = true;

    return Effect.gen(function* () {
      const exit = yield* program.pipe(Effect.exit);

      expect(failureFrom(exit)).toEqual(
        TransientContextFailure.make({ message: "typed dependency" }),
      );
      expect(dependencyRequired && failurePreserved).toBe(true);
      expect(requests).toHaveLength(0);
    }).pipe(
      Effect.provideService(TransientContextDependency, { value: "typed dependency" }),
      Effect.provideService(RunContextPreparation, {
        transientContext: { load: () => Effect.die("The overridden service loader must not run") },
      }),
    );
  });

  it.effect("preserves loader defects before provider I/O", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];
      const defect = new Error("transient loader defect");

      const exit = yield* AgentRuntime.run(makeAgent(requests), "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: { load: () => Effect.die(defect) },
        }),
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isSuccess(exit)) throw new Error("Expected defect");
      expect(exit.cause.reasons.filter(Cause.isDieReason).map(({ defect }) => defect)).toEqual([
        defect,
      ]);
      expect(requests).toHaveLength(0);
    }),
  );

  it.effect("interrupts a loader and closes its scoped resource", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];
      const entered = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();

      const run = AgentRuntime.run(makeAgent(requests), "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: {
            load: () =>
              Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
                Deferred.succeed(released, undefined),
              ).pipe(Effect.andThen(Effect.never), Effect.scoped),
          },
        }),
      );

      const fiber = yield* Effect.forkChild(Effect.scoped(run));

      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      yield* Deferred.await(released);

      expect(requests).toHaveLength(0);
    }),
  );

  it.effect("applies the Run deadline to a loader and closes its resource", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];
      const entered = yield* Deferred.make<void>();
      const finalized = yield* Ref.make(0);

      const fiber = yield* AgentRuntime.run(makeAgent(requests), "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: {
            load: () =>
              Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
                Ref.update(finalized, (count) => count + 1),
              ).pipe(Effect.andThen(Effect.never), Effect.scoped),
          },
        }),
        Effect.forkChild,
      );

      yield* Deferred.await(entered);
      yield* TestClock.adjust("30 seconds");
      const exit = yield* Fiber.await(fiber);

      const failure = failureFrom(exit);

      expect(Schema.is(AgentPolicyError)(failure)).toBe(true);
      if (Schema.is(AgentPolicyError)(failure)) expect(failure.limit).toBe("duration");
      expect(yield* Ref.get(finalized)).toBe(1);
      expect(requests).toHaveLength(0);
    }),
  );
});
