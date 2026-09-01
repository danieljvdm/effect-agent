import {
  Agent,
  AgentInputError,
  AgentPolicy,
  AgentPolicyError,
  ContextBudgetError,
  IdGenerator,
  RunId,
  ThreadId,
  TurnId,
} from "@effect-agent/core";
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
import { LanguageModel, Model, Prompt, type Response, Tool, Toolkit } from "effect/unstable/ai";

import { AgentRuntime, ContextCompactor } from "../src/index.ts";
import { ThreadHistory } from "../src/thread-history.ts";

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
      const exit = yield* AgentRuntime.run(makeAgent(requests), "question", {
        transientContext: { load: () => Effect.succeed([malformedMessage]) },
      }).pipe(Effect.exit);

      const failure = failureFrom(exit);
      expect(Schema.is(AgentInputError)(failure)).toBe(true);
      expect(requests).toHaveLength(0);
    }),
  );

  it.effect("budgets oversized transient context before provider I/O", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];
      const exit = yield* AgentRuntime.run(makeAgent(requests, 2_000), "question", {
        transientContext: { load: () => Effect.succeed("reference".repeat(4_000)) },
      }).pipe(Effect.exit);

      const failure = failureFrom(exit);
      expect(Schema.is(ContextBudgetError)(failure)).toBe(true);
      expect(requests).toHaveLength(0);
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
      const exit = yield* AgentRuntime.run(agent, "question", {
        transientContext: {
          load: () =>
            Ref.getAndUpdate(loads, (count) => count + 1).pipe(
              Effect.map((count) =>
                count === 0 ? Prompt.empty : Prompt.make("new reference".repeat(4_000)),
              ),
            ),
        },
      }).pipe(
        Effect.provide(tools.toLayer({ search: () => Effect.succeed("found") })),
        Effect.exit,
      );

      const failure = failureFrom(exit);
      expect(Schema.is(ContextBudgetError)(failure)).toBe(true);
      expect(yield* Ref.get(loads)).toBe(2);
      expect(requests).toHaveLength(1);
    }),
  );

  it.effect("preserves a transient hook's typed error and service requirement", () => {
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
    }).pipe(Effect.provideService(TransientContextDependency, { value: "typed dependency" }));
  });

  it.effect("preserves loader defects before provider I/O", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];
      const defect = new Error("transient loader defect");
      const exit = yield* AgentRuntime.run(makeAgent(requests), "question", {
        transientContext: { load: () => Effect.die(defect) },
      }).pipe(Effect.exit);

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
      const run = AgentRuntime.run(makeAgent(requests), "question", {
        transientContext: {
          load: () =>
            Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
              Deferred.succeed(released, undefined),
            ).pipe(Effect.andThen(Effect.never)),
        },
      });
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
      const fiber = yield* AgentRuntime.run(makeAgent(requests), "question", {
        transientContext: {
          load: () =>
            Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
              Ref.update(finalized, (count) => count + 1),
            ).pipe(Effect.andThen(Effect.never)),
        },
      }).pipe(Effect.forkChild);
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
