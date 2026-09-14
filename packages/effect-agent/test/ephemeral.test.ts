import { describe, expect, it } from "@effect/vitest";
import { Context, Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { Thread } from "effect-agent";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Prompt, type Response, Toolkit } from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

import * as Subagent from "../src/capabilities/Subagent.ts";
import { type SubagentReservations } from "../src/capabilities/SubagentReservations.ts";
import * as Agent from "../src/core/Agent.ts";
import { RunId, ThreadId, TurnId } from "../src/core/Identifiers.ts";
import { IdGenerator } from "../src/core/IdGenerator.ts";
import { SubagentDelegationCaps } from "../src/core/SubagentContract.ts";
import * as AgentRuntime from "../src/engine/AgentRuntime.ts";
import { RunContextPreparation } from "../src/engine/RunOptions.ts";
import { ThreadHistory } from "../src/engine/ThreadHistory.ts";
import * as Ephemeral from "../src/Ephemeral.ts";

const answer: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const scriptedModel = (
  name: string,
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
  observe: (options: LanguageModel.ProviderOptions) => void = () => {},
  finalized: Effect.Effect<void> = Effect.void,
  beforeTurn: Effect.Effect<void> = Effect.void,
) =>
  Model.make(
    "test",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turn = yield* Ref.make(0);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* beforeTurn;
                observe(options);
                const index = yield* Ref.getAndUpdate(turn, (value) => value + 1);

                return Stream.fromIterable(turns[index] ?? answer).pipe(Stream.ensuring(finalized));
              }),
            ),
        });
      }),
    ),
  );

const child = Agent.make("child", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer the question.",
  toolkit: Toolkit.empty,
});

const first = Subagent.make("first", { target: child });
const second = Subagent.make("second", { target: child });

const parent = Agent.make("parent", {
  input: Schema.String,
  output: child.output,
  instructions: "Delegate, then answer.",
  toolkit: Toolkit.make(first.tool, second.tool),
  policy: { toolConcurrency: 1, maxToolCalls: 4 },
});

const delegate = (...names: ReadonlyArray<string>): ReadonlyArray<Response.StreamPartEncoded> => [
  ...names.map((name, index): Response.StreamPartEncoded => ({
    type: "tool-call",
    id: `call-${index}`,
    name,
    params: "question",
  })),
  { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
];

describe("ephemeral assembly", () => {
  for (const entrypoint of ["run", "stream", "start"] as const) {
    it.effect(`${entrypoint} retains a conversation for later Runs on that Thread`, () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("conversation");
        const prompts: Array<string> = [];

        const model = scriptedModel("conversation", [answer, answer, answer], (options) => {
          prompts.push(JSON.stringify(options.prompt));
        });

        const history = yield* Effect.gen(function* () {
          if (entrypoint === "run") {
            yield* AgentRuntime.run(child, "Plan a trip to Lisbon", { threadId });
          } else if (entrypoint === "stream") {
            yield* AgentRuntime.stream(child, "Plan a trip to Lisbon", { threadId }).pipe(
              Stream.runDrain,
            );
          } else {
            const handle = yield* AgentRuntime.start(child, "Plan a trip to Lisbon", { threadId });

            yield* handle.await;
          }

          yield* AgentRuntime.run(child, "Make it cheaper", { threadId });
          yield* AgentRuntime.run(child, "A separate conversation");
          const history = yield* ThreadHistory;
          const stored = yield* history.load(threadId);
          const threads = yield* Thread.Store;
          const conversation = yield* threads.snapshot(threadId);

          expectTypeOf<typeof conversation>().toEqualTypeOf<Thread.Thread>();
          expect(Thread.toPrompt(conversation)).toEqual(stored);

          expect(prompts[1]).toContain("Plan a trip to Lisbon");
          expect(prompts[1]).toContain("done");
          expect(prompts[1]).toContain("Make it cheaper");
          expect(prompts[2]).not.toContain("Plan a trip to Lisbon");
          expect(JSON.stringify(stored)).toContain("Make it cheaper");

          return history;
        }).pipe(Effect.provide(Layer.merge(Ephemeral.layer, model)));

        // Closing the application Layer releases its store, even if a service reference escapes.
        expect(yield* history.load(threadId).pipe(Effect.flip)).toMatchObject({
          reason: "not-found",
        });

        const fresh: Array<string> = [];

        yield* AgentRuntime.run(child, "Fresh application", { threadId }).pipe(
          Effect.provide(
            Layer.merge(
              Ephemeral.layer,
              scriptedModel("fresh", [answer], (options) => {
                fresh.push(JSON.stringify(options.prompt));
              }),
            ),
          ),
        );
        expect(fresh[0]).not.toContain("Plan a trip to Lisbon");
      }).pipe(Effect.scoped),
    );
  }

  for (const ending of ["failure", "defect", "interruption"] as const) {
    it.effect(`keeps recorded messages after a history observer ${ending}`, () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make(`history-${ending}`);

        const stop =
          ending === "failure"
            ? Effect.fail("observer stopped")
            : ending === "defect"
              ? Effect.die("observer defect")
              : Effect.interrupt;

        const prompts: Array<string> = [];

        const model = scriptedModel("observer", [answer], (options) => {
          prompts.push(JSON.stringify(options.prompt));
        });

        yield* Effect.gen(function* () {
          const exit = yield* AgentRuntime.run(child, "Remember this request", {
            threadId,
            onHistory: () => stop,
          }).pipe(Effect.exit);

          expect(Exit.isFailure(exit)).toBe(true);
          expect(prompts).toHaveLength(0);

          yield* AgentRuntime.run(child, "Continue", { threadId });
          expect(prompts[0]).toContain("Remember this request");
        }).pipe(Effect.provide(Layer.merge(Ephemeral.layer, model)));
      }),
    );
  }

  it.effect("keeps history after timeout and finalizes the pending model stream", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const finalized = yield* Ref.make(0);
      const threadId = ThreadId.make("timed-out");

      const model = scriptedModel(
        "blocked",
        [answer],
        () => {},
        Effect.void,
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Ref.update(finalized, (n) => n + 1)),
        ),
      );

      yield* Effect.gen(function* () {
        const fiber = yield* AgentRuntime.run(child, "Retain before timeout", { threadId }).pipe(
          Effect.timeout("1 second"),
          Effect.forkChild,
        );

        yield* Deferred.await(entered);
        yield* TestClock.adjust("1 second");
        expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
        expect(yield* Ref.get(finalized)).toBe(1);
        const history = yield* ThreadHistory;

        expect(JSON.stringify(yield* history.load(threadId))).toContain("Retain before timeout");
      }).pipe(Effect.provide(Layer.merge(Ephemeral.layer, model)));
    }),
  );

  it.effect(
    "rejects stale concurrent history without erasing the winning conversation or retrying",
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        const threadId = ThreadId.make("concurrent");
        let modelCalls = 0;

        const slow = scriptedModel(
          "slow",
          [answer],
          () => {
            modelCalls++;
          },
          Effect.void,
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(resume))),
        );

        const fast = scriptedModel("fast", [answer], () => {
          modelCalls++;
        });

        yield* Effect.gen(function* () {
          const first = yield* AgentRuntime.run(Agent.withModel(child, slow), "First request", {
            threadId,
          }).pipe(Effect.forkChild);

          yield* Deferred.await(entered);
          yield* AgentRuntime.run(Agent.withModel(child, fast), "Second request", { threadId });
          yield* Deferred.succeed(resume, undefined);
          const failed = yield* Fiber.join(first).pipe(Effect.flip);

          expect(failed).toMatchObject({ _tag: "ThreadHistoryError", reason: "conflict" });
          expect(modelCalls).toBe(2);
          const history = yield* ThreadHistory;
          const stored = JSON.stringify(yield* history.load(threadId));

          expect(stored).toContain("First request");
          expect(stored).toContain("Second request");
        }).pipe(Effect.provide(Ephemeral.layer));
      }),
  );

  it.effect(
    "enforces memory limits before model calls and refuses to replace retained history",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("bounded-history");

        const seed = Prompt.fromMessages(
          Array.from({ length: 1024 }, () =>
            Prompt.userMessage({ content: [Prompt.textPart({ text: "retained" })] }),
          ),
        );

        let calls = 0;

        const model = scriptedModel("bounded", [answer], () => {
          calls++;
        });

        yield* Effect.gen(function* () {
          const failure = yield* AgentRuntime.run(child, "Overflow", {
            threadId,
            history: seed,
          }).pipe(Effect.flip);

          expect(failure).toMatchObject({ _tag: "ThreadHistoryError", reason: "limit" });
          const history = yield* ThreadHistory;

          expect((yield* history.load(threadId)).content).toEqual(seed.content);

          const replaced = yield* AgentRuntime.run(child, "Discard earlier history", {
            threadId,
            history: Prompt.empty,
          }).pipe(Effect.flip);

          expect(replaced).toMatchObject({ _tag: "ThreadHistoryError", reason: "conflict" });
          expect(calls).toBe(0);
        }).pipe(Effect.provide(Layer.merge(Ephemeral.layer, model)));
      }),
  );

  for (const entrypoint of ["run", "stream", "start"] as const) {
    it.effect(`runs through ${entrypoint} with default IDs and no context service`, () =>
      Effect.gen(function* () {
        const model = scriptedModel("child", [answer]);

        const program = Effect.gen(function* () {
          if (entrypoint === "run") {
            const result = yield* AgentRuntime.run(child, "question");

            expect(result.output.answer).toBe("done");
            expect(result.threadId).toMatch(/^thread-/);
          } else if (entrypoint === "stream") {
            const events = yield* AgentRuntime.stream(child, "question").pipe(Stream.runCollect);

            expect(events.at(-1)?._tag).toBe("RunCompleted");
          } else {
            const handle = yield* AgentRuntime.start(child, "question");

            expect((yield* handle.await).output.answer).toBe("done");
          }

          const history = yield* ThreadHistory;

          const failure = yield* history
            .load(Schema.decodeSync(ThreadId)("missing"))
            .pipe(Effect.flip);

          expect(failure.reason).toBe("not-found");
        }).pipe(Effect.scoped, Effect.provide(Layer.merge(model, Ephemeral.layer)));

        expectTypeOf<Effect.Services<typeof program>>().toEqualTypeOf<never>();
        yield* program;
      }),
    );
  }

  it.effect("preserves enclosing ID and context overrides in the parent and child", () =>
    Effect.gen(function* () {
      const prompts: Array<string> = [];
      const finalized = yield* Ref.make(0);
      const sequence = yield* Ref.make(0);
      const next = Ref.updateAndGet(sequence, (n) => n + 1);

      const inspect = (options: LanguageModel.ProviderOptions) => {
        prompts.push(JSON.stringify(options.prompt));
      };

      const childModel = scriptedModel(
        "child",
        [answer],
        inspect,
        Ref.update(finalized, (n) => n + 1),
      );

      const parentModel = scriptedModel("parent", [delegate("first"), answer], inspect);

      const handlers = Layer.merge(
        Subagent.layer(first, childModel),
        Subagent.layer(second, childModel),
      );

      const result = yield* AgentRuntime.run(parent, "question").pipe(
        Effect.provide(
          Layer.merge(handlers, parentModel).pipe(Layer.provideMerge(Ephemeral.layer)),
        ),
        Effect.provideService(IdGenerator, {
          nextThreadId: next.pipe(
            Effect.map((n) => Schema.decodeSync(ThreadId)(`custom-thread-${n}`)),
          ),
          nextRunId: next.pipe(Effect.map((n) => Schema.decodeSync(RunId)(`custom-run-${n}`))),
          nextTurnId: next.pipe(Effect.map((n) => Schema.decodeSync(TurnId)(`custom-turn-${n}`))),
        }),
        Effect.provideService(RunContextPreparation, {
          transientContext: { load: () => Effect.succeed("host context marker") },
        }),
      );

      expect(result.threadId).toBe("custom-thread-1");
      expect(result.runId).toBe("custom-run-2");
      expect(prompts).toHaveLength(3);
      expect(prompts.every((prompt) => prompt.includes("host context marker"))).toBe(true);
      expect(yield* Ref.get(finalized)).toBe(1);
    }),
  );

  it.effect("shares sibling budgets and gives independent builds fresh reservation state", () =>
    Effect.gen(function* () {
      const childCalls = yield* Ref.make(0);

      const model = scriptedModel(
        "child",
        [answer],
        () => {},
        Ref.update(childCalls, (n) => n + 1),
      );

      const parentCaps = SubagentDelegationCaps.make({ maxTotalChildInvocations: 1 });

      const handlers = Layer.merge(
        Subagent.layer(first, model, { parentCaps }),
        Subagent.layer(second, model, { parentCaps }),
      );

      const identifiers = Layer.effect(
        IdGenerator,
        Effect.gen(function* () {
          const sequence = yield* Ref.make(0);
          const next = Ref.updateAndGet(sequence, (n) => n + 1);

          return {
            nextThreadId: next.pipe(Effect.map((n) => Schema.decodeSync(ThreadId)(`thread-${n}`))),
            nextRunId: next.pipe(Effect.map((n) => Schema.decodeSync(RunId)(`run-${n}`))),
            nextTurnId: next.pipe(Effect.map((n) => Schema.decodeSync(TurnId)(`turn-${n}`))),
          };
        }),
      );

      const program = AgentRuntime.run(parent, "question").pipe(
        Effect.provide(
          Layer.merge(
            handlers,
            scriptedModel("parent", [delegate("first", "second"), answer]),
          ).pipe(Layer.provideMerge(Ephemeral.layer), Layer.provideMerge(identifiers)),
        ),
        Effect.flip,
      );

      for (let build = 0; build < 2; build++) {
        const error = yield* program;

        expect(error._tag).toBe("SubagentBudgetExhausted");
        if (error._tag === "SubagentBudgetExhausted") {
          expect(error.dimension).toBe("total-child-invocations");
        }
        expect(yield* Ref.get(childCalls)).toBe(build + 1);
      }
    }),
  );
});

class ProviderClient extends Context.Service<ProviderClient, string>()(
  "ephemeral/ProviderClient",
) {}

it("retains provider requirements and typed failures while default IDs require no service", () => {
  const model = Model.make(
    "test",
    "required-client",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        yield* ProviderClient;

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.fromIterable(answer),
        });
      }),
    ),
  );

  const handlers = Subagent.layer(first, model);

  const program = AgentRuntime.run(Agent.withModel(child, model), "question").pipe(
    Effect.provide(Ephemeral.layer),
  );

  expectTypeOf<Layer.Services<typeof handlers>>().toEqualTypeOf<
    ProviderClient | SubagentReservations
  >();
  expectTypeOf<Effect.Services<typeof program>>().toEqualTypeOf<ProviderClient>();
  expectTypeOf<Effect.Error<typeof program>>().toEqualTypeOf<
    AgentRuntime.AgentRuntimeFailure<typeof child>
  >();
  expectTypeOf<Effect.Services<typeof IdGenerator>>().toEqualTypeOf<never>();
});
