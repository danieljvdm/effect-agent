import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response, Toolkit } from "effect/unstable/ai";
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
