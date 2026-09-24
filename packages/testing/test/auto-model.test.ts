import { AutoModel } from "@effect-agent/ai-decision";
import { expect, it } from "@effect/vitest";
import type { Scope } from "effect";
import { Context, Deferred, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import { Agent, AgentRuntime, Identifiers, InMemory, Subagent } from "effect-agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import {
  DecisionModel,
  AiError,
  LanguageModel,
  Model,
  type Prompt,
  type Response,
  Toolkit,
} from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

const policy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 2,
  maxDuration: "5 seconds",
  toolConcurrency: 2,
  runStatus: "off",
});

const definition = Agent.make("auto-assistant", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer.",
  toolkit: Toolkit.empty,
  policy,
});

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify(text) },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const nativeModel = (
  name: string,
  parts: (prompt: Prompt.Prompt) => ReadonlyArray<Response.StreamPartEncoded> = () =>
    finalParts(name),
  lifecycle?: Array<string>,
) =>
  Model.make(
    "fixture",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        yield* Effect.acquireRelease(
          Effect.sync(() => lifecycle?.push(`open:${name}`)),
          () =>
            Effect.sync(() => {
              lifecycle?.push(`close:${name}`);
            }),
        );

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: ({ prompt }) => Stream.fromIterable(parts(prompt)),
        });
      }),
    ),
  );

const answer = (choice = "routine"): DecisionModel.ProviderResponse => ({
  usage: { inputTokens: 20, outputTokens: 2 },
  answers: {
    model: {
      _tag: "Classify",
      label: choice,
      probabilities: {
        routine: choice === "routine" ? 1 : 0,
        difficult: choice === "difficult" ? 1 : 0,
      },
    },
  },
});

const decisionLayer = (
  evaluate: (
    request: DecisionModel.ProviderOptions,
  ) => Effect.Effect<DecisionModel.ProviderResponse, AiError.AiError, Scope.Scope>,
) =>
  Layer.effect(
    DecisionModel.DecisionModel,
    DecisionModel.make({ decide: (request) => evaluate(request).pipe(Effect.scoped) }),
  );

const catalog = (small = nativeModel("small"), large = nativeModel("large")) =>
  AutoModel.make({
    version: "v1",
    models: {
      routine: { model: small, description: "Routine tasks" },
      difficult: { model: large, description: "Difficult tasks" },
    },
  });

it.effect(
  "interrupts selection in the run lifetime, releases resources, and allows retry of an unselected thread",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const lifecycle: Array<string> = [];
      let finalized = false;
      let retry = false;

      const live = decisionLayer(() =>
        Effect.gen(function* () {
          if (retry) return answer();
          yield* Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
            Effect.sync(() => {
              finalized = true;
            }),
          );

          return yield* Effect.never;
        }),
      );

      const models = catalog(nativeModel("small", undefined, lifecycle));

      yield* Effect.gen(function* () {
        const options = { threadId: Identifiers.ThreadId.make("retry-interruption") };
        const fiber = yield* AgentRuntime.run(definition, "task", options).pipe(Effect.forkChild);

        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(Exit.hasInterrupts(exit)).toBe(true);
        expect(finalized).toBe(true);
        expect(lifecycle).toEqual([]);
        retry = true;
        expect((yield* AgentRuntime.run(definition, "retry", options)).output).toBe("small");
        expect(lifecycle).toEqual(["open:small", "close:small"]);
      }).pipe(
        Effect.provide(
          models.pipe(
            Layer.provideMerge(Layer.mergeAll(live, AutoModel.layerMemory(), InMemory.layer)),
          ),
        ),
      );
    }),
);

class SmallClient extends Context.Service<SmallClient, { readonly small: true }>()(
  "auto/SmallClient",
) {}
class LargeClient extends Context.Service<LargeClient, { readonly large: true }>()(
  "auto/LargeClient",
) {}

const verifyCatalogCompositionRequirements = () => {
  const models = AutoModel.make({
    version: "v1",
    models: {
      routine: {
        description: "Routine",
        model: Layer.unwrap(SmallClient.pipe(Effect.as(nativeModel("small")))),
      },
      difficult: {
        description: "Difficult",
        model: Layer.unwrap(LargeClient.pipe(Effect.as(nativeModel("large")))),
      },
    },
  });

  const required = AgentRuntime.run(definition, "task").pipe(Effect.provide(InMemory.layer));

  expectTypeOf<Effect.Services<typeof required>>().toEqualTypeOf<Agent.ModelServices>();
  const operation = required.pipe(Effect.provide(models));

  expectTypeOf<Effect.Services<typeof operation>>().toEqualTypeOf<
    SmallClient | LargeClient | DecisionModel.DecisionModel | AutoModel.SelectionStore
  >();
  const child = Subagent.make("typed_child", { target: definition });
  const requiredHandlers = Subagent.layer(child).pipe(Layer.provide(InMemory.layer));

  expectTypeOf<Layer.Services<typeof requiredHandlers>>().toEqualTypeOf<Agent.ModelServices>();
  const handlers = requiredHandlers.pipe(Layer.provide(models));

  expectTypeOf<Layer.Services<typeof handlers>>().toEqualTypeOf<
    SmallClient | LargeClient | DecisionModel.DecisionModel | AutoModel.SelectionStore
  >();
};

void verifyCatalogCompositionRequirements;

it.effect(
  "retains a committed choice when generation fails before the first successful response",
  () =>
    Effect.gen(function* () {
      let selections = 0;
      let generations = 0;

      const failure = new AiError.AiError({
        module: "fixture",
        method: "streamText",
        reason: new AiError.InvalidRequestError({ description: "generation failed" }),
      });

      const small = Model.make(
        "fixture",
        "small",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.suspend(() => {
                generations++;

                return generations === 1
                  ? Stream.fail(failure)
                  : Stream.fromIterable(finalParts("small"));
              }),
          }),
        ),
      );

      const models = catalog(small);
      const options = { threadId: Identifiers.ThreadId.make("generation-retry") };

      yield* Effect.gen(function* () {
        expect(yield* AgentRuntime.run(definition, "initial task", options).pipe(Effect.flip)).toBe(
          failure,
        );
        expect((yield* AgentRuntime.run(definition, "different task", options)).output).toBe(
          "small",
        );
        expect(selections).toBe(1);
      }).pipe(
        Effect.provide(
          models.pipe(
            Layer.provideMerge(
              Layer.mergeAll(
                InMemory.layer,
                AutoModel.layerMemory(),
                decisionLayer(() =>
                  Effect.sync(() => {
                    selections++;

                    return answer(selections === 1 ? "routine" : "difficult");
                  }),
                ),
              ),
            ),
          ),
        ),
      );
    }),
);
