import { AutoModel, DecisionModel, type DecisionSchema } from "@effect-agent/ai-decision";
import { expect, it } from "@effect/vitest";
import type { Scope } from "effect";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect";
import { Agent, AgentRuntime, Identifiers, InMemory, Subagent } from "effect-agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import { TestClock } from "effect/testing";
import {
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

const answer = (choice = "routine") => ({
  provider: "fixture",
  model: "selector",
  usage: { inputTokens: 20, outputTokens: 2 },
  answers: {
    model: {
      type: "choice",
      choice,
      probabilities: {
        routine: choice === "routine" ? 1 : 0,
        difficult: choice === "difficult" ? 1 : 0,
      },
    },
  },
});

const decisionLayer = (
  evaluate: (
    request: DecisionSchema.EvaluateRequest,
  ) => Effect.Effect<unknown, AiError.AiError, Scope.Scope>,
) => Layer.effect(DecisionModel.DecisionModel, DecisionModel.make({ evaluate }));

const catalog = (small = nativeModel("small"), large = nativeModel("large")) =>
  AutoModel.make({
    version: "v1",
    models: {
      routine: { model: small, description: "Routine tasks" },
      difficult: { model: large, description: "Difficult tasks" },
    },
  });

it.effect(
  "selects on the first turn of a parent and each spawn, retaining choices across tool turns and follow-ups",
  () =>
    Effect.gen(function* () {
      const requests: Array<DecisionSchema.EvaluateRequest> = [];
      const lifecycle: Array<string> = [];
      const generations: Array<string> = [];

      const child = Subagent.make("research", {
        description: "Research a question",
        target: Agent.make("auto-child", {
          input: Schema.Struct({ task: Schema.String }),
          inputPrompt: ({ task }) => `Child task: ${task}`,
          output: Schema.String,
          instructions: "Research.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            ...policy,
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "1 second",
          }),
        }),
      });

      const parent = Agent.make("auto-parent", {
        input: Schema.Struct({ task: Schema.String, privateKey: Schema.String }),
        inputPrompt: ({ task }) => task,
        output: Schema.String,
        instructions: "Coordinate research.",
        toolkit: Toolkit.make(child.tool),
        policy,
      });

      const generate = (name: string) => (prompt: Prompt.Prompt) => {
        generations.push(name);
        if (
          JSON.stringify(prompt).includes("Coordinate research.") &&
          !prompt.content.some((message) => message.role === "tool")
        ) {
          return [
            {
              type: "tool-call",
              id: "child-a",
              name: "research",
              params: { task: "routine child" },
              providerExecuted: false,
            },
            {
              type: "tool-call",
              id: "child-b",
              name: "research",
              params: { task: "difficult child" },
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
          ] satisfies ReadonlyArray<Response.StreamPartEncoded>;
        }

        return finalParts(name);
      };

      const models = catalog(
        nativeModel("small", generate("small"), lifecycle),
        nativeModel("large", generate("large"), lifecycle),
      );

      const live = decisionLayer((request) =>
        Effect.sync(() => {
          requests.push(request);

          return answer(
            JSON.stringify(request.state).includes("routine child") ? "routine" : "difficult",
          );
        }),
      );

      const threadId = Identifiers.ThreadId.make("parent");
      const identities: Array<readonly [string, string]> = [];

      const options = {
        threadId,
        context: {
          prepare: ({ source }: { readonly source: Prompt.Prompt }) =>
            Effect.gen(function* () {
              identities.push([yield* Model.ProviderName, yield* Model.ModelName]);

              return { prompt: source };
            }),
        },
      };

      yield* Effect.gen(function* () {
        expect(requests).toEqual([]);
        expect(lifecycle).toEqual([]);

        const first = yield* AgentRuntime.run(
          parent,
          { task: "Compare options", privateKey: "HOST-ONLY-SENTINEL" },
          options,
        );

        expect(first.output).toBe("large");
        expect(requests).toHaveLength(3);
        expect(lifecycle.filter((event) => event.startsWith("open:"))).toHaveLength(3);
        expect(lifecycle.filter((event) => event.startsWith("close:"))).toHaveLength(3);

        const later = yield* AgentRuntime.run(
          parent,
          { task: "A routine follow-up", privateKey: "HOST-ONLY-SENTINEL" },
          options,
        );

        expect(later.output).toBe("large");
        expect(requests).toHaveLength(3);
        expect(generations.filter((name) => name === "small")).toHaveLength(1);
        expect(generations.filter((name) => name === "large")).toHaveLength(4);
        expect(lifecycle.filter((event) => event.startsWith("close:"))).toHaveLength(4);
      }).pipe(
        Effect.provide(
          Subagent.layer(child).pipe(
            Layer.provideMerge(models),
            Layer.provideMerge(Layer.mergeAll(live, AutoModel.layerMemory(), InMemory.layer)),
          ),
        ),
      );
      expect(identities).toEqual([
        ["fixture", "large"],
        ["fixture", "large"],
        ["fixture", "large"],
      ]);
      expect(JSON.stringify(requests)).not.toContain("HOST-ONLY-SENTINEL");
      expect(requests[0]?.state).toMatchObject({
        tools: [{ name: "research", description: "Research a question" }],
      });
      expect(requests.slice(1).map((request) => request.state)).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ prompt: expect.stringContaining("routine child"), tools: [] }),
          expect.objectContaining({
            prompt: expect.stringContaining("difficult child"),
            tools: [],
          }),
        ]),
      );
    }),
);

it.effect.each(["failure", "defect", "timeout", "interruption"] as const)(
  "keeps selection %s in the run lifetime, releases resources, and allows retry of an unselected thread",
  (mode) =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const lifecycle: Array<string> = [];
      let finalized = false;
      let retry = false;
      const defect = new Error("selector defect");

      const failure = new AiError.AiError({
        module: "fixture",
        method: "evaluate",
        reason: new AiError.InvalidRequestError({ description: "selector failed" }),
      });

      const live = decisionLayer(() =>
        Effect.gen(function* () {
          if (retry) return answer();
          yield* Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
            Effect.sync(() => {
              finalized = true;
            }),
          );
          if (mode === "failure") return yield* failure;
          if (mode === "defect") return yield* Effect.die(defect);

          return yield* Effect.never;
        }),
      );

      const models = catalog(nativeModel("small", undefined, lifecycle));

      yield* Effect.gen(function* () {
        const options = { threadId: Identifiers.ThreadId.make(`retry-${mode}`) };
        const fiber = yield* AgentRuntime.run(definition, "task", options).pipe(Effect.forkChild);

        yield* Deferred.await(entered);
        if (mode === "timeout") yield* TestClock.adjust("5 seconds");
        if (mode === "interruption") yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        if (mode === "failure")
          expect(Cause.findErrorOption(exit.cause)).toEqual(Option.some(failure));
        if (mode === "timeout")
          expect(Cause.findErrorOption(exit.cause)).toMatchObject({
            value: { _tag: "AgentPolicyError", limit: "duration" },
          });
        if (mode === "defect") expect(Cause.squash(exit.cause)).toBe(defect);
        if (mode === "interruption") expect(Exit.hasInterrupts(exit)).toBe(true);
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

it.effect("rejects a context hook that replaces the selected model", () =>
  Effect.gen(function* () {
    const lifecycle: Array<string> = [];

    const error = yield* AgentRuntime.run(definition, "task", {
      context: {
        prepare: ({ source }) =>
          Effect.succeed({
            prompt: source,
            modelCall: {
              model: nativeModel("replacement", undefined, lifecycle),
              context: {
                contextCapacity: 10000,
                outputReserveTokens: 100,
                uncountedOverheadTokens: 0,
              },
            },
          }),
      },
    }).pipe(
      Effect.provide(
        catalog().pipe(
          Layer.provideMerge(
            Layer.mergeAll(
              decisionLayer(() => Effect.succeed(answer())),
              AutoModel.layerMemory(),
              InMemory.layer,
            ),
          ),
        ),
      ),
      Effect.flip,
    );

    expect(error).toMatchObject({ _tag: "AiError", reason: { _tag: "InvalidRequestError" } });
    expect(lifecycle).toEqual([]);
  }),
);

class SmallClient extends Context.Service<SmallClient, { readonly small: true }>()(
  "auto/SmallClient",
) {}
class LargeClient extends Context.Service<LargeClient, { readonly large: true }>()(
  "auto/LargeClient",
) {}

it("preserves native client, decision, and store requirements in Agent and Subagent composition", () => {
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
  expectTypeOf<Effect.Error<typeof operation>>().toEqualTypeOf<
    Effect.Error<ReturnType<typeof AgentRuntime.run<typeof definition>>>
  >();
  const child = Subagent.make("typed_child", { target: definition });
  const requiredHandlers = Subagent.layer(child).pipe(Layer.provide(InMemory.layer));

  expectTypeOf<Layer.Services<typeof requiredHandlers>>().toEqualTypeOf<Agent.ModelServices>();
  const handlers = requiredHandlers.pipe(Layer.provide(models));

  expectTypeOf<Layer.Services<typeof handlers>>().toEqualTypeOf<
    SmallClient | LargeClient | DecisionModel.DecisionModel | AutoModel.SelectionStore
  >();
  expectTypeOf<Layer.Error<typeof handlers>>().toEqualTypeOf<never>();
});

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
