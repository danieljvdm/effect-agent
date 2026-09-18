import { AutoModel, DecisionModel, type DecisionSchema } from "@effect-agent/ai-decision";
import { expect, it } from "@effect/vitest";
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { AiError, LanguageModel, Model } from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

const nativeModel = (name: string) =>
  Model.make(
    "fixture",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([{ type: "text", text: name }]),
        streamText: () => Stream.empty,
      }),
    ),
  );

const small = nativeModel("small");
const large = nativeModel("large");

const models = {
  routine: { model: small, description: "Low cost; routine tasks" },
  difficult: { model: large, description: "Higher cost; difficult tasks" },
};

const answer = (choice = "routine") => ({
  provider: "fixture",
  model: "selector-v1",
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
  usage: { inputTokens: 20, outputTokens: 2 },
});

const decisionLayer = (
  evaluate: (request: DecisionSchema.EvaluateRequest) => Effect.Effect<unknown, AiError.AiError>,
) => Layer.effect(DecisionModel.DecisionModel, DecisionModel.make({ evaluate }));

it.effect(
  "selects independently for child threads and restores a thread without another decision",
  () =>
    Effect.gen(function* () {
      const requests: Array<DecisionSchema.EvaluateRequest> = [];

      const auto = AutoModel.make({
        models,
        version: "profiles-v1",
        instructions: "Pick for the whole thread",
      });

      const live = decisionLayer((request) =>
        Effect.sync(() => {
          requests.push(request);

          return answer(request.state === "hard child task" ? "difficult" : "routine");
        }),
      );

      const first = yield* auto
        .select({ threadId: "parent", state: "routine parent task" })
        .pipe(Effect.provide(live));

      const child = yield* auto
        .select({ threadId: "child", state: "hard child task" })
        .pipe(Effect.provide(live));

      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(AutoModel.SelectionRecord))(
        first.record,
      );

      const stored = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(encoded);

      // Rebuilding the catalog simulates a new host; restoration requires no DecisionModel.
      const restored = yield* AutoModel.make({ models, version: "profiles-v1" }).restore(
        "parent",
        stored,
      );

      expect(first.model).toBe(small);
      expect(restored.model).toBe(small);
      expect(child.model).toBe(large);
      expect(requests.map((request) => request.state)).toEqual([
        "routine parent task",
        "hard child task",
      ]);
      expect(requests[0]?.questions).toEqual({
        model: {
          type: "choice",
          instructions: "Pick for the whole thread",
          criteria: {
            routine: "Low cost; routine tasks",
            difficult: "Higher cost; difficult tasks",
          },
        },
      });
      expect(restored.record.decision.usage).toEqual({ inputTokens: 20, outputTokens: 2 });

      const identity = yield* Effect.gen(function* () {
        const response = yield* LanguageModel.generateText({ prompt: "A later turn" });

        return [yield* Model.ProviderName, yield* Model.ModelName, response.text];
      }).pipe(Effect.provide(restored.model));

      expect(identity).toEqual(["fixture", "small", "small"]);
    }),
);

it.effect("snapshots profile bindings and descriptions when constructing the catalog", () =>
  Effect.gen(function* () {
    const mutable = { routine: { ...models.routine }, difficult: { ...models.difficult } };
    const auto = AutoModel.make({ models: mutable, version: "v1" });

    mutable.routine.model = large;
    mutable.routine.description = "Changed after construction";

    const selected = yield* auto.select({ threadId: "thread", state: "task" }).pipe(
      Effect.provide(
        decisionLayer((request) => {
          expect(request.questions.model).toMatchObject({
            criteria: { routine: "Low cost; routine tasks" },
          });

          return Effect.succeed(answer());
        }),
      ),
    );

    expect(selected.model).toBe(small);
  }),
);

it.effect(
  "rejects invalid local configuration before selector I/O and rejects unknown choices",
  () =>
    Effect.gen(function* () {
      let calls = 0;

      const live = decisionLayer(() =>
        Effect.sync(() => {
          calls++;

          return answer("unapproved");
        }),
      );

      const invalidCatalogs: ReadonlyArray<readonly [AutoModel.AutoModel<unknown>, string]> = [
        [AutoModel.make({ models: {}, version: "v1" }), "thread"],
        [AutoModel.make({ models, version: "" }), "thread"],
        [AutoModel.make({ models, version: "v1" }), ""],
        [AutoModel.make({ models: { "": models.routine }, version: "v1" }), "thread"],
      ];

      for (const [catalog, threadId] of invalidCatalogs) {
        const error = yield* catalog
          .select({ threadId, state: "task" })
          .pipe(Effect.provide(live), Effect.flip);

        expect(error.reason._tag).toBe("InvalidRequestError");
      }
      expect(calls).toBe(0);

      const error = yield* AutoModel.make({ models, version: "v1" })
        .select({ threadId: "thread", state: "task" })
        .pipe(Effect.provide(live), Effect.flip);

      expect(error.reason._tag).toBe("InvalidOutputError");
      expect(calls).toBe(1);
    }),
);

it.effect("fails closed on incompatible or corrupt thread selections without reselecting", () =>
  Effect.gen(function* () {
    const auto = AutoModel.make({ models, version: "v1" });

    const { record } = yield* auto
      .select({ threadId: "thread", state: "task" })
      .pipe(Effect.provide(decisionLayer(() => Effect.succeed(answer()))));

    for (const invalid of [
      { ...record, version: 2 },
      { ...record, threadId: "another-thread" },
      { ...record, catalogVersion: "v2" },
      { ...record, profileId: "toString" },
      { ...record, profileId: "difficult" },
      { ...record, decision: null },
    ]) {
      const error = yield* auto.restore("thread", invalid).pipe(Effect.flip);

      expect(error.reason._tag).toBe("InvalidRequestError");
    }
    const changed = AutoModel.make({ models: { difficult: models.difficult }, version: "v1" });
    const missing = yield* changed.restore("thread", record).pipe(Effect.flip);

    expect(missing.reason._tag).toBe("InvalidRequestError");
  }),
);

it.effect("preserves selector failures and defects without returning a fallback", () =>
  Effect.gen(function* () {
    const error = new AiError.AiError({
      module: "fixture",
      method: "evaluate",
      reason: new AiError.InvalidOutputError({ description: "bad output" }),
    });

    const defect = new Error("provider defect");
    const auto = AutoModel.make({ models, version: "v1" });

    const failed = yield* auto
      .select({ threadId: "thread", state: "task" })
      .pipe(Effect.provide(decisionLayer(() => Effect.fail(error))), Effect.flip);

    expect(failed).toBe(error);

    const died = yield* auto
      .select({ threadId: "thread", state: "task" })
      .pipe(Effect.provide(decisionLayer(() => Effect.die(defect))), Effect.exit);

    expect(Exit.isFailure(died) && Cause.hasDies(died.cause)).toBe(true);
  }),
);

it.effect("interrupts and times out selection with provider resources released", () =>
  Effect.gen(function* () {
    for (const timeout of [false, true]) {
      const started = yield* Deferred.make<void>();
      let released = false;

      const live = Layer.effect(
        DecisionModel.DecisionModel,
        DecisionModel.make({
          evaluate: () =>
            Effect.gen(function* () {
              yield* Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  released = true;
                }),
              );
              yield* Deferred.succeed(started, undefined);

              return yield* Effect.never;
            }),
        }),
      );

      const selection = AutoModel.make({ models, version: "v1" })
        .select({ threadId: "thread", state: "task" })
        .pipe(Effect.provide(live));

      const fiber = yield* (timeout ? selection.pipe(Effect.timeout("1 second")) : selection).pipe(
        Effect.forkChild,
      );

      yield* Deferred.await(started);
      if (timeout) yield* TestClock.adjust("1 second");
      else yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);

      expect(Exit.isFailure(exit)).toBe(true);
      expect(released).toBe(true);
    }
  }),
);

it("preserves client requirements and selector errors when composing different providers", () => {
  class ClientA extends Context.Service<ClientA, {}>()("ClientA") {}
  class ClientB extends Context.Service<ClientB, {}>()("ClientB") {}

  const a = Model.make(
    "a",
    "small",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.as(
        ClientA,
        LanguageModel.LanguageModel.of({
          [LanguageModel.TypeId]: LanguageModel.TypeId,
          generateText: () => Effect.die("unused"),
          generateObject: () => Effect.die("unused"),
          streamText: () => Stream.die("unused"),
        }),
      ),
    ),
  );

  const b = Model.make(
    "b",
    "large",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.as(
        ClientB,
        LanguageModel.LanguageModel.of({
          [LanguageModel.TypeId]: LanguageModel.TypeId,
          generateText: () => Effect.die("unused"),
          generateObject: () => Effect.die("unused"),
          streamText: () => Stream.die("unused"),
        }),
      ),
    ),
  );

  const auto = AutoModel.make({
    models: { a: { model: a, description: "A" }, b: { model: b, description: "B" } },
    version: "v1",
  });

  const selected = auto.select({ threadId: "thread", state: "task" });
  const restored = auto.restore("thread", {});

  const generate = Effect.flatMap(selected, ({ model }) =>
    LanguageModel.generateText({ prompt: "task" }).pipe(Effect.provide(model)),
  );

  type Selected = Effect.Success<typeof selected>["model"];
  expectTypeOf<Layer.Services<Selected>>().toEqualTypeOf<ClientA | ClientB>();
  expectTypeOf<Effect.Services<typeof selected>>().toEqualTypeOf<DecisionModel.DecisionModel>();
  expectTypeOf<Effect.Error<typeof selected>>().toEqualTypeOf<AiError.AiError>();
  expectTypeOf<Effect.Services<typeof restored>>().toEqualTypeOf<never>();
  expectTypeOf<Effect.Error<typeof restored>>().toEqualTypeOf<AiError.AiError>();
  expectTypeOf<Effect.Services<typeof generate>>().toEqualTypeOf<
    DecisionModel.DecisionModel | ClientA | ClientB
  >();
  expectTypeOf<Effect.Error<typeof generate>>().toEqualTypeOf<AiError.AiError>();
  AutoModel.make({
    // @ts-expect-error A candidate must provide the native model and its identity services.
    models: { invalid: { model: Layer.empty, description: "invalid" } },
    version: "v1",
  });
});
