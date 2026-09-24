import { AutoModel } from "@effect-agent/ai-decision";
import { expect, expectTypeOf, it } from "@effect/vitest";
import { Context, Deferred, Effect, Layer, Schema, Stream } from "effect";
import type { AiError } from "effect/unstable/ai";
import { DecisionModel, LanguageModel, Model } from "effect/unstable/ai";

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

const answer = (choice = "routine"): DecisionModel.ProviderResponse => ({
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
  usage: { inputTokens: 20, outputTokens: 2 },
});

const decisionLayer = (
  evaluate: (
    request: DecisionModel.ProviderOptions,
  ) => Effect.Effect<DecisionModel.ProviderResponse, AiError.AiError>,
) =>
  Layer.effect(
    DecisionModel.DecisionModel,
    DecisionModel.make({ decide: (request) => evaluate(request).pipe(Effect.scoped) }),
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

  const generate = Effect.flatMap(selected, ({ model }) =>
    LanguageModel.generateText({ prompt: "task" }).pipe(Effect.provide(model)),
  );

  expectTypeOf<Layer.Services<typeof auto>>().toEqualTypeOf<
    ClientA | ClientB | DecisionModel.DecisionModel | AutoModel.SelectionStore
  >();
  type Selected = Effect.Success<typeof selected>["model"];
  expectTypeOf<Layer.Services<Selected>>().toEqualTypeOf<ClientA | ClientB>();
  expectTypeOf<Effect.Services<typeof generate>>().toEqualTypeOf<
    DecisionModel.DecisionModel | ClientA | ClientB
  >();
  expectTypeOf<Effect.Error<typeof generate>>().toEqualTypeOf<AiError.AiError>();
});

it.effect(
  "shares one selection for overlapping resolutions while independent threads proceed",
  () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const calls: Array<Schema.Json> = [];
      const auto = AutoModel.make({ models, version: "v1" });

      const live = decisionLayer((request) =>
        Effect.gen(function* () {
          calls.push(request.state);
          if (request.state === "blocked") yield* Deferred.await(release);
          else yield* Deferred.succeed(release, undefined);

          return answer(request.state === "independent" ? "difficult" : "routine");
        }),
      );

      const resolved = yield* Effect.all(
        [
          auto.resolve({ threadId: "same", state: "blocked" }),
          auto.resolve({ threadId: "same", state: "blocked" }),
          auto.resolve({ threadId: "other", state: "independent" }),
        ],
        { concurrency: 3 },
      ).pipe(Effect.provide(Layer.merge(live, AutoModel.layerMemory())));

      expect(calls).toEqual(["blocked", "independent"]);

      const identities = yield* Effect.forEach(resolved, (model) =>
        Model.ModelName.pipe(Effect.provide(model)),
      );

      expect(identities).toEqual(["small", "small", "large"]);
    }),
);

it.effect(
  "restores externally retained records after rebuilding the catalog and rejects incompatible stored choices without reselecting",
  () =>
    Effect.gen(function* () {
      const auto = AutoModel.make({ models, version: "v1" });

      const first = yield* auto
        .select({ threadId: "retained", state: "task" })
        .pipe(Effect.provide(decisionLayer(() => Effect.succeed(answer()))));

      const json = yield* Schema.encodeEffect(Schema.fromJsonString(AutoModel.SelectionRecord))(
        first.record,
      );

      const stored = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(json);
      let calls = 0;

      const live = Layer.merge(
        Layer.succeed(AutoModel.SelectionStore, { getOrCreate: () => Effect.succeed(stored) }),
        decisionLayer(() =>
          Effect.sync(() => {
            calls++;

            return answer("difficult");
          }),
        ),
      );

      const restored = yield* AutoModel.make({ models, version: "v1" })
        .resolve({ threadId: "retained", state: "follow-up" })
        .pipe(Effect.provide(live));

      expect(yield* Model.ModelName.pipe(Effect.provide(restored))).toBe("small");

      const error = yield* AutoModel.make({ models, version: "v2" })
        .resolve({ threadId: "retained", state: "follow-up" })
        .pipe(Effect.provide(live), Effect.flip);

      expect(error.reason._tag).toBe("InvalidRequestError");
      expect(calls).toBe(0);
    }),
);
