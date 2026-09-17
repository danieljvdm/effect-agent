import type { DecisionSchema } from "@effect-agent/ai-decision";
import { DecisionModel, DecisionQuery, DecisionSet } from "@effect-agent/ai-decision";
import { expect, it } from "@effect/vitest";
import { Context, Effect, Schema, SchemaGetter } from "effect";
import type { AiError } from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

const questions = { relevant: DecisionQuery.probability({ instructions: "Is this relevant?" }) };

const response = {
  provider: "fixture",
  model: "decision-v1",
  usage: { inputTokens: null, outputTokens: null },
  answers: { relevant: { type: "probability", probability: 0.8 } },
};

class Encoder extends Context.Service<Encoder, string>()("decision-set-test/Encoder") {}
class Decoder extends Context.Service<Decoder, string>()("decision-set-test/Decoder") {}

const message = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformEffect((value) => Effect.as(Decoder, value)),
    encode: SchemaGetter.transformEffect((value) =>
      Effect.map(Encoder, (suffix) => value + suffix),
    ),
  }),
);

const assessment = DecisionSet.make({
  input: Schema.Struct({ message, count: Schema.FiniteFromString }),
  questions,
});

it.effect("encodes only declared state and resolves input services for each evaluation", () =>
  Effect.gen(function* () {
    const sent: Array<DecisionSchema.Content> = [];

    const model = yield* DecisionModel.make({
      evaluate: (request) =>
        Effect.sync(() => {
          sent.push(request.state);

          return response;
        }),
    });

    const input = { message: "request", count: 3, internalNote: "not model-visible" };

    expect(sent).toEqual([]);
    yield* model.evaluate(assessment, input).pipe(Effect.provideService(Encoder, "-first"));
    yield* model.evaluate(assessment, input).pipe(Effect.provideService(Encoder, "-second"));

    expect(sent).toEqual([
      { message: "request-first", count: "3" },
      { message: "request-second", count: "3" },
    ]);
    expect(input.count).toBe(3);
  }),
);

it.effect(
  "rejects invalid domain input, encoded state, and query rubrics before provider I/O",
  () =>
    Effect.gen(function* () {
      let calls = 0;

      const model = yield* DecisionModel.make({
        evaluate: () =>
          Effect.sync(() => {
            calls++;

            return response;
          }),
      });

      const invalid: Array<Effect.Effect<DecisionSchema.EvaluateResponse, AiError.AiError>> = [
        model.evaluate(DecisionSet.make({ input: Schema.NonEmptyString, questions }), ""),
        model.evaluate(DecisionSet.make({ input: Schema.Finite, questions }), 3),
        model.evaluate(
          DecisionSet.make({ input: Schema.Struct({ value: Schema.Number }), questions }),
          { value: NaN },
        ),
        model.evaluate(
          DecisionSet.make({
            input: Schema.String,
            questions: {
              relevant: DecisionQuery.choice({ instructions: "Choose", options: {} }),
            },
          }),
          "request",
        ),
        model.evaluate(
          DecisionSet.make({
            input: Schema.String,
            questions: {
              relevant: DecisionQuery.score({ instructions: "Rate", levels: ["Only"] }),
            },
          }),
          "request",
        ),
      ];

      for (const evaluation of invalid) {
        const error = yield* Effect.flip(evaluation);

        expect(error.reason._tag).toBe("InvalidRequestError");
      }
      expect(calls).toBe(0);
    }),
);

it.effect(
  "accepts dynamic query records and keeps categorical evidence independent of provider statistics",
  () =>
    Effect.gen(function* () {
      const candidates: Record<string, string | null> = { ["__proto__"]: "Prototype", other: null };

      const dynamic: DecisionSchema.Questions = Object.fromEntries([
        ["route", DecisionQuery.choice({ instructions: "Choose a route", options: candidates })],
      ]);

      const set = DecisionSet.make({ input: Schema.String, questions: dynamic });

      const model = yield* DecisionModel.make({
        evaluate: () =>
          Effect.succeed({
            provider: "another-provider",
            model: "v1",
            usage: { inputTokens: null, outputTokens: null },
            answers: {
              route: {
                type: "choice",
                choice: "__proto__",
                probabilities: { ["__proto__"]: 0.75, other: 0.25 },
              },
            },
            providerMetadata: { "another-provider": { revision: "a" } },
          }),
      });

      const result = yield* model.evaluate(set, "request");

      expect(result.answers.route).toEqual({
        type: "choice",
        choice: "__proto__",
        probabilities: { ["__proto__"]: 0.75, other: 0.25 },
      });
      expect(result.providerMetadata).toEqual({ "another-provider": { revision: "a" } });
      expectTypeOf(result.answers.route).toEqualTypeOf<DecisionSchema.Answer | undefined>();
    }),
);

it("infers typed input, answers, errors, and only the input encoding requirements", () => {
  const program = Effect.gen(function* () {
    const model = yield* DecisionModel.DecisionModel;
    const result = yield* model.evaluate(assessment, { message: "request", count: 3 });

    expectTypeOf(result.answers.relevant).toEqualTypeOf<DecisionSchema.ProbabilityAnswer>();
    // @ts-expect-error Consumers pass the decoded input, not its wire representation.
    yield* model.evaluate(assessment, { message: "request", count: "3" });

    return result;
  });

  expectTypeOf<Effect.Services<typeof program>>().toEqualTypeOf<
    DecisionModel.DecisionModel | Encoder
  >();
  expectTypeOf<Effect.Error<typeof program>>().toEqualTypeOf<AiError.AiError>();
});
