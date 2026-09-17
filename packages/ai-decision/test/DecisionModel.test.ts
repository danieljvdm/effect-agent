import type { DecisionSchema } from "@effect-agent/ai-decision";
import { DecisionModel, DecisionQuery, DecisionSet } from "@effect-agent/ai-decision";
import { expect, it } from "@effect/vitest";
import { Cause, Context, Deferred, Effect, Exit, Fiber, Option, Result, Schema } from "effect";
import { TestClock } from "effect/testing";
import { AiError } from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

const request = {
  state: { message: "Production is blocked by an invoice" },
  questions: {
    team: { type: "choice", instructions: "Route", criteria: { billing: null, engineering: null } },
    severity: { type: "score", instructions: "Severity", criteria: ["Low", "High"] },
    urgent: { type: "probability", instructions: "Is work blocked?" },
  },
} satisfies DecisionSchema.EvaluateRequest;

const assessment = DecisionSet.make({
  input: Schema.Struct({ message: Schema.NonEmptyString }),
  questions: {
    team: DecisionQuery.choice({
      instructions: "Route",
      options: { billing: null, engineering: null },
    }),
    severity: DecisionQuery.score({ instructions: "Severity", levels: ["Low", "High"] }),
    urgent: DecisionQuery.probability({ instructions: "Is work blocked?" }),
  },
});

const response = {
  provider: "fixture",
  model: "decisions-v1",
  usage: { inputTokens: 20, outputTokens: null },
  answers: {
    team: {
      type: "choice",
      choice: "billing",
      probabilities: { billing: 0.8, engineering: 0.2 },
    },
    severity: {
      type: "score",
      score: 0.9,
      legend: { "0": "Low", "1": "High" },
      probabilities: { "0": 0.1, "1": 0.9 },
    },
    urgent: { type: "probability", probability: 0.95 },
  },
};

it.effect("evaluates mixed questions once and exposes evidence for an application transition", () =>
  Effect.gen(function* () {
    let calls = 0;

    const model = yield* DecisionModel.make({
      evaluate: (sent) =>
        Effect.sync(() => {
          calls++;
          expect(sent).toEqual(request);

          return response;
        }),
    });

    const result = yield* model.evaluate(assessment, request.state);

    expect(result).toEqual(response);

    const next =
      result.answers.team.choice === "billing" && result.answers.urgent.probability >= 0.9
        ? "billing-priority"
        : "review";

    expect(next).toBe("billing-priority");
    expect(calls).toBe(1);
    expectTypeOf(result.answers.team.choice).toEqualTypeOf<"billing" | "engineering">();
    expectTypeOf(result.answers.urgent).toEqualTypeOf<{
      readonly type: "probability";
      readonly probability: number;
    }>();
  }),
);

const invalidAnswers = [
  { ...response.answers, extra: { type: "probability", probability: 0.5 } },
  { ...response.answers, urgent: { type: "probability", probability: 1.1 } },
  { ...response.answers, urgent: { type: "noul", noul: 0.5 } },
  { ...response.answers, team: { ...response.answers.team, choice: "engineering" } },
  { ...response.answers, team: { ...response.answers.team, probabilities: { billing: 0.8 } } },
  {
    ...response.answers,
    team: { ...response.answers.team, probabilities: { billing: 0.8, engineering: 0.3 } },
  },
  { ...response.answers, severity: { ...response.answers.severity, score: 0.1 } },
  {
    ...response.answers,
    severity: { ...response.answers.severity, legend: { "0": "Low", "1": "Other" } },
  },
];

it.effect("rejects mismatched answer identities, types, distributions and score rubrics", () =>
  Effect.gen(function* () {
    for (const answers of invalidAnswers) {
      const model = yield* DecisionModel.make({
        evaluate: () => Effect.succeed({ ...response, answers }),
      });

      const error = yield* model.evaluate(request).pipe(Effect.flip);

      expect(error.reason._tag).toBe("InvalidOutputError");
    }
    let called = false;

    const model = yield* DecisionModel.make({
      evaluate: () =>
        Effect.sync(() => {
          called = true;

          return response;
        }),
    });

    const error = yield* model
      .evaluate({
        state: "bad",
        questions: { empty: { type: "choice", instructions: "Pick", criteria: {} } },
      })
      .pipe(Effect.flip);

    expect(error.reason._tag).toBe("InvalidRequestError");
    expect(called).toBe(false);
  }),
);

it.effect("closes provider scopes and preserves success, expected failure and defect", () =>
  Effect.gen(function* () {
    const failure = new AiError.AiError({
      module: "fixture",
      method: "evaluate",
      reason: new AiError.InvalidRequestError({ description: "no" }),
    });

    const defect = new Error("defect");

    for (const mode of ["success", "failure", "defect"] as const) {
      let closed = false;

      const model = yield* DecisionModel.make({
        evaluate: () =>
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                closed = true;
              }),
            );
            if (mode === "failure") return yield* failure;
            if (mode === "defect") return yield* Effect.die(defect);

            return response;
          }),
      });

      const exit = yield* model.evaluate(assessment, request.state).pipe(Effect.exit);

      expect(closed).toBe(true);
      if (mode === "success") expect(Exit.isSuccess(exit)).toBe(true);
      else expect(Exit.isFailure(exit)).toBe(true);
      if (mode === "failure" && Exit.isFailure(exit))
        expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toBe(failure);
      if (mode === "defect" && Exit.isFailure(exit))
        expect(Result.getOrThrow(Cause.findDefect(exit.cause))).toBe(defect);
    }
  }),
);

it.effect("propagates timeout and interruption to the provider and closes its resources", () =>
  Effect.gen(function* () {
    for (const mode of ["timeout", "interrupt"] as const) {
      const started = yield* Deferred.make<void>();
      let closed = false;

      const model = yield* DecisionModel.make({
        evaluate: () =>
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                closed = true;
              }),
            );
            yield* Deferred.succeed(started, undefined);

            return yield* Effect.never;
          }),
      });

      const evaluation = model.evaluate(assessment, request.state);

      const fiber = yield* (
        mode === "timeout" ? evaluation.pipe(Effect.timeout("1 second")) : evaluation
      ).pipe(Effect.forkChild);

      yield* Deferred.await(started);
      if (mode === "timeout") yield* TestClock.adjust("1 second");
      else yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);

      expect(closed).toBe(true);
      expect(Exit.isFailure(exit)).toBe(true);
      if (mode === "timeout" && Exit.isFailure(exit))
        expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))?._tag).toBe("TimeoutError");
      if (mode === "interrupt" && Exit.isFailure(exit))
        expect(Cause.hasInterrupts(exit.cause)).toBe(true);
    }
  }),
);

class Backend extends Context.Service<
  Backend,
  { readonly request: Effect.Effect<unknown, AiError.AiError> }
>()("decision-test/Backend") {}
it("retains provider acquisition requirements and safe dynamic answer types", () => {
  const construction = DecisionModel.make({
    evaluate: () => Effect.flatMap(Backend, (backend) => backend.request),
  });

  expectTypeOf<Effect.Services<typeof construction>>().toEqualTypeOf<Backend>();

  const evaluation = Effect.flatMap(DecisionModel.DecisionModel, (model) =>
    model.evaluate(request),
  );

  expectTypeOf<Effect.Services<typeof evaluation>>().toEqualTypeOf<DecisionModel.DecisionModel>();
  expectTypeOf<Effect.Error<typeof evaluation>>().toEqualTypeOf<AiError.AiError>();
  expectTypeOf<DecisionSchema.EvaluateResponse["answers"][string]>().toEqualTypeOf<
    DecisionSchema.Answer | undefined
  >();
});
