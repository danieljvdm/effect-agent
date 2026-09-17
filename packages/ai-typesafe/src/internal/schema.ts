import * as Schema from "effect/Schema";

import * as TypeSafeSchema from "../TypeSafeSchema.ts";

// Permit floating-point serialization error without changing provider values.
const tolerance = 1e-6;

const probabilitySum = Schema.makeFilter(
  (probabilities: Readonly<Record<string, number>>) =>
    Math.abs(Object.values(probabilities).reduce((sum, value) => sum + value, 0) - 1) <= tolerance,
  { expected: "probabilities summing to 1 (within 1e-6)" },
);

// Jev Choice responses have been observed with two-decimal probabilities totaling 0.99.
// Limit compatibility to one percentage point, even for very large option catalogues.
// Score sums/weighting retain their strict checks; no Score rounding contract is assumed.
export const choiceProbabilitySum = Schema.makeFilter(
  (probabilities: Readonly<Record<string, number>>) => {
    const values = Object.values(probabilities);
    const error = Math.abs(values.reduce((sum, value) => sum + value, 0) - 1);

    return (
      error <= tolerance ||
      (error <= Math.min(0.01, values.length * 0.005) + tolerance &&
        values.every((value) => Math.abs(value * 100 - Math.round(value * 100)) < 1e-8))
    );
  },
  { expected: "probabilities summing to 1 within bounded two-decimal Choice rounding" },
);

const distribution = (keys: ReadonlyArray<string>, sumCheck = probabilitySum) =>
  Schema.Record(Schema.Literals(keys), TypeSafeSchema.Probability).check(sumCheck);

const answerFor = (question: TypeSafeSchema.Question) => {
  switch (question.type) {
    case "choice": {
      const keys = Object.keys(question.criteria);

      return Schema.Struct({
        ...TypeSafeSchema.ChoiceAnswer.fields,
        choice: Schema.Literals(keys),
        probabilities: distribution(keys, choiceProbabilitySum),
      }).check(
        Schema.makeFilter(
          ({ choice, probabilities }) =>
            Object.values(probabilities).every((value) => value <= probabilities[choice]),
          { expected: "a highest-probability choice" },
        ),
      );
    }
    case "score": {
      const maxLevel = question.criteria.length - 1;

      const levels = question.criteria.map(
        (description, index) => [String(index), description] as const,
      );

      return Schema.Struct({
        ...TypeSafeSchema.ScoreAnswer.fields,
        score: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: maxLevel })),
        legend: Schema.Struct(
          Object.fromEntries(
            levels.map(([key, description]) => [key, Schema.Literal(description)]),
          ),
        ),
        probabilities: distribution(levels.map(([key]) => key)),
      }).check(
        Schema.makeFilter(
          ({ score, probabilities }) =>
            Math.abs(
              score -
                Object.entries(probabilities).reduce(
                  (sum, [level, probability]) => sum + Number(level) * probability,
                  0,
                ),
            ) <=
            tolerance * Math.max(1, maxLevel),
          { expected: "the probability-weighted score (within 1e-6 per level)" },
        ),
      );
    }
    case "noul":
      return TypeSafeSchema.NoulAnswer;
  }
};

// The overload describes the dependent type enforced by the literal keys and
// per-question schemas below. No JSON value is asserted to have that type.
export function responseFor<const Q extends TypeSafeSchema.Questions>(
  questions: Q,
): Schema.Codec<TypeSafeSchema.EvaluateResponse<Q>>;

export function responseFor(questions: TypeSafeSchema.Questions): Schema.Top {
  return Schema.Struct({
    ...TypeSafeSchema.EvaluateResponse.fields,
    answers: Schema.Struct(
      Object.fromEntries(
        Object.entries(questions).map(([id, question]) => [id, answerFor(question)]),
      ),
    ),
  });
}
