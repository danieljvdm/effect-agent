import * as Schema from "effect/Schema";

import * as DecisionSchema from "../DecisionSchema.ts";

// Permit floating-point serialization error without changing provider values.
const tolerance = 1e-6;

const distribution = (keys: ReadonlyArray<string>) =>
  Schema.Record(Schema.Literals(keys), DecisionSchema.Probability).check(
    Schema.makeFilter(
      (probabilities) =>
        Math.abs(Object.values(probabilities).reduce((sum, value) => sum + value, 0) - 1) <=
        tolerance,
      { expected: "probabilities summing to 1 (within 1e-6)" },
    ),
  );

const answerFor = (question: DecisionSchema.Question) => {
  switch (question.type) {
    case "choice": {
      const keys = Object.keys(question.criteria);

      return Schema.Struct({
        ...DecisionSchema.ChoiceAnswer.fields,
        choice: Schema.Literals(keys),
        probabilities: distribution(keys),
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
        ...DecisionSchema.ScoreAnswer.fields,
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
    case "probability":
      return DecisionSchema.ProbabilityAnswer;
  }
};

// The overload describes the dependent type enforced by the literal keys and
// per-question schemas below. No JSON value is asserted to have that type.
export function responseFor<const Q extends DecisionSchema.Questions>(
  questions: Q,
): Schema.Codec<DecisionSchema.EvaluateResponse<Q>>;

export function responseFor(questions: DecisionSchema.Questions): Schema.Top {
  return Schema.Struct({
    ...DecisionSchema.EvaluateResponse.fields,
    answers: Schema.Struct(
      Object.fromEntries(
        Object.entries(questions).map(([id, question]) => [id, answerFor(question)]),
      ),
    ),
  });
}
