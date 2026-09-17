import * as Schema from "effect/Schema";
import type * as SchemaAST from "effect/SchemaAST";

import * as DecisionSchema from "../DecisionSchema.ts";

// Permit floating-point serialization error without changing provider values.
const tolerance = 1e-6;

const probabilitySum = Schema.makeFilter(
  (probabilities: Readonly<Record<string, number>>) =>
    Math.abs(Object.values(probabilities).reduce((sum, value) => sum + value, 0) - 1) <= tolerance,
  { expected: "probabilities summing to 1 (within 1e-6)" },
);

const distribution = (
  keys: ReadonlyArray<string>,
  sumCheck: SchemaAST.Check<Readonly<Record<string, number>>> = probabilitySum,
) => Schema.Record(Schema.Literals(keys), DecisionSchema.Probability).check(sumCheck);

const answerFor = (
  question: DecisionSchema.Question,
  choiceProbabilitySum: SchemaAST.Check<Readonly<Record<string, number>>> | undefined,
) => {
  switch (question.type) {
    case "choice": {
      const keys = Object.keys(question.criteria);

      return Schema.Struct({
        ...DecisionSchema.ChoiceAnswer.fields,
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
  choiceProbabilitySum?: SchemaAST.Check<Readonly<Record<string, number>>>,
): Schema.Codec<DecisionSchema.EvaluateResponse<Q>>;

export function responseFor(
  questions: DecisionSchema.Questions,
  choiceProbabilitySum?: SchemaAST.Check<Readonly<Record<string, number>>>,
): Schema.Top {
  return Schema.Struct({
    ...DecisionSchema.EvaluateResponse.fields,
    answers: Schema.Struct(
      Object.fromEntries(
        Object.entries(questions).map(([id, question]) => [
          id,
          answerFor(question, choiceProbabilitySum),
        ]),
      ),
    ),
  });
}
