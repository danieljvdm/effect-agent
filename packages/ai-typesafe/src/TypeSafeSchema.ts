/**
 * Request and response schemas for TypeSafe's System One HTTP API.
 *
 * @since 0.1.0
 */
import * as Schema from "effect/Schema";

/**
 * Text or JSON objects and arrays used as state and question instructions.
 *
 * @category schemas
 * @since 0.1.0
 */
export const Content = Schema.Union([Schema.String, Schema.JsonObject, Schema.Array(Schema.Json)]);

/** @category models
 * @since 0.1.0
 */
export type Content = typeof Content.Type;

/**
 * A choice between named options. A null rubric uses the option name alone.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ChoiceQuestion = Schema.Struct({
  type: Schema.Literal("choice"),
  instructions: Content,
  criteria: Schema.Record(Schema.String, Schema.NullOr(Schema.String)).check(
    Schema.isMinProperties(1),
  ),
});

/** @category models
 * @since 0.1.0
 */
export type ChoiceQuestion = typeof ChoiceQuestion.Type;

/**
 * A rating along at least two ordered, zero-indexed level descriptions.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScoreQuestion = Schema.Struct({
  type: Schema.Literal("score"),
  instructions: Content,
  criteria: Schema.Array(Schema.String).check(Schema.isMinLength(2)),
});

/** @category models
 * @since 0.1.0
 */
export type ScoreQuestion = typeof ScoreQuestion.Type;

/**
 * A yes/no judgment, with optional descriptions of either outcome.
 *
 * @category schemas
 * @since 0.1.0
 */
export const NoulQuestion = Schema.Struct({
  type: Schema.Literal("noul"),
  instructions: Content,
  criteria: Schema.optionalKey(
    Schema.Struct({
      true: Schema.optionalKey(Schema.String),
      false: Schema.optionalKey(Schema.String),
    }),
  ),
});

/** @category models
 * @since 0.1.0
 */
export type NoulQuestion = typeof NoulQuestion.Type;

/** @category schemas
 * @since 0.1.0
 */
export const Question = Schema.Union([ChoiceQuestion, ScoreQuestion, NoulQuestion]);

/** @category models
 * @since 0.1.0
 */
export type Question = typeof Question.Type;

/** @category schemas
 * @since 0.1.0
 */
export const Questions = Schema.Record(Schema.String, Question);

/** @category models
 * @since 0.1.0
 */
export type Questions = typeof Questions.Type;

/** @category schemas
 * @since 0.1.0
 */
export const EvaluateRequest = Schema.Struct({
  model: Schema.String,
  state: Content,
  questions: Questions,
});

/**
 * Evaluation input. Keep question literals with `satisfies Questions` when
 * storing questions separately from the call to `evaluate`.
 *
 * @category models
 * @since 0.1.0
 */
export type EvaluateRequest<Q extends Questions = Questions> = Omit<
  typeof EvaluateRequest.Type,
  "questions"
> & { readonly questions: Q };

/**
 * A finite probability or confidence in the inclusive range [0, 1].
 *
 * @category schemas
 * @since 0.1.0
 */
export const Probability = Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 }));

/**
 * A selected option and its full distribution. Confidence summarizes the
 * distribution; it does not guarantee correctness.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ChoiceAnswer = Schema.Struct({
  type: Schema.Literal("choice"),
  choice: Schema.String,
  probabilities: Schema.Record(Schema.String, Probability),
  confidence: Probability,
});

/**
 * Known option unions infer literal choices and required probability keys.
 * Open string or template-pattern option types allow absent dictionary entries.
 *
 * @category models
 * @since 0.1.0
 */
export type ChoiceAnswer<Choice extends string = string> = Omit<
  typeof ChoiceAnswer.Type,
  "choice" | "probabilities"
> & {
  readonly choice: Choice;
  readonly probabilities: {
    readonly [K in Choice]: {} extends Pick<Record<Choice, unknown>, K>
      ? number | undefined
      : number;
  };
};

/**
 * A fractional, probability-weighted level, with the supplied rubric as legend.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ScoreAnswer = Schema.Struct({
  type: Schema.Literal("score"),
  score: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  legend: Schema.Record(Schema.String, Schema.String),
  probabilities: Schema.Record(Schema.String, Probability),
  confidence: Probability,
});

/**
 * Score levels are keyed at runtime; an arbitrary legend or probability lookup
 * may be absent.
 *
 * @category models
 * @since 0.1.0
 */
export type ScoreAnswer = Omit<typeof ScoreAnswer.Type, "legend" | "probabilities"> & {
  readonly legend: { readonly [level: string]: string | undefined };
  readonly probabilities: { readonly [level: string]: number | undefined };
};

/**
 * The probability of yes. Noul has no separate confidence field.
 *
 * @category schemas
 * @since 0.1.0
 */
export const NoulAnswer = Schema.Struct({ type: Schema.Literal("noul"), noul: Probability });

/** @category models
 * @since 0.1.0
 */
export type NoulAnswer = typeof NoulAnswer.Type;

/** @category schemas
 * @since 0.1.0
 */
export const Answer = Schema.Union([ChoiceAnswer, ScoreAnswer, NoulAnswer]);

/** @category models
 * @since 0.1.0
 */
export type Answer = ChoiceAnswer | ScoreAnswer | NoulAnswer;

/** @category schemas
 * @since 0.1.0
 */
export const Usage = Schema.Struct({ input_tokens: Schema.Natural, output_tokens: Schema.Natural });

/** @category models
 * @since 0.1.0
 */
export type Usage = typeof Usage.Type;

/**
 * The wire response shape. `TypeSafeClient.evaluate` additionally validates
 * answer IDs, question kinds, criteria, distributions, and score correlations.
 *
 * @category schemas
 * @since 0.1.0
 */
export const EvaluateResponse = Schema.Struct({
  model: Schema.String,
  answers: Schema.Record(Schema.String, Answer),
  usage: Usage,
});

type ChoiceAnswerFor<Criteria> = Criteria extends unknown
  ? Omit<typeof ChoiceAnswer.Type, "choice" | "probabilities"> & {
      readonly choice: `${Extract<keyof Criteria, string | number>}`;
      readonly probabilities: {
        readonly [
          K in keyof Criteria as K extends string | number ? `${K}` : never
        ]: {} extends Pick<Criteria, K> ? number | undefined : number;
      };
    }
  : never;

/**
 * Infer a question's answer, preserving optional choice criteria in its probabilities.
 *
 * @category models
 * @since 0.1.0
 */
export type AnswerFor<Q extends Question> = Q extends ChoiceQuestion
  ? ChoiceAnswerFor<Q["criteria"]>
  : Q extends ScoreQuestion
    ? ScoreAnswer
    : NoulAnswer;

/**
 * One answer for each required question. Optional properties and open string,
 * numeric, or template-pattern indexes require checking an entry for absence.
 *
 * @category models
 * @since 0.1.0
 */
export type Answers<Q extends Questions> = {
  readonly [K in keyof Q as K extends string | number ? `${K}` : never]: {} extends Pick<Q, K>
    ? AnswerFor<NonNullable<Q[K]>> | undefined
    : AnswerFor<Q[K]>;
};

/** @category models
 * @since 0.1.0
 */
export type EvaluateResponse<Q extends Questions = Questions> = Omit<
  typeof EvaluateResponse.Type,
  "answers"
> & { readonly answers: Answers<Q> };
