/**
 * Request and response schemas for provider-neutral decision evaluations.
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
export const ProbabilityQuestion = Schema.Struct({
  type: Schema.Literal("probability"),
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
export type ProbabilityQuestion = typeof ProbabilityQuestion.Type;

/** @category schemas
 * @since 0.1.0
 */
export const Question = Schema.Union([ChoiceQuestion, ScoreQuestion, ProbabilityQuestion]);

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
 * A selected option and its full distribution. Provider-specific confidence
 * statistics belong to evaluation metadata, not the shared answer contract.
 * Probabilities retain the provider's reported precision and are not normalized.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ChoiceAnswer = Schema.Struct({
  type: Schema.Literal("choice"),
  choice: Schema.String,
  probabilities: Schema.Record(Schema.String, Probability),
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
 * The probability of yes. A probability answer has no separate confidence field.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ProbabilityAnswer = Schema.Struct({
  type: Schema.Literal("probability"),
  probability: Probability,
});

/** @category models
 * @since 0.1.0
 */
export type ProbabilityAnswer = typeof ProbabilityAnswer.Type;

/** @category schemas
 * @since 0.1.0
 */
export const Answer = Schema.Union([ChoiceAnswer, ScoreAnswer, ProbabilityAnswer]);

/** @category models
 * @since 0.1.0
 */
export type Answer = ChoiceAnswer | ScoreAnswer | ProbabilityAnswer;

/** @category schemas
 * @since 0.1.0
 */
export const Usage = Schema.Struct({
  inputTokens: Schema.NullOr(Schema.Natural),
  outputTokens: Schema.NullOr(Schema.Natural),
});

/** @category models
 * @since 0.1.0
 */
export type Usage = typeof Usage.Type;

/**
 * Provider-namespaced evidence that has no shared interpretation. Consumers
 * decode a namespace with its provider's schema before using its contents.
 *
 * @category schemas
 * @since 0.1.0
 */
export const ProviderMetadata = Schema.Record(Schema.String, Schema.JsonObject);

/** @category models
 * @since 0.1.0
 */
export type ProviderMetadata = typeof ProviderMetadata.Type;

/**
 * The wire response shape. `DecisionModel.evaluate` additionally validates
 * answer IDs, question kinds, criteria, distributions, and score correlations.
 *
 * @category schemas
 * @since 0.1.0
 */
export const EvaluateResponse = Schema.Struct({
  provider: Schema.NonEmptyString,
  model: Schema.String,
  answers: Schema.Record(Schema.String, Answer),
  usage: Usage,
  providerMetadata: Schema.optionalKey(ProviderMetadata),
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
    : ProbabilityAnswer;

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
