/**
 * Pure constructors for questions evaluated by a DecisionModel. Definitions
 * are validated when evaluated; constructing a query performs no model I/O.
 *
 * @since 0.1.0
 */
import type * as DecisionSchema from "./DecisionSchema.ts";

/**
 * Choose one named option and retain the complete categorical distribution.
 * Option keys infer the answer's choice union. Supply at least one option;
 * null descriptions use the option name alone.
 *
 * @category constructors
 * @since 0.1.0
 */
export const choice = <const Options extends DecisionSchema.ChoiceQuestion["criteria"]>(options: {
  readonly instructions: DecisionSchema.Content;
  readonly options: Options;
}) =>
  Object.freeze({
    type: "choice" as const,
    instructions: options.instructions,
    criteria: Object.freeze({ ...options.options }),
  });

/**
 * Rate a state along at least two ordered descriptions. The answer is the
 * probability-weighted, zero-indexed position and may fall between levels.
 *
 * @category constructors
 * @since 0.1.0
 */
export const score = (options: {
  readonly instructions: DecisionSchema.Content;
  readonly levels: ReadonlyArray<string>;
}) =>
  Object.freeze({
    type: "score" as const,
    instructions: options.instructions,
    criteria: Object.freeze([...options.levels]),
  });

/**
 * Estimate the probability a proposition is true. Optional criteria clarify
 * either outcome. No threshold or implicit boolean conversion is applied.
 *
 * @category constructors
 * @since 0.1.0
 */
export const probability = (options: Omit<DecisionSchema.ProbabilityQuestion, "type">) =>
  Object.freeze({
    type: "probability" as const,
    instructions: options.instructions,
    ...(options.criteria === undefined ? {} : { criteria: Object.freeze({ ...options.criteria }) }),
  });
