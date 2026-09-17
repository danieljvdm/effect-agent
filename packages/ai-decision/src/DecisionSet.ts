/**
 * Reusable question collections with a schema-defined input. The set owns no
 * provider, live resources, routing policy, or execution state.
 *
 * @since 0.1.0
 */
import type * as Schema from "effect/Schema";

import type * as DecisionSchema from "./DecisionSchema.ts";

/** @category models
 * @since 0.1.0
 */
export interface DecisionSet<Input extends Schema.Top, Questions extends DecisionSchema.Questions> {
  /** The encoded input becomes model-visible state: a string, JSON object, or JSON array. */
  readonly input: Input;
  /** Independent questions evaluated together against that state. */
  readonly questions: Questions;
}

/**
 * Describe a reusable assessment. Construction performs no encoding or model
 * I/O. Input encoding and question validation occur inside model.evaluate.
 * Treat nested instruction content as readonly; evaluation snapshots it before
 * calling the provider. Dynamic question records are supported.
 *
 * @category constructors
 * @since 0.1.0
 */
export const make = <Input extends Schema.Top, const Questions extends DecisionSchema.Questions>(
  options: DecisionSet<Input, Questions>,
): DecisionSet<Input, Questions> =>
  Object.freeze({
    input: options.input,
    questions: Object.freeze({ ...options.questions }),
  });
