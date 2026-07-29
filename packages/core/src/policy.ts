import { Duration, Schema } from "effect";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const FinitePositiveDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && Duration.isPositive(duration),
    { expected: "a finite positive duration" },
  ),
);

const AgentPolicySchema = Schema.Struct({
  maxTurns: PositiveInt,
  maxToolCalls: PositiveInt,
  maxDuration: FinitePositiveDuration,
  toolConcurrency: PositiveInt,
  repeatedFailureLimit: NonNegativeInt,
});

/** Inputs normalized and validated by `AgentPolicy.make`. */
export interface AgentPolicyInput {
  /** Positive maximum number of model turns. */
  readonly maxTurns: number;
  /** Positive maximum number of Tool Calls across the run. */
  readonly maxToolCalls: number;
  /** Finite, positive wall-clock duration accepted in any Effect Duration input form. */
  readonly maxDuration: Duration.Input;
  /** Positive maximum number of Tool handlers allowed to execute concurrently. */
  readonly toolConcurrency: number;
  /** Non-negative repeated-failure bound; defaults to `3`. */
  readonly repeatedFailureLimit?: number;
}

/** Normalize, validate, and freeze finite policy bounds, throwing on invalid input. */
const make = (input: AgentPolicyInput): typeof AgentPolicySchema.Type =>
  Object.freeze(
    Schema.decodeSync(AgentPolicySchema)({
      ...input,
      maxDuration: Duration.fromInputUnsafe(input.maxDuration),
      repeatedFailureLimit: input.repeatedFailureLimit ?? 3,
    }),
  );

/** Schema for finite agent run limits, augmented with a synchronous `make` constructor. */
export const AgentPolicy = Object.assign(AgentPolicySchema, { make });
export type AgentPolicy = typeof AgentPolicy.Type;
