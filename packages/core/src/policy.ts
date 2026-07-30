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

const AgentPolicyFields = Schema.Struct({
  maxTurns: PositiveInt,
  maxToolCalls: PositiveInt,
  maxDuration: FinitePositiveDuration,
  toolConcurrency: PositiveInt,
  repeatedFailureLimit: NonNegativeInt,
  tokenBudget: Schema.optionalKey(PositiveInt),
  costBudgetMicrousd: Schema.optionalKey(NonNegativeInt),
});
type AgentPolicyFields = typeof AgentPolicyFields.Type;

/** Inputs normalized and validated by `AgentPolicy.make`. */
export type AgentPolicyInput = Readonly<
  Omit<AgentPolicyFields, "maxDuration" | "repeatedFailureLimit"> & {
    /** Finite, positive wall-clock duration accepted in any Effect Duration input form. */
    readonly maxDuration: Duration.Input;
    /** Non-negative repeated-failure bound; defaults to `3`. */
    readonly repeatedFailureLimit?: number;
  }
>;

/** Schema class for finite agent run limits. */
export class AgentPolicy extends Schema.Class<AgentPolicy>("AgentPolicy")(AgentPolicyFields) {
  /** Normalize and validate finite policy bounds, throwing on invalid input. */
  static override make(input: AgentPolicyInput): AgentPolicy {
    return super.make({
      ...input,
      maxDuration: Duration.fromInputUnsafe(input.maxDuration),
      repeatedFailureLimit: input.repeatedFailureLimit ?? 3,
    });
  }
}
