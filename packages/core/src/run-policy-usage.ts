import { Schema } from "effect";

/**
 * Cumulative policy accounting for one logical Run across replacement Attempts.
 * Turns and model-declared calls include a pending batch. The failure streak excludes that
 * entire batch until its declaration-ordered outcomes are folded once during continuation.
 * Programmatic calls are reserved before execution; finalization is reserved before its model call.
 */
export const RunPolicyUsage = Schema.Struct({
  committedTurns: Schema.Natural,
  toolCalls: Schema.Natural,
  programmaticToolCalls: Schema.Natural,
  consecutiveToolFailures: Schema.Natural,
  finalizationUsed: Schema.Boolean,
});
export type RunPolicyUsage = typeof RunPolicyUsage.Type;
