import { Schema } from "effect";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

/** Explicit per-Run override for the engine's finite bounded-parallel scheduler. */
export const RunSchedulingOverride = Schema.Union([
  Schema.Struct({ mode: Schema.Literal("bounded"), concurrency: PositiveInt }),
  Schema.Struct({ mode: Schema.Literal("sequential") }),
]);
export type RunSchedulingOverride = typeof RunSchedulingOverride.Type;

/** Resolve a Run and Tool override without weakening the base finite policy. */
export const resolveToolConcurrency = (
  baseConcurrency: number,
  runOverride: RunSchedulingOverride | undefined,
  toolRequiresSequential: boolean,
): number => {
  if (!Number.isInteger(baseConcurrency) || baseConcurrency <= 0) {
    throw new RangeError("baseConcurrency must be a positive integer");
  }
  if (toolRequiresSequential || runOverride?.mode === "sequential") {
    return 1;
  }
  return runOverride === undefined
    ? baseConcurrency
    : Math.min(baseConcurrency, runOverride.concurrency);
};
