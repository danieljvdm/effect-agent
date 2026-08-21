import type { RunSchedulingOverride as EngineRunSchedulingOverride } from "@effect-agent/engine";
import { Schema } from "effect";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

/**
 * Public validation boundary retained for compatibility. Scheduling behavior
 * and the authoritative TypeScript contract belong to the engine's
 * `RunSchedulingHook`; capabilities only validates values crossing into that
 * adapter.
 */
export const RunSchedulingOverride: Schema.Codec<EngineRunSchedulingOverride> = Schema.Union([
  Schema.Struct({ mode: Schema.Literal("bounded"), concurrency: PositiveInt }),
  Schema.Struct({ mode: Schema.Literal("sequential") }),
]);
export type RunSchedulingOverride = EngineRunSchedulingOverride;
