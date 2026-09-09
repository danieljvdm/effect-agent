import { Schema } from "effect";

export const Mode = Schema.Literals(["native", "sequential", "concurrent"]);
export const Workload = Schema.Literals(["reads", "writes"]);

export const BenchmarkResult = Schema.Struct({
  answer: Schema.Json,
  modelCalls: Schema.Natural,
  toolCalls: Schema.Natural,
  peakConcurrency: Schema.Natural,
  activeAfterRun: Schema.Natural,
});
