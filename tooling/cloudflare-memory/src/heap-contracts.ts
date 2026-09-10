import { Schema } from "effect";

export class HeapProbeError extends Schema.TaggedError<HeapProbeError>()("HeapProbeError", {
  operation: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const HeapUsage = Schema.Struct({
  usedSize: Schema.Number,
  totalSize: Schema.Number,
  embedderHeapUsedSize: Schema.optionalKey(Schema.Number),
  backingStorageSize: Schema.optionalKey(Schema.Number),
});

export const ObjectStatus = Schema.Struct({
  active: Schema.Boolean,
  modelCalls: Schema.Natural,
  toolCalls: Schema.Natural,
});

export const NodeMemory = Schema.Struct({
  rss: Schema.Number,
  heapTotal: Schema.Number,
  heapUsed: Schema.Number,
  external: Schema.Number,
  arrayBuffers: Schema.Number,
});

export const NodeSample = Schema.Struct({
  node: Schema.String,
  v8: Schema.String,
  platform: Schema.String,
  arch: Schema.String,
  baseline: NodeMemory,
  evaluated: NodeMemory,
  retained: NodeMemory,
  evaluatedHeapDelta: Schema.Number,
  retainedHeapDelta: Schema.Number,
  exports: Schema.Array(Schema.String),
});

export const WorkerdSample = Schema.Struct({
  startup: HeapUsage,
  initialized: HeapUsage,
  active: HeapUsage,
  settled: HeapUsage,
  objects: Schema.Array(ObjectStatus),
});
