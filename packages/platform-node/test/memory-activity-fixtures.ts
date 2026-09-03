import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import { MemoryContent } from "@effect-agent/core/MemoryReference";
import { MemoryKey, MemoryScope } from "@effect-agent/core/MemoryStore";
import { ActivityProcessorKey } from "@effect-agent/thread/ActivityStore";
import { ActivityPassResult } from "@effect-agent/thread/CommittedActivity";
import { Digest } from "@effect-agent/thread/Records";
import { Schema } from "effect";

export const MEMORY_NAMESPACE = MemoryNamespace.define({
  name: "test/committed-memory",
  version: 1,
  identity: Schema.Struct({ tenantId: Schema.String, userId: Schema.String }),
}).make({ tenantId: "team-memory", userId: "dan" });

export const MEMORY_SCOPE = MemoryScope.make("participating-channels");
export const MEMORY_SOURCE_ID = "dan-chad-project-atlas";
export const DAN_THREAD = "memory-source-dan-thread";
export const TIM_THREAD = "memory-consumer-tim-thread";

export const ORIGINAL_TEXT =
  "Dan proposed that Chad lead Project Atlas. This remains a proposal, not a decision.";

export const CORRECTED_TEXT =
  "Dan proposed that Chad advise Project Atlas. This remains a proposal, not a decision.";

export const DIVERGENT_TEXT = "Divergent restart extraction must never replace pinned output.";

export const memoryKey = MemoryKey.make({
  namespace: MEMORY_NAMESPACE,
  id: MEMORY_SOURCE_ID,
});

export const activityKey = Schema.decodeSync(ActivityProcessorKey)({
  processorId: "project-atlas-memory",
  processorVersion: "v1",
  threadId: DAN_THREAD,
});

export class DanStatement extends Schema.Class<DanStatement>(
  "PlatformNodeMemoryActivity/DanStatement",
)({
  speaker: Schema.Literal("Dan"),
  observer: Schema.Literal("Chad"),
  text: Schema.NonEmptyString,
  locator: Schema.NonEmptyString,
  activityAt: Schema.Finite,
  recordedAt: Schema.Finite,
  interpretation: Schema.NonEmptyString,
}) {}

export const danStatement = DanStatement.make({
  speaker: "Dan",
  observer: "Chad",
  text: ORIGINAL_TEXT,
  locator: `thread://${DAN_THREAD}/dan-message`,
  activityAt: 1_000,
  recordedAt: 2_000,
  interpretation: "proposal reported by Dan; not a decision",
});

export const ActivityMemoryOutput = Schema.Union([
  Schema.TaggedStruct("Skip", {}),
  Schema.TaggedStruct("Remember", {
    key: MemoryKey.Wire,
    locator: Schema.NonEmptyString,
    content: MemoryContent,
    scopes: Schema.Array(MemoryScope),
  }),
]);

export type ActivityMemoryOutput = typeof ActivityMemoryOutput.Type;

export const MemoryActivityWorkerMode = Schema.Literals(["crash-after-apply", "recover-divergent"]);
export type MemoryActivityWorkerMode = typeof MemoryActivityWorkerMode.Type;

export const MemoryActivityWorkerConfig = Schema.Struct({
  database: Schema.NonEmptyString,
  mode: MemoryActivityWorkerMode,
});

export const MemoryActivityMarker = Schema.TaggedStruct("MemoryActivityMarker", {
  point: Schema.Literal("memory:change:after"),
});

export type MemoryActivityMarker = typeof MemoryActivityMarker.Type;

export const MemoryActivityWorkerResult = Schema.TaggedStruct("MemoryActivityWorkerResult", {
  pass: ActivityPassResult,
  appliedWorkIds: Schema.Array(Digest),
  extractedUserRecords: Schema.Natural,
});

export type MemoryActivityWorkerResult = typeof MemoryActivityWorkerResult.Type;
