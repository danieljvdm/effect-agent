import { describe, it } from "@effect/vitest";
import { MemoryScheduleStoreLive } from "@yielded/agent-storage-memory/memory-schedule-store";
import { scheduleStoreConformanceCases } from "@yielded/agent/testing/schedule-store-conformance";
import { Effect } from "effect";

describe("MemoryScheduleStore", () => {
  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      conformanceCase.run.pipe(Effect.provide(MemoryScheduleStoreLive)),
    );
  }
});
