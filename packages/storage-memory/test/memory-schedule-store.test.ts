import { MemoryScheduleStoreLive } from "@effect-agent/storage-memory/MemoryScheduleStore";
import { scheduleStoreConformanceCases } from "@effect-agent/thread/testing/ScheduleStoreConformance";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

describe("MemoryScheduleStore", () => {
  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      conformanceCase.run.pipe(Effect.provide(MemoryScheduleStoreLive)),
    );
  }
});
