import { MemoryScheduleStoreLive } from "@effect-agent/storage-memory/memory-schedule-store";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { scheduleStoreConformanceCases } from "effect-agent/testing/schedule-store-conformance";

describe("MemoryScheduleStore", () => {
  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      conformanceCase.run.pipe(Effect.provide(MemoryScheduleStoreLive)),
    );
  }
});
