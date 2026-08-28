import { scheduleStoreConformanceCases } from "@effect-agent/session/testing";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { MemoryScheduleStoreLive } from "../src/index.ts";

describe("MemoryScheduleStore", () => {
  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      conformanceCase.run.pipe(Effect.provide(MemoryScheduleStoreLive)),
    );
  }
});
