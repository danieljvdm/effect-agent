import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "@effect-agent/thread/testing";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { memorySubscriptionStoreLayer } from "../src/index.ts";

describe("MemorySubscriptionStore", () => {
  for (const testCase of subscriptionStoreConformanceCases) {
    it.effect(testCase.name, () =>
      testCase.run.pipe(
        Effect.provide(memorySubscriptionStoreLayer(subscriptionConformancePartition)),
      ),
    );
  }
});
