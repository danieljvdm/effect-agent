import { memorySubscriptionStoreLayer } from "@effect-agent/storage-memory/MemorySubscriptionStore";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "@effect-agent/thread/testing/SubscriptionStoreConformance";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

describe("MemorySubscriptionStore", () => {
  for (const testCase of subscriptionStoreConformanceCases) {
    it.effect(testCase.name, () =>
      testCase.run.pipe(
        Effect.provide(memorySubscriptionStoreLayer(subscriptionConformancePartition)),
      ),
    );
  }
});
