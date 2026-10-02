import { describe, it } from "@effect/vitest";
import { memorySubscriptionStoreLayer } from "@yielded/agent-storage-memory/memory-subscription-store";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "@yielded/agent/testing/subscription-store-conformance";
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
