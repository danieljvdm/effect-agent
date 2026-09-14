import { memorySubscriptionStoreLayer } from "@effect-agent/storage-memory/memory-subscription-store";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "effect-agent/testing/subscription-store-conformance";

describe("MemorySubscriptionStore", () => {
  for (const testCase of subscriptionStoreConformanceCases) {
    it.effect(testCase.name, () =>
      testCase.run.pipe(
        Effect.provide(memorySubscriptionStoreLayer(subscriptionConformancePartition)),
      ),
    );
  }
});
