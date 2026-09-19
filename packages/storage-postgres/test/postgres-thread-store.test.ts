import * as PostgresThreadStore from "@effect-agent/storage-postgres/postgres-thread-store";
import { describe, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";
import {
  threadStoreConformanceCases,
  threadCheckpointConformanceCases,
} from "effect-agent/testing/thread-store-conformance";
import type { ThreadStore } from "effect-agent/thread-store";

import { withTemporaryDatabase } from "./harness.ts";

const withStorage = <A, E>(url: string, effect: Effect.Effect<A, E, ThreadStore>) =>
  Effect.provide(
    effect,
    PostgresThreadStore.layer({ client: { url: Redacted.make(url) }, observationPollInterval: 1 }),
  );

describe("PostgresThreadStore", () => {
  describe("shared ThreadStore conformance", () => {
    for (const conformanceCase of [
      ...threadStoreConformanceCases,
      ...threadCheckpointConformanceCases,
    ]) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((url) => withStorage(url, conformanceCase.run)),
      );
    }
  });
});
