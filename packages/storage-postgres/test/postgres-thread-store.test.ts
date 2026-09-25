import { describe, it } from "@effect/vitest";
import { Effect, String } from "effect";
import {
  threadStoreConformanceCases,
  threadCheckpointConformanceCases,
} from "effect-agent/testing/thread-store-conformance";
import type { ThreadStore } from "effect-agent/thread-store";

import { storage as makeStorage, withTemporaryDatabase } from "./harness.ts";

const withStorage = <A, E>(url: string, effect: Effect.Effect<A, E, ThreadStore>) =>
  Effect.provide(
    effect,
    makeStorage(
      url,
      { schema: "select" },
      {
        startupParameters: { search_path: "pg_catalog" },
        transformQueryNames: String.snakeToCamel,
        transformResultNames: String.snakeToCamel,
      },
    ).threadStore,
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
