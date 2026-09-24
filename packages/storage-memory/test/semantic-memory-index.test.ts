import {
  InMemorySemanticIndexCapacity,
  inMemorySemanticIndexLayer,
} from "@effect-agent/storage-memory/memory-semantic-index";
import { describe, expect, it } from "@effect/vitest";
import { Schema as NamespaceSchema, Effect, Encoding, Schema } from "effect";
import * as MemoryNamespace from "effect-agent/memory-namespace";
import {
  MemoryIndexQuery,
  MemoryIndexSource,
  SemanticMemoryChunk,
  SemanticMemoryIndex,
  SemanticMemoryProfile,
} from "effect-agent/semantic-memory-index";

const TestNamespace = MemoryNamespace.define({
  name: "test/memory",
  version: 1,
  identity: NamespaceSchema.String,
});

const profile = SemanticMemoryProfile.make({
  version: 1,
  provider: "test-provider",
  model: "test-model",
  modelRevision: "revision-1",
  dimensions: 2,
  chunker: "utf8-codepoint@1",
  maxChunkBytes: 16,
  distance: "cosine",
});

const capacity = InMemorySemanticIndexCapacity.make({ maxSources: 2, maxChunks: 3 });

const source = (
  id: string,
  sourceGeneration = 1,
  revision = String(sourceGeneration),
  locator = `memory://${id}`,
) =>
  Schema.decodeSync(MemoryIndexSource.Wire)({
    key: { namespace: TestNamespace.make("tenant-a"), id },
    source: { id, locator, revision },
    sourceGeneration,
  });

const chunk = (
  passageId: string,
  ordinal: number,
  startByte: number,
  text: string,
  vector: ReadonlyArray<number>,
) =>
  SemanticMemoryChunk.make({
    passageId,
    ordinal,
    startByte,
    endByte: startByte + Encoding.encodeHex(text).length / 2,
    text,
    vector,
  });

const query = (vector: ReadonlyArray<number>, maxScannedChunks = 3, limit = 128, minScore = -1) =>
  MemoryIndexQuery.make({
    namespace: TestNamespace.make("tenant-a"),
    vector,
    maxScannedChunks,
    limit,
    minScore,
  });

const layer = (
  selectedProfile: SemanticMemoryProfile = profile,
  selectedCapacity: InMemorySemanticIndexCapacity = capacity,
) => inMemorySemanticIndexLayer(selectedProfile, selectedCapacity);

describe("in-memory semantic index", () => {
  it.effect("isolates tenant identities and their tombstones", () =>
    Effect.gen(function* () {
      const identity = Schema.Struct({ tenantId: Schema.String, userId: Schema.String });
      const users = MemoryNamespace.define({ name: "app/users", version: 1, identity });

      const namespaces = [
        users.make({ tenantId: "a", userId: "one" }),
        users.make({ tenantId: "b", userId: "one" }),
      ];

      const index = yield* SemanticMemoryIndex;

      for (const [ordinal, namespace] of namespaces.entries()) {
        yield* index.replace({
          source: MemoryIndexSource.make({ ...source("same"), key: { namespace, id: "same" } }),
          profile,
          chunks: [chunk(`passage-${ordinal}`, 0, 0, "text", [1, 0])],
        });
      }
      for (const [ordinal, namespace] of namespaces.entries()) {
        const reconstructed = Schema.decodeSync(MemoryNamespace.Any)({
          address: namespace.address,
        });

        const found = yield* index.search({ ...query([1, 0]), namespace: reconstructed });

        expect(found.candidates.map((candidate) => candidate.passageId)).toEqual([
          `passage-${ordinal}`,
        ]);
      }

      const withdrawn = MemoryIndexSource.make({
        ...source("same"),
        key: { namespace: namespaces[0], id: "same" },
      });

      yield* index.withdraw(withdrawn);
      expect(
        yield* index
          .replace({ source: withdrawn, profile, chunks: [chunk("late", 0, 0, "late", [1, 0])] })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "fenced" });
      expect(
        (yield* index.search({ ...query([1, 0]), namespace: namespaces[0] })).candidates,
      ).toEqual([]);
      for (const namespace of namespaces.slice(1))
        expect((yield* index.search({ ...query([1, 0]), namespace })).candidates).toHaveLength(1);
    }).pipe(Effect.provide(layer(profile, { maxSources: 5, maxChunks: 5 }))),
  );
});
