import {
  MemoryNamespace,
  MemoryIndexQuery,
  MemoryIndexSource,
  SemanticMemoryChunk,
  SemanticMemoryIndex,
  SemanticMemoryProfile,
} from "@effect-agent/core";
import type { MemoryIndexError } from "@effect-agent/core";
import { describe, expect, it } from "@effect/vitest";
import {
  Schema as NamespaceSchema,
  Context,
  Effect,
  Encoding,
  Exit,
  Layer,
  Scope,
  Schema,
} from "effect";
import { TestClock } from "effect/testing";

import { InMemorySemanticIndexCapacity, inMemorySemanticIndexLayer } from "../src/index.ts";

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
  it.effect("isolates reconstructed identities, definition names, versions, and tombstones", () =>
    Effect.gen(function* () {
      const identity = Schema.Struct({ tenantId: Schema.String, userId: Schema.String });
      const users = MemoryNamespace.define({ name: "app/users", version: 1, identity });
      const other = MemoryNamespace.define({ name: "app/other", version: 1, identity });
      const newer = MemoryNamespace.define({ name: "app/users", version: 2, identity });
      const namespaces = [
        users.make({ tenantId: "a", userId: "one" }),
        users.make({ tenantId: "b", userId: "one" }),
        users.make({ tenantId: "a", userId: "two" }),
        other.make({ tenantId: "a", userId: "one" }),
        newer.make({ tenantId: "a", userId: "one" }),
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

  it.effect(
    "validates configuration, snapshots vectors, ranks deterministically, and bounds scans",
    () =>
      Effect.gen(function* () {
        const invalidCapacity = InMemorySemanticIndexCapacity.make({ maxSources: 1, maxChunks: 1 });
        Object.defineProperty(invalidCapacity, "maxSources", { value: 0 });
        expect(
          yield* Layer.build(layer(profile, invalidCapacity)).pipe(Effect.scoped, Effect.flip),
        ).toMatchObject({ reason: "invalid-input" });
        const invalidProfile = SemanticMemoryProfile.make({ ...profile });
        Object.defineProperty(invalidProfile, "dimensions", { value: 0 });
        expect(
          yield* Layer.build(layer(invalidProfile)).pipe(Effect.scoped, Effect.flip),
        ).toMatchObject({ reason: "invalid-input" });

        const maximumDimensions = SemanticMemoryProfile.make({ ...profile, dimensions: 4_096 });
        yield* Layer.build(layer(maximumDimensions, { maxSources: 1, maxChunks: 4_096 })).pipe(
          Effect.scoped,
        );
        expect(
          yield* Layer.build(layer(maximumDimensions, { maxSources: 1, maxChunks: 4_097 })).pipe(
            Effect.scoped,
            Effect.flip,
          ),
        ).toMatchObject({ reason: "invalid-input" });

        const index = yield* SemanticMemoryIndex;
        yield* index.replace({
          source: source("b"),
          profile,
          chunks: [chunk("b-0", 0, 0, "b", [1, 0])],
        });
        const mutableVector = [1, 0];
        yield* index.replace({
          source: source("a"),
          profile,
          chunks: [chunk("a-0", 0, 0, "a", mutableVector), chunk("a-1", 1, 1, "c", [0, 1])],
        });
        mutableVector[0] = -1;
        const ranked = yield* index.search(query([1, 0]));
        expect(ranked.scannedChunks).toBe(3);
        expect(ranked.candidates.map((candidate) => candidate.passageId)).toEqual([
          "a-0",
          "b-0",
          "a-1",
        ]);
        expect(ranked.candidates.map((candidate) => candidate.score)).toEqual([1, 1, 0]);
        expect((yield* index.search(query([1, 0], 3, 1, 0.5))).candidates).toHaveLength(1);
        expect(
          (yield* index.search({ ...query([1, 0]), namespace: TestNamespace.make("other") }))
            .candidates,
        ).toEqual([]);
        for (const invalidVector of [[0, 0], [1], [Number.MAX_VALUE, 1]]) {
          expect(yield* index.search(query(invalidVector)).pipe(Effect.flip)).toMatchObject({
            reason: "invalid-input",
          });
        }
        expect(yield* index.search(query([1, 0], 2)).pipe(Effect.flip)).toMatchObject({
          reason: "budget",
        });
        yield* index.withdraw(source("a"));
        expect(
          yield* index
            .replace({ source: source("c"), profile, chunks: [chunk("c", 0, 0, "c", [1, 0])] })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
      }).pipe(Effect.provide(layer())),
  );

  it.effect("bounds source identities atomically and releases replaced identity bytes", () => {
    const original = source("a", 1, "1", `memory://${"🌊".repeat(200)}`);
    const originalBytes = Encoding.encodeHex(JSON.stringify(original)).length / 2;
    return Effect.gen(function* () {
      const index = yield* SemanticMemoryIndex;
      const chunks = [chunk("current", 0, 0, "current", [1, 0])];
      const larger = source("a", 2, "2", `${original.source.locator}x`);
      const other = source("b");
      yield* index.replace({ source: original, profile, chunks });
      yield* index.replace({ source: original, profile, chunks });
      const before = yield* index.search(query([1, 0]));
      for (const change of [
        index.replace({ source: larger, profile, chunks }),
        index.withdraw(larger),
        index.replace({ source: other, profile, chunks }),
        index.withdraw(other),
      ]) {
        expect(yield* change.pipe(Effect.flip)).toMatchObject({ reason: "budget" });
        expect(yield* index.search(query([1, 0]))).toEqual(before);
      }
      const smaller = source("a", 2);
      yield* index.replace({ source: smaller, profile, chunks });
      yield* index.withdraw(other);
      yield* index.withdraw(other);
      yield* index.withdraw(smaller);
      yield* index.withdraw(smaller);
      expect((yield* index.search(query([1, 0]))).candidates).toEqual([]);
      expect(
        yield* index.replace({ source: other, profile, chunks }).pipe(Effect.flip),
      ).toMatchObject({ reason: "fenced" });
    }).pipe(
      Effect.provide(
        layer(profile, {
          maxSources: 2,
          maxChunks: 2,
          maxSourceBytes: originalBytes,
        }),
      ),
    );
  });

  it.effect("charges tombstones against the exact UTF-8 source identity boundary", () => {
    const tombstone = source("gone", 1, "1", "memory://🌊");
    const bytes = Encoding.encodeHex(JSON.stringify(tombstone)).length / 2;
    return Effect.gen(function* () {
      for (const maxSourceBytes of [bytes - 1, bytes]) {
        yield* Effect.gen(function* () {
          const index = yield* SemanticMemoryIndex;
          if (maxSourceBytes < bytes) {
            expect(yield* index.withdraw(tombstone).pipe(Effect.flip)).toMatchObject({
              reason: "budget",
            });
            yield* index.withdraw(source("gone", 1, "1", "m"));
          } else {
            yield* index.withdraw(tombstone);
            yield* index.withdraw(tombstone);
            expect(yield* index.withdraw(source("next")).pipe(Effect.flip)).toMatchObject({
              reason: "budget",
            });
          }
        }).pipe(Effect.provide(layer(profile, { maxSources: 2, maxChunks: 2, maxSourceBytes })));
      }
    });
  });

  it.effect(
    "keeps the last index on rejected refresh and measures capacity after replacement",
    () =>
      Effect.gen(function* () {
        const index = yield* SemanticMemoryIndex;
        const current = source("a");
        const original = chunk("old", 0, 0, "old", [1, 0]);
        yield* TestClock.setTime(10);
        yield* index.replace({ source: current, profile, chunks: [original] });
        const before = yield* index.search(query([1, 0]));
        const malformed = [
          [],
          [chunk("zero", 0, 0, "zero", [0, 0])],
          [chunk("dimension", 0, 0, "one", [1])],
          [chunk("overflow", 0, 0, "one", [Number.MAX_VALUE, 1])],
          [chunk("ordinal", 1, 0, "one", [1, 0])],
          [chunk("gap", 0, 1, "one", [1, 0])],
          [SemanticMemoryChunk.make({ ...original, endByte: 4 })],
          [chunk("large", 0, 0, "x".repeat(17), [1, 0])],
          [chunk("duplicate", 0, 0, "a", [1, 0]), chunk("duplicate", 1, 1, "b", [1, 0])],
        ];
        for (const chunks of malformed) {
          expect(
            yield* index.replace({ source: current, profile, chunks }).pipe(Effect.flip),
          ).toMatchObject({ reason: "invalid-input" });
          expect(yield* index.search(query([1, 0]))).toEqual(before);
        }
        expect(
          yield* index
            .replace({
              source: current,
              profile,
              chunks: [chunk("one", 0, 0, "a", [1, 0]), chunk("two", 1, 1, "b", [1, 0])],
            })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
        expect(
          yield* index
            .replace({
              source: current,
              profile: SemanticMemoryProfile.make({ ...profile, modelRevision: "other" }),
              chunks: [original],
            })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "incompatible" });
        expect(
          yield* index
            .replace({
              source: MemoryIndexSource.make({
                ...current,
                source: { ...current.source, id: "wrong" },
              }),
              profile,
              chunks: [original],
            })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "invalid-input" });
        expect(yield* index.search(query([1, 0]))).toEqual(before);

        yield* TestClock.setTime(20);
        yield* index.replace({
          source: current,
          profile,
          chunks: [chunk("new", 0, 0, "new", [0, 1])],
        });
        expect((yield* index.search(query([1, 0]))).candidates).toMatchObject([
          { passageId: "new", text: "new", score: 0, indexedAt: 20 },
        ]);
      }).pipe(Effect.provide(layer(profile, { maxSources: 1, maxChunks: 1 }))),
  );

  it.effect(
    "fences older generations, divergent revisions and locators, and terminal withdrawal",
    () =>
      Effect.gen(function* () {
        const index = yield* SemanticMemoryIndex;
        const chunks = [chunk("current", 0, 0, "current", [1, 0])];
        yield* index.replace({ source: source("a", 2), profile, chunks });
        for (const stale of [
          source("a", 1),
          source("a", 2, "different"),
          source("a", 2, "2", "other://a"),
        ]) {
          expect(
            yield* index.replace({ source: stale, profile, chunks }).pipe(Effect.flip),
          ).toMatchObject({ reason: "fenced" });
          expect(yield* index.withdraw(stale).pipe(Effect.flip)).toMatchObject({
            reason: "fenced",
          });
        }
        yield* index.withdraw(source("a", 3));
        yield* index.withdraw(source("a", 3));
        for (const delayed of [source("a", 2), source("a", 3), source("a", 4)]) {
          expect(
            yield* index.replace({ source: delayed, profile, chunks }).pipe(Effect.flip),
          ).toMatchObject({ reason: "fenced" });
        }
        yield* index.withdraw(source("b"));
        expect(
          yield* index.replace({ source: source("b"), profile, chunks }).pipe(Effect.flip),
        ).toMatchObject({ reason: "fenced" });
        expect(yield* index.withdraw(source("c")).pipe(Effect.flip)).toMatchObject({
          reason: "budget",
        });
        expect((yield* index.search(query([1, 0]))).candidates).toEqual([]);
      }).pipe(Effect.provide(layer())),
  );

  it.effect("clears scoped state and rejects all captured methods after closure", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(layer(), scope);
      const index = Context.get(context, SemanticMemoryIndex);
      const request = {
        source: source("closed"),
        profile,
        chunks: [chunk("closed", 0, 0, "closed", [1, 0])],
      };
      yield* index.replace(request);
      yield* Scope.close(scope, Exit.void);
      const calls: ReadonlyArray<Effect.Effect<unknown, MemoryIndexError>> = [
        index.replace(request),
        index.withdraw(request.source),
        index.search(query([1, 0])),
      ];
      for (const call of calls) {
        expect(yield* call.pipe(Effect.flip)).toMatchObject({ reason: "unavailable" });
      }
      expect((yield* (yield* SemanticMemoryIndex).search(query([1, 0]))).candidates).toEqual([]);
    }).pipe(Effect.provide(layer())),
  );
});
