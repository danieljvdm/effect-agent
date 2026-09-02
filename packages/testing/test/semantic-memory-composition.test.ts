import {
  MemoryAccess,
  indexMemorySource,
  querySemanticMemory,
  recallMemory,
} from "@effect-agent/capabilities";
import {
  MemoryNamespace,
  MemoryScope,
  MemoryKey,
  MemoryWriter,
  SemanticMemoryIndex,
  SemanticMemoryProfile,
} from "@effect-agent/core";
import { inMemorySemanticIndexLayer } from "@effect-agent/storage-memory";
import { memoryStoreLayer } from "@effect-agent/storage-sqlite";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import { Schema as NamespaceSchema, Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import { AiError, EmbeddingModel } from "effect/unstable/ai";

const TestNamespace = MemoryNamespace.define({
  name: "test/memory",
  version: 1,
  identity: NamespaceSchema.String,
});

const key = MemoryKey.make({ namespace: TestNamespace.make("team"), id: "proposal" });
const access = MemoryAccess.make({ namespace: key.namespace, scope: MemoryScope.make("channel") });
const profile = SemanticMemoryProfile.make({
  version: 1,
  provider: "deterministic-port-fixture",
  model: "two-dimensional",
  modelRevision: "1",
  dimensions: 2,
  chunker: "utf8-codepoint@1",
  maxChunkBytes: 64,
  distance: "cosine",
});
const indexLimits = { maxSourceBytes: 1_024, maxChunks: 8, timeoutMillis: 1_000 };
const queryLimits = {
  maxQueryBytes: 128,
  maxCandidates: 3,
  maxScannedChunks: 8,
  minScore: 0.35,
  timeoutMillis: 1_000,
};
const sql = SqliteClient.layer({ filename: ":memory:" });
const services = Layer.mergeAll(
  memoryStoreLayer.pipe(Layer.provide(sql)),
  inMemorySemanticIndexLayer(profile, { maxSources: 1, maxChunks: 8 }),
  Layer.effect(
    EmbeddingModel.EmbeddingModel,
    EmbeddingModel.make({
      embedMany: ({ inputs }) =>
        Effect.succeed({
          results: inputs.map(() => [1, 0]),
          usage: { inputTokens: undefined },
        }),
    }),
  ),
  NodeCrypto.layer,
);

const content = {
  text: "Dan proposes a queue.",
  attributions: [
    {
      originId: "dan:1",
      speaker: "Dan",
      observers: ["Chad"],
      locator: "chat://engineering/1",
      activityAt: 10,
      interpretation: "proposal",
    },
  ],
  metadata: {},
  recordedAt: 20,
  extractedAt: 30,
};

it.effect(
  "composes SQLite authority, native embeddings, the real index, and bounded recall across correction and withdrawal",
  () =>
    Effect.gen(function* () {
      const writer = yield* MemoryWriter;
      yield* writer.change({
        _tag: "Put",
        key,
        operationId: "initial",
        expectedRevision: null,
        locator: "memory://proposal",
        content,
        scopes: [access.scope],
      });
      yield* indexMemorySource(key, indexLimits);
      const initial = yield* querySemanticMemory("queue", access, queryLimits);
      const recalled = yield* recallMemory(
        [{ id: "semantic", essential: true, read: Effect.succeed(initial.lookup) }],
        {
          maxSources: 1,
          maxItems: 1,
          maxBytes: 4_096,
          maxTokens: 4_096,
          timeoutMillis: 1_000,
        },
      );
      expect(recalled.passages).toMatchObject([
        { authority: access.namespace.address, source: { id: key.id, revision: "1" }, content },
      ]);

      const corrected = { ...content, text: "Dan proposes a scheduler instead." };
      yield* writer.change({
        _tag: "Put",
        key,
        operationId: "correction",
        expectedRevision: "1",
        locator: "memory://proposal",
        content: corrected,
        scopes: [access.scope],
      });
      expect(yield* querySemanticMemory("queue", access, queryLimits)).toMatchObject({
        lookup: { _tag: "NoMatch" },
        staleExcluded: 1,
      });
      yield* indexMemorySource(key, indexLimits);
      expect((yield* querySemanticMemory("scheduler", access, queryLimits)).lookup).toMatchObject({
        _tag: "Found",
        passages: [{ source: { revision: "2" }, content: corrected }],
      });
      yield* writer.change({
        _tag: "Withdraw",
        key,
        operationId: "withdrawal",
        expectedRevision: "2",
        reason: "withdrawn",
      });
      expect(yield* querySemanticMemory("scheduler", access, queryLimits)).toMatchObject({
        lookup: { _tag: "NoMatch" },
        staleExcluded: 1,
      });
      expect(yield* indexMemorySource(key, indexLimits)).toMatchObject({
        status: "Withdrawn",
        embeddedChunks: 0,
      });
      expect(yield* querySemanticMemory("scheduler", access, queryLimits)).toMatchObject({
        lookup: { _tag: "NoMatch" },
        scannedChunks: 0,
      });
    }).pipe(Effect.provide(services)),
);

it.effect("preserves usable recall when an unchanged-source refresh fails or is cancelled", () =>
  Effect.gen(function* () {
    const writer = yield* MemoryWriter;
    yield* writer.change({
      _tag: "Put",
      key,
      operationId: "initial",
      expectedRevision: null,
      locator: "memory://proposal",
      content,
      scopes: [access.scope],
    });
    yield* indexMemorySource(key, indexLimits);
    const index = yield* SemanticMemoryIndex;
    const search = {
      namespace: key.namespace,
      vector: [1, 0],
      limit: 8,
      minScore: 0,
      maxScannedChunks: 8,
    };
    const original = yield* index.search(search);
    const providerFailure = AiError.make({
      module: "refresh-fixture",
      method: "embedMany",
      reason: new AiError.InvalidOutputError({ description: "provider unavailable" }),
    });

    for (const mode of ["failure", "defect", "timeout", "interrupt"] as const) {
      const started = yield* Deferred.make<void>();
      let finalized = 0;
      const model = yield* EmbeddingModel.make({
        embedMany: () =>
          Effect.acquireRelease(Deferred.succeed(started, undefined), () =>
            Effect.sync(() => {
              finalized += 1;
            }),
          ).pipe(
            Effect.andThen(
              mode === "failure"
                ? Effect.fail(providerFailure)
                : mode === "defect"
                  ? Effect.die("embedding defect")
                  : Effect.never,
            ),
            Effect.scoped,
          ),
      });
      const refreshing = yield* indexMemorySource(key, indexLimits).pipe(
        Effect.provideService(EmbeddingModel.EmbeddingModel, model),
        Effect.forkChild,
      );
      yield* Deferred.await(started);
      expect((yield* querySemanticMemory("queue", access, queryLimits)).lookup._tag).toBe("Found");
      if (mode === "timeout") yield* TestClock.adjust(1_001);
      if (mode === "interrupt") yield* Fiber.interrupt(refreshing);
      const exit = yield* Fiber.await(refreshing);
      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        if (mode === "failure")
          expect(Cause.findErrorOption(exit.cause)).toMatchObject({ value: providerFailure });
        if (mode === "defect") expect(Cause.hasDies(exit.cause)).toBe(true);
        if (mode === "timeout")
          expect(Cause.findErrorOption(exit.cause)).toMatchObject({ value: { reason: "timeout" } });
        if (mode === "interrupt") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
      }
      expect(finalized).toBe(1);
      expect(yield* index.search(search)).toEqual(original);
    }
  }).pipe(Effect.provide(services)),
);

it.effect("fences delayed replacement after a newer index or terminal withdrawal commits", () =>
  Effect.gen(function* () {
    for (const mutation of ["correction", "withdrawal"] as const) {
      yield* Effect.gen(function* () {
        const writer = yield* MemoryWriter;
        yield* writer.change({
          _tag: "Put",
          key,
          operationId: "initial",
          expectedRevision: null,
          locator: "memory://proposal",
          content,
          scopes: [access.scope],
        });
        yield* indexMemorySource(key, indexLimits);
        const index = yield* SemanticMemoryIndex;
        const reached = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        const delayed = yield* indexMemorySource(key, indexLimits).pipe(
          Effect.provideService(
            SemanticMemoryIndex,
            SemanticMemoryIndex.fromAdapter({
              ...index,
              replace: (request) =>
                Deferred.succeed(reached, undefined).pipe(
                  Effect.andThen(Deferred.await(resume)),
                  Effect.andThen(index.replace(request)),
                ),
            }),
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(reached);
        if (mutation === "correction") {
          yield* writer.change({
            _tag: "Put",
            key,
            operationId: "correction",
            expectedRevision: "1",
            locator: "memory://proposal",
            content: { ...content, text: "Dan proposes a scheduler." },
            scopes: [access.scope],
          });
        } else {
          yield* writer.change({
            _tag: "Withdraw",
            key,
            operationId: "withdrawal",
            expectedRevision: "1",
            reason: "retracted",
          });
        }
        yield* indexMemorySource(key, indexLimits);
        const current = yield* querySemanticMemory("queue", access, queryLimits);
        expect(current.lookup).toMatchObject(
          mutation === "correction"
            ? { _tag: "Found", passages: [{ source: { revision: "2" } }] }
            : { _tag: "NoMatch" },
        );
        yield* Deferred.succeed(resume, undefined);
        expect(yield* Fiber.join(delayed).pipe(Effect.flip)).toMatchObject({
          _tag: "MemoryIndexError",
          reason: "fenced",
        });
        expect((yield* querySemanticMemory("queue", access, queryLimits)).lookup).toEqual(
          current.lookup,
        );
      }).pipe(Effect.provide(services));
    }
  }),
);
