import {
  MemoryNamespace,
  ActiveMemoryDocument,
  type MemoryDocument,
  MemoryIndexCandidate,
  MemoryIndexError,
  MemoryKey,
  MemoryReader,
  MemoryRecallLimits,
  type MemoryStorageError,
  type SemanticMemoryChunk,
  SemanticMemoryIndex,
  SemanticMemoryProfile,
  WithdrawnMemoryDocument,
} from "@effect-agent/core";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Schema as NamespaceSchema,
  Cause,
  type Crypto,
  Deferred,
  Effect,
  Encoding,
  Exit,
  Fiber,
  Layer,
} from "effect";
import { TestClock } from "effect/testing";
import { AiError, EmbeddingModel } from "effect/unstable/ai";

import {
  MemoryAccess,
  SemanticIndexLimits,
  type SemanticMemoryError,
  SemanticQueryLimits,
  indexMemorySource,
  querySemanticMemory,
  recallMemory,
} from "../src/index.ts";

const TestNamespace = MemoryNamespace.define({
  name: "test/memory",
  version: 1,
  identity: NamespaceSchema.String,
});

const key = MemoryKey.make({ namespace: TestNamespace.make("team"), id: "proposal" });
const access = MemoryAccess.make({ namespace: key.namespace, scope: "channel" });
const profile = SemanticMemoryProfile.make({
  version: 1,
  provider: "test",
  model: "fixture",
  modelRevision: "1",
  dimensions: 2,
  chunker: "utf8-codepoint@1",
  maxChunkBytes: 8,
  distance: "cosine",
});
const document = ActiveMemoryDocument.make({
  version: 1,
  key,
  source: { id: key.id, locator: "chat://dan-chad/1", revision: "1" },
  generation: 1,
  predecessor: null,
  modifiedAt: 30,
  scopes: [access.scope],
  content: {
    text: "Dan 🌊 proposes a queue.",
    attributions: [
      {
        originId: "dan:1",
        speaker: "Dan",
        observers: ["Chad"],
        locator: "chat://dan-chad/1",
        activityAt: 10,
        interpretation: "proposal",
      },
    ],
    metadata: { confidence: "unresolved" },
    recordedAt: 20,
    extractedAt: 25,
  },
});
const indexLimits = SemanticIndexLimits.make({
  maxSourceBytes: 1_024,
  maxChunks: 16,
  timeoutMillis: 100,
});
const queryLimits = SemanticQueryLimits.make({
  maxQueryBytes: 128,
  maxCandidates: 8,
  maxScannedChunks: 128,
  minScore: 0,
  timeoutMillis: 100,
});
const recallLimits = MemoryRecallLimits.make({
  maxSources: 4,
  maxItems: 2,
  maxBytes: 4_096,
  maxTokens: 4_096,
  timeoutMillis: 100,
});
const corrected = ActiveMemoryDocument.make({
  ...document,
  generation: 2,
  source: { ...document.source, revision: "2" },
  predecessor: document.source,
  content: { ...document.content, text: "Dan withdraws the queue proposal." },
});
const withdrawn = WithdrawnMemoryDocument.make({
  ...corrected,
  _tag: "WithdrawnMemoryDocument",
  reason: "withdrawn",
});
const byteLength = (text: string) => Encoding.encodeHex(text).length / 2;
const candidate = (text = document.content.text, ordinal = 0) =>
  MemoryIndexCandidate.make({
    key,
    source: document.source,
    sourceGeneration: 1,
    passageId: `chunk:${ordinal}`,
    ordinal,
    startByte: 0,
    endByte: byteLength(text),
    text,
    score: 1,
    indexedAt: 40,
  });
const providerFailure = AiError.make({
  module: "fixture",
  method: "embedMany",
  reason: new AiError.InvalidOutputError({ description: "provider unavailable" }),
});

/** A local workflow probe. The storage-memory suite owns index atomicity and cosine ranking. */
const probe = () => {
  const state: {
    current: MemoryDocument | null;
    chunks: ReadonlyArray<SemanticMemoryChunk>;
    candidates: ReadonlyArray<MemoryIndexCandidate>;
    inputs: Array<ReadonlyArray<string>>;
    reads: number;
    published: number;
    withdrawals: number;
    embedding: Effect.Effect<void, AiError.AiError>;
    beforePublish: Effect.Effect<void, MemoryIndexError>;
    searchFailure: MemoryIndexError | null;
  } = {
    current: document,
    chunks: [],
    candidates: [],
    inputs: [],
    reads: 0,
    published: 0,
    withdrawals: 0,
    embedding: Effect.void,
    beforePublish: Effect.void,
    searchFailure: null,
  };
  const index = SemanticMemoryIndex.fromAdapter({
    profile,
    replace: ({ chunks }) =>
      Effect.gen(function* () {
        yield* state.beforePublish;
        state.chunks = chunks;
        state.published += 1;
      }),
    withdraw: () =>
      Effect.sync(() => {
        state.withdrawals += 1;
      }),
    search: () =>
      Effect.suspend(() =>
        state.searchFailure === null
          ? Effect.succeed({
              candidates: state.candidates,
              scannedChunks: state.candidates.length,
            })
          : Effect.fail(state.searchFailure),
      ),
  });
  const layer = Layer.mergeAll(
    Layer.succeed(
      MemoryReader,
      MemoryReader.fromAdapter({
        get: () =>
          Effect.sync(() => {
            state.reads += 1;
            return state.current;
          }),
      }),
    ),
    Layer.succeed(SemanticMemoryIndex, index),
    Layer.effect(
      EmbeddingModel.EmbeddingModel,
      EmbeddingModel.make({
        embedMany: ({ inputs }) =>
          Effect.gen(function* () {
            state.inputs.push(inputs);
            yield* state.embedding;
            return { results: inputs.map(() => [1, 0]), usage: { inputTokens: inputs.length * 2 } };
          }),
      }),
    ),
    NodeCrypto.layer,
  );
  return { state, index, layer };
};

describe("optional semantic workflows", () => {
  it.effect(
    "chunks complete UTF-8 codepoints deterministically with model/config identities and bounds",
    () => {
      const test = probe();
      return Effect.gen(function* () {
        const first = yield* indexMemorySource(key, indexLimits);
        const chunks = test.state.chunks;
        expect(chunks.map((chunk) => chunk.text)).toEqual(["Dan 🌊", " propose", "s a queu", "e."]);
        expect(chunks.map((chunk) => [chunk.startByte, chunk.endByte])).toEqual([
          [0, 8],
          [8, 16],
          [16, 24],
          [24, 26],
        ]);
        expect(chunks.map((chunk) => chunk.text).join("")).toBe(document.content.text);
        expect(first).toMatchObject({
          status: "Indexed",
          embeddedChunks: 4,
          embeddedBytes: 26,
          inputTokens: 8,
        });
        yield* indexMemorySource(key, indexLimits);
        expect(test.state.chunks.map((chunk) => chunk.passageId)).toEqual(
          chunks.map((chunk) => chunk.passageId),
        );
        const changedProfile = SemanticMemoryProfile.make({ ...profile, modelRevision: "2" });
        const changedIndex = { ...test.index, profile: changedProfile };
        yield* indexMemorySource(key, indexLimits).pipe(
          Effect.provideService(SemanticMemoryIndex, changedIndex),
        );
        expect(test.state.chunks[0]?.passageId).not.toBe(chunks[0]?.passageId);
        const calls = test.state.inputs.length;
        expect(
          yield* indexMemorySource(key, { ...indexLimits, maxChunks: 1 }).pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
        expect(
          yield* indexMemorySource(key, { ...indexLimits, maxSourceBytes: 3 }).pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
        expect(
          yield* querySemanticMemory("long query", access, {
            ...queryLimits,
            maxQueryBytes: 3,
          }).pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
        expect(test.state.inputs).toHaveLength(calls);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "does not publish sources corrected or withdrawn while embedding and skips missing sources",
    () => {
      const test = probe();
      return Effect.gen(function* () {
        for (const next of [corrected, withdrawn]) {
          test.state.current = document;
          test.state.embedding = Effect.sync(() => {
            test.state.current = next;
          });
          expect(yield* indexMemorySource(key, indexLimits).pipe(Effect.flip)).toMatchObject({
            reason: "source-changed",
          });
        }
        expect(test.state.published).toBe(0);
        expect(test.state.withdrawals).toBe(1);
        test.state.current = null;
        expect(yield* indexMemorySource(key, indexLimits)).toMatchObject({
          status: "Missing",
          embeddedChunks: 0,
        });
        test.state.current = withdrawn;
        expect(yield* indexMemorySource(key, indexLimits)).toMatchObject({
          status: "Withdrawn",
          embeddedChunks: 0,
        });
        expect(test.state.inputs).toHaveLength(2);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "rereads current attribution once per source, excludes stale/access-revoked excerpts and obeys rendered budgets",
    () => {
      const test = probe();
      test.state.candidates = [candidate("Dan 🌊"), candidate("Dan 🌊", 1)];
      return Effect.gen(function* () {
        const queried = yield* querySemanticMemory("queue", access, queryLimits);
        expect(test.state.reads).toBe(1);
        expect(queried.lookup).toMatchObject({
          _tag: "Found",
          passages: [
            {
              authority: access.namespace.address,
              content: {
                text: "Dan 🌊",
                attributions: document.content.attributions,
                recordedAt: 20,
                extractedAt: 25,
              },
            },
            {
              authority: access.namespace.address,
              content: { text: "Dan 🌊", attributions: document.content.attributions },
            },
          ],
        });
        const recalled = yield* recallMemory(
          [{ id: "semantic", essential: true, read: Effect.succeed(queried.lookup) }],
          { ...recallLimits, maxItems: 1 },
        );
        expect(recalled.passages).toHaveLength(1);
        expect(recalled.text).toContain('"speaker":"Dan"');
        expect(recalled.text).toContain('"observers":["Chad"]');
        expect(recalled.text).not.toContain(access.namespace.address);
        expect(recalled.text).toContain('"authority":"memory-authority:1"');
        const limited = yield* recallMemory(
          [{ id: "semantic", essential: false, read: Effect.succeed(queried.lookup) }],
          { ...recallLimits, maxTokens: recalled.estimatedTokens - 1 },
        );
        expect(limited.passages).toEqual([]);
        for (const current of [corrected, withdrawn, null]) {
          test.state.current = current;
          expect(yield* querySemanticMemory("queue", access, queryLimits)).toMatchObject({
            lookup: { _tag: "NoMatch" },
            staleExcluded: 2,
          });
        }
        test.state.current = ActiveMemoryDocument.make({ ...document, scopes: [] });
        expect(yield* querySemanticMemory("queue", access, queryLimits)).toMatchObject({
          lookup: { _tag: "NoMatch" },
          unauthorizedExcluded: 2,
        });
        test.state.current = document;
        test.state.candidates = [candidate("invented")];
        expect(yield* querySemanticMemory("queue", access, queryLimits)).toMatchObject({
          lookup: { _tag: "NoMatch" },
          staleExcluded: 1,
        });
        test.state.candidates = [
          MemoryIndexCandidate.make({
            ...candidate(),
            key: { ...key, namespace: TestNamespace.make("other") },
          }),
        ];
        expect(
          yield* querySemanticMemory("queue", access, queryLimits).pipe(Effect.flip),
        ).toMatchObject({ _tag: "MemoryIndexError", reason: "corrupt" });
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("validates 128 candidate chunks from one maximum-size source", () => {
    const test = probe();
    const chunks = Array.from({ length: 128 }, (_, ordinal) => String(ordinal % 10).repeat(8_192));
    test.state.current = ActiveMemoryDocument.make({
      ...document,
      content: { ...document.content, text: chunks.join("") },
    });
    test.state.candidates = chunks.map((text, ordinal) =>
      MemoryIndexCandidate.make({
        ...candidate(text, ordinal),
        startByte: ordinal * 8_192,
        endByte: (ordinal + 1) * 8_192,
      }),
    );
    const query = querySemanticMemory("queue", access, {
      ...queryLimits,
      maxCandidates: 128,
      maxSourceBytes: byteLength(JSON.stringify(test.state.current)),
      timeoutMillis: 10_000,
    }).pipe(
      Effect.provideService(
        SemanticMemoryIndex,
        SemanticMemoryIndex.fromAdapter({
          ...test.index,
          profile: SemanticMemoryProfile.make({ ...profile, maxChunkBytes: 8_192 }),
        }),
      ),
    );
    return Effect.gen(function* () {
      const result = yield* query;
      expect(result).toMatchObject({
        lookup: { _tag: "Found", passages: chunks.map((text) => ({ content: { text } })) },
        staleExcluded: 0,
      });
      expect(test.state.reads).toBe(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "groups source reads without changing rank and bounds aggregate UTF-8 source input",
    () => {
      const test = probe();
      const otherKey = MemoryKey.make({ ...key, id: "other" });
      const other = ActiveMemoryDocument.make({
        ...document,
        key: otherKey,
        source: { ...document.source, id: otherKey.id, locator: "chat://other/1" },
        content: { ...document.content, text: '"🌊"\\ tail' },
      });
      const laterKey = MemoryKey.make({ ...key, id: "later" });
      test.state.candidates = [
        candidate("Dan 🌊"),
        MemoryIndexCandidate.make({ ...candidate('"🌊"'), key: otherKey, source: other.source }),
        MemoryIndexCandidate.make({ ...candidate(" propose", 1), startByte: 8, endByte: 16 }),
        MemoryIndexCandidate.make({
          ...candidate("missing"),
          key: laterKey,
          source: { ...document.source, id: laterKey.id },
        }),
      ];
      const reads: Array<string> = [];
      const sourceBytes = [document, other].reduce(
        (total, item) => total + new TextEncoder().encode(JSON.stringify(item)).byteLength,
        0,
      );
      const read = MemoryReader.fromAdapter({
        get: (requested) =>
          Effect.sync(() => {
            reads.push(requested.id);
            return requested.id === key.id ? document : requested.id === otherKey.id ? other : null;
          }),
      });
      return Effect.gen(function* () {
        expect(
          yield* querySemanticMemory("queue", access, {
            ...queryLimits,
            maxSourceBytes: sourceBytes - 1,
          }).pipe(Effect.flip),
        ).toMatchObject({
          _tag: "SemanticMemoryError",
          operation: "query source bytes",
          reason: "budget",
        });
        expect(reads).toEqual([key.id, otherKey.id]);
        reads.length = 0;
        const exact = yield* querySemanticMemory("queue", access, {
          ...queryLimits,
          maxSourceBytes: sourceBytes,
        });
        expect(reads).toEqual([key.id, otherKey.id, laterKey.id]);
        expect(exact).toMatchObject({
          staleExcluded: 1,
          lookup: {
            _tag: "Found",
            passages: [
              { source: document.source, passageId: "chunk:0", content: { text: "Dan 🌊" } },
              { source: other.source, passageId: "chunk:0", content: { text: '"🌊"' } },
              { source: document.source, passageId: "chunk:1", content: { text: " propose" } },
            ],
          },
        });
      }).pipe(Effect.provideService(MemoryReader, read), Effect.provide(test.layer));
    },
  );

  it.effect("does not charge missing, withdrawn, unauthorized, or identity-stale sources", () => {
    const test = probe();
    test.state.candidates = [candidate("Dan 🌊"), candidate("Dan 🌊", 1)];
    const largeContent = { ...document.content, text: "🌊".repeat(512) };
    return Effect.gen(function* () {
      for (const current of [
        null,
        withdrawn,
        ActiveMemoryDocument.make({ ...corrected, content: largeContent }),
        ActiveMemoryDocument.make({ ...document, scopes: [], content: largeContent }),
      ]) {
        test.state.current = current;
        expect(
          yield* querySemanticMemory("queue", access, {
            ...queryLimits,
            maxSourceBytes: 1,
            maxOutputBytes: 1,
          }),
        ).toMatchObject({
          lookup: { _tag: "NoMatch" },
          staleExcluded:
            current?._tag === "ActiveMemoryDocument" && current.scopes.length === 0 ? 0 : 2,
          unauthorizedExcluded:
            current?._tag === "ActiveMemoryDocument" && current.scopes.length === 0 ? 2 : 0,
        });
      }
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("counts repeated provenance in the semantic output byte budget", () => {
    const test = probe();
    const current = ActiveMemoryDocument.make({
      ...document,
      content: {
        ...document.content,
        attributions: document.content.attributions.map((attribution) => ({
          ...attribution,
          interpretation: '"🌊"\\'.repeat(128),
        })),
        metadata: { evidence: "🌊".repeat(128) },
      },
    });
    test.state.current = current;
    test.state.candidates = [candidate("Dan 🌊"), candidate("Dan 🌊")];
    const expected = {
      version: 1,
      authority: access.namespace.address,
      source: current.source,
      passageId: "chunk:0",
      content: { ...current.content, text: "Dan 🌊" },
    };
    const outputBytes = new TextEncoder().encode(JSON.stringify(expected)).byteLength * 2;
    return Effect.gen(function* () {
      const exact = yield* querySemanticMemory("queue", access, {
        ...queryLimits,
        maxOutputBytes: outputBytes,
      });
      expect(exact.lookup).toEqual({ _tag: "Found", passages: [expected, expected] });
      expect(
        yield* querySemanticMemory("queue", access, {
          ...queryLimits,
          maxOutputBytes: outputBytes - 1,
        }).pipe(Effect.flip),
      ).toMatchObject({
        _tag: "SemanticMemoryError",
        operation: "query output bytes",
        reason: "budget",
      });
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("filters the independent authority/index publication race before recall", () => {
    const test = probe();
    test.state.beforePublish = Effect.sync(() => {
      test.state.current = withdrawn;
    });
    return Effect.gen(function* () {
      yield* indexMemorySource(key, indexLimits);
      test.state.candidates = test.state.chunks.map((chunk) =>
        MemoryIndexCandidate.make({
          ...chunk,
          key,
          source: document.source,
          sourceGeneration: 1,
          score: 1,
          indexedAt: 40,
        }),
      );
      expect(test.state.published).toBe(1);
      expect(yield* querySemanticMemory("queue", access, queryLimits)).toMatchObject({
        lookup: { _tag: "NoMatch" },
        staleExcluded: 4,
      });
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "preserves provider/index failures and defects, and rejects malformed embeddings",
    () => {
      const test = probe();
      return Effect.gen(function* () {
        test.state.embedding = Effect.fail(providerFailure);
        expect(yield* indexMemorySource(key, indexLimits).pipe(Effect.flip)).toEqual(
          providerFailure,
        );
        expect(yield* querySemanticMemory("queue", access, queryLimits).pipe(Effect.flip)).toEqual(
          providerFailure,
        );
        test.state.embedding = Effect.die("embedding defect");
        const defect = yield* querySemanticMemory("queue", access, queryLimits).pipe(Effect.exit);
        expect(Exit.isFailure(defect) && Cause.hasDies(defect.cause)).toBe(true);
        test.state.embedding = Effect.void;
        test.state.searchFailure = MemoryIndexError.make({
          operation: "search",
          reason: "unavailable",
        });
        expect(yield* querySemanticMemory("queue", access, queryLimits).pipe(Effect.flip)).toEqual(
          test.state.searchFailure,
        );
        for (const vector of [[0, 0], [1], [Number.MAX_VALUE, Number.MAX_VALUE]]) {
          const malformed = yield* EmbeddingModel.make({
            embedMany: ({ inputs }) =>
              Effect.succeed({
                results: inputs.map(() => vector),
                usage: { inputTokens: undefined },
              }),
          });
          expect(
            yield* querySemanticMemory("queue", access, queryLimits).pipe(
              Effect.provideService(EmbeddingModel.EmbeddingModel, malformed),
              Effect.flip,
            ),
          ).toMatchObject({ reason: "invalid-embedding" });
        }
        expect(test.state.published).toBe(0);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("deadlines and caller interruption finalize embedding and index scopes", () =>
    Effect.gen(function* () {
      for (const phase of ["embedding", "publication", "query"] as const) {
        for (const mode of ["timeout", "interrupt"] as const) {
          const test = probe();
          const started = yield* Deferred.make<void>();
          let finalized = 0;
          const wait = Effect.acquireRelease(Deferred.succeed(started, undefined), () =>
            Effect.sync(() => {
              finalized += 1;
            }),
          ).pipe(Effect.andThen(Effect.never), Effect.scoped);
          if (phase === "publication") test.state.beforePublish = wait;
          else test.state.embedding = wait;
          const work = Effect.gen(function* () {
            if (phase === "query") yield* querySemanticMemory("queue", access, queryLimits);
            else yield* indexMemorySource(key, indexLimits);
          });
          const fiber = yield* work.pipe(Effect.provide(test.layer), Effect.forkChild);
          yield* Deferred.await(started);
          if (mode === "timeout") {
            yield* TestClock.adjust(101);
            expect(yield* Fiber.join(fiber).pipe(Effect.flip)).toMatchObject({
              _tag: "SemanticMemoryError",
              reason: "timeout",
            });
          } else {
            yield* Fiber.interrupt(fiber);
            const exit = yield* Fiber.await(fiber);
            expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
          }
          expect(finalized).toBe(1);
          expect(test.state.published).toBe(0);
        }
      }
    }),
  );
});

it("keeps native provider, source, index E/R visible and owns its temporary Scope", () => {
  const indexing = indexMemorySource(key, indexLimits);
  const querying = querySemanticMemory("queue", access, queryLimits);
  type IndexErrors = SemanticMemoryError | MemoryStorageError | MemoryIndexError | AiError.AiError;
  type QueryErrors = SemanticMemoryError | MemoryStorageError | MemoryIndexError | AiError.AiError;
  type IndexServices =
    | MemoryReader
    | SemanticMemoryIndex
    | EmbeddingModel.EmbeddingModel
    | Crypto.Crypto;
  type QueryServices = MemoryReader | SemanticMemoryIndex | EmbeddingModel.EmbeddingModel;
  const indexErrorExact: [Effect.Error<typeof indexing>] extends [IndexErrors] ? true : false =
    true;
  const allIndexErrors: [IndexErrors] extends [Effect.Error<typeof indexing>] ? true : false = true;
  const queryErrorExact: [Effect.Error<typeof querying>] extends [QueryErrors] ? true : false =
    true;
  const allQueryErrors: [QueryErrors] extends [Effect.Error<typeof querying>] ? true : false = true;
  const indexR: [Effect.Services<typeof indexing>] extends [IndexServices] ? true : false = true;
  const allIndexR: [IndexServices] extends [Effect.Services<typeof indexing>] ? true : false = true;
  const queryR: [Effect.Services<typeof querying>] extends [QueryServices] ? true : false = true;
  const allQueryR: [QueryServices] extends [Effect.Services<typeof querying>] ? true : false = true;
  expect([
    indexErrorExact,
    allIndexErrors,
    queryErrorExact,
    allQueryErrors,
    indexR,
    allIndexR,
    queryR,
    allQueryR,
  ]).toEqual(Array(8).fill(true));
});
