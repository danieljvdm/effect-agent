import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Schema as NamespaceSchema, type Crypto, Effect, Layer } from "effect";
import * as MemoryNamespace from "effect-agent/memory-namespace";
import { MemoryAccess } from "effect-agent/memory-revalidation";
import {
  MemoryScope,
  ActiveMemoryDocument,
  type MemoryDocument,
  MemoryKey,
  MemoryReader,
  type MemoryStorageError,
  WithdrawnMemoryDocument,
} from "effect-agent/memory-store";
import {
  SemanticIndexLimits,
  SemanticQueryLimits,
  indexMemorySource,
  querySemanticMemory,
} from "effect-agent/semantic-memory";
import {
  type MemoryIndexError,
  MemoryIndexCandidate,
  type SemanticMemoryChunk,
  SemanticMemoryIndex,
  SemanticMemoryProfile,
} from "effect-agent/semantic-memory-index";
import { type SemanticMemoryError } from "effect-agent/semantic-memory-revalidation";
import type { AiError } from "effect/unstable/ai";
import { EmbeddingModel } from "effect/unstable/ai";

const TestNamespace = MemoryNamespace.define({
  name: "test/memory",
  version: 1,
  identity: NamespaceSchema.String,
});

const key = MemoryKey.make({ namespace: TestNamespace.make("team"), id: "proposal" });
const access = MemoryAccess.make({ namespace: key.namespace, scope: MemoryScope.make("channel") });

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

/** A local workflow probe. The storage-memory suite owns index atomicity and cosine ranking. */
const probe = () => {
  const state: {
    current: MemoryDocument | null;
    chunks: ReadonlyArray<SemanticMemoryChunk>;
    candidates: ReadonlyArray<MemoryIndexCandidate>;
    published: number;
    beforePublish: Effect.Effect<void, MemoryIndexError>;
  } = {
    current: document,
    chunks: [],
    candidates: [],
    published: 0,
    beforePublish: Effect.void,
  };

  const index = SemanticMemoryIndex.fromAdapter({
    profile,
    replace: ({ chunks }) =>
      Effect.gen(function* () {
        yield* state.beforePublish;
        state.chunks = chunks;
        state.published += 1;
      }),
    withdraw: () => Effect.void,
    search: () =>
      Effect.sync(() => ({ candidates: state.candidates, scannedChunks: state.candidates.length })),
  });

  const layer = Layer.mergeAll(
    Layer.succeed(
      MemoryReader,
      MemoryReader.fromAdapter({
        get: () =>
          Effect.sync(() => {
            return state.current;
          }),
      }),
    ),
    Layer.succeed(SemanticMemoryIndex, index),
    Layer.effect(
      EmbeddingModel.EmbeddingModel,
      EmbeddingModel.make({
        embedMany: ({ inputs }) =>
          Effect.sync(() => {
            return { results: inputs.map(() => [1, 0]), usage: { inputTokens: inputs.length * 2 } };
          }),
      }),
    ),
    NodeCrypto.layer,
  );

  return { state, layer };
};

describe("optional semantic workflows", () => {
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
      });
    }).pipe(Effect.provide(test.layer));
  });
});

export const verifyKeepsNativeProviderSourceIndexERVisibleAndOwnsItsTemporaryScope = () => {
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

  void [
    indexErrorExact,
    allIndexErrors,
    queryErrorExact,
    allQueryErrors,
    indexR,
    allIndexR,
    queryR,
    allQueryR,
  ];
};
