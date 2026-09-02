import {
  MemoryDocument,
  type MemoryIndexCandidate,
  MemoryIndexError,
  MemoryIndexQuery,
  MemoryIndexSearch,
  MemoryIndexSource,
  MemoryKey,
  MemoryLookup,
  MemoryPassage,
  MemoryReader,
  MemoryStorageError,
  SemanticMemoryChunk,
  SemanticMemoryIndex,
  SemanticMemoryProfile,
} from "@effect-agent/core";
import { Clock, Crypto, Effect, Encoding, Schema } from "effect";
import { EmbeddingModel } from "effect/unstable/ai";

import { MemoryAccess } from "./memory-lifecycle.ts";

const Timestamp = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const InputTokens = Schema.NullOr(Schema.Natural);

export class SemanticMemoryError extends Schema.TaggedError<SemanticMemoryError>()(
  "SemanticMemoryError",
  {
    operation: Schema.NonEmptyString,
    reason: Schema.Literals([
      "invalid-input",
      "invalid-embedding",
      "budget",
      "timeout",
      "source-changed",
      "unavailable",
    ]),
  },
) {}

export class SemanticIndexLimits extends Schema.Class<SemanticIndexLimits>(
  "@effect-agent/capabilities/SemanticIndexLimits",
)({
  maxSourceBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4_194_304 })),
  maxChunks: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 256 })),
  timeoutMillis: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 300_000 })),
}) {}

export class SemanticQueryLimits extends Schema.Class<SemanticQueryLimits>(
  "@effect-agent/capabilities/SemanticQueryLimits",
)({
  maxQueryBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
  maxCandidates: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
  maxScannedChunks: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
  /** Aggregate UTF-8 JSON of current authorized candidate sources; defaults to 16 MiB. */
  maxSourceBytes: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 67_108_864 })),
  ),
  minScore: Schema.Finite.check(Schema.isBetween({ minimum: -1, maximum: 1 })),
  timeoutMillis: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 60_000 })),
}) {}

export class SemanticIndexResult extends Schema.Class<SemanticIndexResult>(
  "@effect-agent/capabilities/SemanticIndexResult",
)({
  key: MemoryKey,
  status: Schema.Literals(["Missing", "Withdrawn", "Indexed"]),
  embeddedChunks: Schema.Natural,
  embeddedBytes: Schema.Natural,
  inputTokens: InputTokens,
  startedAt: Timestamp,
  finishedAt: Timestamp,
}) {}

/** Candidates have already passed current-source checks; compose lookup with recallMemory. */
export class SemanticQueryResult extends Schema.Class<SemanticQueryResult>(
  "@effect-agent/capabilities/SemanticQueryResult",
)({
  lookup: MemoryLookup,
  scannedChunks: Schema.Natural,
  staleExcluded: Schema.Natural,
  unauthorizedExcluded: Schema.Natural,
  queryBytes: Schema.Natural,
  inputTokens: InputTokens,
  startedAt: Timestamp,
  finishedAt: Timestamp,
}) {}

const utf8 = (text: string): Uint8Array => {
  const hex = Encoding.encodeHex(text);
  return Uint8Array.from({ length: hex.length / 2 }, (_, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
};
const byteLength = (text: string): number => Encoding.encodeHex(text).length / 2;
const invalid = (operation: string) =>
  SemanticMemoryError.make({ operation, reason: "invalid-input" });
const asSource = (document: MemoryDocument): MemoryIndexSource =>
  MemoryIndexSource.make({
    key: document.key,
    source: document.source,
    sourceGeneration: document.generation,
  });
const sameSource = (left: MemoryDocument, right: MemoryDocument): boolean =>
  left.key.namespace === right.key.namespace &&
  left.key.id === right.key.id &&
  left.generation === right.generation &&
  left.source.revision === right.source.revision &&
  left.source.locator === right.source.locator &&
  left._tag === right._tag;

const readDocument = Effect.fn("semanticMemory.readDocument")(function* (key: MemoryKey) {
  const reader = yield* MemoryReader;
  const document = yield* reader.get(key).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.NullOr(MemoryDocument))),
    Effect.catchTag("SchemaError", () =>
      Effect.fail(
        MemoryStorageError.make({ operation: "validate semantic source", reason: "corrupt" }),
      ),
    ),
  );
  if (
    document !== null &&
    (document.key.namespace !== key.namespace ||
      document.key.id !== key.id ||
      document.source.id !== key.id)
  ) {
    return yield* MemoryStorageError.make({
      operation: "validate semantic source identity",
      reason: "corrupt",
    });
  }
  return document;
});

const embeddings = Effect.fn("semanticMemory.embeddings")(function* (
  inputs: ReadonlyArray<string>,
  profile: SemanticMemoryProfile,
) {
  const model = yield* EmbeddingModel.EmbeddingModel;
  const response = yield* model.embedMany(inputs).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(EmbeddingModel.EmbedManyResponse)),
    Effect.catchTag("SchemaError", () =>
      Effect.fail(
        SemanticMemoryError.make({ operation: "decode embeddings", reason: "invalid-embedding" }),
      ),
    ),
  );
  const tokens = response.usage.inputTokens;
  if (
    response.embeddings.length !== inputs.length ||
    (tokens !== undefined && (!Number.isSafeInteger(tokens) || tokens < 0)) ||
    response.embeddings.some(({ vector }) => {
      const norm = vector.reduce((sum, value) => sum + value * value, 0);
      return vector.length !== profile.dimensions || norm <= 0 || !Number.isFinite(norm);
    })
  ) {
    return yield* SemanticMemoryError.make({
      operation: "validate embeddings",
      reason: "invalid-embedding",
    });
  }
  return response;
});

const chunkText = Effect.fn("semanticMemory.chunkText")(function* (
  text: string,
  profile: SemanticMemoryProfile,
  limits: SemanticIndexLimits,
) {
  if (byteLength(text) > limits.maxSourceBytes) {
    return yield* SemanticMemoryError.make({ operation: "chunk source", reason: "budget" });
  }
  const chunks: Array<{
    readonly ordinal: number;
    readonly startByte: number;
    readonly endByte: number;
    readonly text: string;
  }> = [];
  let current = "";
  let startByte = 0;
  let currentBytes = 0;
  for (const codepoint of text) {
    const size = byteLength(codepoint);
    if (currentBytes + size > profile.maxChunkBytes) {
      chunks.push({
        ordinal: chunks.length,
        startByte,
        endByte: startByte + currentBytes,
        text: current,
      });
      startByte += currentBytes;
      current = "";
      currentBytes = 0;
      if (chunks.length >= limits.maxChunks) {
        return yield* SemanticMemoryError.make({ operation: "chunk source", reason: "budget" });
      }
    }
    current += codepoint;
    currentBytes += size;
  }
  if (current.length > 0)
    chunks.push({
      ordinal: chunks.length,
      startByte,
      endByte: startByte + currentBytes,
      text: current,
    });
  if (chunks.length === 0) return yield* invalid("chunk empty source");
  return chunks;
});

/**
 * Rebuild one selected source through the native Effect EmbeddingModel. The host binds the
 * index profile to that provider's immutable model revision. A fresh Layer starts empty.
 * Every invocation rebuilds; the host owns source discovery, scheduling, and freshness policy.
 *
 * Chunking and embedding leave the last successful index intact. A final authoritative read
 * rejects a source changed during embedding, then replace atomically exchanges its chunks.
 * An independent source can still change between that read and replacement;
 * querySemanticMemory always revalidates and excludes such stale candidates.
 * No model or source text is attached to telemetry. Errors retain native provider/index types.
 */
export const indexMemorySource = Effect.fn("indexMemorySource")(function* (
  key: MemoryKey,
  limits: SemanticIndexLimits,
) {
  const checkedKey = yield* Schema.decodeUnknownEffect(MemoryKey)(key).pipe(
    Effect.mapError(() => invalid("index key")),
  );
  const checkedLimits = yield* Schema.decodeUnknownEffect(SemanticIndexLimits)(limits).pipe(
    Effect.mapError(() => invalid("index limits")),
  );
  return yield* Effect.gen(function* () {
    const startedAt = yield* Clock.currentTimeMillis;
    const index = yield* SemanticMemoryIndex;
    const profile = yield* Schema.decodeUnknownEffect(SemanticMemoryProfile)(index.profile).pipe(
      Effect.mapError(() => invalid("index profile")),
    );
    const document = yield* readDocument(checkedKey);
    if (document === null || document._tag === "WithdrawnMemoryDocument") {
      if (document !== null) yield* index.withdraw(asSource(document));
      return SemanticIndexResult.make({
        key: checkedKey,
        status: document === null ? "Missing" : "Withdrawn",
        embeddedChunks: 0,
        embeddedBytes: 0,
        inputTokens: null,
        startedAt,
        finishedAt: yield* Clock.currentTimeMillis,
      });
    }
    const texts = yield* chunkText(document.content.text, profile, checkedLimits);
    const encodedProfile = yield* Schema.encodeEffect(Schema.fromJsonString(SemanticMemoryProfile))(
      profile,
    ).pipe(Effect.mapError(() => invalid("encode profile")));
    const crypto = yield* Crypto.Crypto;
    const fingerprint = yield* crypto.digest("SHA-256", utf8(encodedProfile)).pipe(
      Effect.map(Encoding.encodeHex),
      Effect.mapError(() =>
        SemanticMemoryError.make({ operation: "fingerprint profile", reason: "unavailable" }),
      ),
    );
    const response = yield* embeddings(
      texts.map((chunk) => chunk.text),
      profile,
    );
    const chunks: Array<SemanticMemoryChunk> = [];
    for (const text of texts) {
      const vector = response.embeddings[text.ordinal]?.vector;
      if (vector === undefined)
        return yield* SemanticMemoryError.make({
          operation: "embedding count",
          reason: "invalid-embedding",
        });
      chunks.push(
        SemanticMemoryChunk.make({
          ...text,
          passageId: `chunk:${fingerprint}:${text.ordinal}`,
          vector: [...vector],
        }),
      );
    }
    const latest = yield* readDocument(checkedKey);
    if (latest === null || !sameSource(document, latest)) {
      if (latest?._tag === "WithdrawnMemoryDocument") yield* index.withdraw(asSource(latest));
      return yield* SemanticMemoryError.make({
        operation: "publish current source",
        reason: "source-changed",
      });
    }
    yield* index.replace({ source: asSource(document), profile, chunks });
    return SemanticIndexResult.make({
      key: checkedKey,
      status: "Indexed",
      embeddedChunks: chunks.length,
      embeddedBytes: byteLength(document.content.text),
      inputTokens: response.usage.inputTokens ?? null,
      startedAt,
      finishedAt: yield* Clock.currentTimeMillis,
    });
  }).pipe(
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: checkedLimits.timeoutMillis,
      orElse: () =>
        Effect.fail(SemanticMemoryError.make({ operation: "index source", reason: "timeout" })),
    }),
  );
});

/**
 * Query an optional derivative index, then read each distinct source once. Attribution and
 * metadata always come from that current source. Revision, generation, namespace, access,
 * locator, and exact UTF-8 excerpt checks run before returning any passage. Older candidates
 * are omitted rather than assigning their similarity score to changed text.
 * Process candidates in source groups, retain only passage excerpts, and restore index rank.
 * maxSourceBytes bounds aggregate UTF-8 JSON for distinct authorized sources with matching
 * generation/revision/locator candidates. Its default is 16 MiB; exhaustion returns no partial
 * result. Reader allocation, decoding, and one source serialization precede this bound.
 *
 * Compose result.lookup through recallMemory to enforce the final rendered item/byte/token
 * budget and overall deadline. An empty result says nothing about undiscovered sources.
 * Checks begun before an acknowledged correction/withdrawal may finish with their already
 * captured source view.
 */
export const querySemanticMemory = Effect.fn("querySemanticMemory")(function* (
  query: string,
  access: MemoryAccess,
  limits: SemanticQueryLimits,
) {
  const checkedQuery = yield* Schema.decodeUnknownEffect(Schema.NonEmptyString)(query).pipe(
    Effect.mapError(() => invalid("query text")),
  );
  const checkedAccess = yield* Schema.decodeUnknownEffect(MemoryAccess)(access).pipe(
    Effect.mapError(() => invalid("query access")),
  );
  const checkedLimits = yield* Schema.decodeUnknownEffect(SemanticQueryLimits)(limits).pipe(
    Effect.mapError(() => invalid("query limits")),
  );
  const queryBytes = byteLength(checkedQuery);
  if (queryBytes > checkedLimits.maxQueryBytes)
    return yield* SemanticMemoryError.make({ operation: "query bytes", reason: "budget" });
  return yield* Effect.gen(function* () {
    const startedAt = yield* Clock.currentTimeMillis;
    const index = yield* SemanticMemoryIndex;
    const profile = yield* Schema.decodeUnknownEffect(SemanticMemoryProfile)(index.profile).pipe(
      Effect.mapError(() => invalid("query profile")),
    );
    const response = yield* embeddings([checkedQuery], profile);
    const vector = response.embeddings[0]?.vector;
    if (vector === undefined)
      return yield* SemanticMemoryError.make({
        operation: "query embedding count",
        reason: "invalid-embedding",
      });
    const found = yield* index
      .search(
        MemoryIndexQuery.make({
          namespace: checkedAccess.namespace,
          vector,
          limit: checkedLimits.maxCandidates,
          minScore: checkedLimits.minScore,
          maxScannedChunks: checkedLimits.maxScannedChunks,
        }),
      )
      .pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(MemoryIndexSearch)),
        Effect.catchTag("SchemaError", () =>
          Effect.fail(
            MemoryIndexError.make({ operation: "validate candidates", reason: "corrupt" }),
          ),
        ),
      );
    if (
      found.candidates.length > checkedLimits.maxCandidates ||
      found.scannedChunks > checkedLimits.maxScannedChunks ||
      found.scannedChunks < found.candidates.length
    ) {
      return yield* MemoryIndexError.make({
        operation: "validate candidate bounds",
        reason: "corrupt",
      });
    }
    const groups = new Map<
      string,
      {
        readonly key: MemoryKey;
        readonly candidates: Array<{
          readonly rank: number;
          readonly candidate: MemoryIndexCandidate;
        }>;
      }
    >();
    const rankedPassages: Array<{ readonly rank: number; readonly passage: MemoryPassage }> = [];
    let staleExcluded = 0;
    let unauthorizedExcluded = 0;
    for (const [rank, candidate] of found.candidates.entries()) {
      if (
        candidate.key.namespace !== checkedAccess.namespace ||
        candidate.source.id !== candidate.key.id ||
        candidate.score < checkedLimits.minScore
      ) {
        return yield* MemoryIndexError.make({
          operation: "validate candidate identity",
          reason: "corrupt",
        });
      }
      const group = groups.get(candidate.key.id);
      if (group === undefined) {
        groups.set(candidate.key.id, { key: candidate.key, candidates: [{ rank, candidate }] });
      } else group.candidates.push({ rank, candidate });
    }
    const maxSourceBytes = checkedLimits.maxSourceBytes ?? 16_777_216;
    let sourceBytes = 0;
    for (const group of groups.values()) {
      yield* Effect.yieldNow;
      const document = yield* readDocument(group.key);
      if (document === null || document._tag === "WithdrawnMemoryDocument") {
        staleExcluded += group.candidates.length;
        continue;
      }
      if (!document.scopes.includes(checkedAccess.scope)) {
        unauthorizedExcluded += group.candidates.length;
        continue;
      }
      const current = group.candidates.filter(
        ({ candidate }) =>
          document.generation === candidate.sourceGeneration &&
          document.source.revision === candidate.source.revision &&
          document.source.locator === candidate.source.locator,
      );
      staleExcluded += group.candidates.length - current.length;
      if (current.length === 0) continue;
      const encoded = JSON.stringify(document);
      const remainingSourceBytes = maxSourceBytes - sourceBytes;
      const encodedBytes = encoded.length <= remainingSourceBytes ? byteLength(encoded) : undefined;
      if (encodedBytes === undefined || encodedBytes > remainingSourceBytes) {
        return yield* SemanticMemoryError.make({
          operation: "query source bytes",
          reason: "budget",
        });
      }
      sourceBytes += encodedBytes;
      const encodedDocument = Encoding.encodeHex(document.content.text);
      for (const { rank, candidate } of current) {
        if (!sameExcerpt(encodedDocument, candidate, profile.maxChunkBytes)) {
          staleExcluded += 1;
          continue;
        }
        rankedPassages.push({
          rank,
          passage: MemoryPassage.make({
            version: 1,
            authority: checkedAccess.namespace,
            source: document.source,
            passageId: candidate.passageId,
            content: { ...document.content, text: candidate.text },
          }),
        });
      }
    }
    const passages = rankedPassages
      .sort((left, right) => left.rank - right.rank)
      .map(({ passage }) => passage);
    return SemanticQueryResult.make({
      lookup: passages.length === 0 ? { _tag: "NoMatch" } : { _tag: "Found", passages },
      scannedChunks: found.scannedChunks,
      staleExcluded,
      unauthorizedExcluded,
      queryBytes,
      inputTokens: response.usage.inputTokens ?? null,
      startedAt,
      finishedAt: yield* Clock.currentTimeMillis,
    });
  }).pipe(
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: checkedLimits.timeoutMillis,
      orElse: () =>
        Effect.fail(SemanticMemoryError.make({ operation: "query memory", reason: "timeout" })),
    }),
  );
});

const sameExcerpt = (
  encodedDocument: string,
  candidate: {
    readonly startByte: number;
    readonly endByte: number;
    readonly text: string;
  },
  maxChunkBytes: number,
): boolean => {
  const hex = Encoding.encodeHex(candidate.text);
  return (
    candidate.startByte < candidate.endByte &&
    hex.length / 2 === candidate.endByte - candidate.startByte &&
    hex.length / 2 <= maxChunkBytes &&
    encodedDocument.slice(candidate.startByte * 2, candidate.endByte * 2) === hex
  );
};
