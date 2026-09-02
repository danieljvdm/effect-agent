import { Effect, Encoding, Schema } from "effect";

import type { MemoryKey } from "./memory-lifecycle.ts";
import { MemoryDocument, MemoryReader, MemoryStorageError } from "./memory-lifecycle.ts";
import { MemoryAccess } from "./memory-revalidation.ts";
import { MemoryLookup, MemoryPassage } from "./memory.ts";
import type { MemoryIndexCandidate } from "./semantic-memory.ts";
import { MemoryIndexError, MemoryIndexSearch, SemanticMemoryProfile } from "./semantic-memory.ts";

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

export class SemanticCandidateLimits extends Schema.Class<SemanticCandidateLimits>(
  "@effect-agent/core/SemanticCandidateLimits",
)({
  maxCandidates: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
  maxScannedChunks: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
  maxSourceBytes: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 67_108_864 })),
  ),
  /** Aggregate UTF-8 JSON passage bytes, including repeated provenance; defaults to 16 MiB. */
  maxOutputBytes: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 67_108_864 })),
  ),
  minScore: Schema.Finite.check(Schema.isBetween({ minimum: -1, maximum: 1 })),
}) {}

export class SemanticCandidateResult extends Schema.Class<SemanticCandidateResult>(
  "@effect-agent/core/SemanticCandidateResult",
)({
  lookup: MemoryLookup,
  scannedChunks: Schema.Natural,
  staleExcluded: Schema.Natural,
  unauthorizedExcluded: Schema.Natural,
}) {}

const byteLength = (text: string): number => Encoding.encodeHex(text).length / 2;
const readDocument = Effect.fn("semanticCandidates.readDocument")(function* (key: MemoryKey) {
  const reader = yield* MemoryReader;
  const document = yield* reader.get(key).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.NullOr(MemoryDocument.Wire))),
    Effect.catchTag("SchemaError", () =>
      Effect.fail(
        MemoryStorageError.make({ operation: "validate semantic source", reason: "corrupt" }),
      ),
    ),
  );
  if (
    document !== null &&
    (document.key.namespace.address !== key.namespace.address ||
      document.key.id !== key.id ||
      document.source.id !== key.id)
  )
    return yield* MemoryStorageError.make({
      operation: "validate semantic source identity",
      reason: "corrupt",
    });
  return document;
});

/**
 * Revalidate one ranked batch locally. No embedding or index access; stale scored text is excluded.
 * Charge each accepted passage's full JSON encoding before retaining it, including duplicates and
 * authoritative metadata/attribution. maxOutputBytes bounds their aggregate, not source reads or
 * a transport envelope. One passage is constructed at a time before this check.
 */
export const revalidateSemanticMemoryCandidates = Effect.fn("revalidateSemanticMemoryCandidates")(
  function* (
    found: MemoryIndexSearch,
    access: MemoryAccess,
    profile: SemanticMemoryProfile,
    limits: SemanticCandidateLimits,
  ) {
    const input = yield* Schema.decodeUnknownEffect(
      Schema.Struct({
        found: MemoryIndexSearch.Wire,
        access: MemoryAccess.Wire,
        profile: SemanticMemoryProfile,
        limits: SemanticCandidateLimits,
      }),
    )({ found, access, profile, limits }).pipe(
      Effect.mapError(() =>
        MemoryIndexError.make({ operation: "validate candidates", reason: "corrupt" }),
      ),
    );
    found = input.found;
    access = input.access;
    profile = input.profile;
    limits = input.limits;
    if (
      found.candidates.length > limits.maxCandidates ||
      found.scannedChunks > limits.maxScannedChunks ||
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
        candidate.key.namespace.address !== access.namespace.address ||
        candidate.source.id !== candidate.key.id ||
        candidate.score < limits.minScore
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
    const maxSourceBytes = limits.maxSourceBytes ?? 16_777_216;
    const maxOutputBytes = limits.maxOutputBytes ?? 16_777_216;
    let sourceBytes = 0;
    let outputBytes = 0;
    for (const group of groups.values()) {
      yield* Effect.yieldNow;
      const document = yield* readDocument(group.key);
      if (document === null || document._tag === "WithdrawnMemoryDocument") {
        staleExcluded += group.candidates.length;
        continue;
      }
      if (!document.scopes.includes(access.scope)) {
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
        const passage = MemoryPassage.make({
          version: 1,
          authority: access.namespace.address,
          source: document.source,
          passageId: candidate.passageId,
          content: { ...document.content, text: candidate.text },
        });
        const encodedPassage = JSON.stringify(passage);
        const remainingOutputBytes = maxOutputBytes - outputBytes;
        const passageBytes =
          encodedPassage.length <= remainingOutputBytes ? byteLength(encodedPassage) : undefined;
        if (passageBytes === undefined || passageBytes > remainingOutputBytes) {
          return yield* SemanticMemoryError.make({
            operation: "query output bytes",
            reason: "budget",
          });
        }
        outputBytes += passageBytes;
        rankedPassages.push({ rank, passage });
      }
    }
    const passages = rankedPassages
      .sort((left, right) => left.rank - right.rank)
      .map(({ passage }) => passage);

    return SemanticCandidateResult.make({
      lookup: passages.length === 0 ? { _tag: "NoMatch" } : { _tag: "Found", passages },
      scannedChunks: found.scannedChunks,
      staleExcluded,
      unauthorizedExcluded,
    });
  },
);

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
