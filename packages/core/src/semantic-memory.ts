import type { Effect } from "effect";
import { Context, Schema } from "effect";

import { ActiveMemoryDocument, MemoryKey } from "./memory-lifecycle.ts";
import { MemoryPassage } from "./memory.ts";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const Positive = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
const Timestamp = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const Vector = Schema.Array(Schema.Finite).check(Schema.isMinLength(1), Schema.isMaxLength(4_096));

/** A host declaration of the exact embedding space and deterministic chunking policy. */
export class SemanticMemoryProfile extends Schema.Class<SemanticMemoryProfile>(
  "@effect-agent/core/SemanticMemoryProfile",
)({
  version: Schema.Literal(1),
  provider: Identity,
  model: Identity,
  modelRevision: Identity,
  dimensions: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4_096 })),
  chunker: Schema.Literal("utf8-codepoint@1"),
  maxChunkBytes: Schema.Int.check(Schema.isBetween({ minimum: 4, maximum: 8_192 })),
  distance: Schema.Literal("cosine"),
}) {}

export class SemanticMemoryChunk extends Schema.Class<SemanticMemoryChunk>(
  "@effect-agent/core/SemanticMemoryChunk",
)({
  passageId: MemoryPassage.fields.passageId,
  ordinal: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 255 })),
  startByte: Schema.Natural,
  endByte: Schema.Natural,
  text: Schema.NonEmptyString.check(Schema.isMaxLength(8_192)),
  vector: Vector,
}) {}

const SourceFields = {
  key: MemoryKey,
  source: ActiveMemoryDocument.fields.source,
  sourceGeneration: Positive,
};

export class MemoryIndexSource extends Schema.Class<MemoryIndexSource>(
  "@effect-agent/core/MemoryIndexSource",
)(SourceFields) {}

/** A complete replacement, prepared before touching the disposable index. */
export class MemoryIndexReplacement extends Schema.Class<MemoryIndexReplacement>(
  "@effect-agent/core/MemoryIndexReplacement",
)({
  source: MemoryIndexSource,
  profile: SemanticMemoryProfile,
  chunks: Schema.Array(SemanticMemoryChunk).check(Schema.isMinLength(1), Schema.isMaxLength(256)),
}) {}

export class MemoryIndexCandidate extends Schema.Class<MemoryIndexCandidate>(
  "@effect-agent/core/MemoryIndexCandidate",
)({
  ...SourceFields,
  passageId: SemanticMemoryChunk.fields.passageId,
  ordinal: SemanticMemoryChunk.fields.ordinal,
  startByte: SemanticMemoryChunk.fields.startByte,
  endByte: SemanticMemoryChunk.fields.endByte,
  text: SemanticMemoryChunk.fields.text,
  score: Schema.Finite.check(Schema.isBetween({ minimum: -1, maximum: 1 })),
  indexedAt: Timestamp,
}) {}

export class MemoryIndexQuery extends Schema.Class<MemoryIndexQuery>(
  "@effect-agent/core/MemoryIndexQuery",
)({
  namespace: MemoryKey.fields.namespace,
  vector: Vector,
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
  minScore: Schema.Finite.check(Schema.isBetween({ minimum: -1, maximum: 1 })),
  maxScannedChunks: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
}) {}

export class MemoryIndexSearch extends Schema.Class<MemoryIndexSearch>(
  "@effect-agent/core/MemoryIndexSearch",
)({
  candidates: Schema.Array(MemoryIndexCandidate).check(Schema.isMaxLength(128)),
  scannedChunks: Schema.Natural,
}) {}

export class MemoryIndexError extends Schema.TaggedError<MemoryIndexError>()("MemoryIndexError", {
  operation: Identity,
  reason: Schema.Literals([
    "invalid-input",
    "unavailable",
    "corrupt",
    "incompatible",
    "fenced",
    "budget",
  ]),
}) {}

/**
 * Optional disposable ranking index with one fixed embedding/chunking profile.
 * replace validates a complete replacement before atomically exchanging chunks. Failure leaves
 * the last successful index intact. Older generations and divergent same-generation source
 * identities are fenced; refreshing the same source is allowed. Source IDs must equal key IDs.
 * withdraw retains a terminal tombstone that fences every later replacement for that key.
 *
 * Vectors match the profile dimensions and have positive finite squared norms. Chunks form a
 * nonempty contiguous ordinal/byte sequence starting at zero, with unique passage IDs and exact
 * UTF-8 byte ranges bounded by maxChunkBytes. Implementations snapshot mutable caller values.
 *
 * Bounded adapters count every source key, including tombstones, against capacity. Replacement
 * capacity is measured after removing the old chunks. Search rejects scans over maxScannedChunks
 * instead of ranking a truncated prefix. scannedChunks counts all indexed chunks in the namespace
 * before threshold/limit. Ties sort by source ID, revision, then ordinal in UTF-16 code-unit order.
 * Scope close clears the index and makes captured methods fail unavailable.
 *
 * Search is ranking only: callers MUST recheck authoritative revision, access, and withdrawal.
 * This port tracks no builds, extraction progress, or corpus completeness. The host owns source
 * discovery, refresh scheduling, and required freshness.
 */
export class SemanticMemoryIndex extends Context.Service<
  SemanticMemoryIndex,
  {
    readonly profile: SemanticMemoryProfile;
    readonly replace: (request: MemoryIndexReplacement) => Effect.Effect<void, MemoryIndexError>;
    readonly withdraw: (source: MemoryIndexSource) => Effect.Effect<void, MemoryIndexError>;
    readonly search: (
      query: MemoryIndexQuery,
    ) => Effect.Effect<MemoryIndexSearch, MemoryIndexError>;
  }
>()("@effect-agent/core/SemanticMemoryIndex") {}
