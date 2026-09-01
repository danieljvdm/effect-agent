import { Context, Effect, Layer, Schema } from "effect";

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

export class MemoryIndexBuild extends Schema.Class<MemoryIndexBuild>(
  "@effect-agent/core/MemoryIndexBuild",
)({
  ...SourceFields,
  profile: SemanticMemoryProfile,
  epoch: Positive,
}) {}

/** Disposable per-source progress, independent of committed-activity extraction progress. */
export class MemoryIndexState extends Schema.Class<MemoryIndexState>(
  "@effect-agent/core/MemoryIndexState",
)({
  version: Schema.Literal(1),
  ...SourceFields,
  epoch: Positive,
  status: Schema.Literals(["building", "ready", "withdrawn"]),
  chunkCount: Schema.Natural,
  indexedAt: Schema.NullOr(Timestamp),
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
  /** At least one registered source in this namespace has an unfinished build. */
  incomplete: Schema.Boolean,
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

export const MemoryIndexMutationPoint = Schema.Literals([
  "index:begin:before",
  "index:begin:after",
  "index:publish:before",
  "index:publish:after",
  "index:withdraw:before",
  "index:withdraw:after",
]);
export type MemoryIndexMutationPoint = typeof MemoryIndexMutationPoint.Type;

export class MemoryIndexMutationFailure extends Schema.TaggedError<MemoryIndexMutationFailure>()(
  "MemoryIndexMutationFailure",
  { point: MemoryIndexMutationPoint },
) {}

export class MemoryIndexFailpoint extends Context.Service<
  MemoryIndexFailpoint,
  {
    readonly hit: (
      point: MemoryIndexMutationPoint,
    ) => Effect.Effect<void, MemoryIndexMutationFailure>;
  }
>()("@effect-agent/core/MemoryIndexFailpoint") {
  static readonly layer = Layer.succeed(this, { hit: () => Effect.void });
}

/**
 * Replaceable derivative index. The profile is fixed for an instance; incompatible builds
 * must fail rather than mix vector spaces. begin allocates a newer epoch and hides obsolete
 * chunks. publish atomically exposes all chunks only for the current epoch and source version;
 * identical replay is idempotent, divergent replay fails. Withdraw is terminal for the key and
 * fences delayed publication. Older source generations cannot supersede newer ones.
 * Source IDs must equal key IDs. Equal generations with different revisions or locators are
 * fenced, and an exact withdrawal replay returns its original state.
 *
 * Vectors must match profile dimensions and have a positive finite squared norm. Published
 * chunks form one nonempty contiguous ordinal/byte sequence starting at zero, have unique
 * passage IDs, and each text's UTF-8 byte length equals its byte range and fits maxChunkBytes.
 * A publication contains at most 256 chunks. Ready state counts that complete array and records
 * publication time; building/withdrawn state has zero chunks and no indexedAt. Exact publication
 * replay keeps its original time. Implementations take snapshots rather than retain mutable
 * caller vectors.
 *
 * A bounded in-memory adapter counts ALL registered keys (including tombstones) against
 * maxSources and all ready chunks against maxChunks. begin removes prior chunks. Publication
 * over capacity leaves the build pending. Search rejects a scan over maxScannedChunks; it never
 * silently ranks a truncated prefix. scannedChunks counts all ready chunks in the namespace
 * before threshold/limit. Ties sort by key ID, source revision, then ordinal in UTF-16 code-unit
 * order. Scope close clears state and makes every captured method fail unavailable.
 *
 * Search is ranking only: callers MUST recheck authoritative revision, access, and withdrawal.
 * Source discovery, required freshness, and ranking thresholds belong to the host. Incomplete
 * describes registered builds, not undiscovered sources or a global freshness watermark.
 */
export class SemanticMemoryIndex extends Context.Service<
  SemanticMemoryIndex,
  {
    readonly profile: SemanticMemoryProfile;
    readonly begin: (
      source: MemoryIndexSource,
    ) => Effect.Effect<MemoryIndexBuild, MemoryIndexError | MemoryIndexMutationFailure>;
    readonly publish: (request: {
      readonly build: MemoryIndexBuild;
      readonly chunks: ReadonlyArray<SemanticMemoryChunk>;
    }) => Effect.Effect<MemoryIndexState, MemoryIndexError | MemoryIndexMutationFailure>;
    readonly withdraw: (
      source: MemoryIndexSource,
    ) => Effect.Effect<MemoryIndexState, MemoryIndexError | MemoryIndexMutationFailure>;
    readonly inspect: (key: MemoryKey) => Effect.Effect<MemoryIndexState | null, MemoryIndexError>;
    readonly search: (
      query: MemoryIndexQuery,
    ) => Effect.Effect<MemoryIndexSearch, MemoryIndexError>;
  }
>()("@effect-agent/core/SemanticMemoryIndex") {}
