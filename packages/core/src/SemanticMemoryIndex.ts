import { Context, Effect, Schema } from "effect";

import * as MemoryNamespace from "./MemoryNamespace.ts";
import { MemoryPassage } from "./MemoryReference.ts";
import { ActiveMemoryDocument, MemoryKey } from "./MemoryStore.ts";

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
  key: MemoryKey.Wire,
  source: ActiveMemoryDocument.Wire.fields.source,
  sourceGeneration: Positive,
};

class MemoryIndexSourceWire extends Schema.Class<MemoryIndexSourceWire>(
  "@effect-agent/core/MemoryIndexSource",
)(SourceFields) {}

export type MemoryIndexSource<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> = Omit<
  MemoryIndexSourceWire,
  "key"
> & { readonly key: MemoryKey<Namespace> };

export const MemoryIndexSource = {
  Wire: MemoryIndexSourceWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: MemoryIndexSource<Namespace>,
  ): MemoryIndexSource<Namespace> =>
    Object.assign(Schema.decodeUnknownSync(MemoryIndexSourceWire)(fields), {
      key: MemoryKey.make(fields.key),
    }),
};

/** A complete replacement, prepared before touching the disposable index. */
class MemoryIndexReplacementWire extends Schema.Class<MemoryIndexReplacementWire>(
  "@effect-agent/core/MemoryIndexReplacement",
)({
  source: MemoryIndexSource.Wire,
  profile: SemanticMemoryProfile,
  chunks: Schema.Array(SemanticMemoryChunk).check(Schema.isMinLength(1), Schema.isMaxLength(256)),
}) {}

export type MemoryIndexReplacement<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> =
  Omit<MemoryIndexReplacementWire, "source"> & { readonly source: MemoryIndexSource<Namespace> };

export const MemoryIndexReplacement = {
  Wire: MemoryIndexReplacementWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: MemoryIndexReplacement<Namespace>,
  ): MemoryIndexReplacement<Namespace> =>
    Object.assign(Schema.decodeUnknownSync(MemoryIndexReplacementWire)(fields), {
      source: fields.source,
    }),
};

class MemoryIndexCandidateWire extends Schema.Class<MemoryIndexCandidateWire>(
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

export type MemoryIndexCandidate<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> =
  Omit<MemoryIndexCandidateWire, "key"> & { readonly key: MemoryKey<Namespace> };

export const MemoryIndexCandidate = {
  Wire: MemoryIndexCandidateWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: MemoryIndexCandidate<Namespace>,
  ): MemoryIndexCandidate<Namespace> =>
    Object.assign(Schema.decodeUnknownSync(MemoryIndexCandidateWire)(fields), {
      key: MemoryKey.make(fields.key),
    }),
};

class MemoryIndexQueryWire extends Schema.Class<MemoryIndexQueryWire>(
  "@effect-agent/core/MemoryIndexQuery",
)({
  namespace: MemoryKey.Wire.fields.namespace,
  vector: Vector,
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
  minScore: Schema.Finite.check(Schema.isBetween({ minimum: -1, maximum: 1 })),
  maxScannedChunks: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
}) {}

export type MemoryIndexQuery<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> = Omit<
  MemoryIndexQueryWire,
  "namespace"
> & { readonly namespace: Namespace };

export const MemoryIndexQuery = {
  Wire: MemoryIndexQueryWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: MemoryIndexQuery<Namespace>,
  ): MemoryIndexQuery<Namespace> =>
    Object.assign(Schema.decodeUnknownSync(MemoryIndexQueryWire)(fields), {
      namespace: fields.namespace,
    }),
};

class MemoryIndexSearchWire extends Schema.Class<MemoryIndexSearchWire>(
  "@effect-agent/core/MemoryIndexSearch",
)({
  candidates: Schema.Array(MemoryIndexCandidate.Wire).check(Schema.isMaxLength(128)),
  scannedChunks: Schema.Natural,
}) {}

export type MemoryIndexSearch<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> = Omit<
  MemoryIndexSearchWire,
  "candidates"
> & { readonly candidates: ReadonlyArray<MemoryIndexCandidate<Namespace>> };

export const MemoryIndexSearch = {
  Wire: MemoryIndexSearchWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: MemoryIndexSearch<Namespace>,
  ): MemoryIndexSearch<Namespace> =>
    Object.assign(Schema.decodeUnknownSync(MemoryIndexSearchWire)(fields), {
      candidates: fields.candidates,
    }),
};

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
    readonly replace: <Namespace extends MemoryNamespace.Any>(
      request: MemoryIndexReplacement<Namespace>,
    ) => Effect.Effect<void, MemoryIndexError>;
    readonly withdraw: <Namespace extends MemoryNamespace.Any>(
      source: MemoryIndexSource<Namespace>,
    ) => Effect.Effect<void, MemoryIndexError>;
    readonly search: <Namespace extends MemoryNamespace.Any>(
      query: MemoryIndexQuery<Namespace>,
    ) => Effect.Effect<MemoryIndexSearch<Namespace>, MemoryIndexError>;
  }
>()("@effect-agent/core/SemanticMemoryIndex") {
  static fromAdapter(adapter: {
    readonly profile: SemanticMemoryProfile;
    readonly replace: (request: MemoryIndexReplacement) => Effect.Effect<void, MemoryIndexError>;
    readonly withdraw: (source: MemoryIndexSource) => Effect.Effect<void, MemoryIndexError>;
    readonly search: (
      query: MemoryIndexQuery,
    ) => Effect.Effect<MemoryIndexSearch, MemoryIndexError>;
  }): SemanticMemoryIndex["Service"] {
    return {
      ...adapter,
      search: Effect.fn("SemanticMemoryIndex.search")(function* <
        Namespace extends MemoryNamespace.Any,
      >(query: MemoryIndexQuery<Namespace>) {
        const result = yield* adapter.search(query);

        const checked = yield* Schema.decodeUnknownEffect(MemoryIndexSearch.Wire)(result).pipe(
          Effect.mapError(() =>
            MemoryIndexError.make({ operation: "restore index search", reason: "corrupt" }),
          ),
        );

        const candidates: Array<MemoryIndexCandidate<Namespace>> = [];

        for (const candidate of checked.candidates) {
          if (!MemoryNamespace.equals(candidate.key.namespace, query.namespace))
            return yield* MemoryIndexError.make({
              operation: "restore index namespace",
              reason: "corrupt",
            });
          candidates.push(
            MemoryIndexCandidate.make({
              ...candidate,
              key: MemoryKey.make({ ...candidate.key, namespace: query.namespace }),
            }),
          );
        }

        return MemoryIndexSearch.make({ ...checked, candidates });
      }),
    };
  }
}
