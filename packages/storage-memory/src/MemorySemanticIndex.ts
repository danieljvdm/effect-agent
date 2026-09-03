import { MemoryKey } from "@effect-agent/core/MemoryStore";
import {
  MemoryIndexCandidate,
  MemoryIndexError,
  MemoryIndexQuery,
  MemoryIndexReplacement,
  MemoryIndexSearch,
  MemoryIndexSource,
  SemanticMemoryChunk,
  SemanticMemoryIndex,
  SemanticMemoryProfile,
} from "@effect-agent/core/SemanticMemoryIndex";
import { Clock, Effect, Encoding, Layer, Ref, Schema } from "effect";

const PositiveCapacity = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 }));
const MaxStoredVectorComponents = 16_777_216;

/**
 * Hard per-Layer bounds for disposable semantic index state. maxChunks times profile dimensions
 * must not exceed 16,777,216 vector components. maxSourceBytes bounds the aggregate UTF-8 JSON
 * of retained source identities and defaults to 16 MiB; it is not a general heap limit.
 */
export class InMemorySemanticIndexCapacity extends Schema.Class<InMemorySemanticIndexCapacity>(
  "@effect-agent/storage-memory/InMemorySemanticIndexCapacity",
)({
  maxSources: PositiveCapacity,
  maxChunks: PositiveCapacity,
  maxSourceBytes: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 67_108_864 })),
  ),
}) {}

type StoredEntry = {
  readonly source: MemoryIndexSource;
  readonly sourceBytes: number;
} & (
  | {
      readonly _tag: "Indexed";
      readonly chunks: ReadonlyArray<SemanticMemoryChunk>;
      readonly indexedAt: number;
    }
  | { readonly _tag: "Withdrawn" }
);

interface IndexData {
  readonly closed: boolean;
  readonly entries: ReadonlyMap<string, StoredEntry>;
  readonly sourceBytes: number;
}

const sameProfile = Schema.toEquivalence(SemanticMemoryProfile);
const sameSource = Schema.toEquivalence(MemoryIndexSource.Wire);

const error = (operation: string, reason: MemoryIndexError["reason"]): MemoryIndexError =>
  MemoryIndexError.make({ operation, reason });

const keyString = (key: MemoryKey): string => JSON.stringify([key.namespace.address, key.id]);

const sourceIdentityBytes = (source: MemoryIndexSource): number =>
  Encoding.encodeHex(JSON.stringify(source)).length / 2;

const decodeBoundary = Effect.fn("InMemorySemanticIndex.decodeBoundary")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  value: unknown,
  operation: string,
): Effect.fn.Return<A, MemoryIndexError> {
  return yield* Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.flatMap((decoded) => Schema.encodeEffect(schema)(decoded).pipe(Effect.as(decoded))),
    Effect.mapError(() => error(operation, "invalid-input")),
  );
});

const freezeSource = (source: MemoryIndexSource): MemoryIndexSource =>
  Object.freeze(
    MemoryIndexSource.make({
      key: Object.freeze(
        MemoryKey.make({
          ...source.key,
          namespace: Object.freeze({ address: source.key.namespace.address }),
        }),
      ),
      source: Object.freeze({ ...source.source }),
      sourceGeneration: source.sourceGeneration,
    }),
  );

const freezeChunk = (chunk: SemanticMemoryChunk): SemanticMemoryChunk =>
  Object.freeze(SemanticMemoryChunk.make({ ...chunk, vector: Object.freeze([...chunk.vector]) }));

const sourceIsFenced = (source: MemoryIndexSource, existing: StoredEntry): boolean =>
  source.sourceGeneration < existing.source.sourceGeneration ||
  (source.sourceGeneration === existing.source.sourceGeneration &&
    !sameSource(source, existing.source));

const squaredNorm = (vector: ReadonlyArray<number>): number | null => {
  let sum = 0;

  for (const value of vector) {
    sum += value * value;
    if (!Number.isFinite(sum)) return null;
  }

  return sum > 0 ? sum : null;
};

const validVector = (vector: ReadonlyArray<number>, profile: SemanticMemoryProfile): boolean =>
  vector.length === profile.dimensions && squaredNorm(vector) !== null;

const validateChunks = Effect.fn("InMemorySemanticIndex.validateChunks")(function* (
  chunks: ReadonlyArray<SemanticMemoryChunk>,
  profile: SemanticMemoryProfile,
  operation: string,
): Effect.fn.Return<void, MemoryIndexError> {
  let nextByte = 0;
  const passageIds = new Set<string>();

  for (let index = 0; index < chunks.length; index++) {
    const chunk = chunks[index];
    const byteLength = Encoding.encodeHex(chunk.text).length / 2;

    if (
      chunk.ordinal !== index ||
      chunk.startByte !== nextByte ||
      chunk.endByte <= chunk.startByte ||
      chunk.endByte - chunk.startByte !== byteLength ||
      byteLength > profile.maxChunkBytes ||
      passageIds.has(chunk.passageId) ||
      !validVector(chunk.vector, profile)
    ) {
      return yield* error(operation, "invalid-input");
    }
    passageIds.add(chunk.passageId);
    nextByte = chunk.endByte;
  }
});

const cosine = (left: ReadonlyArray<number>, right: ReadonlyArray<number>): number => {
  const leftNorm = Math.sqrt(squaredNorm(left) ?? 1);
  const rightNorm = Math.sqrt(squaredNorm(right) ?? 1);
  let score = 0;

  for (let index = 0; index < left.length; index++) {
    score += (left[index] / leftNorm) * (right[index] / rightNorm);
  }
  const bounded = Math.max(-1, Math.min(1, score));

  return bounded === 0 ? 0 : bounded;
};

const compareText = (left: string, right: string): number =>
  left < right ? -1 : left > right ? 1 : 0;

const makeIndex = Effect.fn("InMemorySemanticIndex.make")(function* (
  rawProfile: SemanticMemoryProfile,
  rawCapacity: InMemorySemanticIndexCapacity,
) {
  const profile = Object.freeze(
    SemanticMemoryProfile.make({
      ...(yield* decodeBoundary(
        SemanticMemoryProfile,
        rawProfile,
        "configure semantic memory index",
      )),
    }),
  );

  const capacity = yield* decodeBoundary(
    InMemorySemanticIndexCapacity,
    rawCapacity,
    "configure semantic memory index",
  );

  if (capacity.maxChunks * profile.dimensions > MaxStoredVectorComponents) {
    return yield* error("configure semantic memory index", "invalid-input");
  }
  const maxSourceBytes = capacity.maxSourceBytes ?? 16_777_216;
  const data = yield* Ref.make<IndexData>({ closed: false, entries: new Map(), sourceBytes: 0 });

  yield* Effect.addFinalizer(() =>
    Ref.set(data, { closed: true, entries: new Map(), sourceBytes: 0 }),
  );

  const ensureOpen = Effect.fn("InMemorySemanticIndex.ensureOpen")(function* (operation: string) {
    if ((yield* Ref.get(data)).closed) return yield* error(operation, "unavailable");
  });

  const replace: SemanticMemoryIndex["Service"]["replace"] = Effect.fn(
    "InMemorySemanticIndex.replace",
  )(function* (rawRequest) {
    const operation = "replace semantic memory source";

    yield* ensureOpen(operation);
    const request = yield* decodeBoundary(MemoryIndexReplacement.Wire, rawRequest, operation);
    const source = freezeSource(request.source);
    const sourceBytes = sourceIdentityBytes(source);
    const chunks = Object.freeze(request.chunks.map(freezeChunk));

    if (source.source.id !== source.key.id) return yield* error(operation, "invalid-input");
    if (!sameProfile(request.profile, profile)) return yield* error(operation, "incompatible");
    yield* validateChunks(chunks, profile, operation);
    const indexedAt = yield* Clock.currentTimeMillis;

    const failure = yield* Ref.modify(
      data,
      (current): readonly [MemoryIndexError | undefined, IndexData] => {
        if (current.closed) return [error(operation, "unavailable"), current];
        const id = keyString(source.key);
        const existing = current.entries.get(id);

        if (
          existing !== undefined &&
          (existing._tag === "Withdrawn" || sourceIsFenced(source, existing))
        ) {
          return [error(operation, "fenced"), current];
        }
        if (existing === undefined && current.entries.size >= capacity.maxSources) {
          return [error(operation, "budget"), current];
        }
        const nextSourceBytes = current.sourceBytes - (existing?.sourceBytes ?? 0) + sourceBytes;

        if (nextSourceBytes > maxSourceBytes) return [error(operation, "budget"), current];
        let count = chunks.length;

        for (const [entryId, entry] of current.entries) {
          if (entryId !== id && entry._tag === "Indexed") count += entry.chunks.length;
        }
        if (count > capacity.maxChunks) return [error(operation, "budget"), current];
        const entries = new Map(current.entries);

        entries.set(id, { _tag: "Indexed", source, sourceBytes, chunks, indexedAt });

        return [undefined, { ...current, entries, sourceBytes: nextSourceBytes }];
      },
    );

    if (failure !== undefined) return yield* failure;
  });

  const withdraw: SemanticMemoryIndex["Service"]["withdraw"] = Effect.fn(
    "InMemorySemanticIndex.withdraw",
  )(function* (rawSource) {
    const operation = "withdraw semantic memory source";

    yield* ensureOpen(operation);

    const source = freezeSource(
      yield* decodeBoundary(MemoryIndexSource.Wire, rawSource, operation),
    );

    const sourceBytes = sourceIdentityBytes(source);

    if (source.source.id !== source.key.id) return yield* error(operation, "invalid-input");

    const failure = yield* Ref.modify(
      data,
      (current): readonly [MemoryIndexError | undefined, IndexData] => {
        if (current.closed) return [error(operation, "unavailable"), current];
        const id = keyString(source.key);
        const existing = current.entries.get(id);

        if (existing !== undefined) {
          if (existing._tag === "Withdrawn") {
            return [
              sameSource(source, existing.source) ? undefined : error(operation, "fenced"),
              current,
            ];
          }
          if (sourceIsFenced(source, existing)) return [error(operation, "fenced"), current];
        } else if (current.entries.size >= capacity.maxSources) {
          return [error(operation, "budget"), current];
        }
        const nextSourceBytes = current.sourceBytes - (existing?.sourceBytes ?? 0) + sourceBytes;

        if (nextSourceBytes > maxSourceBytes) return [error(operation, "budget"), current];
        const entries = new Map(current.entries);

        entries.set(id, { _tag: "Withdrawn", source, sourceBytes });

        return [undefined, { ...current, entries, sourceBytes: nextSourceBytes }];
      },
    );

    if (failure !== undefined) return yield* failure;
  });

  const search = Effect.fn("InMemorySemanticIndex.search")(function* (rawQuery: MemoryIndexQuery) {
    const operation = "search semantic memory index";

    yield* ensureOpen(operation);
    const query = yield* decodeBoundary(MemoryIndexQuery.Wire, rawQuery, operation);
    const vector = Object.freeze([...query.vector]);

    if (!validVector(vector, profile)) return yield* error(operation, "invalid-input");
    const current = yield* Ref.get(data);

    if (current.closed) return yield* error(operation, "unavailable");
    let scannedChunks = 0;
    let inspectedSources = 0;
    const candidates: Array<MemoryIndexCandidate> = [];

    for (const entry of current.entries.values()) {
      inspectedSources += 1;
      if (inspectedSources % 128 === 0) yield* Effect.yieldNow;
      if (
        entry.source.key.namespace.address !== query.namespace.address ||
        entry._tag !== "Indexed"
      )
        continue;
      scannedChunks += entry.chunks.length;
      if (scannedChunks > query.maxScannedChunks) return yield* error(operation, "budget");
    }
    for (const entry of current.entries.values()) {
      if (
        entry.source.key.namespace.address !== query.namespace.address ||
        entry._tag !== "Indexed"
      )
        continue;
      yield* Effect.yieldNow;
      for (const chunk of entry.chunks) {
        const score = cosine(vector, chunk.vector);

        if (score < query.minScore) continue;
        candidates.push(
          MemoryIndexCandidate.make({
            ...entry.source,
            passageId: chunk.passageId,
            ordinal: chunk.ordinal,
            startByte: chunk.startByte,
            endByte: chunk.endByte,
            text: chunk.text,
            score,
            indexedAt: entry.indexedAt,
          }),
        );
      }
    }
    candidates.sort(
      (left, right) =>
        right.score - left.score ||
        compareText(left.key.id, right.key.id) ||
        compareText(left.source.revision, right.source.revision) ||
        left.ordinal - right.ordinal,
    );
    yield* ensureOpen(operation);

    return MemoryIndexSearch.make({ candidates: candidates.slice(0, query.limit), scannedChunks });
  });

  return SemanticMemoryIndex.fromAdapter({ profile, replace, withdraw, search });
});

/** Scoped disposable semantic index. No persistent build or recovery state is retained. */
export const inMemorySemanticIndexLayer = (
  profile: SemanticMemoryProfile,
  capacity: InMemorySemanticIndexCapacity,
): Layer.Layer<SemanticMemoryIndex, MemoryIndexError> =>
  Layer.effect(SemanticMemoryIndex, makeIndex(profile, capacity));
