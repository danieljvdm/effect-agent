import {
  MemoryIndexBuild,
  MemoryIndexCandidate,
  MemoryIndexError,
  MemoryIndexFailpoint,
  MemoryIndexQuery,
  MemoryIndexSearch,
  MemoryIndexSource,
  MemoryIndexState,
  MemoryKey,
  SemanticMemoryChunk,
  SemanticMemoryIndex,
  SemanticMemoryProfile,
} from "@effect-agent/core";
import { Clock, Effect, Encoding, Layer, Ref, Schema } from "effect";

const PositiveCapacity = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 }));

/** Hard per-Layer bounds for disposable semantic index state. */
export class InMemorySemanticIndexCapacity extends Schema.Class<InMemorySemanticIndexCapacity>(
  "@effect-agent/storage-memory/InMemorySemanticIndexCapacity",
)({
  maxSources: PositiveCapacity,
  maxChunks: PositiveCapacity,
}) {}

const PublishRequest = Schema.Struct({
  build: MemoryIndexBuild,
  chunks: Schema.Array(SemanticMemoryChunk).check(Schema.isMinLength(1), Schema.isMaxLength(256)),
});

interface StoredEntry {
  readonly state: MemoryIndexState;
  readonly build: MemoryIndexBuild | null;
  readonly chunks: ReadonlyArray<SemanticMemoryChunk>;
}

interface IndexData {
  readonly closed: boolean;
  readonly entries: ReadonlyMap<string, StoredEntry>;
}

type Decision<A> =
  | { readonly _tag: "success"; readonly value: A; readonly changed: boolean }
  | { readonly _tag: "failure"; readonly error: MemoryIndexError };

const sameProfile = Schema.toEquivalence(SemanticMemoryProfile);
const sameBuild = Schema.toEquivalence(MemoryIndexBuild);
const sameChunks = Schema.toEquivalence(Schema.Array(SemanticMemoryChunk));

const sameSource = (left: MemoryIndexSource, right: MemoryIndexSource): boolean =>
  left.key.namespace === right.key.namespace &&
  left.key.id === right.key.id &&
  left.source.id === right.source.id &&
  left.source.locator === right.source.locator &&
  left.source.revision === right.source.revision &&
  left.sourceGeneration === right.sourceGeneration;

const error = (operation: string, reason: MemoryIndexError["reason"]): MemoryIndexError =>
  MemoryIndexError.make({ operation, reason });

const keyString = (key: MemoryKey): string => JSON.stringify([key.namespace, key.id]);

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

const freezeProfile = (profile: SemanticMemoryProfile): SemanticMemoryProfile =>
  Object.freeze(SemanticMemoryProfile.make({ ...profile }));

const freezeKey = (key: MemoryKey): MemoryKey => Object.freeze(MemoryKey.make({ ...key }));

const freezeSource = (source: MemoryIndexSource): MemoryIndexSource =>
  Object.freeze(
    MemoryIndexSource.make({
      key: freezeKey(source.key),
      source: Object.freeze({ ...source.source }),
      sourceGeneration: source.sourceGeneration,
    }),
  );

const freezeBuild = (build: MemoryIndexBuild, profile: SemanticMemoryProfile): MemoryIndexBuild => {
  const source = freezeSource(build);
  return Object.freeze(
    MemoryIndexBuild.make({
      ...source,
      profile,
      epoch: build.epoch,
    }),
  );
};

const freezeChunk = (chunk: SemanticMemoryChunk): SemanticMemoryChunk =>
  Object.freeze(
    SemanticMemoryChunk.make({
      ...chunk,
      vector: Object.freeze([...chunk.vector]),
    }),
  );

const freezeState = (state: MemoryIndexState): MemoryIndexState => {
  const source = freezeSource(state);
  return Object.freeze(
    MemoryIndexState.make({
      ...source,
      version: 1,
      epoch: state.epoch,
      status: state.status,
      chunkCount: state.chunkCount,
      indexedAt: state.indexedAt,
    }),
  );
};

const sourceIdentityIsValid = (source: MemoryIndexSource): boolean =>
  source.source.id === source.key.id;

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

const readyChunkCount = (entries: ReadonlyMap<string, StoredEntry>): number => {
  let count = 0;
  for (const entry of entries.values()) {
    if (entry.state.status === "ready") count += entry.chunks.length;
  }
  return count;
};

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

const resolve = <A>(decision: Decision<A>): Effect.Effect<A, MemoryIndexError> =>
  decision._tag === "failure" ? Effect.fail(decision.error) : Effect.succeed(decision.value);

const makeIndex = Effect.fn("InMemorySemanticIndex.make")(function* (
  rawProfile: SemanticMemoryProfile,
  rawCapacity: InMemorySemanticIndexCapacity,
) {
  const profile = freezeProfile(
    yield* decodeBoundary(SemanticMemoryProfile, rawProfile, "configure semantic memory index"),
  );
  const capacity = yield* decodeBoundary(
    InMemorySemanticIndexCapacity,
    rawCapacity,
    "configure semantic memory index",
  );
  const failpoint = yield* MemoryIndexFailpoint;
  const data = yield* Ref.make<IndexData>({ closed: false, entries: new Map() });
  yield* Effect.addFinalizer(() => Ref.set(data, { closed: true, entries: new Map() }));

  const ensureOpen = Effect.fn("InMemorySemanticIndex.ensureOpen")(function* (
    operation: string,
  ): Effect.fn.Return<void, MemoryIndexError> {
    if ((yield* Ref.get(data)).closed) return yield* error(operation, "unavailable");
  });

  const begin: SemanticMemoryIndex["Service"]["begin"] = Effect.fn("InMemorySemanticIndex.begin")(
    function* (rawSource) {
      const operation = "begin semantic memory build";
      yield* ensureOpen(operation);
      const source = freezeSource(yield* decodeBoundary(MemoryIndexSource, rawSource, operation));
      if (!sourceIdentityIsValid(source)) return yield* error(operation, "invalid-input");
      yield* ensureOpen(operation);
      yield* failpoint.hit("index:begin:before");
      const decision = yield* Ref.modify(
        data,
        (current): readonly [Decision<MemoryIndexBuild>, IndexData] => {
          if (current.closed)
            return [{ _tag: "failure", error: error(operation, "unavailable") }, current];
          const id = keyString(source.key);
          const existing = current.entries.get(id);
          if (existing?.state.status === "withdrawn") {
            return [{ _tag: "failure", error: error(operation, "fenced") }, current];
          }
          if (existing !== undefined) {
            if (
              source.sourceGeneration < existing.state.sourceGeneration ||
              (source.sourceGeneration === existing.state.sourceGeneration &&
                !sameSource(source, existing.state))
            ) {
              return [{ _tag: "failure", error: error(operation, "fenced") }, current];
            }
          } else if (current.entries.size >= capacity.maxSources) {
            return [{ _tag: "failure", error: error(operation, "budget") }, current];
          }
          const epoch = (existing?.state.epoch ?? 0) + 1;
          if (!Number.isSafeInteger(epoch)) {
            return [{ _tag: "failure", error: error(operation, "budget") }, current];
          }
          const build = freezeBuild(MemoryIndexBuild.make({ ...source, profile, epoch }), profile);
          const state = freezeState(
            MemoryIndexState.make({
              version: 1,
              ...source,
              epoch,
              status: "building",
              chunkCount: 0,
              indexedAt: null,
            }),
          );
          const entries = new Map(current.entries);
          entries.set(id, { state, build, chunks: Object.freeze([]) });
          return [
            { _tag: "success", value: build, changed: true },
            { ...current, entries },
          ];
        },
      );
      const build = yield* resolve(decision);
      yield* failpoint.hit("index:begin:after");
      yield* ensureOpen(operation);
      return build;
    },
  );

  const publish: SemanticMemoryIndex["Service"]["publish"] = Effect.fn(
    "InMemorySemanticIndex.publish",
  )(function* (rawRequest) {
    const operation = "publish semantic memory build";
    yield* ensureOpen(operation);
    const decoded = yield* decodeBoundary(PublishRequest, rawRequest, operation);
    const build = freezeBuild(decoded.build, freezeProfile(decoded.build.profile));
    const chunks = Object.freeze(decoded.chunks.map(freezeChunk));
    if (!sourceIdentityIsValid(build) || !sameProfile(build.profile, profile)) {
      return yield* error(
        operation,
        sameProfile(build.profile, profile) ? "invalid-input" : "incompatible",
      );
    }
    yield* validateChunks(chunks, profile, operation);
    yield* ensureOpen(operation);
    yield* failpoint.hit("index:publish:before");
    const indexedAt = yield* Clock.currentTimeMillis;
    const decision = yield* Ref.modify(
      data,
      (current): readonly [Decision<MemoryIndexState>, IndexData] => {
        if (current.closed)
          return [{ _tag: "failure", error: error(operation, "unavailable") }, current];
        const id = keyString(build.key);
        const existing = current.entries.get(id);
        if (
          existing === undefined ||
          existing.build === null ||
          !sameBuild(existing.build, build)
        ) {
          return [{ _tag: "failure", error: error(operation, "fenced") }, current];
        }
        if (existing.state.status === "ready") {
          return sameChunks(existing.chunks, chunks)
            ? [{ _tag: "success", value: existing.state, changed: false }, current]
            : [{ _tag: "failure", error: error(operation, "fenced") }, current];
        }
        if (existing.state.status !== "building") {
          return [{ _tag: "failure", error: error(operation, "fenced") }, current];
        }
        if (readyChunkCount(current.entries) + chunks.length > capacity.maxChunks) {
          return [{ _tag: "failure", error: error(operation, "budget") }, current];
        }
        const state = freezeState(
          MemoryIndexState.make({
            version: 1,
            key: build.key,
            source: build.source,
            sourceGeneration: build.sourceGeneration,
            epoch: build.epoch,
            status: "ready",
            chunkCount: chunks.length,
            indexedAt,
          }),
        );
        const entries = new Map(current.entries);
        entries.set(id, { state, build, chunks });
        return [
          { _tag: "success", value: state, changed: true },
          { ...current, entries },
        ];
      },
    );
    const state = yield* resolve(decision);
    if (decision._tag === "success" && decision.changed) {
      yield* failpoint.hit("index:publish:after");
    }
    yield* ensureOpen(operation);
    return state;
  });

  const withdraw: SemanticMemoryIndex["Service"]["withdraw"] = Effect.fn(
    "InMemorySemanticIndex.withdraw",
  )(function* (rawSource) {
    const operation = "withdraw semantic memory source";
    yield* ensureOpen(operation);
    const source = freezeSource(yield* decodeBoundary(MemoryIndexSource, rawSource, operation));
    if (!sourceIdentityIsValid(source)) return yield* error(operation, "invalid-input");
    yield* ensureOpen(operation);
    yield* failpoint.hit("index:withdraw:before");
    const decision = yield* Ref.modify(
      data,
      (current): readonly [Decision<MemoryIndexState>, IndexData] => {
        if (current.closed)
          return [{ _tag: "failure", error: error(operation, "unavailable") }, current];
        const id = keyString(source.key);
        const existing = current.entries.get(id);
        if (existing === undefined && current.entries.size >= capacity.maxSources) {
          return [{ _tag: "failure", error: error(operation, "budget") }, current];
        }
        if (existing !== undefined) {
          if (existing.state.status === "withdrawn") {
            return sameSource(existing.state, source)
              ? [{ _tag: "success", value: existing.state, changed: false }, current]
              : [{ _tag: "failure", error: error(operation, "fenced") }, current];
          }
          if (
            source.sourceGeneration < existing.state.sourceGeneration ||
            (source.sourceGeneration === existing.state.sourceGeneration &&
              !sameSource(source, existing.state))
          ) {
            return [{ _tag: "failure", error: error(operation, "fenced") }, current];
          }
        }
        const epoch = (existing?.state.epoch ?? 0) + 1;
        if (!Number.isSafeInteger(epoch)) {
          return [{ _tag: "failure", error: error(operation, "budget") }, current];
        }
        const state = freezeState(
          MemoryIndexState.make({
            version: 1,
            ...source,
            epoch,
            status: "withdrawn",
            chunkCount: 0,
            indexedAt: null,
          }),
        );
        const entries = new Map(current.entries);
        entries.set(id, { state, build: null, chunks: Object.freeze([]) });
        return [
          { _tag: "success", value: state, changed: true },
          { ...current, entries },
        ];
      },
    );
    const state = yield* resolve(decision);
    if (decision._tag === "success" && decision.changed) {
      yield* failpoint.hit("index:withdraw:after");
    }
    yield* ensureOpen(operation);
    return state;
  });

  const inspect: SemanticMemoryIndex["Service"]["inspect"] = Effect.fn(
    "InMemorySemanticIndex.inspect",
  )(function* (rawKey) {
    const operation = "inspect semantic memory index";
    yield* ensureOpen(operation);
    const key = freezeKey(yield* decodeBoundary(MemoryKey, rawKey, operation));
    yield* ensureOpen(operation);
    const state = (yield* Ref.get(data)).entries.get(keyString(key))?.state ?? null;
    yield* ensureOpen(operation);
    return state;
  });

  const search: SemanticMemoryIndex["Service"]["search"] = Effect.fn(
    "InMemorySemanticIndex.search",
  )(function* (rawQuery) {
    const operation = "search semantic memory index";
    yield* ensureOpen(operation);
    const query = yield* decodeBoundary(MemoryIndexQuery, rawQuery, operation);
    const vector = Object.freeze([...query.vector]);
    if (!validVector(vector, profile)) return yield* error(operation, "invalid-input");
    yield* ensureOpen(operation);
    const current = yield* Ref.get(data);
    if (current.closed) return yield* error(operation, "unavailable");
    let scannedChunks = 0;
    let inspectedSources = 0;
    let incomplete = false;
    const candidates: Array<MemoryIndexCandidate> = [];
    for (const entry of current.entries.values()) {
      inspectedSources += 1;
      if (inspectedSources % 128 === 0) yield* Effect.yieldNow;
      if (entry.state.key.namespace !== query.namespace) continue;
      if (entry.state.status === "building") incomplete = true;
      if (entry.state.status !== "ready" || entry.state.indexedAt === null) continue;
      scannedChunks += entry.chunks.length;
      if (scannedChunks > query.maxScannedChunks) return yield* error(operation, "budget");
    }
    for (const entry of current.entries.values()) {
      if (
        entry.state.key.namespace !== query.namespace ||
        entry.state.status !== "ready" ||
        entry.state.indexedAt === null
      )
        continue;
      yield* Effect.yieldNow;
      for (const chunk of entry.chunks) {
        const score = cosine(vector, chunk.vector);
        if (score < query.minScore) continue;
        candidates.push(
          MemoryIndexCandidate.make({
            key: entry.state.key,
            source: entry.state.source,
            sourceGeneration: entry.state.sourceGeneration,
            passageId: chunk.passageId,
            ordinal: chunk.ordinal,
            startByte: chunk.startByte,
            endByte: chunk.endByte,
            text: chunk.text,
            score,
            indexedAt: entry.state.indexedAt,
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
    return MemoryIndexSearch.make({
      candidates: candidates.slice(0, query.limit),
      scannedChunks,
      incomplete,
    });
  });

  return SemanticMemoryIndex.of({ profile, begin, publish, withdraw, inspect, search });
});

/** Scoped disposable semantic index with an injectable mutation failpoint. */
export const inMemorySemanticIndexLayerWithFailpoints = (
  profile: SemanticMemoryProfile,
  capacity: InMemorySemanticIndexCapacity,
): Layer.Layer<SemanticMemoryIndex, MemoryIndexError, MemoryIndexFailpoint> =>
  Layer.effect(SemanticMemoryIndex, makeIndex(profile, capacity));

/** Scoped disposable semantic index with production no-op mutation failpoints. */
export const inMemorySemanticIndexLayer = (
  profile: SemanticMemoryProfile,
  capacity: InMemorySemanticIndexCapacity,
): Layer.Layer<SemanticMemoryIndex, MemoryIndexError> =>
  inMemorySemanticIndexLayerWithFailpoints(profile, capacity).pipe(
    Layer.provide(MemoryIndexFailpoint.layer),
  );
