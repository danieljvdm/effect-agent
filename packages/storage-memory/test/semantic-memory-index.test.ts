import {
  MemoryIndexBuild,
  MemoryIndexError,
  MemoryIndexFailpoint,
  MemoryIndexMutationFailure,
  MemoryIndexQuery,
  MemoryIndexSource,
  MemoryKey,
  SemanticMemoryChunk,
  SemanticMemoryIndex,
  SemanticMemoryProfile,
} from "@effect-agent/core";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Encoding,
  Exit,
  Fiber,
  Layer,
  Scope,
  Schema,
} from "effect";
import { TestClock } from "effect/testing";

import {
  InMemorySemanticIndexCapacity,
  inMemorySemanticIndexLayer,
  inMemorySemanticIndexLayerWithFailpoints,
} from "../src/index.ts";

const profile = SemanticMemoryProfile.make({
  version: 1,
  provider: "test-provider",
  model: "test-model",
  modelRevision: "revision-1",
  dimensions: 2,
  chunker: "utf8-codepoint@1",
  maxChunkBytes: 16,
  distance: "cosine",
});

const capacity = InMemorySemanticIndexCapacity.make({ maxSources: 2, maxChunks: 3 });

const source = (
  id: string,
  sourceGeneration = 1,
  revision = String(sourceGeneration),
  locator = `memory://${id}`,
) =>
  Schema.decodeSync(MemoryIndexSource)({
    key: { namespace: "tenant-a", id },
    source: { id, locator, revision },
    sourceGeneration,
  });

const chunk = (
  passageId: string,
  ordinal: number,
  startByte: number,
  text: string,
  vector: ReadonlyArray<number>,
) =>
  SemanticMemoryChunk.make({
    passageId,
    ordinal,
    startByte,
    endByte: startByte + Encoding.encodeHex(text).length / 2,
    text,
    vector,
  });

const query = (vector: ReadonlyArray<number>, maxScannedChunks = 3, limit = 128, minScore = -1) =>
  MemoryIndexQuery.make({ namespace: "tenant-a", vector, maxScannedChunks, limit, minScore });

const layer = (
  selectedProfile: SemanticMemoryProfile = profile,
  selectedCapacity: InMemorySemanticIndexCapacity = capacity,
) => inMemorySemanticIndexLayer(selectedProfile, selectedCapacity);

const layerWithFailpoints = (
  handler: MemoryIndexFailpoint["Service"]["hit"],
  selectedCapacity: InMemorySemanticIndexCapacity = capacity,
) =>
  inMemorySemanticIndexLayerWithFailpoints(profile, selectedCapacity).pipe(
    Layer.provide(Layer.succeed(MemoryIndexFailpoint)({ hit: handler })),
  );

describe("in-memory semantic index", () => {
  it.effect(
    "validates configuration and vectors, ranks deterministically, and enforces budgets",
    () =>
      Effect.gen(function* () {
        const invalidCapacity = InMemorySemanticIndexCapacity.make({ maxSources: 1, maxChunks: 1 });
        Object.defineProperty(invalidCapacity, "maxSources", { value: 0 });
        expect(
          yield* Layer.build(layer(profile, invalidCapacity)).pipe(Effect.scoped, Effect.flip),
        ).toEqual(
          MemoryIndexError.make({
            operation: "configure semantic memory index",
            reason: "invalid-input",
          }),
        );
        const invalidProfile = SemanticMemoryProfile.make({ ...profile });
        Object.defineProperty(invalidProfile, "dimensions", { value: 0 });
        expect(
          yield* Layer.build(layer(invalidProfile, capacity)).pipe(Effect.scoped, Effect.flip),
        ).toEqual(
          MemoryIndexError.make({
            operation: "configure semantic memory index",
            reason: "invalid-input",
          }),
        );

        yield* Effect.gen(function* () {
          const index = yield* SemanticMemoryIndex;
          const sourceB = source("b");
          const buildB = yield* index.begin(sourceB);
          yield* index.publish({
            build: buildB,
            chunks: [chunk("b-0", 0, 0, "b", [1, 0])],
          });
          const mutableVector = [1, 0];
          const sourceA = source("a");
          const buildA = yield* index.begin(sourceA);
          yield* index.publish({
            build: buildA,
            chunks: [chunk("a-0", 0, 0, "a", mutableVector), chunk("a-1", 1, 1, "c", [0, 1])],
          });
          mutableVector[0] = -1;

          const ranked = yield* index.search({ ...query([1, 0]) });
          expect(ranked.scannedChunks).toBe(3);
          expect(ranked.incomplete).toBe(false);
          expect(ranked.candidates.map((candidate) => candidate.passageId)).toEqual([
            "a-0",
            "b-0",
            "a-1",
          ]);
          expect(ranked.candidates.map((candidate) => candidate.score)).toEqual([1, 1, 0]);

          for (const invalidVector of [[0, 0], [1], [Number.MAX_VALUE, 1]]) {
            expect(yield* index.search(query(invalidVector)).pipe(Effect.flip)).toEqual(
              MemoryIndexError.make({
                operation: "search semantic memory index",
                reason: "invalid-input",
              }),
            );
          }
          expect(yield* index.search(query([1, 0], 2)).pipe(Effect.flip)).toEqual(
            MemoryIndexError.make({ operation: "search semantic memory index", reason: "budget" }),
          );

          const withdrawn = yield* index.withdraw(sourceA);
          expect(withdrawn.status).toBe("withdrawn");
          expect(yield* index.begin(source("c")).pipe(Effect.flip)).toEqual(
            MemoryIndexError.make({ operation: "begin semantic memory build", reason: "budget" }),
          );
        }).pipe(Effect.provide(layer()));

        yield* Effect.gen(function* () {
          const index = yield* SemanticMemoryIndex;
          const build = yield* index.begin(source("capacity"));
          expect(
            yield* index
              .publish({
                build,
                chunks: [
                  chunk("capacity-0", 0, 0, "a", [1, 0]),
                  chunk("capacity-1", 1, 1, "b", [0, 1]),
                ],
              })
              .pipe(Effect.flip),
          ).toEqual(
            MemoryIndexError.make({ operation: "publish semantic memory build", reason: "budget" }),
          );
          const pending = yield* index.inspect(build.key);
          expect(pending?.status).toBe("building");
          expect(pending?.chunkCount).toBe(0);
        }).pipe(
          Effect.provide(
            layer(profile, InMemorySemanticIndexCapacity.make({ maxSources: 1, maxChunks: 1 })),
          ),
        );
      }),
  );

  it.effect("rebuilds corrections with fresh epochs and terminally fences withdrawal", () =>
    Effect.gen(function* () {
      const index = yield* SemanticMemoryIndex;
      const firstSource = source("profile", 1);
      const first = yield* index.begin(firstSource);
      yield* index.publish({ build: first, chunks: [chunk("first", 0, 0, "old", [1, 0])] });

      const second = yield* index.begin(firstSource);
      expect(second.epoch).toBe(first.epoch + 1);
      const hidden = yield* index.search(query([1, 0]));
      expect(hidden.candidates).toEqual([]);
      expect(hidden.scannedChunks).toBe(0);
      expect(hidden.incomplete).toBe(true);
      expect(
        yield* index
          .publish({ build: first, chunks: [chunk("late", 0, 0, "late", [1, 0])] })
          .pipe(Effect.flip),
      ).toEqual(
        MemoryIndexError.make({ operation: "publish semantic memory build", reason: "fenced" }),
      );
      expect(
        yield* index.begin(source("profile", 1, "different", "memory://changed")).pipe(Effect.flip),
      ).toEqual(
        MemoryIndexError.make({ operation: "begin semantic memory build", reason: "fenced" }),
      );
      yield* index.publish({
        build: second,
        chunks: [chunk("second", 0, 0, "new", [0, 1])],
      });

      const correctedSource = source("profile", 2);
      const third = yield* index.begin(correctedSource);
      expect(third.epoch).toBe(second.epoch + 1);
      yield* index.publish({
        build: third,
        chunks: [chunk("third", 0, 0, "current", [1, 0])],
      });
      const tombstone = yield* index.withdraw(correctedSource);
      expect(tombstone.status).toBe("withdrawn");
      expect(tombstone.chunkCount).toBe(0);
      expect(tombstone.indexedAt).toBeNull();
      expect(yield* index.withdraw(correctedSource)).toEqual(tombstone);
      expect(
        yield* index
          .publish({ build: third, chunks: [chunk("delayed", 0, 0, "delayed", [1, 0])] })
          .pipe(Effect.flip),
      ).toEqual(
        MemoryIndexError.make({ operation: "publish semantic memory build", reason: "fenced" }),
      );
      expect(yield* index.begin(source("profile", 3)).pipe(Effect.flip)).toEqual(
        MemoryIndexError.make({ operation: "begin semantic memory build", reason: "fenced" }),
      );
      expect((yield* index.search(query([1, 0]))).candidates).toEqual([]);
    }).pipe(Effect.provide(layer())),
  );

  it.effect("keeps exact publication replay and committed state across failpoint failures", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(1_000);
      yield* Effect.gen(function* () {
        const index = yield* SemanticMemoryIndex;
        const build = yield* index.begin(source("replay"));
        const chunks = [chunk("replay-0", 0, 0, "replay", [1, 0])];
        expect(yield* index.publish({ build, chunks }).pipe(Effect.flip)).toEqual(
          MemoryIndexMutationFailure.make({ point: "index:publish:after" }),
        );
        const committed = yield* index.inspect(build.key);
        expect(committed?.status).toBe("ready");
        expect(committed?.indexedAt).toBe(1_000);
        yield* TestClock.setTime(2_000);
        expect(yield* index.publish({ build, chunks })).toEqual(committed);
        expect(
          yield* index
            .publish({ build, chunks: [chunk("different", 0, 0, "replay", [0, 1])] })
            .pipe(Effect.flip),
        ).toEqual(
          MemoryIndexError.make({ operation: "publish semantic memory build", reason: "fenced" }),
        );
      }).pipe(
        Effect.provide(
          layerWithFailpoints((point) =>
            point === "index:publish:after"
              ? Effect.fail(MemoryIndexMutationFailure.make({ point }))
              : Effect.void,
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const index = yield* SemanticMemoryIndex;
        expect(yield* index.begin(source("before")).pipe(Effect.flip)).toEqual(
          MemoryIndexMutationFailure.make({ point: "index:begin:before" }),
        );
        expect(
          yield* index.inspect(MemoryKey.make({ namespace: "tenant-a", id: "before" })),
        ).toBeNull();
      }).pipe(
        Effect.provide(
          layerWithFailpoints((point) =>
            point === "index:begin:before"
              ? Effect.fail(MemoryIndexMutationFailure.make({ point }))
              : Effect.void,
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const index = yield* SemanticMemoryIndex;
        const defect = yield* index.begin(source("defect")).pipe(Effect.exit);
        expect(Exit.isFailure(defect) && Cause.hasDies(defect.cause)).toBe(true);
        expect(
          (yield* index.inspect(MemoryKey.make({ namespace: "tenant-a", id: "defect" })))?.status,
        ).toBe("building");
      }).pipe(
        Effect.provide(
          layerWithFailpoints((point) =>
            point === "index:begin:after" ? Effect.die("injected defect") : Effect.void,
          ),
        ),
      );

      const timeoutReached = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const index = yield* SemanticMemoryIndex;
        const build = yield* index.begin(source("timeout"));
        const publishing = yield* index
          .publish({ build, chunks: [chunk("timeout-0", 0, 0, "timeout", [1, 0])] })
          .pipe(Effect.timeout("1 second"), Effect.forkChild);
        yield* Deferred.await(timeoutReached);
        yield* TestClock.adjust("1 second");
        expect(Exit.isFailure(yield* Fiber.await(publishing))).toBe(true);
        expect((yield* index.inspect(build.key))?.status).toBe("ready");
      }).pipe(
        Effect.provide(
          layerWithFailpoints((point) =>
            point === "index:publish:after"
              ? Deferred.succeed(timeoutReached, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.void,
          ),
        ),
      );

      const interruptedReached = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const index = yield* SemanticMemoryIndex;
        const currentSource = source("interrupted");
        const build = yield* index.begin(currentSource);
        yield* index.publish({
          build,
          chunks: [chunk("interrupted-0", 0, 0, "current", [1, 0])],
        });
        const withdrawing = yield* index.withdraw(currentSource).pipe(Effect.forkChild);
        yield* Deferred.await(interruptedReached);
        yield* Fiber.interrupt(withdrawing);
        const interrupted = yield* Fiber.await(withdrawing);
        expect(Exit.isFailure(interrupted) && Cause.hasInterruptsOnly(interrupted.cause)).toBe(
          true,
        );
        expect((yield* index.inspect(build.key))?.status).toBe("ready");
      }).pipe(
        Effect.provide(
          layerWithFailpoints((point) =>
            point === "index:withdraw:before"
              ? Deferred.succeed(interruptedReached, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.void,
          ),
        ),
      );
    }),
  );

  it.effect("clears scoped state, rejects captured methods, and prevents a late begin", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const scope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(
        layerWithFailpoints((point) =>
          point === "index:begin:before"
            ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void,
        ),
        scope,
      );
      const index = Context.get(context, SemanticMemoryIndex);
      const lateSource = source("closed");
      const beginning = yield* index.begin(lateSource).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* Scope.close(scope, Exit.void);
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(beginning).pipe(Effect.flip)).toEqual(
        MemoryIndexError.make({ operation: "begin semantic memory build", reason: "unavailable" }),
      );

      const build = MemoryIndexBuild.make({ ...lateSource, profile, epoch: 1 });
      const expectUnavailable = <A>(
        call: Effect.Effect<A, MemoryIndexError | MemoryIndexMutationFailure>,
      ) =>
        Effect.gen(function* () {
          const failure = yield* call.pipe(Effect.flip);
          expect(failure._tag).toBe("MemoryIndexError");
          if (failure._tag === "MemoryIndexError") expect(failure.reason).toBe("unavailable");
        });
      yield* expectUnavailable(index.begin(lateSource));
      yield* expectUnavailable(
        index.publish({ build, chunks: [chunk("closed-0", 0, 0, "closed", [1, 0])] }),
      );
      yield* expectUnavailable(index.withdraw(lateSource));
      yield* expectUnavailable(index.inspect(lateSource.key));
      yield* expectUnavailable(index.search(query([1, 0])));
    }),
  );
});
