import {
  activityProcessorStoreLayer,
  activityProcessorStoreLayerWithFailpoints,
} from "@effect-agent/storage-sqlite/SqliteActivityStore";
import {
  ActivityBusy,
  ActivityClaimRequest,
  ActivityMutationFailpoint,
  ActivityMutationFailure,
  ActivityOwnershipLost,
  ActivityProcessorKey,
  ActivityProcessorStore,
  ActivityProgress,
  ActivityStoreError,
  ActivityWorkConflict,
  PreparedActivity,
} from "@effect-agent/thread/ActivityStore";
import { RecordId } from "@effect-agent/thread/Records";
import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import { Cause, Deferred, Effect, Exit, Fiber, FileSystem, Layer, Result, Schema } from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

const key = Schema.decodeSync(ActivityProcessorKey)({
  processorId: "profile",
  processorVersion: "v1",
  threadId: "thread-1",
});

const independentKeys = [
  key,
  Schema.decodeSync(ActivityProcessorKey)({ ...key, threadId: "thread-2" }),
  Schema.decodeSync(ActivityProcessorKey)({ ...key, processorId: "audit" }),
  Schema.decodeSync(ActivityProcessorKey)({ ...key, processorVersion: "v2" }),
];

const request = (owner: string, activityKey = key, leaseMillis = 10_000) =>
  ActivityClaimRequest.make({ key: activityKey, owner, leaseMillis });

const work = (
  sequence: number,
  marker: string,
  activityKey = key,
  output: Schema.Json = { marker },
) =>
  Schema.decodeSync(PreparedActivity)({
    version: 1,
    key: activityKey,
    sequence,
    workId: marker.repeat(64),
    recordId: `record-${sequence}`,
    recordDigest: marker.toUpperCase().repeat(64).toLowerCase(),
    output,
  });

const storeLayer = (filename: string) =>
  activityProcessorStoreLayer.pipe(
    Layer.provide(SqliteClient.layer({ filename, busyTimeout: 5_000 })),
  );

const failpointLayer = (filename: string, handler: ActivityMutationFailpoint["Service"]["hit"]) =>
  activityProcessorStoreLayerWithFailpoints.pipe(
    Layer.provide(
      Layer.mergeAll(
        SqliteClient.layer({ filename, busyTimeout: 5_000 }),
        Layer.succeed(ActivityMutationFailpoint)({ hit: handler }),
      ),
    ),
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-activity-sqlite-",
      });

      return yield* use(`${directory}/activity.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const runStore = <A, E>(filename: string, effect: Effect.Effect<A, E, ActivityProcessorStore>) =>
  effect.pipe(Effect.provide(storeLayer(filename)));

const inspect = (filename: string, activityKey = key) =>
  runStore(
    filename,
    Effect.gen(function* () {
      const store = yield* ActivityProcessorStore;

      return yield* store.inspect(activityKey);
    }),
  );

const runRaw = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  effect.pipe(Effect.provide(SqliteClient.layer({ filename, busyTimeout: 5_000 })));

describe("SQLite activity processor store", () => {
  it.effect(
    "rejects oversized encoded progress without mutation and reopens the exact boundary",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const prepared = yield* runStore(
            filename,
            Effect.gen(function* () {
              const store = yield* ActivityProcessorStore;
              const claim = yield* store.claim(request("worker"));
              const before = yield* store.inspect(key);
              const initial = work(1, "a");

              const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ActivityProgress))(
                ActivityProgress.make({ ...claim, version: 1, pending: initial, advancedAt: null }),
              );

              const boundary = PreparedActivity.make({
                ...initial,
                recordId: yield* Schema.decodeEffect(RecordId)(
                  "r".repeat(16 * 1024 * 1024 - encoded.length + initial.recordId.length),
                ),
              });

              const oversized = PreparedActivity.make({
                ...boundary,
                recordId: yield* Schema.decodeEffect(RecordId)(`${boundary.recordId}x`),
              });

              const escaped = PreparedActivity.make({
                ...initial,
                recordId: yield* Schema.decodeEffect(RecordId)("\0".repeat(3 * 1024 * 1024)),
              });

              for (const rejected of [oversized, escaped]) {
                expect(yield* store.prepare({ claim, work: rejected }).pipe(Effect.flip)).toEqual(
                  ActivityStoreError.make({
                    operation: "prepare activity output",
                    reason: "invalid-input",
                  }),
                );
                expect(yield* store.inspect(key)).toEqual(before);
              }
              yield* store.prepare({ claim, work: boundary });
              expect(yield* store.prepare({ claim, work: boundary })).toEqual(boundary);
              yield* store.release(claim);

              return boundary;
            }),
          );

          expect((yield* inspect(filename))?.pending).toEqual(prepared);
          yield* runStore(
            filename,
            Effect.gen(function* () {
              const store = yield* ActivityProcessorStore;
              const claim = yield* store.claim(request("worker"));

              expect(claim.pending).toEqual(prepared);
              const next = yield* store.advance({ claim, workId: prepared.workId });

              expect(next.throughSequence).toBe(1);
              expect(next.pending).toBeNull();
            }),
          );
        }),
      ),
  );

  it.effect("preserves pending output across takeover and release, then fences reacquisition", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        expect(yield* inspect(filename)).toBeNull();
        yield* TestClock.setTime(1_000);

        const first = yield* runStore(
          filename,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const claim = yield* store.claim(request("worker"));
            const prepared = yield* store.prepare({ claim, work: work(1, "a") });

            return { claim, prepared };
          }),
        );

        yield* TestClock.setTime(12_000);

        const takeover = yield* runStore(
          filename,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const claim = yield* store.claim(request("takeover"));

            expect(claim.epoch).toBe(first.claim.epoch + 1);
            expect(claim.pending).toEqual(first.prepared);
            yield* store.release(claim);

            return claim;
          }),
        );

        const released = yield* inspect(filename);

        expect(released?.owner).toBeNull();
        expect(released?.pending).toEqual(first.prepared);

        const second = yield* runStore(
          filename,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const claim = yield* store.claim(request("takeover"));

            expect(claim.epoch).toBe(takeover.epoch + 1);
            expect(claim.pending).toEqual(first.prepared);
            yield* TestClock.setTime(13_000);
            const advanced = yield* store.advance({ claim, workId: first.prepared.workId });

            yield* store.release(claim);

            return { claim, advanced };
          }),
        );

        expect(second.advanced.throughSequence).toBe(1);
        expect(second.advanced.pending).toBeNull();
        const progressed = yield* inspect(filename);

        expect(progressed?.throughSequence).toBe(1);
        expect(progressed?.pending).toBeNull();
        expect(progressed?.advancedAt).toBe(13_000);

        const third = yield* runStore(
          filename,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const claim = yield* store.claim(request("takeover"));

            const stale = yield* store
              .advance({ claim: second.claim, workId: first.prepared.workId })
              .pipe(Effect.flip);

            return { claim, stale };
          }),
        );

        expect(third.claim.epoch).toBe(second.claim.epoch + 1);
        expect(third.stale).toEqual(
          ActivityOwnershipLost.make({
            key,
            owner: second.claim.owner,
            epoch: second.claim.epoch,
          }),
        );
      }),
    ),
  );

  it.effect("pins one exact next output and rejects divergent preparation", () =>
    withTemporaryDatabase((filename) =>
      runStore(
        filename,
        Effect.gen(function* () {
          const store = yield* ActivityProcessorStore;
          const claim = yield* store.claim(request("worker"));
          const wrongSequence = work(2, "1");

          expect(yield* store.prepare({ claim, work: wrongSequence }).pipe(Effect.flip)).toEqual(
            ActivityWorkConflict.make({ key, workId: wrongSequence.workId }),
          );
          const wrongKey = independentKeys[1];
          const wrongOwner = work(1, "2", wrongKey);

          expect(yield* store.prepare({ claim, work: wrongOwner }).pipe(Effect.flip)).toEqual(
            ActivityWorkConflict.make({ key, workId: wrongOwner.workId }),
          );
          const pinned = work(1, "b", key, { value: "pinned" });

          expect(yield* store.prepare({ claim, work: pinned })).toEqual(pinned);
          expect(yield* store.prepare({ claim, work: pinned })).toEqual(pinned);
          const divergent = work(1, "c", key, { value: "different" });

          expect(yield* store.prepare({ claim, work: divergent }).pipe(Effect.flip)).toEqual(
            ActivityWorkConflict.make({ key, workId: divergent.workId }),
          );
          const wrongWorkId = work(1, "3").workId;

          expect(yield* store.advance({ claim, workId: wrongWorkId }).pipe(Effect.flip)).toEqual(
            ActivityWorkConflict.make({ key, workId: wrongWorkId }),
          );
        }),
      ),
    ),
  );

  it.effect("keeps thread, processor, and processor-version progress independent", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* runStore(
          filename,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;

            for (const [index, activityKey] of independentKeys.entries()) {
              const claim = yield* store.claim(request(`worker-${index}`, activityKey));

              if (index === 0) {
                const prepared = work(1, "d", activityKey);

                yield* store.prepare({ claim, work: prepared });
                yield* store.advance({ claim, workId: prepared.workId });
              }
            }
          }),
        );
        expect((yield* inspect(filename, independentKeys[0]))?.throughSequence).toBe(1);
        for (const activityKey of independentKeys.slice(1)) {
          expect((yield* inspect(filename, activityKey))?.throughSequence).toBe(0);
        }
      }),
    ),
  );

  it.effect("serializes competing claims across independent clients", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);

        const claim = (owner: string) =>
          runStore(
            filename,
            Effect.gen(function* () {
              const store = yield* ActivityProcessorStore;

              return yield* store.claim(request(owner));
            }),
          ).pipe(Effect.result);

        const outcomes = yield* Effect.all([claim("one"), claim("two")], {
          concurrency: "unbounded",
        });

        expect(outcomes.filter(Result.isSuccess)).toHaveLength(1);
        expect(outcomes.filter(Result.isFailure)).toHaveLength(1);
        const failure = outcomes.find(Result.isFailure);

        expect(failure?.failure).toEqual(ActivityBusy.make({ key, leaseExpiresAt: 11_000 }));
      }),
    ),
  );

  it.effect("recovers prepared output and an advanced cursor after lost acknowledgements", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);

        const claim = yield* runStore(
          filename,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;

            return yield* store.claim(request("worker"));
          }),
        );

        const prepared = work(1, "e");

        const prepareFailure = yield* Effect.gen(function* () {
          const store = yield* ActivityProcessorStore;

          return yield* store.prepare({ claim, work: prepared });
        }).pipe(
          Effect.provide(
            failpointLayer(filename, (point) =>
              point === "activity:prepare:after"
                ? Effect.fail(ActivityMutationFailure.make({ point }))
                : Effect.void,
            ),
          ),
          Effect.flip,
        );

        expect(prepareFailure).toEqual(
          ActivityMutationFailure.make({ point: "activity:prepare:after" }),
        );
        expect((yield* inspect(filename))?.pending).toEqual(prepared);

        yield* TestClock.setTime(2_000);

        const advanceFailure = yield* Effect.gen(function* () {
          const store = yield* ActivityProcessorStore;

          return yield* store.advance({ claim, workId: prepared.workId });
        }).pipe(
          Effect.provide(
            failpointLayer(filename, (point) =>
              point === "activity:advance:after"
                ? Effect.fail(ActivityMutationFailure.make({ point }))
                : Effect.void,
            ),
          ),
          Effect.flip,
        );

        expect(advanceFailure).toEqual(
          ActivityMutationFailure.make({ point: "activity:advance:after" }),
        );
        const recovered = yield* inspect(filename);

        expect(recovered?.throughSequence).toBe(1);
        expect(recovered?.pending).toBeNull();
        expect(recovered?.advancedAt).toBe(2_000);
        expect(
          yield* runStore(
            filename,
            Effect.gen(function* () {
              const store = yield* ActivityProcessorStore;

              return yield* store.advance({ claim, workId: prepared.workId });
            }),
          ).pipe(Effect.flip),
        ).toEqual(ActivityOwnershipLost.make({ key, owner: claim.owner, epoch: claim.epoch }));
      }),
    ),
  );

  it.effect("rolls back defects, timeout, and interruption inside state transactions", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const claim = yield* runStore(
          filename,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;

            return yield* store.claim(request("worker"));
          }),
        );

        const prepared = work(1, "f");

        const defect = yield* Effect.gen(function* () {
          const store = yield* ActivityProcessorStore;

          return yield* store.prepare({ claim, work: prepared });
        }).pipe(
          Effect.provide(
            failpointLayer(filename, (point) =>
              point === "activity:prepare:after-state" ? Effect.die("prepare defect") : Effect.void,
            ),
          ),
          Effect.exit,
        );

        expect(Exit.isFailure(defect) && Cause.hasDies(defect.cause)).toBe(true);
        expect((yield* inspect(filename))?.pending).toBeNull();

        yield* runStore(
          filename,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;

            yield* store.prepare({ claim, work: prepared });
          }),
        );
        const reached = yield* Deferred.make<void>();

        const advancing = yield* Effect.gen(function* () {
          const store = yield* ActivityProcessorStore;

          return yield* store.advance({ claim, workId: prepared.workId });
        }).pipe(
          Effect.provide(
            failpointLayer(filename, (point) =>
              point === "activity:advance:after-state"
                ? Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never))
                : Effect.void,
            ),
          ),
          Effect.timeout("1 second"),
          Effect.forkChild,
        );

        yield* Deferred.await(reached);
        yield* TestClock.adjust("1 second");
        expect(Exit.isFailure(yield* Fiber.await(advancing))).toBe(true);
        const afterTimeout = yield* inspect(filename);

        expect(afterTimeout?.throughSequence).toBe(0);
        expect(afterTimeout?.pending).toEqual(prepared);

        const releaseReached = yield* Deferred.make<void>();

        const releasing = yield* Effect.gen(function* () {
          const store = yield* ActivityProcessorStore;

          return yield* store.release(claim);
        }).pipe(
          Effect.provide(
            failpointLayer(filename, (point) =>
              point === "activity:release:after-state"
                ? Deferred.succeed(releaseReached, undefined).pipe(Effect.andThen(Effect.never))
                : Effect.void,
            ),
          ),
          Effect.forkChild,
        );

        yield* Deferred.await(releaseReached);
        yield* Fiber.interrupt(releasing);
        expect(Exit.isFailure(yield* Fiber.await(releasing))).toBe(true);
        expect((yield* inspect(filename))?.owner).toBe(claim.owner);
      }),
    ),
  );

  it.effect("rejects incompatible stored formats", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* runStore(
          filename,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;

            yield* store.claim(request("worker"));
          }),
        );
        yield* runRaw(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            yield* sql`
              UPDATE effect_agent_activity_processor_state_v1
              SET format_version = 2
              WHERE processor_id = ${key.processorId}
                AND processor_version = ${key.processorVersion}
                AND thread_id = ${key.threadId}
            `;
          }),
        );
        expect(yield* inspect(filename).pipe(Effect.flip)).toEqual(
          ActivityStoreError.make({
            operation: "inspect activity progress",
            reason: "incompatible",
          }),
        );
      }),
    ),
  );
});
