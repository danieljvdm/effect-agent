import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Result, Schema } from "effect";
import type { ActivityMutationFailpoint } from "effect-agent/activity-store";
import {
  ActivityBusy,
  ActivityClaimRequest,
  ActivityMutationFailure,
  ActivityOwnershipLost,
  ActivityProcessorKey,
  ActivityProcessorStore,
  ActivityStoreError,
  ActivityWorkConflict,
  PreparedActivity,
} from "effect-agent/activity-store";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { clientLayer, storage, withTemporaryDatabase } from "./harness.ts";

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

const storeLayer = (url: string) => storage(url).activityStore;

const failpointLayer = (url: string, handler: ActivityMutationFailpoint["Service"]["hit"]) =>
  storage(url, { activityFailpoint: handler }).activityStore;

const runStore = <A, E>(url: string, effect: Effect.Effect<A, E, ActivityProcessorStore>) =>
  effect.pipe(Effect.provide(storeLayer(url)));

const inspect = (url: string, activityKey = key) =>
  runStore(
    url,
    Effect.gen(function* () {
      const store = yield* ActivityProcessorStore;

      return yield* store.inspect(activityKey);
    }),
  );

const runRaw = <A, E>(url: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  effect.pipe(Effect.provide(clientLayer(url)));

describe("Postgres activity processor store", () => {
  it.effect("preserves pending output across takeover and release, then fences reacquisition", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        expect(yield* inspect(url)).toBeNull();
        yield* TestClock.setTime(1_000);

        const first = yield* runStore(
          url,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const claim = yield* store.claim(request("worker"));
            const prepared = yield* store.prepare({ claim, work: work(1, "a") });

            return { claim, prepared };
          }),
        );

        yield* TestClock.setTime(12_000);

        const takeover = yield* runStore(
          url,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;
            const claim = yield* store.claim(request("takeover"));

            expect(claim.epoch).toBe(first.claim.epoch + 1);
            expect(claim.pending).toEqual(first.prepared);
            yield* store.release(claim);

            return claim;
          }),
        );

        const released = yield* inspect(url);

        expect(released?.owner).toBeNull();
        expect(released?.pending).toEqual(first.prepared);

        const second = yield* runStore(
          url,
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
        const progressed = yield* inspect(url);

        expect(progressed?.throughSequence).toBe(1);
        expect(progressed?.pending).toBeNull();
        expect(progressed?.advancedAt).toBe(13_000);

        const third = yield* runStore(
          url,
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
    withTemporaryDatabase((url) =>
      runStore(
        url,
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
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* runStore(
          url,
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
        expect((yield* inspect(url, independentKeys[0]))?.throughSequence).toBe(1);
        for (const activityKey of independentKeys.slice(1)) {
          expect((yield* inspect(url, activityKey))?.throughSequence).toBe(0);
        }
      }),
    ),
  );

  it.effect("serializes competing claims across independent clients", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);

        const claim = (owner: string) =>
          runStore(
            url,
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
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(1_000);

        const claim = yield* runStore(
          url,
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
            failpointLayer(url, (point) =>
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
        expect((yield* inspect(url))?.pending).toEqual(prepared);

        yield* TestClock.setTime(2_000);

        const advanceFailure = yield* Effect.gen(function* () {
          const store = yield* ActivityProcessorStore;

          return yield* store.advance({ claim, workId: prepared.workId });
        }).pipe(
          Effect.provide(
            failpointLayer(url, (point) =>
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
        const recovered = yield* inspect(url);

        expect(recovered?.throughSequence).toBe(1);
        expect(recovered?.pending).toBeNull();
        expect(recovered?.advancedAt).toBe(2_000);
        expect(
          yield* runStore(
            url,
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
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const claim = yield* runStore(
          url,
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
            failpointLayer(url, (point) =>
              point === "activity:prepare:after-state" ? Effect.die("prepare defect") : Effect.void,
            ),
          ),
          Effect.exit,
        );

        expect(Exit.isFailure(defect) && Cause.hasDies(defect.cause)).toBe(true);
        expect((yield* inspect(url))?.pending).toBeNull();

        yield* runStore(
          url,
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
            failpointLayer(url, (point) =>
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
        const afterTimeout = yield* inspect(url);

        expect(afterTimeout?.throughSequence).toBe(0);
        expect(afterTimeout?.pending).toEqual(prepared);

        const releaseReached = yield* Deferred.make<void>();

        const releasing = yield* Effect.gen(function* () {
          const store = yield* ActivityProcessorStore;

          return yield* store.release(claim);
        }).pipe(
          Effect.provide(
            failpointLayer(url, (point) =>
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
        expect((yield* inspect(url))?.owner).toBe(claim.owner);
      }),
    ),
  );

  it.effect("rejects incompatible stored formats", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* runStore(
          url,
          Effect.gen(function* () {
            const store = yield* ActivityProcessorStore;

            yield* store.claim(request("worker"));
          }),
        );
        yield* runRaw(
          url,
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
        expect(yield* inspect(url).pipe(Effect.flip)).toEqual(
          ActivityStoreError.make({
            operation: "inspect activity progress",
            reason: "incompatible",
          }),
        );
      }),
    ),
  );
});
