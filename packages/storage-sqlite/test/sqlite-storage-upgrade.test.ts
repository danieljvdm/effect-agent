import { SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import { SubscriptionStore, type SubscriptionError } from "@effect-agent/thread/Subscription";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer, Exit, Deferred, Fiber } from "effect";
import { TestClock } from "effect/testing";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  restoreV2,
  assertPreserved,
  assertReceiptReplay,
  assertSubscriptionReplay,
  snapshotStore,
  fixturePartition,
} from "../../../test/fixtures/storage-upgrade.ts";
import {
  SqliteStorageFailpointError,
  type SqliteStorageFailpointLocation,
} from "../src/SqliteStorageError.ts";
import { SqliteStorageFailpoint } from "../src/SqliteStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/SqliteSubmissionLedger.ts";
import { subscriptionStoreLayer } from "../src/SqliteSubscriptionStore.ts";
import {
  storageConfigLayer,
  type SqliteStorageInitializationError,
} from "../src/SqliteThreadStore.ts";

const database = <A, E, R>(use: (filename: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "storage-upgrade-" });

      return yield* use(`${dir}/state.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const withFixture = <A, E>(
  use: (
    open: Effect.Effect<void, SqliteStorageInitializationError | SubscriptionError>,
    filename: string,
  ) => Effect.Effect<A, E, SqlClient.SqlClient>,
  fail?: (
    point: SqliteStorageFailpointLocation,
  ) => Effect.Effect<void, SqliteStorageFailpointError>,
) =>
  database((filename) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* restoreV2("sqlite");

      const deps = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient)(sql),
        storageConfigLayer({ filename }),
        NodeCrypto.layer,
        fail === undefined
          ? SqliteStorageFailpoint.layer
          : Layer.succeed(SqliteStorageFailpoint)({ hit: fail }),
      );

      const open = Effect.gen(function* () {
        yield* SubmissionLedger;
        yield* SubscriptionStore;
      }).pipe(
        Effect.provide(
          Layer.merge(submissionLedgerLayer, subscriptionStoreLayer(fixturePartition)).pipe(
            Layer.provide(deps),
          ),
        ),
      );

      return yield* use(open, filename);
    }).pipe(Effect.provide(SqliteClient.layer({ filename }))),
  );

describe("supported beta50 storage upgrade", () => {
  it.effect("preserves v7 state and exactly replays retained admissions after reopen", () =>
    withFixture((open, filename) =>
      Effect.gen(function* () {
        yield* open;
        yield* assertPreserved("sqlite");
        const after = yield* snapshotStore;

        yield* open;
        expect(yield* snapshotStore).toEqual(after);

        const deps = Layer.mergeAll(
          storageConfigLayer({ filename }),
          NodeCrypto.layer,
          SqliteStorageFailpoint.layer,
          SqliteClient.layer({ filename }),
        );

        yield* assertReceiptReplay.pipe(
          Effect.provide(submissionLedgerLayer.pipe(Layer.provide(deps))),
        );
        yield* assertSubscriptionReplay.pipe(
          Effect.provide(subscriptionStoreLayer(fixturePartition).pipe(Layer.provide(deps))),
        );
        const sql = yield* SqlClient.SqlClient;

        expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 8 }]);
      }),
    ),
  );
  for (const point of [
    "upgrade:before-mutation",
    "upgrade:after-mutation",
    "upgrade:before-version",
    "upgrade:after-version",
  ] as const) {
    it.effect(`rolls back ${point} and reopens safely`, () => {
      let armed = true;

      return withFixture(
        (open) =>
          Effect.gen(function* () {
            const before = yield* snapshotStore;

            expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
            expect(yield* snapshotStore).toEqual(before);
            armed = false;
            yield* open;
            yield* assertPreserved("sqlite");
          }),
        (location) =>
          armed && location === point
            ? SqliteStorageFailpointError.make({ location })
            : Effect.void,
      );
    });
  }
  it.effect("rolls back a timed-out upgrade, releases the transaction and reopens", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      let armed = true;

      yield* withFixture(
        (open) =>
          Effect.gen(function* () {
            const before = yield* snapshotStore;
            const fiber = yield* open.pipe(Effect.timeout(1), Effect.forkChild);

            yield* Deferred.await(entered);
            yield* TestClock.adjust(1);
            expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
            expect(yield* snapshotStore).toEqual(before);
            const sql = yield* SqlClient.SqlClient;

            expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 7 }]);
            armed = false;
            yield* open;
            yield* assertPreserved("sqlite");
          }),
        (point) =>
          armed && point === "upgrade:after-mutation"
            ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.void,
      );
    }),
  );

  it.effect(
    "retains v7 and all bytes when a delivery's historical configuration is ambiguous",
    () =>
      withFixture((open) =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;

          yield* sql`UPDATE effect_agent_subscription_deliveries SET record_json=json_set(record_json,'$.subscriptionFingerprint',${"f".repeat(64)}) WHERE event_id='prepared'`;
          const before = yield* snapshotStore;
          const failure = yield* open.pipe(Effect.result);

          expect(failure).toMatchObject({
            _tag: "Failure",
            failure: {
              _tag: "SqliteStorageCorruptionError",
              table: "effect_agent_subscription_deliveries",
            },
          });
          expect(yield* snapshotStore).toEqual(before);
        }),
      ),
  );
});
