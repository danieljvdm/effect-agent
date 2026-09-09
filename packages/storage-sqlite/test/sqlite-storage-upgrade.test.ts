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
  for (const point of [
    "upgrade:before-mutation",
    "upgrade:after-mutation",
    "upgrade:before-version",
    "upgrade:after-version",
  ] as const) {
    it.effect(`preserves v9 data and atomically adds recovery checkpoints at ${point}`, () => {
      let armed = false;

      return withFixture(
        (open) =>
          Effect.gen(function* () {
            yield* open;
            const sql = yield* SqlClient.SqlClient;

            yield* sql`DROP INDEX effect_agent_submissions_nonterminal`;
            yield* sql`DROP TABLE effect_agent_recovery_checkpoints`;
            yield* sql`PRAGMA user_version = 9`;
            const before = yield* snapshotStore;

            armed = true;
            expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
            expect(yield* snapshotStore).toEqual(before);
            expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 9 }]);
            armed = false;
            yield* open;
            yield* assertPreserved("sqlite");
            expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 11 }]);
            expect(yield* sql`SELECT * FROM effect_agent_recovery_checkpoints`).toEqual([]);
            const upgraded = yield* snapshotStore;

            yield* open;
            expect(yield* snapshotStore).toEqual(upgraded);
          }),
        (location) =>
          armed && location === point
            ? SqliteStorageFailpointError.make({ location })
            : Effect.void,
      );
    });
  }

  it.effect("rejects an ambiguous v9 layout without resetting or changing retained data", () =>
    withFixture((open) =>
      Effect.gen(function* () {
        yield* open;
        const sql = yield* SqlClient.SqlClient;

        yield* sql`DROP INDEX effect_agent_submissions_nonterminal`;
        yield* sql`DROP TABLE effect_agent_recovery_checkpoints`;
        yield* sql`ALTER TABLE effect_agent_submissions RENAME COLUMN message_admission_json TO malformed_column`;
        yield* sql`PRAGMA user_version = 9`;
        const before = yield* snapshotStore;

        expect(yield* open.pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "SqliteStorageCompatibilityError", actualVersion: 9 },
        });
        expect(yield* snapshotStore).toEqual(before);
      }),
    ),
  );

  for (const [table, column] of [
    ["effect_agent_submissions", "admission_fence_json"],
    ["effect_agent_subscriptions", "recovery_present"],
  ] as const) {
    it.effect(`rejects v8 missing ${table}.${column} without mutating storage`, () => {
      let armed = false;
      let mutations = 0;

      return withFixture(
        (open) =>
          Effect.gen(function* () {
            yield* open;
            const sql = yield* SqlClient.SqlClient;

            yield* sql`DROP INDEX effect_agent_submissions_nonterminal`;
            yield* sql`DROP TABLE effect_agent_recovery_checkpoints`;
            yield* sql`DROP TABLE effect_agent_message_deliveries`;
            yield* sql`ALTER TABLE effect_agent_submissions DROP COLUMN worker_admission_json`;
            yield* sql`ALTER TABLE effect_agent_submissions DROP COLUMN message_admission_json`;
            yield* sql`PRAGMA user_version = 8`;
            yield* sql.unsafe(`ALTER TABLE ${table} RENAME COLUMN ${column} TO malformed_column`);
            const before = yield* snapshotStore;

            armed = true;
            expect(yield* open.pipe(Effect.result)).toMatchObject({
              _tag: "Failure",
              failure: { _tag: "SqliteStorageCompatibilityError", actualVersion: 8 },
            });
            expect(mutations).toBe(0);
            expect(yield* snapshotStore).toEqual(before);
            expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 8 }]);
          }),
        (location) =>
          Effect.sync(() => {
            if (armed && location === "upgrade:before-mutation") mutations += 1;
          }),
      );
    });
  }

  it.effect("adds message storage to v8 atomically without changing existing records", () => {
    let armed = false;

    return withFixture(
      (open) =>
        Effect.gen(function* () {
          yield* open;
          const sql = yield* SqlClient.SqlClient;

          yield* sql`DROP INDEX effect_agent_submissions_nonterminal`;
          yield* sql`DROP TABLE effect_agent_recovery_checkpoints`;
          yield* sql`DROP TABLE effect_agent_message_deliveries`;
          yield* sql`ALTER TABLE effect_agent_submissions DROP COLUMN worker_admission_json`;
          yield* sql`ALTER TABLE effect_agent_submissions DROP COLUMN message_admission_json`;
          yield* sql`PRAGMA user_version = 8`;
          const before = yield* snapshotStore;

          armed = true;
          expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
          expect(yield* snapshotStore).toEqual(before);
          expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 8 }]);
          armed = false;
          yield* open;
          yield* assertPreserved("sqlite");
          expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 11 }]);
          expect(yield* sql`SELECT * FROM effect_agent_message_deliveries`).toEqual([]);
        }),
      (location) =>
        armed && location === "upgrade:after-version"
          ? SqliteStorageFailpointError.make({ location })
          : Effect.void,
    );
  });
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

        expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 11 }]);
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

describe("nonterminal index upgrade", () => {
  for (const point of [
    "upgrade:before-mutation",
    "upgrade:after-mutation",
    "upgrade:before-version",
    "upgrade:after-version",
  ] as const) {
    for (const mode of ["failure", "interrupt", "defect"] as const) {
      it.effect(
        `preserves v10 rows and recovery checkpoints atomically at ${point} (${mode})`,
        () => {
          let armed = false;

          return withFixture(
            (open) =>
              Effect.gen(function* () {
                yield* open;
                const sql = yield* SqlClient.SqlClient;

                yield* sql`INSERT INTO effect_agent_recovery_checkpoints (thread_id, through_sequence, tail_digest, checkpoint_json) SELECT thread_id, tail_sequence, tail_digest, '{"retained":true}' FROM effect_agent_threads LIMIT 1`;
                yield* sql`DROP INDEX effect_agent_submissions_nonterminal`;
                yield* sql`PRAGMA user_version = 10`;
                const before = yield* snapshotStore;

                armed = true;
                expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
                expect(yield* snapshotStore).toEqual(before);
                expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 10 }]);
                armed = false;
                yield* open;
                const after = yield* snapshotStore;

                for (const [table, rows] of Object.entries(before.contents)) {
                  if (table !== "effect_agent_meta") expect(after.contents[table]).toEqual(rows);
                }
                expect(
                  after.definitions.filter(
                    (entry) => entry.name !== "effect_agent_submissions_nonterminal",
                  ),
                ).toEqual(before.definitions);
                expect(
                  after.definitions.find(
                    (entry) => entry.name === "effect_agent_submissions_nonterminal",
                  )?.sql,
                ).toContain("WHERE state <> 'settled'");
                expect(yield* sql`PRAGMA user_version`).toEqual([{ user_version: 11 }]);
                yield* open;
                expect(yield* snapshotStore).toEqual(after);
              }),
            (location) =>
              armed && location === point
                ? mode === "interrupt"
                  ? Effect.interrupt
                  : mode === "defect"
                    ? Effect.die("upgrade defect")
                    : SqliteStorageFailpointError.make({ location })
                : Effect.void,
          );
        },
      );
    }
  }

  for (const [table, column] of [
    ["effect_agent_submissions", "queue_sequence"],
    ["effect_agent_recovery_checkpoints", "checkpoint_json"],
  ] as const) {
    it.effect(`rejects malformed v10 ${table} before any mutation`, () => {
      let armed = false;
      let mutations = 0;

      return withFixture(
        (open) =>
          Effect.gen(function* () {
            yield* open;
            const sql = yield* SqlClient.SqlClient;

            yield* sql`DROP INDEX effect_agent_submissions_nonterminal`;
            yield* sql`PRAGMA user_version = 10`;
            yield* sql.unsafe(`ALTER TABLE ${table} RENAME COLUMN ${column} TO malformed_column`);
            const before = yield* snapshotStore;

            armed = true;
            expect(yield* open.pipe(Effect.result)).toMatchObject({
              _tag: "Failure",
              failure: { _tag: "SqliteStorageCompatibilityError", actualVersion: 10 },
            });
            expect(mutations).toBe(0);
            expect(yield* snapshotStore).toEqual(before);
          }),
        (location) => {
          if (armed && location === "upgrade:before-mutation") mutations++;

          return Effect.void;
        },
      );
    });
  }

  it.effect("rejects a predecessor with a conflicting index without mutation", () =>
    withFixture((open) =>
      Effect.gen(function* () {
        yield* open;
        const sql = yield* SqlClient.SqlClient;

        yield* sql`PRAGMA user_version = 10`;
        const before = yield* snapshotStore;

        expect(yield* open.pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "SqliteStorageCompatibilityError", actualVersion: 10 },
        });
        expect(yield* snapshotStore).toEqual(before);
      }),
    ),
  );
});
