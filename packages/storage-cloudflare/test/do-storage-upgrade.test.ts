import {
  ScheduleFailpoint,
  ScheduleFailpointError,
  ScheduleStorageError,
  ScheduleStore,
} from "@effect-agent/thread/Schedule";
import { SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import {
  SubscriptionError,
  SubscriptionFailpoint,
  SubscriptionFailpointError,
  SubscriptionStore,
} from "@effect-agent/thread/Subscription";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Exit, Layer } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
  restoreV2,
  assertPreserved,
  assertReceiptReplay,
  assertSubscriptionReplay,
  snapshotStore,
  fixturePartition,
  type FixtureStore,
} from "../../../test/fixtures/storage-upgrade.ts";
import { DoScheduleTransaction, scheduleStoreLayer } from "../src/DoScheduleStore.ts";
import { DoStorageFailpointError } from "../src/DoStorageError.ts";
import { DoStorageFailpoint } from "../src/DoStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/DoSubmissionLedger.ts";
import { DoSubscriptionTransaction, doSubscriptionStoreLayer } from "../src/DoSubscriptionStore.ts";
import { storageConfigLayer, type DoStorageInitializationError } from "../src/DoThreadStore.ts";
import { withScheduleStorage } from "./harness.ts";

let counter = 0;

const fixture = <A, E>(
  store: Exclude<FixtureStore, "sqlite">,
  body: (
    open: Effect.Effect<
      void,
      | DoStorageFailpointError
      | ScheduleStorageError
      | SubscriptionError
      | DoStorageInitializationError,
      SqlClientService.SqlClient
    >,
    dependencies: ReturnType<typeof services>,
  ) => Effect.Effect<A, E, SqlClientService.SqlClient>,
  hit: (point: string) => "failure" | "interrupt" | "defect" | undefined = () => undefined,
) =>
  withScheduleStorage(`v2-upgrade-${counter++}`, (storage) =>
    Effect.gen(function* () {
      yield* restoreV2(store);
      yield* Effect.promise(() => storage.setAlarm(4_000_000_000_000));
      const deps = services(storage, hit);

      const open =
        store === "thread"
          ? SubmissionLedger.pipe(
              Effect.asVoid,
              Effect.provide(submissionLedgerLayer.pipe(Layer.provide(deps))),
            )
          : store === "schedule"
            ? ScheduleStore.pipe(
                Effect.asVoid,
                Effect.provide(scheduleStoreLayer.pipe(Layer.provide(deps))),
              )
            : SubscriptionStore.pipe(
                Effect.asVoid,
                Effect.provide(
                  doSubscriptionStoreLayer(fixturePartition).pipe(Layer.provide(deps)),
                ),
              );

      const value = yield* body(open, deps);

      expect(yield* Effect.promise(() => storage.getAlarm())).toBe(4_000_000_000_000);

      return value;
    }).pipe(Effect.provide(SqliteClient.layer({ storage }))),
  );

const services = (
  storage: DurableObjectStorage,
  hit: (point: string) => "failure" | "interrupt" | "defect" | undefined,
) => {
  const fault = <E>(point: string, error: E): Effect.Effect<void, E> => {
    switch (hit(point)) {
      case "failure":
        return Effect.fail(error);
      case "interrupt":
        return Effect.interrupt;
      case "defect":
        return Effect.die("upgrade fixture defect");
      case undefined:
        return Effect.void;
    }
  };

  return Layer.mergeAll(
    storageConfigLayer({ storage }),
    BrowserCrypto.layer,
    Layer.succeed(DoStorageFailpoint)({
      hit: (location) => fault(location, DoStorageFailpointError.make({ location })),
    }),
    Layer.succeed(ScheduleFailpoint)({
      hit: (point) => fault(point, ScheduleFailpointError.make({ point })),
    }),
    Layer.succeed(SubscriptionFailpoint)({
      hit: (point) => fault(point, SubscriptionFailpointError.make({ point })),
    }),
    Layer.effect(DoScheduleTransaction)(
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;

        return DoScheduleTransaction.of({
          run: (body) =>
            sql.withTransaction(body(() => Effect.void)).pipe(
              Effect.catchTag("SqlError", () =>
                ScheduleStorageError.make({
                  operation: "fixture transaction",
                  reason: "unavailable",
                }),
              ),
            ),
        });
      }),
    ),
    Layer.effect(DoSubscriptionTransaction)(
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;

        return DoSubscriptionTransaction.of({
          run: (body) =>
            sql
              .withTransaction(body(() => Effect.void))
              .pipe(
                Effect.catchTag("SqlError", () =>
                  SubscriptionError.make({ reason: "storage", code: "fixture-transaction" }),
                ),
              ),
        });
      }),
    ),
  );
};

describe("unpatched v2 native storage upgrade", () => {
  for (const point of [
    "upgrade:before-mutation",
    "upgrade:after-mutation",
    "upgrade:before-version",
    "upgrade:after-version",
  ] as const) {
    it(`preserves v4 data and atomically adds recovery checkpoints at ${point}`, () => {
      let armed = false;

      return fixture(
        "thread",
        (open) =>
          Effect.gen(function* () {
            yield* open;
            const sql = yield* SqlClientService.SqlClient;

            yield* sql`DROP TABLE effect_agent_recovery_checkpoints`;
            yield* sql`UPDATE effect_agent_meta SET value='4' WHERE key='storage_version'`;
            const before = yield* snapshotStore;

            armed = true;
            expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
            expect(yield* snapshotStore).toEqual(before);
            expect(
              yield* sql`SELECT value FROM effect_agent_meta WHERE key='storage_version'`,
            ).toEqual([{ value: "4" }]);
            armed = false;
            yield* open;
            yield* assertPreserved("thread");
            expect(
              yield* sql`SELECT value FROM effect_agent_meta WHERE key='storage_version'`,
            ).toEqual([{ value: "5" }]);
            expect(yield* sql`SELECT * FROM effect_agent_recovery_checkpoints`).toEqual([]);
            const upgraded = yield* snapshotStore;

            yield* open;
            expect(yield* snapshotStore).toEqual(upgraded);
          }),
        (location) => (armed && location === point ? "failure" : undefined),
      );
    });
  }

  it("rejects an ambiguous v4 layout without changing retained data or native alarm", () =>
    fixture("thread", (open) =>
      Effect.gen(function* () {
        yield* open;
        const sql = yield* SqlClientService.SqlClient;

        yield* sql`DROP TABLE effect_agent_recovery_checkpoints`;
        yield* sql`ALTER TABLE effect_agent_submissions RENAME COLUMN message_admission_json TO malformed_column`;
        yield* sql`UPDATE effect_agent_meta SET value='4' WHERE key='storage_version'`;
        const before = yield* snapshotStore;

        expect(yield* open.pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "DoStorageCompatibilityError", actualVersion: 4 },
        });
        expect(yield* snapshotStore).toEqual(before);
      }),
    ));

  for (const [table, column] of [
    ["effect_agent_submissions", "admission_fence_json"],
    ["effect_agent_child_settlements", "child_outcome"],
  ] as const) {
    it(`rejects v3 missing ${table}.${column} without mutating storage or its alarm`, () => {
      let armed = false;
      let mutations = 0;

      return fixture(
        "thread",
        (open) =>
          Effect.gen(function* () {
            yield* open;
            const sql = yield* SqlClientService.SqlClient;

            yield* sql`DROP TABLE effect_agent_recovery_checkpoints`;
            yield* sql`DROP TABLE effect_agent_message_deliveries`;
            yield* sql`ALTER TABLE effect_agent_submissions DROP COLUMN worker_admission_json`;
            yield* sql`ALTER TABLE effect_agent_submissions DROP COLUMN message_admission_json`;
            yield* sql`UPDATE effect_agent_meta SET value='3' WHERE key='storage_version'`;
            yield* sql.unsafe(`ALTER TABLE ${table} DROP COLUMN ${column}`);
            const before = yield* snapshotStore;

            armed = true;
            expect(yield* open.pipe(Effect.result)).toMatchObject({
              _tag: "Failure",
              failure: { _tag: "DoStorageCompatibilityError", actualVersion: 3 },
            });
            expect(mutations).toBe(0);
            expect(yield* snapshotStore).toEqual(before);
            expect(
              yield* sql`SELECT value FROM effect_agent_meta WHERE key='storage_version'`,
            ).toEqual([{ value: "3" }]);
          }),
        (location) => {
          if (armed && location === "upgrade:before-mutation") mutations += 1;

          return undefined;
        },
      );
    });
  }

  it("adds message storage to v3 atomically and preserves native alarm and canonical data", () => {
    let armed = false;

    return fixture(
      "thread",
      (open) =>
        Effect.gen(function* () {
          yield* open;
          const sql = yield* SqlClientService.SqlClient;

          yield* sql`DROP TABLE effect_agent_recovery_checkpoints`;
          yield* sql`DROP TABLE effect_agent_message_deliveries`;
          yield* sql`ALTER TABLE effect_agent_submissions DROP COLUMN worker_admission_json`;
          yield* sql`ALTER TABLE effect_agent_submissions DROP COLUMN message_admission_json`;
          yield* sql`UPDATE effect_agent_meta SET value='3' WHERE key='storage_version'`;
          const before = yield* snapshotStore;

          armed = true;
          expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
          expect(yield* snapshotStore).toEqual(before);
          armed = false;
          yield* open;
          yield* assertPreserved("thread");
          expect(
            yield* sql`SELECT value FROM effect_agent_meta WHERE key='storage_version'`,
          ).toEqual([{ value: "5" }]);
          expect(yield* sql`SELECT * FROM effect_agent_message_deliveries`).toEqual([]);
        }),
      (point) => (armed && point === "upgrade:after-version" ? "failure" : undefined),
    );
  });
  for (const store of ["thread", "schedule", "subscription"] as const) {
    it(`preserves ${store} state, native alarm and repeated acquisition`, () =>
      fixture(store, (open, deps) =>
        Effect.gen(function* () {
          yield* open;
          yield* assertPreserved(store);
          const after = yield* snapshotStore;

          yield* open;
          expect(yield* snapshotStore).toEqual(after);
          const sql = yield* SqlClientService.SqlClient;

          if (store === "thread") {
            expect(
              yield* sql`SELECT value FROM effect_agent_meta WHERE key='storage_version'`,
            ).toEqual([{ value: "5" }]);
            expect(yield* sql`SELECT * FROM effect_agent_child_settlements`).toEqual([
              {
                parent_submission_id: "parent",
                child_submission_id: "child",
                child_outcome: null,
                recorded_at: "1970-01-01T00:00:01.000Z",
              },
            ]);
            yield* assertReceiptReplay.pipe(
              Effect.provide(submissionLedgerLayer.pipe(Layer.provide(deps))),
            );
          } else {
            expect(
              yield* sql`SELECT storage_version,alarm_generation FROM ${sql(store === "schedule" ? "effect_agent_schedule_store_state" : "effect_agent_subscription_store_state")}`,
            ).toEqual([{ storage_version: 3, alarm_generation: store === "schedule" ? 17 : 23 }]);
            if (store === "subscription")
              yield* assertSubscriptionReplay.pipe(
                Effect.provide(
                  doSubscriptionStoreLayer(fixturePartition).pipe(Layer.provide(deps)),
                ),
              );
          }
        }),
      ));
    for (const mode of ["failure", "interrupt", "defect"] as const) {
      it(`rolls back ${store} on ${mode} after mutation and reopens`, () => {
        let armed = true;

        return fixture(
          store,
          (open) =>
            Effect.gen(function* () {
              const before = yield* snapshotStore;

              expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
              expect(yield* snapshotStore).toEqual(before);
              armed = false;
              yield* open;
              yield* assertPreserved(store);
            }),
          (point) => (armed && point === "upgrade:after-mutation" ? mode : undefined),
        );
      });
    }
    it(`rolls back ${store} when version write fails`, () => {
      let armed = true;

      return fixture(
        store,
        (open) =>
          Effect.gen(function* () {
            const before = yield* snapshotStore;

            expect(Exit.isFailure(yield* open.pipe(Effect.exit))).toBe(true);
            expect(yield* snapshotStore).toEqual(before);
            armed = false;
            yield* open;
            yield* assertPreserved(store);
          }),
        (point) => (armed && point === "upgrade:after-version" ? "failure" : undefined),
      );
    });
  }
  it("rejects a missing required v2 table without adding columns or advancing the version", () =>
    fixture("thread", (open) =>
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;

        yield* sql`DROP TABLE effect_agent_child_settlements`;
        const before = yield* snapshotStore;
        const failure = yield* open.pipe(Effect.result);

        expect(failure).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "DoStorageCompatibilityError", actualVersion: 2 },
        });
        expect(yield* snapshotStore).toEqual(before);
      }),
    ));

  it("retains ambiguous prepared delivery bytes and the v2 version", () =>
    fixture("subscription", (open) =>
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;

        yield* sql`UPDATE effect_agent_subscription_deliveries SET record_json=json_set(record_json,'$.subscriptionFingerprint',${"f".repeat(64)}) WHERE event_id='prepared'`;
        const before = yield* snapshotStore;
        const failure = yield* open.pipe(Effect.result);

        expect(failure).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "SubscriptionError", reason: "corrupt" },
        });
        expect(yield* snapshotStore).toEqual(before);
      }),
    ));
});
