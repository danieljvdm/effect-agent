import { CloudflareAlarms } from "@effect-agent/platform-cloudflare/cloudflare-alarms";
import {
  DurableObjectState as AlchemyState,
  fromDurableObjectState,
} from "alchemy/Cloudflare/Workers/DurableObjectState";
import {
  scheduledEventsTransaction,
  handleScheduledEvents,
} from "alchemy/Cloudflare/Workers/ScheduledEvents";
import { env, runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { describe, expect, it } from "vite-plus/test";

import * as Alarms from "../src/Alarms.ts";

const withStorage = <A, E>(
  make: (state: DurableObjectState) => Effect.Effect<A, E, AlchemyState>,
) =>
  runInDurableObject(env.PROBES.getByName(`alarms-${crypto.randomUUID()}`), (_instance, state) =>
    Effect.runPromise(
      make(state).pipe(
        // Both names describe workerd's native object; avoid recursively comparing
        // the imported and ambient Workers RPC declarations.
        Effect.provideService(
          AlchemyState,
          fromDurableObjectState(state as unknown as Parameters<typeof fromDurableObjectState>[0]),
        ),
      ),
    ),
  );

const snapshot = (state: DurableObjectState) =>
  state.storage.sql
    .exec("SELECT id, run_at, payload FROM alchemy_scheduled_events ORDER BY id")
    .toArray();

describe("Alchemy transactional scheduled events", () => {
  it.each(["failure", "defect", "interruption"] as const)(
    "rolls back application SQL, events, and the native alarm on %s",
    (mode) =>
      withStorage((state) =>
        Effect.gen(function* () {
          state.storage.sql.exec("CREATE TABLE application (value INTEGER)");
          // Initialize the scheduler outside the failing transaction so rollback is observable.
          yield* scheduledEventsTransaction(() => Effect.void);
          const entered = yield* Deferred.make<void>();

          const action = scheduledEventsTransaction((tx) =>
            Effect.gen(function* () {
              state.storage.sql.exec("INSERT INTO application VALUES (1)");
              yield* tx.upsert({
                id: "rollback",
                runAt: Date.now() + 60_000,
                payload: { committed: false },
              });
              yield* Deferred.succeed(entered, undefined);
              if (mode === "failure") return yield* Effect.fail("rollback");
              if (mode === "defect") return yield* Effect.die("rollback");

              return yield* Effect.never;
            }),
          );

          if (mode === "interruption") {
            const fiber = yield* action.pipe(Effect.forkChild);

            yield* Deferred.await(entered);
            yield* Fiber.interrupt(fiber);
          } else {
            expect(Exit.isFailure(yield* Effect.exit(action))).toBe(true);
          }
          expect(state.storage.sql.exec("SELECT value FROM application").toArray()).toEqual([]);
          expect(snapshot(state)).toEqual([]);
          expect(yield* Effect.promise(() => state.storage.getAlarm())).toBeNull();
        }),
      ),
  );

  it("rejects a transaction handle after its callback and from a forked fiber", () =>
    withStorage(() =>
      Effect.gen(function* () {
        const escaped = yield* scheduledEventsTransaction((tx) => Effect.succeed(tx));
        const after = yield* Effect.result(escaped.delete("missing"));

        expect(after).toMatchObject({
          _tag: "Failure",
          failure: { reason: "invalid-transaction" },
        });

        const forked = yield* Effect.result(
          scheduledEventsTransaction((tx) =>
            Effect.gen(function* () {
              const fiber = yield* tx.delete("missing").pipe(Effect.forkChild);

              return yield* Fiber.join(fiber);
            }),
          ),
        );

        expect(forked).toMatchObject({
          _tag: "Failure",
          failure: { reason: "invalid-transaction" },
        });
      }),
    ));

  it("preserves failed work and a handler's replacement, then acknowledges success", () =>
    withStorage((state) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;

        yield* scheduledEventsTransaction((tx) =>
          tx.upsert({ id: "first", runAt: now, payload: { version: 1 } }),
        );
        const failed = yield* Effect.result(handleScheduledEvents(() => Effect.fail("retry")));

        expect(failed).toMatchObject({ _tag: "Failure", failure: "retry" });
        expect(snapshot(state)).toHaveLength(1);
        yield* handleScheduledEvents((event) =>
          scheduledEventsTransaction((tx) =>
            tx.upsert({ ...event, runAt: now + 60_000, payload: { version: 2 } }),
          ),
        );
        expect(snapshot(state)).toMatchObject([{ id: "first", run_at: now + 60_000 }]);
        yield* scheduledEventsTransaction((tx) =>
          tx.upsert({ id: "complete", runAt: now, payload: null }),
        );
        expect(yield* handleScheduledEvents(() => Effect.void)).toBe(1);
        expect(snapshot(state).map((row) => row.id)).toEqual(["first"]);
      }),
    ));

  it("bounds isolated processing and reschedules typed failures without losing other events", () =>
    withStorage((state) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;

        yield* scheduledEventsTransaction((tx) =>
          Effect.forEach(["a", "b", "c"], (id) => tx.upsert({ id, runAt: now, payload: null })),
        );
        const seen: Array<string> = [];

        const handled = yield* handleScheduledEvents(
          (event) => {
            seen.push(event.id);

            return event.id === "a" ? Effect.fail("retry") : Effect.void;
          },
          { mode: "isolated", limit: 2, retryMillis: 60_000 },
        );

        expect(handled).toBe(1);
        expect(seen).toEqual(["a", "b"]);
        expect(snapshot(state).map((row) => row.id)).toEqual(["a", "c"]);
        expect(snapshot(state)[0]?.run_at).toBeGreaterThanOrEqual(now + 60_000);
      }),
    ));

  it("retains an event interrupted during its handler", () =>
    withStorage((state) =>
      Effect.gen(function* () {
        yield* scheduledEventsTransaction((tx) =>
          tx.upsert({ id: "interrupted", runAt: Date.now(), payload: null }),
        );
        const result = yield* Effect.exit(handleScheduledEvents(() => Effect.interrupt));

        expect(Exit.isFailure(result) && Cause.hasInterrupts(result.cause)).toBe(true);
        expect(snapshot(state)).toMatchObject([{ id: "interrupted" }]);
      }),
    ));
  it("rejects recursive processing and transaction nesting without deadlocking", () =>
    withStorage(() =>
      Effect.gen(function* () {
        yield* scheduledEventsTransaction((tx) =>
          tx.upsert({ id: "nested", runAt: Date.now(), payload: null }),
        );
        for (const action of [
          scheduledEventsTransaction(() => handleScheduledEvents(() => Effect.void)),
          handleScheduledEvents(() => handleScheduledEvents(() => Effect.void)),
        ]) {
          expect(yield* Effect.result(action)).toMatchObject({
            _tag: "Failure",
            failure: { reason: "invalid-transaction" },
          });
        }
      }),
    ));

  it("serializes overlapping processors while permitting cancellation from the handler", () =>
    withStorage((state) =>
      Effect.gen(function* () {
        yield* scheduledEventsTransaction((tx) =>
          tx.upsert({ id: "overlap", runAt: Date.now(), payload: null }),
        );
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let calls = 0;

        const first = yield* handleScheduledEvents((event) =>
          Effect.gen(function* () {
            calls++;
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
            yield* scheduledEventsTransaction((tx) => tx.delete(event.id));
          }),
        ).pipe(Effect.forkChild);

        yield* Deferred.await(entered);

        const second = yield* handleScheduledEvents(() =>
          Effect.sync(() => {
            calls++;
          }),
        ).pipe(Effect.forkChild);

        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(first);
        yield* Fiber.join(second);
        expect(calls).toBe(1);
        expect(snapshot(state)).toEqual([]);
        expect(yield* Effect.promise(() => state.storage.getAlarm())).toBeNull();
      }),
    ));

  it("validates the whole selected batch before isolated dispatch or alarm reconciliation", () =>
    withStorage((state) =>
      Effect.gen(function* () {
        yield* scheduledEventsTransaction((tx) =>
          Effect.forEach(["a", "b"], (id) => tx.upsert({ id, runAt: Date.now(), payload: null })),
        );
        const before = snapshot(state);
        const alarm = yield* Effect.promise(() => state.storage.getAlarm());
        let calls = 0;

        const result = yield* Effect.result(
          handleScheduledEvents(
            () =>
              Effect.sync(() => {
                calls++;
              }),
            {
              mode: "isolated",
              validate: (event) => (event.id === "b" ? Effect.fail("unsupported") : Effect.void),
            },
          ),
        );

        expect(result).toMatchObject({ _tag: "Failure", failure: "unsupported" });
        expect(calls).toBe(0);
        expect(snapshot(state)).toEqual(before);
        expect(yield* Effect.promise(() => state.storage.getAlarm())).toBe(alarm);
      }),
    ));

  it("reschedules a successful repeat and rejects out-of-range persisted timestamps", () =>
    withStorage((state) =>
      Effect.gen(function* () {
        const now = Date.now();

        yield* scheduledEventsTransaction((tx) =>
          tx.upsert({ id: "repeat", runAt: now, repeatMs: 60_000, payload: null }),
        );
        expect(yield* handleScheduledEvents(() => Effect.void)).toBe(1);
        expect(snapshot(state)[0]?.run_at).toBeGreaterThanOrEqual(now + 60_000);
        state.storage.sql.exec(
          "UPDATE alchemy_scheduled_events SET run_at = 9000000000000000 WHERE id = 'repeat'",
        );
        const corrupt = snapshot(state);

        expect(yield* Effect.result(scheduledEventsTransaction((tx) => tx.list()))).toMatchObject({
          _tag: "Failure",
          failure: { reason: "invalid-event" },
        });
        expect(snapshot(state)).toEqual(corrupt);
      }),
    ));
});

const seedLegacy = (state: DurableObjectState, payload: string) => {
  state.storage.sql.exec(
    "CREATE TABLE effect_cf_scheduled_alarms (storage_id TEXT PRIMARY KEY, alarm_id TEXT NOT NULL, tag TEXT NOT NULL, run_at INTEGER NOT NULL, repeat_every_ms INTEGER, payload TEXT NOT NULL)",
  );
  state.storage.sql.exec(
    "INSERT INTO effect_cf_scheduled_alarms VALUES (?, ?, ?, ?, NULL, ?)",
    "effect-cf-alarm:reminder:one",
    "one",
    "reminder",
    Date.now() + 60_000,
    payload,
  );
};

describe("alarm host adoption", () => {
  it("atomically retains legacy identity, payload, and native wake time", () =>
    withStorage((state) =>
      Effect.gen(function* () {
        seedLegacy(state, '{"version":3}');
        yield* Layer.build(Alarms.layer).pipe(Effect.scoped);
        const events = yield* scheduledEventsTransaction((tx) => tx.list());

        expect(events).toMatchObject([
          {
            id: "effect-agent/alarm:reminder:one",
            payload: { tag: "reminder", id: "one", payload: { version: 3 } },
          },
        ]);
        expect(
          state.storage.sql
            .exec("SELECT name FROM sqlite_master WHERE name='effect_cf_scheduled_alarms'")
            .toArray(),
        ).toEqual([]);
        expect(yield* Effect.promise(() => state.storage.getAlarm())).toBe(events[0]?.runAt);
      }),
    ));

  it.each(["before-copy", "after-copy", "before-drop", "after-drop"] as const)(
    "preserves legacy data when adoption fails at %s",
    (location) =>
      withStorage((state) =>
        Effect.gen(function* () {
          seedLegacy(state, '{"version":3}');
          yield* scheduledEventsTransaction((tx) =>
            tx.upsert({ id: "existing", runAt: Date.now() + 120_000, payload: { retained: true } }),
          );

          const before = state.storage.sql
            .exec("SELECT * FROM effect_cf_scheduled_alarms")
            .toArray();

          const destination = snapshot(state);
          const alarm = yield* Effect.promise(() => state.storage.getAlarm());

          const result = yield* Effect.exit(
            Layer.build(Alarms.layer).pipe(
              Effect.provideService(Alarms.AlarmMigrationFailpoint, {
                hit: (current) =>
                  current === location ? Effect.die("migration failpoint") : Effect.void,
              }),
              Effect.scoped,
            ),
          );

          expect(Exit.isFailure(result)).toBe(true);
          expect(
            state.storage.sql.exec("SELECT * FROM effect_cf_scheduled_alarms").toArray(),
          ).toEqual(before);
          expect(snapshot(state)).toEqual(destination);
          expect(yield* Effect.promise(() => state.storage.getAlarm())).toBe(alarm);
        }),
      ),
  );

  it("refuses malformed persisted payloads without changing the previous table", () =>
    withStorage((state) =>
      Effect.gen(function* () {
        seedLegacy(state, "not-json");
        const result = yield* Effect.result(Layer.build(Alarms.layer).pipe(Effect.scoped));

        expect(result).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "CloudflareAlarmError", reason: "invalid" },
        });
        expect(
          state.storage.sql.exec("SELECT payload FROM effect_cf_scheduled_alarms").toArray(),
        ).toEqual([{ payload: "not-json" }]);
      }),
    ));

  it("refuses unsupported host envelopes without rescheduling them in isolated mode", () =>
    withStorage((state) =>
      Effect.gen(function* () {
        yield* scheduledEventsTransaction((tx) =>
          tx.upsert({
            id: "effect-agent/alarm:reminder:one",
            runAt: Date.now(),
            payload: {
              _tag: "EffectAgentAlarm",
              version: 2,
              tag: "reminder",
              id: "one",
              payload: null,
            },
          }),
        );
        const before = snapshot(state);

        const outcome = yield* CloudflareAlarms.use((alarms) =>
          alarms.processDue(() => Effect.die("must not dispatch"), { mode: "isolated" }),
        ).pipe(Effect.provide(Alarms.layer), Effect.result);

        expect(outcome).toMatchObject({ _tag: "Failure", failure: { reason: "invalid" } });
        expect(snapshot(state)).toEqual(before);
      }),
    ));
});
