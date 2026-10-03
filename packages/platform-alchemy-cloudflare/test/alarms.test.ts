import { CloudflareAlarms } from "@yielded/agent-platform-cloudflare/cloudflare-alarms";
import {
  DurableObjectState as AlchemyState,
  fromDurableObjectState,
} from "alchemy/Cloudflare/Workers/DurableObjectState";
import { scheduleEvent } from "alchemy/Cloudflare/Workers/ScheduledEvents";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { env, runInDurableObject } from "cloudflare:test";
import { Cause, Clock, DateTime, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { describe, expect, it } from "vite-plus/test";

import * as Alarms from "../src/Alarms.ts";

const withStorage = <A, E>(
  make: (state: DurableObjectState) => Effect.Effect<A, E, AlchemyState | RuntimeContext>,
) =>
  runInDurableObject(env.PROBES.getByName(`alarms-${crypto.randomUUID()}`), (_instance, state) =>
    Effect.runPromise(
      make(state).pipe(
        Effect.provide(RuntimeContext.phantom),
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

const input = (
  id: string,
  runAt: number,
  payload: null | { version: number } = null,
  repeatEvery?: number,
) => ({
  id,
  tag: "reminder",
  runAt: DateTime.makeUnsafe(runAt),
  payload,
  ...(repeatEvery === undefined ? {} : { repeatEvery }),
});

const withAlarms = <A, E, R>(f: (alarms: CloudflareAlarms["Service"]) => Effect.Effect<A, E, R>) =>
  Effect.flatMap(CloudflareAlarms, f).pipe(Effect.provide(Alarms.layer));

describe("Alchemy alarm adapter", () => {
  it.each(["failure", "defect", "interruption"] as const)(
    "rolls back application SQL, events, and the native alarm on %s",
    (mode) =>
      withStorage((state) =>
        withAlarms((alarms) =>
          Effect.gen(function* () {
            state.storage.sql.exec("CREATE TABLE application (value INTEGER)");
            yield* scheduleEvent("initialize", new Date(Date.now() + 120_000), null);
            const before = snapshot(state);
            const alarm = yield* Effect.promise(() => state.storage.getAlarm());
            const entered = yield* Deferred.make<void>();

            const action = alarms.transaction((tx) =>
              Effect.gen(function* () {
                state.storage.sql.exec("INSERT INTO application VALUES (1)");
                yield* tx.scheduleAlarm(input("rollback", Date.now() + 60_000));
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
            } else expect(Exit.isFailure(yield* Effect.exit(action))).toBe(true);
            expect(state.storage.sql.exec("SELECT value FROM application").toArray()).toEqual([]);
            expect(snapshot(state)).toEqual(before);
            expect(yield* Effect.promise(() => state.storage.getAlarm())).toBe(alarm);
          }),
        ),
      ),
  );

  it("rejects a transaction handle after its callback and from a forked fiber", () =>
    withStorage(() =>
      withAlarms((alarms) =>
        Effect.gen(function* () {
          const escaped = yield* alarms.transaction((tx) => Effect.succeed(tx));

          expect(
            yield* Effect.result(escaped.cancelAlarm({ id: "missing", tag: "reminder" })),
          ).toMatchObject({ _tag: "Failure", failure: { reason: "invalid" } });

          const forked = yield* Effect.result(
            alarms.transaction((tx) =>
              Effect.gen(function* () {
                const fiber = yield* tx
                  .cancelAlarm({ id: "missing", tag: "reminder" })
                  .pipe(Effect.forkChild);

                return yield* Fiber.join(fiber);
              }),
            ),
          );

          expect(forked).toMatchObject({ _tag: "Failure", failure: { reason: "invalid" } });
        }),
      ),
    ));

  it("preserves failed work and a handler's replacement, then acknowledges success", () =>
    withStorage((state) =>
      withAlarms((alarms) =>
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;

          yield* alarms.transaction((tx) => tx.scheduleAlarm(input("first", now, { version: 1 })));
          expect(
            yield* Effect.result(
              alarms.processDue(() => Effect.fail("retry"), { mode: "ordered" }),
            ),
          ).toMatchObject({ _tag: "Failure", failure: "retry" });
          expect(snapshot(state)).toHaveLength(1);
          yield* alarms.processDue(
            (event) =>
              alarms.transaction((tx) =>
                tx.scheduleAlarm(input(event.id, now + 60_000, { version: 2 })),
              ),
            { mode: "ordered" },
          );
          expect(snapshot(state)).toMatchObject([
            { id: "effect-agent/alarm:reminder:first", run_at: now + 60_000 },
          ]);
          yield* alarms.transaction((tx) => tx.scheduleAlarm(input("complete", now)));
          yield* alarms.processDue(() => Effect.void, { mode: "ordered" });
          expect(snapshot(state).map((row) => row.id)).toEqual([
            "effect-agent/alarm:reminder:first",
          ]);
        }),
      ),
    ));

  it("bounds isolated processing and reschedules typed failures without losing other events", () =>
    withStorage((state) =>
      withAlarms((alarms) =>
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;

          yield* alarms.transaction((tx) =>
            Effect.forEach(["a", "b", "c"], (id) => tx.scheduleAlarm(input(id, now))),
          );
          const seen: Array<string> = [];

          yield* alarms.processDue(
            (event) => {
              seen.push(event.id);

              return event.id === "a" ? Effect.fail("retry") : Effect.void;
            },
            { mode: "isolated", limit: 2, retryFailedAfter: 60_000 },
          );
          expect(seen).toEqual(["a", "b"]);
          expect(snapshot(state).map((row) => row.id)).toEqual([
            "effect-agent/alarm:reminder:a",
            "effect-agent/alarm:reminder:c",
          ]);
          expect(snapshot(state)[0]?.run_at).toBeGreaterThanOrEqual(now + 60_000);
        }),
      ),
    ));

  it("retains an event interrupted during its handler", () =>
    withStorage((state) =>
      withAlarms((alarms) =>
        Effect.gen(function* () {
          yield* alarms.transaction((tx) => tx.scheduleAlarm(input("interrupted", Date.now())));

          const result = yield* Effect.exit(
            alarms.processDue(() => Effect.interrupt, { mode: "ordered" }),
          );

          expect(Exit.isFailure(result) && Cause.hasInterrupts(result.cause)).toBe(true);
          expect(snapshot(state)).toMatchObject([
            { id: "effect-agent/alarm:reminder:interrupted" },
          ]);
        }),
      ),
    ));

  it("serializes overlapping processors while permitting cancellation from the handler", () =>
    withStorage((state) =>
      withAlarms((alarms) =>
        Effect.gen(function* () {
          yield* alarms.transaction((tx) => tx.scheduleAlarm(input("overlap", Date.now())));
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          let calls = 0;

          const first = yield* alarms
            .processDue(
              (event) =>
                Effect.gen(function* () {
                  calls++;
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(release);
                  yield* alarms.transaction((tx) => tx.cancelAlarm(event));
                }),
              { mode: "ordered" },
            )
            .pipe(Effect.forkChild);

          yield* Deferred.await(entered);

          const second = yield* alarms
            .processDue(
              () =>
                Effect.sync(() => {
                  calls++;
                }),
              { mode: "ordered" },
            )
            .pipe(Effect.forkChild);

          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(first);
          yield* Fiber.join(second);
          expect(calls).toBe(1);
          expect(snapshot(state)).toEqual([]);
          expect(yield* Effect.promise(() => state.storage.getAlarm())).toBeNull();
        }),
      ),
    ));

  it("reschedules a successful repeat and rejects out-of-range persisted timestamps", () =>
    withStorage((state) =>
      withAlarms((alarms) =>
        Effect.gen(function* () {
          const now = Date.now();

          yield* alarms.transaction((tx) => tx.scheduleAlarm(input("repeat", now, null, 60_000)));
          yield* alarms.processDue(() => Effect.void, { mode: "ordered" });
          expect(snapshot(state)[0]?.run_at).toBeGreaterThanOrEqual(now + 60_000);
          state.storage.sql.exec(
            "UPDATE alchemy_scheduled_events SET run_at = -1 WHERE id = 'effect-agent/alarm:reminder:repeat'",
          );
          const corrupt = snapshot(state);

          expect(
            yield* Effect.result(alarms.processDue(() => Effect.void, { mode: "ordered" })),
          ).toMatchObject({ _tag: "Failure", failure: { reason: "invalid" } });
          expect(snapshot(state)).toEqual(corrupt);
        }),
      ),
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

        const events = snapshot(state).map((row) => ({
          ...row,
          payload: typeof row.payload === "string" ? JSON.parse(row.payload) : row.payload,
        }));

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
        expect(yield* Effect.promise(() => state.storage.getAlarm())).toBe(
          snapshot(state)[0]?.run_at,
        );
      }),
    ));

  it.each(["before-copy", "after-copy", "before-drop", "after-drop"] as const)(
    "preserves legacy data when adoption fails at %s",
    (location) =>
      withStorage((state) =>
        Effect.gen(function* () {
          seedLegacy(state, '{"version":3}');
          yield* scheduleEvent("existing", new Date(Date.now() + 120_000), { retained: true });

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
        yield* scheduleEvent("effect-agent/alarm:reminder:one", new Date(Date.now()), {
          _tag: "EffectAgentAlarm",
          version: 2,
          tag: "reminder",
          id: "one",
          payload: null,
        });
        const before = snapshot(state);

        const outcome = yield* CloudflareAlarms.use((alarms) =>
          alarms.processDue(() => Effect.die("must not dispatch"), { mode: "isolated" }),
        ).pipe(Effect.provide(Alarms.layer), Effect.result);

        expect(outcome).toMatchObject({ _tag: "Failure", failure: { reason: "invalid" } });
        expect(snapshot(state)).toEqual(before);
      }),
    ));
});
