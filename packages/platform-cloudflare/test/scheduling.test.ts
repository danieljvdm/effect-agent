import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Schema } from "effect";
import { ScheduleId, defaultSchedulingLimits, type ScheduleOwner } from "effect-agent/schedule";
import { scheduleOwnerKey } from "effect-agent/schedule-transition";
import { Scheduling } from "effect-agent/scheduling";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { scheduleAlarmHandler } from "../src/CloudflareScheduling.ts";
import {
  TEST_DIGESTS,
  TEST_PRINCIPAL,
  armScheduleAdmissionPause,
  armScheduleEviction,
  armScheduleFailure,
  decodeThreadId,
  holdScheduleAuthorizationFailures,
  observedCommittedPrepareBeforeEviction,
  observeScheduleIdle,
  plannerDefinition,
  schedulePrepareHolds,
} from "./fixtures.ts";
import { laneRows, runScheduleClient, scheduleStubFor } from "./harness.ts";

const AlarmRow = Schema.Struct({ run_at: Schema.Number, payload: Schema.String });

const StoreProbe = Schema.Struct({
  schedule_count: Schema.Natural,
  alarm_generation: Schema.Natural,
});

let identity = 0;

const fixture = (label: string) => {
  const suffix = identity++;

  const owner: ScheduleOwner = {
    tenantId: `cf-schedule-tenant-${label}-${suffix}`,
    ownerId: `cf-schedule-owner-${label}-${suffix}`,
  };

  return {
    owner,
    scope: { owner, principal: TEST_PRINCIPAL },
    scheduleId: Schema.decodeSync(ScheduleId)(`schedule-${label}-${suffix}`),
    thread: `cf-schedule-thread-${label}-${suffix}`,
  };
};

const alarmRows = (owner: ScheduleOwner) =>
  runInDurableObject(scheduleStubFor(owner), (_instance, state) => {
    const tables = state.storage.sql
      .exec(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_cf_scheduled_alarms'",
      )
      .toArray();

    if (tables.length === 0) return [];

    const rows = state.storage.sql
      .exec("SELECT run_at, payload FROM effect_cf_scheduled_alarms ORDER BY storage_id")
      .toArray();

    return Schema.decodeUnknownSync(Schema.Array(AlarmRow))(rows);
  });

const storeProbe = (owner: ScheduleOwner) =>
  runInDurableObject(scheduleStubFor(owner), (_instance, state) => {
    const rows = state.storage.sql
      .exec(
        `SELECT
           (SELECT COUNT(*) FROM effect_agent_schedules) AS schedule_count,
           alarm_generation
         FROM effect_agent_schedule_store_state
         WHERE singleton = 1`,
      )
      .toArray();

    return Schema.decodeUnknownSync(Schema.Array(StoreProbe))(rows)[0];
  });

const snapshotFor = (data: ReturnType<typeof fixture>) =>
  runScheduleClient(
    Effect.gen(function* () {
      const client = yield* Scheduling;

      return yield* client.get(data.scope, data.scheduleId);
    }),
  );

const manage = (
  data: ReturnType<typeof fixture>,
  atMillis: number,
  question: string,
  expectedRevision?: number,
) =>
  runScheduleClient(
    Effect.gen(function* () {
      const client = yield* Scheduling;

      const options = {
        scope: data.scope,
        scheduleId: data.scheduleId,
        timing: { _tag: "At" as const, atMillis },
        destination: {
          _tag: "ExistingThread" as const,
          threadId: decodeThreadId(data.thread),
        },
        deliveryPrincipal: TEST_PRINCIPAL,
        definitions: TEST_DIGESTS,
      };

      return expectedRevision === undefined
        ? yield* client.create(
            { definition: plannerDefinition },
            { question, ref: data.thread },
            options,
          )
        : yield* client.update(
            { definition: plannerDefinition },
            { question, ref: data.thread },
            { ...options, expectedRevision },
          );
    }),
  );

describe("Cloudflare Schedule Owner", () => {
  it("reaches healthy work after a full corrupt due page and retains its recovery alarm", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const healthy = fixture("healthy-after-page");

        const broken = Array.from({ length: 16 }, () => ({
          ...fixture("broken-first-page"),
          owner: healthy.owner,
          scope: healthy.scope,
        }));

        for (const entry of [...broken, healthy]) {
          yield* Effect.promise(() =>
            manage(entry, Date.now() + 86_400_000, "progress past failed work"),
          );
        }
        yield* Effect.promise(() =>
          runInDurableObject(scheduleStubFor(healthy.owner), (_instance, state) => {
            state.storage.sql.exec(
              "UPDATE effect_agent_schedules SET deadline_at_millis = 0, record_json = json_set(record_json, '$.nextAtMillis', 0)",
            );
            state.storage.sql.exec("UPDATE effect_cf_scheduled_alarms SET run_at = 0");
            for (const entry of broken) {
              state.storage.sql.exec(
                "UPDATE effect_agent_schedules SET record_json = ? WHERE schedule_id = ?",
                "{invalid-json",
                entry.scheduleId,
              );
            }
          }),
        );
        yield* TestClock.setTime(Date.now() + 3_600_000);
        const clock = yield* Clock.Clock;
        const before = yield* Effect.promise(() => storeProbe(healthy.owner));

        const runPass = () =>
          Effect.promise(() =>
            runInDurableObject(scheduleStubFor(healthy.owner), (instance) =>
              instance[DurableObject.RunSymbol](
                scheduleAlarmHandler(defaultSchedulingLimits).pipe(
                  Effect.provideService(Clock.Clock, clock),
                ),
              ),
            ),
          );

        yield* runPass();

        expect((yield* Effect.promise(() => snapshotFor(healthy))).lastReceipt).not.toBeNull();
        expect(yield* Effect.promise(() => laneRows(healthy.thread))).toHaveLength(1);
        const after = yield* Effect.promise(() => storeProbe(healthy.owner));

        expect(after?.alarm_generation).toBeGreaterThan(before?.alarm_generation ?? 0);
        const alarms = yield* Effect.promise(() => alarmRows(healthy.owner));

        expect(alarms).toHaveLength(1);
        expect(alarms[0]?.run_at).toBeGreaterThan(yield* Clock.currentTimeMillis);
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));

  it("interrupts an overlong schedule alarm and preserves its replacement wake and scoped cleanup", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const data = fixture("alarm-deadline");
        const entered = yield* Deferred.make<void>();
        const finished = yield* Deferred.make<void>();

        yield* Effect.promise(() => manage(data, Date.now() + 86_400_000, "deadline recovery"));
        yield* Effect.promise(() =>
          runInDurableObject(scheduleStubFor(data.owner), (_instance, state) => {
            state.storage.sql.exec(
              "UPDATE effect_agent_schedules SET deadline_at_millis = 0, record_json = json_set(record_json, '$.nextAtMillis', 0)",
            );
            state.storage.sql.exec("UPDATE effect_cf_scheduled_alarms SET run_at = 0");
          }),
        );
        yield* TestClock.setTime(Date.now() + 3_600_000);
        const clock = yield* Clock.Clock;
        const key = scheduleOwnerKey(data.owner);

        schedulePrepareHolds.set(key, { entered, finished });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            schedulePrepareHolds.delete(key);
          }),
        );
        const before = yield* Effect.promise(() => storeProbe(data.owner));

        const runPass = () =>
          Effect.tryPromise({
            try: () =>
              runInDurableObject(scheduleStubFor(data.owner), (instance) =>
                instance[DurableObject.RunSymbol](
                  scheduleAlarmHandler(defaultSchedulingLimits).pipe(
                    Effect.provideService(Clock.Clock, clock),
                  ),
                ),
              ),
            catch: (cause) => String(cause),
          });

        const pending = yield* runPass().pipe(Effect.exit, Effect.forkChild);

        yield* Deferred.await(entered);
        yield* TestClock.adjust("14 minutes");
        const exit = yield* Fiber.join(pending);

        expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "success").toContain(
          "TimeoutError",
        );
        expect(yield* Deferred.isDone(finished)).toBe(true);
        expect(
          (yield* Effect.promise(() => storeProbe(data.owner)))?.alarm_generation,
        ).toBeGreaterThan(before?.alarm_generation ?? 0);
        expect(yield* Effect.promise(() => alarmRows(data.owner))).toHaveLength(1);
        expect((yield* Effect.promise(() => snapshotFor(data))).lastReceipt).toBeNull();
        yield* runPass();
        expect((yield* Effect.promise(() => snapshotFor(data))).lastReceipt).not.toBeNull();
        expect(yield* Effect.promise(() => laneRows(data.thread))).toHaveLength(1);
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));

  it("rolls back the schedule row and logical/native alarm when alarm mutation fails", async () => {
    const data = fixture("alarm-rollback");

    armScheduleFailure(data.owner, "schedule:alarm:after");
    await expect(manage(data, Date.now() + 60_000, "must roll back")).rejects.toMatchObject({
      _tag: "ScheduleFailpointError",
      point: "schedule:alarm:after",
    });

    expect(await storeProbe(data.owner)).toEqual({ schedule_count: 0, alarm_generation: 0 });
    expect(await alarmRows(data.owner)).toEqual([]);
    expect(
      await runInDurableObject(scheduleStubFor(data.owner), (_instance, state) =>
        state.storage.getAlarm(),
      ),
    ).toBeNull();

    const retried = await manage(data, Date.now() + 60_000, "must roll back");

    expect(retried.configurationRevision).toBe(1);
  });

  for (const operation of ["cancel"] as const) {
    it(`${operation}s without stranding an occurrence already accepted by Thread`, async () => {
      const data = fixture(`pending-${operation}`);
      const gate = armScheduleAdmissionPause(data.owner);
      const created = await manage(data, Date.now(), `${operation} after Thread accepts`);
      const alarm = runDurableObjectAlarm(scheduleStubFor(data.owner));

      await gate.reached;

      const controlled = await runScheduleClient(
        Effect.gen(function* () {
          const client = yield* Scheduling;

          return yield* client[operation](
            data.scope,
            data.scheduleId,
            created.configurationRevision,
          );
        }),
      );

      expect(controlled.state).toBe("cancelled");
      expect(controlled.pending).not.toBeNull();
      expect(await alarmRows(data.owner)).toHaveLength(1);

      gate.release();
      await alarm;
      const recovered = await snapshotFor(data);

      expect(recovered.pending).toBeNull();
      expect(recovered.lastReceipt).not.toBeNull();
      expect(await laneRows(data.thread)).toHaveLength(1);
      expect(await alarmRows(data.owner)).toEqual([]);
    });
  }

  it("a stale handler acknowledgement retains a newer update alarm", async () => {
    const data = fixture("stale-ack");
    const gate = armScheduleAdmissionPause(data.owner);
    const created = await manage(data, Date.now(), "old occurrence");
    const alarm = runDurableObjectAlarm(scheduleStubFor(data.owner));

    await gate.reached;
    const beforeUpdate = await alarmRows(data.owner);

    const replacementDeadline = Date.now() + 120_000;

    await manage(
      data,
      replacementDeadline,
      "new schedule generation",
      created.configurationRevision,
    );
    const afterUpdate = await alarmRows(data.owner);

    expect(afterUpdate[0]?.payload).not.toBe(beforeUpdate[0]?.payload);

    gate.release();
    await alarm;
    const afterOldAcknowledgement = await alarmRows(data.owner);

    expect(afterOldAcknowledgement).toHaveLength(1);
    expect(afterOldAcknowledgement[0]?.run_at).toBe(replacementDeadline);
    expect(await laneRows(data.thread)).toHaveLength(1);
  });

  it("recovers a committed pending occurrence after eviction immediately after prepare", async () => {
    const data = fixture("prepare-eviction");
    const idle = observeScheduleIdle(data.owner);
    const admissionGate = armScheduleAdmissionPause(data.owner);

    armScheduleEviction(data.owner, "schedule:prepare:after");
    await manage(data, Date.now(), "recover committed prepare");
    await admissionGate.reached;

    expect(observedCommittedPrepareBeforeEviction(data.owner)).toBe(true);
    admissionGate.release();
    await idle;
    const recovered = await snapshotFor(data);

    expect(recovered.pending).toBeNull();
    expect(recovered.lastReceipt).not.toBeNull();
    expect(await laneRows(data.thread)).toHaveLength(1);
  });

  it("keeps recovery wakes through repeated processing failures", async () => {
    const data = fixture("retry-exhaustion");
    const idle = observeScheduleIdle(data.owner);
    const authorizationFailures = holdScheduleAuthorizationFailures(data.owner);

    await manage(data, Date.now(), "recover beyond bounded native retries");
    expect(await authorizationFailures.reached(6)).toBeGreaterThanOrEqual(6);
    expect(await alarmRows(data.owner)).toHaveLength(1);

    authorizationFailures.release();
    await idle;
    const recovered = await snapshotFor(data);

    expect(recovered.lastReceipt).not.toBeNull();
    expect(await laneRows(data.thread)).toHaveLength(1);
    expect(await alarmRows(data.owner)).toEqual([]);
  });
});
