import {
  ScheduleId,
  type ScheduleAuthorizer,
  type ScheduleOwner,
  Scheduling,
} from "@effect-agent/session";
import { DoScheduleAlarmControl } from "@effect-agent/storage-cloudflare";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect, type Layer, Schema } from "effect";
import { DurableObject, type DurableObjectState, type WorkerEnvironment } from "effect-cf";
import { describe, expect, it } from "vite-plus/test";

import {
  type ConversationObjectNamespace,
  type ScheduleOwnerIdentity,
  type makeScheduleOwnerObjectClass,
} from "../src/index.ts";
import {
  TEST_DIGESTS,
  TEST_PRINCIPAL,
  armScheduleAdmissionEviction,
  armScheduleAdmissionPause,
  armScheduleEviction,
  armScheduleFailure,
  decodeConversationId,
  holdScheduleAuthorizationFailures,
  observedCommittedPrepareBeforeEviction,
  observeSchedulePolicyResources,
  plannerDefinition,
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
    conversation: `cf-schedule-conversation-${label}-${suffix}`,
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

const runAlarmExit = (owner: ScheduleOwner) =>
  Effect.runPromise(
    Effect.tryPromise({
      try: () => runDurableObjectAlarm(scheduleStubFor(owner)),
      catch: () => "alarm-failed" as const,
    }).pipe(Effect.exit),
  );

const runAlarmDirectExit = (owner: ScheduleOwner) =>
  runInDurableObject(scheduleStubFor(owner), (instance) =>
    Effect.runPromise(
      Effect.tryPromise({
        try: () => Promise.resolve(instance.alarm()),
        catch: () => "alarm-failed" as const,
      }).pipe(Effect.exit),
    ),
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
          _tag: "ExistingConversation" as const,
          conversationId: decodeConversationId(data.conversation),
        },
        deliveryPrincipal: TEST_PRINCIPAL,
        definitions: TEST_DIGESTS,
      };
      return expectedRevision === undefined
        ? yield* client.create(
            { definition: plannerDefinition },
            { question, ref: data.conversation },
            options,
          )
        : yield* client.update(
            { definition: plannerDefinition },
            { question, ref: data.conversation },
            { ...options, expectedRevision },
          );
    }),
  );

describe("Cloudflare Schedule Owner", () => {
  it("requires host policy and routing Layers with all application dependencies provided", () => {
    type Host = Parameters<typeof makeScheduleOwnerObjectClass>[0];
    type Ports = ScheduleAuthorizer | ConversationObjectNamespace;
    const policyRequired: Layer.Layer<ConversationObjectNamespace> extends Host ? false : true =
      true;
    const routingRequired: Layer.Layer<ScheduleAuthorizer> extends Host ? false : true = true;
    const applicationDependenciesRequired: Layer.Layer<Ports, never, Scheduling> extends Host
      ? false
      : true = true;
    const nativeDependenciesAccepted: Layer.Layer<
      Ports,
      "host-initialization-failed",
      DurableObjectState.DurableObjectState | WorkerEnvironment | ScheduleOwnerIdentity
    > extends Host
      ? true
      : false = true;

    expect([
      policyRequired,
      routingRequired,
      applicationDependenciesRequired,
      nativeDependenciesAccepted,
    ]).toEqual([true, true, true, true]);
  });

  it("stays idle without work and cancels its only alarm", async () => {
    const data = fixture("idle");
    const stub = scheduleStubFor(data.owner);
    expect(
      await runInDurableObject(stub, (_instance, state) => state.storage.getAlarm()),
    ).toBeNull();

    const deadline = Date.now() + 60_000;
    const created = await manage(data, deadline, "first configuration");
    expect(created.configurationRevision).toBe(1);
    expect(await alarmRows(data.owner)).toHaveLength(1);

    await runScheduleClient(
      Effect.gen(function* () {
        const client = yield* Scheduling;
        return yield* client.cancel(data.scope, data.scheduleId, created.configurationRevision);
      }),
    );
    expect(await alarmRows(data.owner)).toEqual([]);
    expect(
      await runInDurableObject(stub, (_instance, state) => state.storage.getAlarm()),
    ).toBeNull();
  });

  it("delivers healthy work beside a corrupt record and retains a future recovery alarm", async () => {
    const broken = fixture("corrupt-a");
    const healthy = { ...fixture("healthy-b"), owner: broken.owner, scope: broken.scope };
    await manage(broken, Date.now() + 60_000, "corrupt this record");
    await manage(healthy, Date.now() + 60_000, "deliver this record");
    await runInDurableObject(scheduleStubFor(broken.owner), (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE effect_agent_schedules SET deadline_at_millis = 0, record_json = json_set(record_json, '$.nextAtMillis', 0)",
      );
      state.storage.sql.exec(
        "UPDATE effect_agent_schedules SET record_json = ? WHERE schedule_id = ?",
        "{invalid-json",
        broken.scheduleId,
      );
      state.storage.sql.exec("UPDATE effect_cf_scheduled_alarms SET run_at = 0");
    });
    const passStartedAt = Date.now();
    const result = await runAlarmDirectExit(broken.owner);
    expect(result._tag).toBe("Success");
    expect((await snapshotFor(healthy)).lastReceipt).not.toBeNull();
    expect(await laneRows(healthy.conversation)).toHaveLength(1);
    const alarms = await alarmRows(broken.owner);
    expect(alarms).toHaveLength(1);
    expect(alarms[0]?.run_at).toBeGreaterThan(passStartedAt);
  });

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

  it("rejects an invalid recovery deadline before changing the persisted wake", async () => {
    const data = fixture("invalid-recovery-deadline");
    await manage(data, Date.now() + 60_000, "retain the valid wake");
    const before = await alarmRows(data.owner);
    const beforeState = await storeProbe(data.owner);
    const error = await runInDurableObject(scheduleStubFor(data.owner), (instance) =>
      instance[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const alarms = yield* DoScheduleAlarmControl;
          return yield* alarms.prearm(Number.MAX_SAFE_INTEGER).pipe(Effect.flip);
        }),
      ),
    );

    expect(error).toMatchObject({ _tag: "ScheduleStorageError", reason: "corrupt" });
    expect(await alarmRows(data.owner)).toEqual(before);
    expect(await storeProbe(data.owner)).toEqual(beforeState);
  });

  it("releases policy resources within RPC and alarm operations while the owner stays alive", async () => {
    const data = fixture("policy-resource-scope");
    const resources = observeSchedulePolicyResources(data.owner);
    const created = await manage(data, Date.now() + 60_000, "scoped policy");
    expect(resources.acquired).toBeGreaterThan(0);
    expect(resources.released).toBe(resources.acquired);

    resources.fail = true;
    await expect(snapshotFor(data)).rejects.toMatchObject({
      _tag: "ScheduleStorageError",
      reason: "unavailable",
    });
    expect(resources.released).toBe(resources.acquired);

    resources.fail = false;
    await manage(data, Date.now(), "scoped policy", created.configurationRevision);
    for (let attempt = 0; attempt < 10; attempt++) {
      await runDurableObjectAlarm(scheduleStubFor(data.owner));
      if ((await snapshotFor(data)).lastReceipt !== null) break;
    }
    expect((await snapshotFor(data)).lastReceipt).not.toBeNull();
    expect(resources.released).toBe(resources.acquired);
  });

  it("replays a create whose reply was lost after its atomic commit without replacing the alarm", async () => {
    const data = fixture("create-lost-reply");
    const deadline = Date.now() + 60_000;
    armScheduleFailure(data.owner, "schedule:insert:after");
    await expect(manage(data, deadline, "commit before reply loss")).rejects.toMatchObject({
      _tag: "ScheduleFailpointError",
      point: "schedule:insert:after",
    });
    const committedAlarm = await alarmRows(data.owner);
    expect(await storeProbe(data.owner)).toEqual({ schedule_count: 1, alarm_generation: 1 });

    const replay = await manage(data, deadline, "commit before reply loss");
    expect(replay.configurationRevision).toBe(1);
    expect(await storeProbe(data.owner)).toEqual({ schedule_count: 1, alarm_generation: 1 });
    expect(await alarmRows(data.owner)).toEqual(committedAlarm);
  });

  it("gives same-deadline replacements a distinct alarm generation", async () => {
    const data = fixture("replacement");
    const deadline = Date.now() + 120_000;
    const created = await manage(data, deadline, "first configuration");
    const before = await alarmRows(data.owner);

    const updated = await manage(
      data,
      deadline,
      "replacement at the identical deadline",
      created.configurationRevision,
    );
    const after = await alarmRows(data.owner);

    expect(updated.configurationRevision).toBe(2);
    expect(before).toHaveLength(1);
    expect(after).toHaveLength(1);
    expect(after[0]?.run_at).toBe(before[0]?.run_at);
    expect(after[0]?.payload).not.toBe(before[0]?.payload);
  });

  it("admits one due occurrence and quiesces after replayed alarm delivery", async () => {
    const data = fixture("due");
    await manage(data, Date.now(), "deliver exactly one occurrence");
    const stub = scheduleStubFor(data.owner);

    for (let attempt = 0; attempt < 10; attempt++) {
      await runDurableObjectAlarm(stub);
      const snapshot = await snapshotFor(data);
      if (snapshot.lastReceipt !== null) break;
      await Effect.runPromise(Effect.yieldNow);
    }

    const rows = await laneRows(data.conversation);
    expect(rows).toHaveLength(1);
    await runDurableObjectAlarm(stub);
    expect(await laneRows(data.conversation)).toHaveLength(1);
    expect(await alarmRows(data.owner)).toEqual([]);
  });

  for (const operation of ["pause", "cancel"] as const) {
    it(`${operation}s without stranding an occurrence already accepted by Conversation`, async () => {
      const data = fixture(`pending-${operation}`);
      const gate = armScheduleAdmissionPause(data.owner);
      const created = await manage(data, Date.now(), `${operation} after Conversation accepts`);
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
      expect(controlled.state).toBe(operation === "pause" ? "paused" : "cancelled");
      expect(controlled.pending).not.toBeNull();
      expect(await alarmRows(data.owner)).toHaveLength(1);

      gate.release();
      await alarm;
      const recovered = await snapshotFor(data);
      expect(recovered.pending).toBeNull();
      expect(recovered.lastReceipt).not.toBeNull();
      expect(await laneRows(data.conversation)).toHaveLength(1);
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
    expect(await laneRows(data.conversation)).toHaveLength(1);
  });

  it("recovers after admission succeeds but the Schedule Owner incarnation loses the reply", async () => {
    const data = fixture("lost-reply");
    armScheduleAdmissionEviction(data.owner);
    await manage(data, Date.now(), "survive owner eviction after admission");

    await runAlarmExit(data.owner);
    expect(await alarmRows(data.owner)).toHaveLength(1);

    for (let attempt = 0; attempt < 10; attempt++) {
      await Effect.runPromise(Effect.sleep("110 millis"));
      await runAlarmExit(data.owner);
      const rows = await laneRows(data.conversation);
      if (rows.length === 1) {
        const snapshot = await snapshotFor(data);
        if (snapshot.lastReceipt !== null) break;
      }
    }
    const recovered = await snapshotFor(data);
    expect(recovered.lastReceipt).not.toBeNull();
    expect(await laneRows(data.conversation)).toHaveLength(1);
    expect(await alarmRows(data.owner)).toEqual([]);
  });

  it("recovers a committed pending occurrence after eviction immediately after prepare", async () => {
    const data = fixture("prepare-eviction");
    const admissionGate = armScheduleAdmissionPause(data.owner);
    armScheduleEviction(data.owner, "schedule:prepare:after");
    await manage(data, Date.now(), "recover committed prepare");
    const alarm = runAlarmExit(data.owner);
    await admissionGate.reached;

    expect(observedCommittedPrepareBeforeEviction(data.owner)).toBe(true);
    admissionGate.release();
    await alarm;

    for (let attempt = 0; attempt < 10; attempt++) {
      await Effect.runPromise(Effect.sleep("20 millis"));
      const recovered = await snapshotFor(data);
      if (recovered.lastReceipt !== null) break;
    }
    const recovered = await snapshotFor(data);
    expect(recovered.pending).toBeNull();
    expect(recovered.lastReceipt).not.toBeNull();
    expect(await laneRows(data.conversation)).toHaveLength(1);
  });

  it("keeps a recovery wake through Cloudflare's six native retry attempts", async () => {
    const data = fixture("retry-exhaustion");
    const authorizationFailures = holdScheduleAuthorizationFailures(data.owner);
    await manage(data, Date.now(), "recover beyond bounded native retries");
    await runAlarmExit(data.owner);
    expect(await authorizationFailures.reached(6)).toBeGreaterThanOrEqual(6);
    expect(await alarmRows(data.owner)).toHaveLength(1);

    authorizationFailures.release();
    await Effect.runPromise(Effect.sleep("110 millis"));
    await runAlarmExit(data.owner);
    const recovered = await snapshotFor(data);
    expect(recovered.lastReceipt).not.toBeNull();
    expect(await laneRows(data.conversation)).toHaveLength(1);
    expect(await alarmRows(data.owner)).toEqual([]);
  });

  for (const corruption of ["tag", "version"] as const) {
    it(`fails closed on an unknown alarm ${corruption}`, async () => {
      const data = fixture(`unknown-${corruption}`);
      await manage(data, Date.now() + 60_000, "invalid alarm protocol row");
      await runInDurableObject(scheduleStubFor(data.owner), async (_instance, state) => {
        if (corruption === "tag") {
          state.storage.sql.exec(
            "UPDATE effect_cf_scheduled_alarms SET tag = ?, run_at = ?",
            "unknown-schedule-alarm",
            Date.now() - 1,
          );
        } else {
          state.storage.sql.exec(
            "UPDATE effect_cf_scheduled_alarms SET payload = ?, run_at = ?",
            JSON.stringify({ schemaVersion: 2, generation: 1 }),
            Date.now() - 1,
          );
        }
      });

      const result = await runAlarmDirectExit(data.owner);
      expect(result._tag).toBe("Failure");
      expect(await alarmRows(data.owner)).toHaveLength(1);
    });
  }
});
