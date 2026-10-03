import { DurableObject } from "cloudflare:workers";

import { alarmId, alarmTag, objectName } from "./alarm-restart-contract.ts";

interface Environment {
  readonly RESTART_ALARMS: DurableObjectNamespace<RestartAlarmOwner>;
}

let alarmDeliveries = 0;

/** The supported old persisted format, written by a native Object before the host upgrade. */
export class RestartAlarmOwner extends DurableObject<Environment> {
  async seed() {
    const runAt = Date.now() + 2_000;

    await this.ctx.storage.transaction(async () => {
      this.ctx.storage.sql.exec(
        "CREATE TABLE effect_cf_scheduled_alarms (storage_id TEXT PRIMARY KEY, alarm_id TEXT NOT NULL, tag TEXT NOT NULL, run_at INTEGER NOT NULL, repeat_every_ms INTEGER, payload TEXT NOT NULL)",
      );
      this.ctx.storage.sql.exec(
        "INSERT INTO effect_cf_scheduled_alarms VALUES (?, ?, ?, ?, NULL, ?)",
        `effect-cf-alarm:${alarmTag}:${alarmId}`,
        alarmId,
        alarmTag,
        runAt,
        JSON.stringify({ message: "accepted before the runtime upgrade" }),
      );
      await this.ctx.storage.setAlarm(runAt);
    });

    return {
      runAt,
      pendingEvents: this.ctx.storage.sql
        .exec("SELECT storage_id FROM effect_cf_scheduled_alarms")
        .toArray().length,
      alarmDeliveries,
    };
  }

  alarm() {
    alarmDeliveries += 1;
  }
}

export default {
  async fetch(request: Request, env: Environment) {
    if (new URL(request.url).pathname !== "/seed")
      return new Response("not found", { status: 404 });

    return Response.json(await env.RESTART_ALARMS.getByName(objectName).seed());
  },
};
