import { CloudflareAlarms } from "@effect-agent/platform-cloudflare/cloudflare-alarms";
import { makeDurableObjectBridge, makeWorkerBridge } from "alchemy/Cloudflare/Bridge";
import { DurableObject as AlchemyDurableObject } from "alchemy/Cloudflare/Workers/DurableObject";
import { DurableObjectState } from "alchemy/Cloudflare/Workers/DurableObjectState";
import { Request as WorkerRequest } from "alchemy/Cloudflare/Workers/Request";
import { Worker } from "alchemy/Cloudflare/Workers/Worker";
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { DateTime, Effect, Schema } from "effect";
import { HttpServerResponse } from "effect/unstable/http";

import * as Alarms from "../../src/Alarms.ts";
import { objectName, Payload, Status } from "./alarm-restart-contract.ts";

let alarmDeliveries = 0;
let completedAlarmDeliveries = 0;
let handledEvents = 0;
let objectRequests = 0;

interface AlarmRpc {
  readonly alarm: () => Effect.Effect<void>;
  readonly status: () => Effect.Effect<typeof Status.Type>;
}

class AlarmsOwner extends AlchemyDurableObject<AlarmsOwner, AlarmRpc>()("RESTART_ALARMS") {}

const AlarmsOwnerLive = AlarmsOwner.make(
  Effect.succeed(
    Effect.gen(function* () {
      const state = yield* DurableObjectState;
      const alarms = yield* CloudflareAlarms.pipe(Effect.provide(Alarms.layer));

      state.raw.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS delivered_events (id TEXT NOT NULL, tag TEXT NOT NULL, message TEXT NOT NULL, scheduled_at INTEGER NOT NULL)",
      );

      return {
        alarm: () =>
          Effect.gen(function* () {
            alarmDeliveries += 1;
            yield* alarms.processDue(
              (event) =>
                Effect.gen(function* () {
                  const payload = yield* Schema.decodeUnknownEffect(Payload)(event.payload);

                  state.raw.storage.sql.exec(
                    "INSERT INTO delivered_events VALUES (?, ?, ?, ?)",
                    event.id,
                    event.tag,
                    payload.message,
                    DateTime.toEpochMillis(event.scheduledAt),
                  );
                  handledEvents += 1;
                }),
              { mode: "ordered" },
            );
            completedAlarmDeliveries += 1;
          }).pipe(Effect.orDie),
        status: () =>
          Effect.gen(function* () {
            objectRequests += 1;

            return yield* Schema.decodeUnknownEffect(Status)({
              objectName: state.raw.id.name,
              deliveries: state.raw.storage.sql
                .exec("SELECT * FROM delivered_events ORDER BY rowid")
                .toArray(),
              pendingEvents: state.raw.storage.sql
                .exec("SELECT id FROM alchemy_scheduled_events")
                .toArray().length,
              legacyTables: state.raw.storage.sql
                .exec("SELECT name FROM sqlite_master WHERE name = 'effect_cf_scheduled_alarms'")
                .toArray().length,
              nativeAlarm: yield* Effect.promise(() => state.raw.storage.getAlarm()),
            });
          }).pipe(Effect.orDie),
      };
    }).pipe(Effect.orDie),
  ),
);

export const entrypoint = Worker(
  "AlchemyAlarmRestart",
  { main: import.meta.url },
  Effect.gen(function* () {
    const owners = yield* AlarmsOwner;

    return {
      fetch: Effect.gen(function* () {
        const request = yield* WorkerRequest;
        const path = new URL(request.url).pathname;

        // This branch reads isolate counters and never obtains a Durable Object stub.
        if (path === "/introspect")
          return HttpServerResponse.jsonUnsafe({
            alarmDeliveries,
            completedAlarmDeliveries,
            handledEvents,
            objectRequests,
          });
        if (path === "/status")
          return HttpServerResponse.jsonUnsafe(yield* owners.getByName(objectName).status());

        return HttpServerResponse.text("not found", { status: 404 });
      }).pipe(Effect.orDie),
    };
  }).pipe(Effect.provide(AlarmsOwnerLive)),
);

const meta = { entrypoint, stack: { name: "effect-agent-alchemy-restart", stage: "test" } };
const bridge = makeDurableObjectBridge(DurableObject, meta);

export class RestartAlarmOwner extends bridge("RESTART_ALARMS") {}
export default makeWorkerBridge(WorkerEntrypoint, meta);
