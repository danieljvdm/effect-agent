import { Effect, Layer } from "effect";
import { DurableObjectAlarm, DurableObjectStorage } from "effect-cf";

import { CloudflareAlarmError, CloudflareAlarms } from "../CloudflareAlarms.ts";

const alarmError = (cause: { readonly _tag: unknown }) =>
  new CloudflareAlarmError({
    reason: cause._tag === "StorageOperationError" ? "storage" : "invalid",
    cause,
  });

/** Keep effect-cf's native alarm error contract at its existing public boundary. */
export const restoreEffectCfAlarmError = (
  error: CloudflareAlarmError,
): Effect.Effect<never, DurableObjectAlarm.DurableObjectAlarmError> => {
  const cause = error.cause;

  return cause instanceof DurableObjectAlarm.InvalidAlarmRefError ||
    cause instanceof DurableObjectAlarm.InvalidAlarmPayloadError ||
    cause instanceof DurableObjectAlarm.InvalidProcessDueAlarmsOptionsError ||
    cause instanceof DurableObjectAlarm.InvalidRepeatEveryError ||
    cause instanceof DurableObjectAlarm.StoredAlarmDecodeError ||
    cause instanceof DurableObjectStorage.StorageOperationError
    ? Effect.fail(cause)
    : Effect.die(cause);
};

export const effectCfAlarmsLayer = Layer.effect(
  CloudflareAlarms,
  Effect.gen(function* () {
    const alarms = yield* DurableObjectAlarm.DurableObjectAlarm;

    return CloudflareAlarms.of({
      transaction: (body) =>
        alarms
          .transaction((transaction) =>
            body({
              scheduleAlarm: (input) =>
                transaction.scheduleAlarm(input).pipe(Effect.mapError(alarmError)),
              cancelAlarm: (input) =>
                transaction.cancelAlarm(input).pipe(Effect.mapError(alarmError)),
            }),
          )
          .pipe(Effect.catchTag("StorageOperationError", (cause) => alarmError(cause))),
      processDue: (handle, options) =>
        alarms.processDueAlarms(handle, options).pipe(
          Effect.asVoid,
          Effect.catchTag(
            [
              "InvalidAlarmRefError",
              "InvalidAlarmPayloadError",
              "InvalidProcessDueAlarmsOptionsError",
              "InvalidRepeatEveryError",
              "StorageOperationError",
              "StoredAlarmDecodeError",
            ],
            (cause) => alarmError(cause),
          ),
        ),
    });
  }),
);
