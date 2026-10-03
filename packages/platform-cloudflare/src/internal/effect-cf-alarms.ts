import { Cause, Effect, Layer, Option } from "effect";
import { DurableObjectAlarm, DurableObjectStorage } from "effect-cf";

import {
  type AlarmEvent,
  CloudflareAlarmError,
  CloudflareAlarms,
  type ProcessOptions,
} from "../CloudflareAlarms.ts";

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
    cause instanceof DurableObjectAlarm.InvalidScheduleConfigurationError ||
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
      processDue: <E, R>(
        handle: (event: AlarmEvent) => Effect.Effect<void, E, R>,
        options: ProcessOptions,
      ): Effect.Effect<void, E | CloudflareAlarmError, R> =>
        Effect.suspend(() => {
          let handlerFailure = Option.none<Cause.Cause<E>>();

          const process =
            options.mode === "isolated"
              ? alarms.processDueAlarms(handle, options)
              : alarms.processDueAlarms(
                  (event) =>
                    Effect.suspend(() => handle(event)).pipe(
                      Effect.tapCause((cause) =>
                        Effect.sync(() => {
                          handlerFailure = Option.some(cause);
                        }),
                      ),
                    ),
                  {
                    ...options,
                    onFailure: (failure) =>
                      Effect.gen(function* () {
                        if (options.onFailure !== undefined) yield* options.onFailure();

                        // effect-cf commits retry bookkeeping before propagating this
                        // hook's failure. Keep the original typed cause, including defects.
                        if (Option.isSome(handlerFailure))
                          return yield* Effect.failCause(handlerFailure.value);

                        // A stored-row decode failure never entered the handler.
                        const decodeError = (cause: unknown) =>
                          cause instanceof DurableObjectAlarm.StoredAlarmDecodeError
                            ? cause
                            : new DurableObjectAlarm.StoredAlarmDecodeError({
                                cause,
                                storageId: failure.storageId,
                              });

                        return yield* Cause.isCause(failure.cause)
                          ? Effect.failCause(Cause.map(failure.cause, decodeError))
                          : Effect.fail(decodeError(failure.cause));
                      }),
                  },
                );

          return process.pipe(
            Effect.asVoid,
            Effect.catchTag(
              [
                "InvalidAlarmRefError",
                "InvalidAlarmPayloadError",
                "InvalidProcessDueAlarmsOptionsError",
                "InvalidRepeatEveryError",
                "InvalidScheduleConfigurationError",
                "StorageOperationError",
                "StoredAlarmDecodeError",
              ],
              (cause): Effect.Effect<never, E | CloudflareAlarmError> => {
                const original = Option.flatMap(handlerFailure, Cause.findErrorOption);

                // A handler may fail with a native error tag too. Map only adapter failures.
                return Option.isSome(handlerFailure) &&
                  Option.isSome(original) &&
                  original.value === cause
                  ? Effect.failCause(handlerFailure.value)
                  : alarmError(cause);
              },
            ),
          );
        }),
    });
  }),
);
