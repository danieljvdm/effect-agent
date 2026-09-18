import {
  AlarmEvent,
  CloudflareAlarmError,
  CloudflareAlarms,
  type AlarmInput,
  type AlarmRef,
  type AlarmTransaction,
} from "@effect-agent/platform-cloudflare/cloudflare-alarms";
import { DurableObjectState } from "alchemy/Cloudflare/Workers/DurableObjectState";
import {
  scheduledEventsTransaction,
  handleScheduledEvents,
  type ScheduledEventEntry,
  ScheduledEventError,
  type ScheduledEventTransaction,
} from "alchemy/Cloudflare/Workers/ScheduledEvents";
import { Context, DateTime, Duration, Effect, Layer, Option, Schema } from "effect";

const Payload = Schema.Struct({
  _tag: Schema.Literal("EffectAgentAlarm"),
  version: Schema.Literal(1),
  tag: Schema.NonEmptyString,
  id: Schema.NonEmptyString,
  payload: Schema.Json,
});

const LegacyRow = Schema.Struct({
  storage_id: Schema.NonEmptyString,
  alarm_id: Schema.NonEmptyString,
  tag: Schema.NonEmptyString,
  run_at: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 8_640_000_000_000_000 })),
  repeat_every_ms: Schema.NullOr(Schema.Int.check(Schema.isGreaterThan(0))),
  payload: Schema.String,
});

/** Deterministic migration crash probes; the default never changes execution. */
export const AlarmMigrationFailpoint = Context.Reference<{
  readonly hit: (
    location: "before-copy" | "after-copy" | "before-drop" | "after-drop",
  ) => Effect.Effect<void>;
}>("@effect-agent/platform-alchemy-cloudflare/AlarmMigrationFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

const key = (ref: AlarmRef) =>
  `effect-agent/alarm:${encodeURIComponent(ref.tag)}:${encodeURIComponent(ref.id)}`;

const invalid = (cause: unknown) => CloudflareAlarmError.make({ reason: "invalid", cause });
const storage = (cause: unknown) => CloudflareAlarmError.make({ reason: "storage", cause });

const mapError = (error: ScheduledEventError) =>
  error.reason === "storage" ? storage(error) : invalid(error);

const mapSchedulerError = <E>(error: E | ScheduledEventError): E | CloudflareAlarmError =>
  error instanceof ScheduledEventError ? mapError(error) : error;

const entry = Effect.fnUntraced(function* (input: AlarmInput) {
  const payload = yield* Schema.encodeEffect(Payload)({
    _tag: "EffectAgentAlarm",
    version: 1,
    tag: input.tag,
    id: input.id,
    payload: input.payload,
  }).pipe(Effect.mapError(invalid));

  const repeatEvery = input.repeatEvery;

  const repeatMs =
    repeatEvery === undefined
      ? undefined
      : yield* Effect.try({
          try: () => Math.ceil(Duration.toMillis(repeatEvery)),
          catch: invalid,
        });

  return {
    id: key(input),
    runAt: DateTime.toEpochMillis(input.runAt),
    payload,
    ...(repeatMs === undefined ? {} : { repeatMs }),
  } satisfies ScheduledEventEntry;
});

const mutations = (tx: ScheduledEventTransaction): AlarmTransaction => ({
  scheduleAlarm: (input) =>
    entry(input).pipe(Effect.flatMap((value) => tx.upsert(value).pipe(Effect.mapError(mapError)))),
  cancelAlarm: (input) => tx.delete(key(input)).pipe(Effect.mapError(mapError)),
});

/**
 * Adopt the one supported previous host format atomically. Validate every legacy row and
 * destination collision before copying. A failed/crashed transaction leaves both tables
 * unchanged. Deploy upgraded writers exclusively before relying on this ownership transfer.
 */
const adoptLegacy = Effect.fnUntraced(function* (state: DurableObjectState["Service"]) {
  const failpoint = yield* AlarmMigrationFailpoint;

  yield* scheduledEventsTransaction((tx) =>
    Effect.gen(function* () {
      const present = yield* Effect.try({
        try: () =>
          state.raw.storage.sql
            .exec<{ name: string }>(
              "SELECT name FROM sqlite_master WHERE type='table' AND name='effect_cf_scheduled_alarms'",
            )
            .toArray().length > 0,
        catch: storage,
      });

      if (!present) return;

      const rows = yield* Effect.try({
        try: () =>
          state.raw.storage.sql
            .exec(
              "SELECT storage_id, alarm_id, tag, run_at, repeat_every_ms, payload FROM effect_cf_scheduled_alarms ORDER BY storage_id",
            )
            .toArray(),
        catch: storage,
      }).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(LegacyRow))),
        Effect.mapError(invalid),
      );

      const values: Array<ScheduledEventEntry> = [];

      for (const row of rows) {
        if (
          row.storage_id !==
          `effect-cf-alarm:${encodeURIComponent(row.tag)}:${encodeURIComponent(row.alarm_id)}`
        ) {
          return yield* invalid("The previous alarm identity is unsupported");
        }

        const payload = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
          row.payload,
        ).pipe(Effect.mapError(invalid));

        const value = yield* entry({
          id: row.alarm_id,
          tag: row.tag,
          payload,
          runAt: DateTime.makeUnsafe(row.run_at),
          ...(row.repeat_every_ms === null ? {} : { repeatEvery: row.repeat_every_ms }),
        });

        const existing = yield* tx.get(value.id).pipe(Effect.mapError(mapError));

        // Even equal-looking rows from two schedulers have ambiguous ownership; fail closed.
        if (Option.isSome(existing))
          return yield* invalid("An adopted alarm already exists in Alchemy storage");
        values.push(value);
      }
      for (const value of values) {
        yield* failpoint.hit("before-copy");
        yield* tx.upsert(value).pipe(Effect.mapError(mapError));
        yield* failpoint.hit("after-copy");
      }
      yield* failpoint.hit("before-drop");
      yield* Effect.try({
        try: () => {
          state.raw.storage.sql.exec("DROP TABLE effect_cf_scheduled_alarms");
        },
        catch: storage,
      });
      yield* failpoint.hit("after-drop");
    }),
  ).pipe(Effect.provideService(DurableObjectState, state), Effect.mapError(mapSchedulerError));
});

const decodeEvent = Effect.fnUntraced(function* (event: ScheduledEventEntry) {
  const payload = yield* Schema.decodeUnknownEffect(Payload)(event.payload).pipe(
    Effect.mapError(invalid),
  );

  if (event.id !== key(payload))
    return yield* invalid("The stored alarm identity does not match its payload");

  return AlarmEvent.make({
    _tag: "AlarmDue",
    id: payload.id,
    tag: payload.tag,
    payload: payload.payload,
    scheduledAt: DateTime.makeUnsafe(event.runAt),
  });
});

/** Alchemy's transactional scheduler owns the single native alarm for this object. */
export const layer = Layer.effect(CloudflareAlarms)(
  Effect.gen(function* () {
    const state = yield* DurableObjectState;

    yield* adoptLegacy(state);

    return CloudflareAlarms.of({
      transaction: (body) =>
        scheduledEventsTransaction((tx) => body(mutations(tx))).pipe(
          Effect.provideService(DurableObjectState, state),
          Effect.mapError(mapSchedulerError),
        ),
      processDue: (handle, options) =>
        Effect.gen(function* () {
          const retry = options.retryFailedAfter;

          const retryMillis =
            retry === undefined
              ? undefined
              : yield* Effect.try({
                  try: () => Math.ceil(Duration.toMillis(retry)),
                  catch: invalid,
                });

          yield* handleScheduledEvents((event) => decodeEvent(event).pipe(Effect.flatMap(handle)), {
            mode: options.mode,
            validate: (event) => decodeEvent(event).pipe(Effect.asVoid),
            ...(options.limit === undefined ? {} : { limit: options.limit }),
            ...(retryMillis === undefined ? {} : { retryMillis }),
            ...(options.onFailure === undefined ? {} : { onFailure: options.onFailure }),
          }).pipe(
            Effect.provideService(DurableObjectState, state),
            Effect.mapError(mapSchedulerError),
          );
        }),
    });
  }),
);
