import {
  AlarmEvent,
  CloudflareAlarmError,
  CloudflareAlarms,
  type AlarmInput,
  type AlarmRef,
  type AlarmTransaction,
} from "@yielded/agent-platform-cloudflare/cloudflare-alarms";
import { DurableObjectState } from "alchemy/Cloudflare/Workers/DurableObjectState";
import { cancelEvent, scheduleEvent } from "alchemy/Cloudflare/Workers/ScheduledEvents";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, DateTime, Duration, Effect, Layer, Schema, Semaphore } from "effect";

const TimeMillis = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: 8_640_000_000_000_000 }),
);

const StoredEvent = Schema.Struct({
  id: Schema.NonEmptyString,
  run_at: TimeMillis,
  repeat_ms: Schema.NullOr(Schema.Int.check(Schema.isGreaterThan(0))),
  payload: Schema.String,
});

type StoredEvent = typeof StoredEvent.Type;

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
}>("@yielded/agent-platform-alchemy-cloudflare/AlarmMigrationFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

const key = (ref: AlarmRef) =>
  `effect-agent/alarm:${encodeURIComponent(ref.tag)}:${encodeURIComponent(ref.id)}`;

const invalid = (cause: unknown) => CloudflareAlarmError.make({ reason: "invalid", cause });
const storage = (cause: unknown) => CloudflareAlarmError.make({ reason: "storage", cause });

const entry = Effect.fnUntraced(function* (input: AlarmInput) {
  const payload = yield* Schema.encodeEffect(Schema.fromJsonString(Payload))({
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

  return yield* Schema.decodeEffect(StoredEvent)({
    id: key(input),
    run_at: DateTime.toEpochMillis(input.runAt),
    payload,
    repeat_ms: repeatMs ?? null,
  }).pipe(Effect.mapError(invalid));
});

const read = (
  state: DurableObjectState["Service"],
  query: string,
  ...params: Array<string | number | null>
) =>
  Effect.try({ try: () => state.raw.storage.sql.exec(query, ...params).toArray(), catch: storage });

const schedule = Effect.fnUntraced(function* (row: StoredEvent) {
  const payload = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(row.payload).pipe(
    Effect.mapError(invalid),
  );

  yield* scheduleEvent(row.id, new Date(row.run_at), payload, row.repeat_ms ?? undefined).pipe(
    Effect.catchDefect((cause) => Effect.fail(storage(cause))),
  );
});

/**
 * Adopt the one supported previous host format atomically. Validate every legacy row and
 * destination collision before copying. A failed/crashed transaction leaves both tables
 * unchanged. Deploy upgraded writers exclusively before relying on this ownership transfer.
 */
const adoptLegacy = Effect.fnUntraced(function* (state: DurableObjectState["Service"]) {
  const failpoint = yield* AlarmMigrationFailpoint;

  yield* state.storage
    .transaction(
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

        const columns = yield* Effect.try({
          try: () =>
            state.raw.storage.sql.exec("PRAGMA table_info(effect_cf_scheduled_alarms)").toArray(),
          catch: storage,
        }).pipe(
          Effect.flatMap(
            Schema.decodeUnknownEffect(
              Schema.Array(Schema.Struct({ name: Schema.NonEmptyString })),
            ),
          ),
          Effect.mapError(invalid),
        );

        const hasAttempts = yield* Effect.try({
          try: () =>
            state.raw.storage.sql
              .exec(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='effect_cf_alarm_attempts'",
              )
              .toArray().length > 0,
          catch: storage,
        });

        // Newer effect-cf formats own retry/parking state that this legacy transfer cannot preserve.
        if (
          hasAttempts ||
          columns.length !== Object.keys(LegacyRow.fields).length ||
          columns.some(({ name }) => !Object.hasOwn(LegacyRow.fields, name))
        )
          return yield* invalid("The previous alarm storage format is unsupported");

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

        const values: Array<StoredEvent> = [];

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

          const destination = yield* read(
            state,
            "SELECT name FROM sqlite_master WHERE name = 'alchemy_scheduled_events'",
          );

          const existing =
            destination.length === 0
              ? []
              : yield* read(
                  state,
                  "SELECT id FROM alchemy_scheduled_events WHERE id = ?",
                  value.id,
                );

          // Even equal-looking rows from two schedulers have ambiguous ownership; fail closed.
          if (existing.length > 0)
            return yield* invalid("An adopted alarm already exists in Alchemy storage");
          values.push(value);
        }
        for (const value of values) {
          yield* failpoint.hit("before-copy");
          yield* schedule(value);
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
    )
    .pipe(
      Effect.provideService(DurableObjectState, state),
      Effect.catchTag("DurableObjectStorageError", (cause) => Effect.fail(storage(cause))),
      Effect.catchDefect((cause) => Effect.fail(storage(cause))),
    );
});

const decodeEvent = Effect.fnUntraced(function* (row: StoredEvent) {
  const payload = yield* Schema.decodeEffect(Schema.fromJsonString(Payload))(row.payload).pipe(
    Effect.mapError(invalid),
  );

  if (row.id !== key(payload))
    return yield* invalid("The stored alarm identity does not match its payload");

  return AlarmEvent.make({
    _tag: "AlarmDue",
    id: payload.id,
    tag: payload.tag,
    payload: payload.payload,
    scheduledAt: DateTime.makeUnsafe(row.run_at),
  });
});

const Processing = Context.Reference<boolean>(
  "@yielded/agent-platform-alchemy-cloudflare/Alarms/Processing",
  {
    defaultValue: () => false,
  },
);

/** Reuse Alchemy's scheduler and transaction owner; retain the host's acknowledgement policy. */
export const layer = Layer.effect(CloudflareAlarms)(
  Effect.gen(function* () {
    const state = yield* DurableObjectState;

    const context = yield* Effect.context<DurableObjectState | RuntimeContext>().pipe(
      Effect.map(Context.pick(DurableObjectState, RuntimeContext)),
    );

    const native = <A, E, R>(
      effect: Effect.Effect<A, E, R | DurableObjectState | RuntimeContext>,
    ) => effect.pipe(Effect.provide(context));

    const transaction = <A, E, R>(effect: Effect.Effect<A, E, R | RuntimeContext>) =>
      native(state.storage.transaction(effect)).pipe(
        Effect.catchTag("DurableObjectStorageError", (cause) => Effect.fail(storage(cause))),
      );

    const lock = yield* Semaphore.make(1);

    const matching = (row: StoredEvent) =>
      read(
        state,
        "SELECT id FROM alchemy_scheduled_events WHERE id = ? AND run_at = ? AND repeat_ms IS ? AND payload = ?",
        row.id,
        row.run_at,
        row.repeat_ms,
        row.payload,
      ).pipe(Effect.map((rows) => rows.length > 0));

    const acknowledge = (row: StoredEvent, next?: number) =>
      transaction(
        Effect.gen(function* () {
          if (!(yield* matching(row))) return;
          if (next === undefined)
            yield* cancelEvent(row.id).pipe(
              Effect.catchDefect((cause) => Effect.fail(storage(cause))),
            );
          else
            yield* schedule(
              yield* Schema.decodeEffect(StoredEvent)({ ...row, run_at: next }).pipe(
                Effect.mapError(invalid),
              ),
            );
        }),
      );

    yield* native(adoptLegacy(state));

    return CloudflareAlarms.of({
      transaction: (body) =>
        transaction(
          Effect.gen(function* () {
            const owner = yield* Effect.fiberId;
            let open = true;

            const guard = Effect.flatMap(Effect.fiberId, (fiber) =>
              open && fiber === owner
                ? Effect.void
                : Effect.fail(invalid("The alarm transaction has no active owner")),
            );

            const mutations: AlarmTransaction = {
              scheduleAlarm: (input) =>
                guard.pipe(
                  Effect.andThen(entry(input)),
                  Effect.flatMap((row) => native(schedule(row))),
                ),
              cancelAlarm: (ref) =>
                guard.pipe(
                  Effect.andThen(
                    native(cancelEvent(key(ref))).pipe(
                      Effect.catchDefect((cause) => Effect.fail(storage(cause))),
                    ),
                  ),
                ),
            };

            return yield* Effect.suspend(() => body(mutations)).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  open = false;
                }),
              ),
            );
          }),
        ),
      processDue: (handle, options) =>
        Effect.gen(function* () {
          if (yield* Processing)
            return yield* invalid("Alarm processing cannot recursively enter its handler");

          const retryMillis = yield* Effect.try({
            try: () => Math.ceil(Duration.toMillis(options.retryFailedAfter ?? 1000)),
            catch: invalid,
          });

          const config = yield* Schema.decodeEffect(
            Schema.Struct({
              mode: Schema.Literals(["ordered", "isolated"]),
              limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
              retryMillis: Schema.Int.check(Schema.isGreaterThan(0)),
            }),
          )({
            ...options,
            limit: options.limit ?? 100,
            retryMillis,
          }).pipe(Effect.mapError(invalid));

          yield* lock.withPermit(
            Effect.gen(function* () {
              const present = yield* read(
                state,
                "SELECT name FROM sqlite_master WHERE name = 'alchemy_scheduled_events'",
              );

              if (present.length === 0) return;

              const rows = yield* read(
                state,
                "SELECT id, run_at, repeat_ms, payload FROM alchemy_scheduled_events WHERE run_at <= ? ORDER BY run_at, id LIMIT ?",
                yield* Clock.currentTimeMillis,
                config.limit,
              ).pipe(
                Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(StoredEvent))),
                Effect.mapError(invalid),
              );

              // Decode the entire selected snapshot before handlers or alarm writes.
              const events = yield* Effect.forEach(rows, (row) =>
                decodeEvent(row).pipe(Effect.map((event) => ({ row, event }))),
              );

              for (const { row, event } of events) {
                if (!(yield* matching(row))) continue;
                const outcome = yield* Effect.result(Effect.suspend(() => handle(event)));

                if (outcome._tag === "Failure") {
                  if (config.mode === "isolated")
                    yield* acknowledge(row, (yield* Clock.currentTimeMillis) + config.retryMillis);
                  if (options.onFailure) yield* options.onFailure();
                  if (config.mode === "ordered") return yield* Effect.fail(outcome.failure);
                } else {
                  yield* acknowledge(
                    row,
                    row.repeat_ms === null
                      ? undefined
                      : (yield* Clock.currentTimeMillis) + row.repeat_ms,
                  );
                }
              }
            }).pipe(Effect.provideService(Processing, true)),
          );
        }),
    });
  }),
);
