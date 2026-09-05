import {
  makeSqlSubscriptionStore,
  SqlSubscriptionTransaction,
} from "@effect-agent/thread/SqlSubscriptionStore";
import {
  SourcePartition,
  SubscriptionError,
  SubscriptionFailpoint,
  type SubscriptionFailpointError,
  SubscriptionStore,
} from "@effect-agent/thread/Subscription";
import { Context, Effect, Layer, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

const CURRENT_SUBSCRIPTION_STORE_VERSION = 2;

const ScanRow = Schema.Struct({
  event_scan_cursor: Schema.String,
  delivery_scan_cursor: Schema.String,
  recovery_scan_cursor: Schema.Natural,
});

const StoreStateRow = Schema.Struct({
  storage_version: Schema.Int,
  alarm_generation: Schema.Natural,
});

export interface DoSubscriptionAlarmReplacement {
  readonly deadlineAtMillis: number | null;
  readonly generation: number;
}

export type DoSubscriptionReplaceAlarm = (
  replacement: DoSubscriptionAlarmReplacement,
) => Effect.Effect<void, SubscriptionError>;

/** One Durable Object transaction combining subscription SQL and its single native alarm. */
export class DoSubscriptionTransaction extends Context.Service<
  DoSubscriptionTransaction,
  {
    readonly run: <A, E, R>(
      body: (replaceAlarm: DoSubscriptionReplaceAlarm) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | SubscriptionError, R>;
  }
>()("@effect-agent/storage-cloudflare/DoSubscriptionTransaction") {}

export class DoSubscriptionAlarmControl extends Context.Service<
  DoSubscriptionAlarmControl,
  {
    readonly prearm: (
      deadlineAtMillis: number,
    ) => Effect.Effect<void, SubscriptionError | SubscriptionFailpointError>;
    readonly reconcile: Effect.Effect<void, SubscriptionError | SubscriptionFailpointError>;
  }
>()("@effect-agent/storage-cloudflare/DoSubscriptionAlarmControl") {}

const error = (reason: SubscriptionError["reason"], code: string) =>
  SubscriptionError.make({ reason, code });

const unavailable = (operation: string) => error("storage", operation);
const corrupt = (operation: string) => error("corrupt", operation);

const validate = <A, I>(schema: Schema.Codec<A, I>, value: unknown, code: string) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => error("validation", code)));

const decodeRows = <A, I>(schema: Schema.Codec<A, I>, rows: unknown, code: string) =>
  Schema.decodeUnknownEffect(Schema.Array(schema))(rows).pipe(Effect.mapError(() => corrupt(code)));

const initializeDoSubscriptionStore = Effect.fn("DoSubscriptionStore.initialize")(function* () {
  const sql = yield* SqlClientService.SqlClient;

  const names = yield* sql<Record<string, unknown>>`
    SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'effect_agent_subscription_%'
  `.pipe(Effect.mapError(() => unavailable("inspect subscription storage")));

  const decodedNames = yield* decodeRows(
    Schema.Struct({ name: Schema.String }),
    names,
    "inspect subscription storage",
  );

  const expected = new Set([
    "effect_agent_subscription_store_state",
    "effect_agent_subscription_sequences",
    "effect_agent_subscriptions",
    "effect_agent_subscription_events",
    "effect_agent_subscription_deliveries",
  ]);

  const hasState = decodedNames.some(
    ({ name }) => name === "effect_agent_subscription_store_state",
  );

  if (!hasState && decodedNames.length > 0) return yield* corrupt("partial subscription storage");
  if (!hasState) {
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`CREATE TABLE effect_agent_subscription_store_state (
        singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton=1), storage_version INTEGER NOT NULL, alarm_generation INTEGER NOT NULL
      )`.withoutTransform;
          yield* sql`CREATE TABLE effect_agent_subscription_sequences (
        tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, sequence INTEGER NOT NULL,
        event_scan_cursor TEXT NOT NULL, delivery_scan_cursor TEXT NOT NULL, recovery_scan_cursor INTEGER NOT NULL,
        PRIMARY KEY (tenant_id, source_address)
      )`.withoutTransform;
          yield* sql`CREATE TABLE effect_agent_subscriptions (
        tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, owner_id TEXT NOT NULL, subscription_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL, source_name TEXT NOT NULL, source_version TEXT NOT NULL, matching_key TEXT NOT NULL,
        state TEXT NOT NULL, expires_at_millis INTEGER NOT NULL, recovery_at_millis INTEGER, record_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id), UNIQUE (tenant_id, source_address, ordinal)
      )`.withoutTransform;
          yield* sql`CREATE INDEX effect_agent_subscriptions_owner ON effect_agent_subscriptions (tenant_id, source_address, owner_id, ordinal)`
            .withoutTransform;
          yield* sql`CREATE INDEX effect_agent_subscriptions_candidates ON effect_agent_subscriptions (tenant_id, source_address, source_name, source_version, matching_key, ordinal)`
            .withoutTransform;
          yield* sql`CREATE INDEX effect_agent_subscriptions_recovery ON effect_agent_subscriptions (tenant_id, source_address, recovery_at_millis, ordinal) WHERE recovery_at_millis IS NOT NULL`
            .withoutTransform;
          yield* sql`CREATE TABLE effect_agent_subscription_events (
        tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, event_id TEXT NOT NULL, source_name TEXT NOT NULL,
        source_version TEXT NOT NULL, matching_key TEXT NOT NULL, payload_digest TEXT NOT NULL, cutoff INTEGER NOT NULL,
        cursor INTEGER NOT NULL, routing_complete INTEGER NOT NULL, next_attempt_at_millis INTEGER NOT NULL, record_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, source_address, event_id)
      )`.withoutTransform;
          yield* sql`CREATE INDEX effect_agent_subscription_events_pending ON effect_agent_subscription_events (tenant_id, source_address, routing_complete, next_attempt_at_millis, event_id)`
            .withoutTransform;
          yield* sql`CREATE TABLE effect_agent_subscription_deliveries (
        tenant_id TEXT NOT NULL, source_address TEXT NOT NULL, owner_id TEXT NOT NULL, subscription_id TEXT NOT NULL,
        event_id TEXT NOT NULL, delivery_key TEXT NOT NULL, state TEXT NOT NULL, next_attempt_at_millis INTEGER NOT NULL,
        record_json TEXT NOT NULL, PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id, event_id),
        UNIQUE (tenant_id, source_address, delivery_key)
      )`.withoutTransform;
          yield* sql`CREATE INDEX effect_agent_subscription_deliveries_pending ON effect_agent_subscription_deliveries (tenant_id, source_address, state, next_attempt_at_millis, delivery_key)`
            .withoutTransform;
          yield* sql`CREATE INDEX effect_agent_subscription_deliveries_registration ON effect_agent_subscription_deliveries (tenant_id, source_address, owner_id, subscription_id, delivery_key)`
            .withoutTransform;
          yield* sql`INSERT INTO effect_agent_subscription_store_state (singleton, storage_version, alarm_generation)
        VALUES (1, ${CURRENT_SUBSCRIPTION_STORE_VERSION}, 0)`.withoutTransform;
        }),
      )
      .pipe(Effect.mapError(() => unavailable("initialize subscription storage")));

    return;
  }
  if (decodedNames.length !== expected.size || decodedNames.some(({ name }) => !expected.has(name)))
    return yield* corrupt("subscription storage tables");

  const rows = yield* sql<
    Record<string, unknown>
  >`SELECT storage_version, alarm_generation FROM effect_agent_subscription_store_state WHERE singleton=1`.pipe(
    Effect.mapError(() => unavailable("read subscription storage version")),
  );

  const state = yield* decodeRows(StoreStateRow, rows, "read subscription storage version");

  if (state.length !== 1 || state[0].storage_version !== CURRENT_SUBSCRIPTION_STORE_VERSION)
    return yield* corrupt(
      state.length === 1
        ? `incompatible subscription storage version ${state[0].storage_version}; expected ${CURRENT_SUBSCRIPTION_STORE_VERSION}`
        : "invalid subscription storage version row",
    );
});

const makeSubscriptionStore = Effect.fn("DoSubscriptionStore.make")(function* (
  owned: SourcePartition,
) {
  const partition = yield* validate(SourcePartition, owned, "partition");
  const sql = yield* SqlClientService.SqlClient;
  const failpoint = yield* SubscriptionFailpoint;
  const transactions = yield* DoSubscriptionTransaction;

  yield* initializeDoSubscriptionStore();

  const query = <A extends object>(
    effect: Effect.Effect<ReadonlyArray<A>, SqlError>,
    code: string,
  ) => effect.pipe(Effect.mapError(() => unavailable(code)));

  const readIndexedDeadline = Effect.fn("DoSubscriptionStore.readIndexedDeadline")(function* () {
    const scanRows = yield* query(
      sql<Record<string, unknown>>`
      SELECT event_scan_cursor, delivery_scan_cursor, recovery_scan_cursor FROM effect_agent_subscription_sequences
      WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address}
    `,
      "read scan cursor deadline",
    );

    const scans = yield* decodeRows(ScanRow, scanRows, "read scan cursor deadline");

    if (scans.length !== 1) return yield* corrupt("scan cursor deadline");
    if (
      scans[0].event_scan_cursor !== "" ||
      scans[0].delivery_scan_cursor !== "" ||
      scans[0].recovery_scan_cursor !== 0
    )
      return 0;

    const rows = yield* query(
      sql<Record<string, unknown>>`
      SELECT MIN(deadline) AS deadline FROM (
        SELECT next_attempt_at_millis AS deadline FROM effect_agent_subscription_events
          WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND routing_complete=0
        UNION ALL SELECT next_attempt_at_millis FROM effect_agent_subscription_deliveries
          WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND state NOT IN ('delivered','refused')
        UNION ALL SELECT recovery_at_millis FROM effect_agent_subscriptions
          WHERE tenant_id=${partition.tenantId} AND source_address=${partition.address} AND state='active' AND recovery_at_millis IS NOT NULL
      )
    `,
      "read subscription deadline",
    );

    const decoded = yield* decodeRows(
      Schema.Struct({ deadline: Schema.NullOr(Schema.Number) }),
      rows,
      "read subscription deadline",
    );

    if (decoded.length !== 1) return yield* corrupt("subscription deadline");

    return decoded[0].deadline;
  });

  const replaceAlarm = Effect.fn("DoSubscriptionStore.replaceAlarm")(function* (
    replace: DoSubscriptionReplaceAlarm,
    deadlineAtMillis: number | null,
  ) {
    const rows = yield* query(
      sql<Record<string, unknown>>`
      UPDATE effect_agent_subscription_store_state SET alarm_generation=alarm_generation+1
      WHERE singleton=1 RETURNING storage_version, alarm_generation
    `,
      "advance subscription alarm generation",
    );

    const state = yield* decodeRows(StoreStateRow, rows, "advance subscription alarm generation");

    if (state.length !== 1 || state[0].storage_version !== CURRENT_SUBSCRIPTION_STORE_VERSION)
      return yield* corrupt("subscription alarm state");
    yield* failpoint.hit("subscription:alarm:before");
    yield* replace({ deadlineAtMillis, generation: state[0].alarm_generation });
    yield* failpoint.hit("subscription:alarm:after");
  });

  const transact = <A>(effect: Effect.Effect<A, SubscriptionError | SubscriptionFailpointError>) =>
    transactions.run((replace) =>
      Effect.gen(function* () {
        const value = yield* effect;

        yield* replaceAlarm(replace, yield* readIndexedDeadline());

        return value;
      }),
    );

  const store = yield* makeSqlSubscriptionStore(partition, {
    maxStoredJsonLength: 1_900_000,
  }).pipe(Effect.provide(Layer.succeed(SqlSubscriptionTransaction)({ run: transact })));

  const prearm = Effect.fn("DoSubscriptionStore.prearm")(function* (deadlineAtMillis: number) {
    yield* transactions.run((replace) => replaceAlarm(replace, deadlineAtMillis));
    yield* failpoint.hit("subscription:prearm:after");
  });

  const reconcile = Effect.gen(function* () {
    yield* transactions.run((replace) =>
      Effect.gen(function* () {
        yield* replaceAlarm(replace, yield* readIndexedDeadline());
      }),
    );
    yield* failpoint.hit("subscription:reconcile:after");
  });

  return Context.make(SubscriptionStore, store).pipe(
    Context.add(DoSubscriptionAlarmControl, { prearm, reconcile }),
  );
});

export const doSubscriptionStoreLayer = (
  partition: SourcePartition,
): Layer.Layer<
  SubscriptionStore | DoSubscriptionAlarmControl,
  SubscriptionError,
  DoSubscriptionTransaction | SqlClientService.SqlClient
> => Layer.effectContext(makeSubscriptionStore(partition));
