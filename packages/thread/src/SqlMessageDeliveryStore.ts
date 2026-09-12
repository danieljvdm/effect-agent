import { ThreadId } from "@effect-agent/core/Identifiers";
import { Context, Effect, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import {
  applyMessageDeliveryChange,
  defaultMessageDeliveryStoreLimits,
  MessageDeliveryChange,
  MessageDeliveryError,
  type MessageDeliveryFailure,
  MessageDeliveryFailpoint,
  MessageDeliveryKey,
  MessageDeliveryPageRequest,
  MessageDeliveryRecord,
  MessageDeliveryStore,
  MessageDeliveryStoreLimits,
  messageDeliveryDeadline,
  isWorkerUpdateDelivery,
  messageDeliveryCapacity,
  sameMessageDeliveryIdentity,
  validateMessageDelivery,
} from "./MessageDelivery.ts";
import { ScheduleInstant } from "./Schedule.ts";
import { IdempotencyKey } from "./SubmissionLedger.ts";

/** The adapter owns the atomic transaction and any associated native wake/alarm update. */
export class SqlMessageDeliveryTransaction extends Context.Service<
  SqlMessageDeliveryTransaction,
  {
    readonly run: <A>(
      body: Effect.Effect<A, MessageDeliveryFailure>,
    ) => Effect.Effect<A, MessageDeliveryFailure>;
  }
>()("@effect-agent/thread/SqlMessageDeliveryTransaction") {}

export interface SqlMessageDeliveryStoreOptions {
  /** UTF-8 bound on the complete persisted record, including a processed Settlement. */
  readonly maxStoredValueBytes?: number;
}

const Row = Schema.Struct({
  owner_thread_id: ThreadId,
  message_id: IdempotencyKey,
  version: Schema.Int,
  state: Schema.String,
  deadline_at_millis: Schema.NullOr(ScheduleInstant),
  record_json: Schema.String,
});

const Count = Schema.Struct({ retained: Schema.Natural, pending: Schema.Natural });
const Deadline = Schema.Struct({ deadline: Schema.NullOr(ScheduleInstant) });

const Scan = Schema.Struct({
  nowMillis: ScheduleInstant,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
  ownerThreadId: Schema.optionalKey(ThreadId),
});

const codec = Schema.fromJsonString(MessageDeliveryRecord);
const bytes = (text: string): number => new TextEncoder().encode(text).byteLength;
const storage = (operation: string) => MessageDeliveryError.make({ reason: "storage", operation });
const corrupt = (operation: string) => MessageDeliveryError.make({ reason: "corrupt", operation });

const query = <A>(operation: string, effect: Effect.Effect<A, SqlError>) =>
  effect.pipe(Effect.mapError(() => storage(operation)));

/**
 * Shared SQL implementation over the adapter-owned effect_agent_message_deliveries table.
 * Required columns: owner_thread_id, message_id (composite primary key), version, state,
 * deadline_at_millis (nullable effective wake deadline), record_json. Index non-null deadlines
 * by (deadline_at_millis, owner_thread_id, message_id). No source/receiver ledger joins.
 * Before failpoints precede the atomic transaction; after failpoints run after its commit.
 */
export const makeSqlMessageDeliveryStore = Effect.fn("SqlMessageDeliveryStore.make")(function* (
  limits: MessageDeliveryStoreLimits = defaultMessageDeliveryStoreLimits,
  options: SqlMessageDeliveryStoreOptions = {},
) {
  const config = yield* validateMessageDelivery(MessageDeliveryStoreLimits, limits, "limits");

  const maxStoredValueBytes = yield* validateMessageDelivery(
    Schema.Int.check(Schema.isGreaterThan(0)),
    options.maxStoredValueBytes ?? 16 * 1_024 * 1_024,
    "stored-value-limit",
  );

  const sql = yield* SqlClient.SqlClient;
  const transaction = yield* SqlMessageDeliveryTransaction;
  const failpoint = yield* MessageDeliveryFailpoint;

  const encode = Effect.fn("SqlMessageDeliveryStore.encode")(function* (
    record: MessageDeliveryRecord,
  ) {
    const text = yield* Schema.encodeEffect(codec)(record).pipe(
      Effect.mapError(() => corrupt("encode")),
    );

    if (bytes(text) > maxStoredValueBytes)
      return yield* MessageDeliveryError.make({
        reason: "capacity",
        operation: "stored-value-bytes",
      });

    return text;
  });

  const decode = Effect.fn("SqlMessageDeliveryStore.decode")(function* (row: typeof Row.Type) {
    if (bytes(row.record_json) > maxStoredValueBytes) return yield* corrupt("stored-value-bytes");

    const record = yield* Schema.decodeEffect(codec)(row.record_json).pipe(
      Effect.mapError(() => corrupt("decode")),
    );

    if (
      record.key.ownerThreadId !== row.owner_thread_id ||
      record.key.messageId !== row.message_id ||
      record.version !== row.version ||
      record.status !== row.state ||
      messageDeliveryDeadline(record) !== row.deadline_at_millis
    ) {
      return yield* corrupt("row-metadata");
    }

    return record;
  });

  const decodeRows = (rows: unknown) =>
    Schema.decodeUnknownEffect(Schema.Array(Row))(rows).pipe(
      Effect.mapError(() => corrupt("rows")),
      Effect.flatMap((rows) => Effect.forEach(rows, decode)),
    );

  const get: MessageDeliveryStore["Service"]["get"] = Effect.fn("SqlMessageDeliveryStore.get")(
    function* (key) {
      const input = yield* validateMessageDelivery(MessageDeliveryKey, key, "get");

      const rows = yield* query(
        "get",
        sql`SELECT owner_thread_id, message_id, version, state, deadline_at_millis, record_json FROM effect_agent_message_deliveries WHERE owner_thread_id = ${input.ownerThreadId} AND message_id = ${input.messageId}`,
      );

      return (yield* decodeRows(rows))[0] ?? null;
    },
  );

  const insert: MessageDeliveryStore["Service"]["insert"] = Effect.fn(
    "SqlMessageDeliveryStore.insert",
  )(function* (record) {
    const text = yield* encode(record);

    const input = yield* Schema.decodeEffect(codec)(text).pipe(
      Effect.mapError(() => corrupt("insert")),
    );

    if (
      input.version !== 1 ||
      input.status !== "pending" ||
      input.leaseUntilMillis !== null ||
      input.retry.attempts !== 0 ||
      input.retry.generation !== 0 ||
      input.retry.automaticAttempts !== 0 ||
      input.retry.nextAttemptAtMillis !== input.createdAtMillis ||
      input.retry.lastAttemptAtMillis !== null ||
      input.retry.lastFailure !== null ||
      input.deadlineAtMillis !== input.initialDeadlineAtMillis
    ) {
      return yield* MessageDeliveryError.make({ reason: "validation", operation: "insert-state" });
    }
    if (bytes(JSON.stringify(input.envelope)) > config.maxEnvelopeBytes)
      return yield* MessageDeliveryError.make({ reason: "capacity", operation: "envelope-bytes" });
    yield* failpoint.hit("message-delivery:insert:before");

    const result = yield* transaction.run(
      Effect.gen(function* () {
        const existing = yield* get(input.key);

        if (existing !== null) {
          if (!sameMessageDeliveryIdentity(existing, input))
            return yield* MessageDeliveryError.make({ reason: "conflict", operation: "insert" });

          return existing;
        }

        const update = isWorkerUpdateDelivery(input);
        const capacity = messageDeliveryCapacity(config, update);

        const counts = yield* query(
          "count",
          sql`SELECT COUNT(*) AS retained, COALESCE(SUM(CASE WHEN state IN ('pending', 'accepted', 'parked') THEN 1 ELSE 0 END), 0) AS pending FROM effect_agent_message_deliveries WHERE owner_thread_id = ${input.key.ownerThreadId} AND COALESCE(json_extract(record_json, '$.envelope.messageAdmission._tag'), '') ${update ? sql`= 'WorkerUpdate'` : sql`<> 'WorkerUpdate'`}`,
        );

        const count = (yield* Schema.decodeUnknownEffect(Schema.Array(Count))(counts).pipe(
          Effect.mapError(() => corrupt("count")),
        ))[0];

        if (count === undefined) return yield* corrupt("count");
        if (count.retained >= capacity.retained || count.pending >= capacity.pending)
          return yield* MessageDeliveryError.make({ reason: "capacity", operation: "insert" });
        yield* query(
          "insert",
          sql`INSERT INTO effect_agent_message_deliveries (owner_thread_id, message_id, version, state, deadline_at_millis, record_json) VALUES (${input.key.ownerThreadId}, ${input.key.messageId}, ${input.version}, ${input.status}, ${messageDeliveryDeadline(input)}, ${text})`,
        );

        return input;
      }),
    );

    yield* failpoint.hit("message-delivery:insert:after");

    return result;
  });

  const change: MessageDeliveryStore["Service"]["change"] = Effect.fn(
    "SqlMessageDeliveryStore.change",
  )(function* (key, change) {
    const decodedKey = yield* validateMessageDelivery(MessageDeliveryKey, key, "change");
    const input = yield* validateMessageDelivery(MessageDeliveryChange, change, "change");
    const point = `message-delivery:${input._tag.toLowerCase()}`;

    yield* failpoint.hit(`${point}:before`);

    const result = yield* transaction.run(
      Effect.gen(function* () {
        const current = yield* get(decodedKey);

        if (current === null)
          return yield* MessageDeliveryError.make({ reason: "not-found", operation: "change" });
        const next = yield* Effect.fromResult(applyMessageDeliveryChange(current, input));
        const text = yield* encode(next);

        const updated = yield* query(
          "change",
          sql`UPDATE effect_agent_message_deliveries SET version = ${next.version}, state = ${next.status}, deadline_at_millis = ${messageDeliveryDeadline(next)}, record_json = ${text} WHERE owner_thread_id = ${decodedKey.ownerThreadId} AND message_id = ${decodedKey.messageId} AND version = ${input.expectedVersion} RETURNING owner_thread_id, message_id, version, state, deadline_at_millis, record_json`,
        );

        const rows = yield* decodeRows(updated);

        if (rows.length !== 1 || rows[0] === undefined)
          return yield* MessageDeliveryError.make({ reason: "conflict", operation: "change" });

        return rows[0];
      }),
    );

    yield* failpoint.hit(`${point}:after`);

    return result;
  });

  return MessageDeliveryStore.of({
    limits: config,
    maxStoredValueBytes,
    insert,
    get,
    change,
    list: Effect.fn("SqlMessageDeliveryStore.list")(function* (request) {
      const input = yield* validateMessageDelivery(MessageDeliveryPageRequest, request, "list");

      const rows = yield* query(
        "list",
        sql`SELECT owner_thread_id, message_id, version, state, deadline_at_millis, record_json FROM effect_agent_message_deliveries WHERE owner_thread_id = ${input.ownerThreadId} ${input.after === undefined ? sql`` : sql`AND message_id > ${input.after}`} ORDER BY message_id LIMIT ${input.limit + 1}`,
      );

      const records = yield* decodeRows(rows);
      const items = records.slice(0, input.limit);

      return {
        items,
        next: records.length > input.limit ? (items.at(-1)?.key.messageId ?? null) : null,
      };
    }),
    due: Effect.fn("SqlMessageDeliveryStore.due")(function* (nowMillis, limit, ownerThreadId) {
      const input = yield* validateMessageDelivery(
        Scan,
        { nowMillis, limit, ...(ownerThreadId === undefined ? {} : { ownerThreadId }) },
        "due",
      );

      const rows = yield* query(
        "due",
        sql`SELECT owner_thread_id, message_id, version, state, deadline_at_millis, record_json FROM effect_agent_message_deliveries WHERE deadline_at_millis IS NOT NULL AND deadline_at_millis <= ${input.nowMillis} ${input.ownerThreadId === undefined ? sql`` : sql`AND owner_thread_id = ${input.ownerThreadId}`} ORDER BY deadline_at_millis, owner_thread_id, message_id LIMIT ${input.limit}`,
      );

      return (yield* decodeRows(rows)).map((record) => record.key);
    }),
    nextDeadline: Effect.fn("SqlMessageDeliveryStore.nextDeadline")(function* (ownerThreadId) {
      if (ownerThreadId !== undefined)
        yield* validateMessageDelivery(ThreadId, ownerThreadId, "nextDeadline");

      const rows = yield* query(
        "next-deadline",
        sql`SELECT MIN(deadline_at_millis) AS deadline FROM effect_agent_message_deliveries ${ownerThreadId === undefined ? sql`` : sql`WHERE owner_thread_id = ${ownerThreadId}`}`,
      );

      const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(Deadline))(rows).pipe(
        Effect.mapError(() => corrupt("next-deadline")),
      );

      return decoded[0]?.deadline ?? null;
    }),
  });
});
