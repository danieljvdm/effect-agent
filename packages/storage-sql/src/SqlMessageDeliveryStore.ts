import { DateTime, Effect, Schema } from "effect";
import { ThreadId } from "effect-agent/identifiers";
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
} from "effect-agent/message-delivery";
import { ScheduleInstant } from "effect-agent/schedule";
import { IdempotencyKey } from "effect-agent/submission-ledger";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import type { Statement } from "effect/unstable/sql/Statement";

import { sqliteJsonText, queryIdentifier } from "./internal/sql-json.ts";
import { makeSqlLifecyclePublication } from "./SqlLifecyclePublication.ts";
import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";

const workerPaths = {
  delegationId: ["envelope", "workerAdmission", "origin", "worker", "delegationId"],
  targetAgentId: ["envelope", "workerAdmission", "origin", "worker", "targetAgentId"],
  threadId: ["envelope", "workerAdmission", "origin", "worker", "threadId"],
} as const;

const workerField = (sql: SqlClient.SqlClient, field: keyof typeof workerPaths) =>
  sql.onDialectOrElse({
    orElse: () => sqliteJsonText(sql, "record_json", workerPaths[field]),
    pg: () => sql.literal(`(read_metadata ->> '${field}')`),
  });

const workerStart = (sql: SqlClient.SqlClient) =>
  sql.onDialectOrElse({
    orElse: () =>
      sql`message_id = ${sqliteJsonText(sql, "record_json", ["envelope", "workerAdmission", "origin", "firstMessageId"])}`,
    pg: () => sql`(read_metadata ->> 'workerStart') = 'true'`,
  });

const withoutReceipt = (sql: SqlClient.SqlClient) =>
  sql.onDialectOrElse({
    orElse: () => sql`${sqliteJsonText(sql, "record_json", ["receipt"])} IS NULL`,
    pg: () => sql`(read_metadata ->> 'hasReceipt') = 'false'`,
  });

const encodeMetadata = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      workerUpdate: Schema.Boolean,
      delegationId: Schema.NullOr(Schema.String),
      targetAgentId: Schema.NullOr(Schema.String),
      threadId: Schema.NullOr(Schema.String),
      workerStart: Schema.Boolean,
      hasReceipt: Schema.Boolean,
    }),
  ),
);

const deliveryMetadata = (record: MessageDeliveryRecord): string => {
  const origin = record.envelope.workerAdmission?.origin;

  return encodeMetadata({
    workerUpdate: isWorkerUpdateDelivery(record),
    delegationId: origin === undefined ? null : JSON.stringify(origin.worker.delegationId),
    targetAgentId: origin === undefined ? null : JSON.stringify(origin.worker.targetAgentId),
    threadId: origin === undefined ? null : JSON.stringify(origin.worker.threadId),
    workerStart: record.key.messageId === origin?.firstMessageId,
    hasReceipt: record.receipt !== null,
  });
};

/** Mirrors the adapter's worker-control lookups without parsing arbitrary payload text on Postgres. */
export const createWorkerControlIndexes = (namespace?: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const { table: relation, execute } = yield* makeSqlQuery(namespace);

    yield* sql`CREATE INDEX effect_agent_worker_starts ON ${relation("effect_agent_message_deliveries")}(owner_thread_id, ${workerField(sql, "delegationId")}, ${workerField(sql, "targetAgentId")}, message_id) WHERE ${workerStart(sql)}`.pipe(
      execute,
    );
    yield* sql`CREATE INDEX effect_agent_worker_pending ON ${relation("effect_agent_message_deliveries")}(owner_thread_id, ${workerField(sql, "threadId")}, message_id) WHERE state IN ('pending', 'parked') AND ${withoutReceipt(sql)}`.pipe(
      execute,
    );
  });

export interface SqlMessageDeliveryStoreOptions {
  readonly namespace?: string;
  /** UTF-8 bound on the complete persisted record, including a processed Settlement. */
  readonly maxStoredValueBytes?: number;
  /** Defaults to the client's transaction; Postgres supplies its writer-lock transaction. */
  readonly transaction?: <A>(
    body: Effect.Effect<A, MessageDeliveryFailure>,
  ) => Effect.Effect<A, MessageDeliveryFailure | SqlError>;
}

const Row = Schema.Struct({
  owner_thread_id: ThreadId,
  message_id: IdempotencyKey,
  version: SqlInteger,
  state: Schema.String,
  deadline_at_millis: Schema.NullOr(SqlInteger.pipe(Schema.decodeTo(ScheduleInstant))),
  record_json: Schema.String,
});

const Count = Schema.Struct({
  retained: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)),
  pending: SqlInteger.pipe(Schema.decodeTo(Schema.Natural)),
});

const Deadline = Schema.Struct({
  deadline: Schema.NullOr(SqlInteger.pipe(Schema.decodeTo(ScheduleInstant))),
});

const Scan = Schema.Struct({
  nowMillis: ScheduleInstant,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
  ownerThreadId: Schema.optionalKey(ThreadId),
});

const codec = Schema.fromJsonString(MessageDeliveryRecord);
const bytes = (text: string): number => new TextEncoder().encode(text).byteLength;

const storage = (operation: string, cause?: unknown) =>
  MessageDeliveryError.make({
    reason: "storage",
    operation,
    ...(cause === undefined ? {} : { cause }),
  });

const corrupt = (operation: string, cause?: unknown) =>
  MessageDeliveryError.make({
    reason: "corrupt",
    operation,
    ...(cause === undefined ? {} : { cause }),
  });

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
  const { table: relation, execute } = yield* makeSqlQuery(options.namespace);

  const query = <A extends object>(operation: string, statement: Statement<A>) =>
    execute(statement).pipe(Effect.mapError((cause) => storage(operation, cause)));

  const transaction = <A>(body: Effect.Effect<A, MessageDeliveryFailure>) =>
    (options.transaction ?? sql.withTransaction)(body).pipe(
      Effect.catchTag("SqlError", () => storage("transaction")),
    );

  const lifecycle = yield* makeSqlLifecyclePublication(options.namespace, maxStoredValueBytes).pipe(
    Effect.mapError((cause) => storage("lifecycle", cause)),
  );

  const failpoint = yield* MessageDeliveryFailpoint;

  const encode = Effect.fn("SqlMessageDeliveryStore.encode")(function* (
    record: MessageDeliveryRecord,
  ) {
    const text = yield* Schema.encodeEffect(codec)(record).pipe(
      Effect.mapError((cause) => corrupt("encode", cause)),
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
      Effect.mapError((cause) => corrupt("decode", cause)),
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
      Effect.mapError((cause) => corrupt("rows", cause)),
      Effect.flatMap((rows) => Effect.forEach(rows, decode)),
    );

  const get: MessageDeliveryStore["Service"]["get"] = Effect.fn("SqlMessageDeliveryStore.get")(
    function* (key) {
      const input = yield* validateMessageDelivery(MessageDeliveryKey, key, "get");

      const rows = yield* query(
        "get",
        sql`SELECT owner_thread_id, message_id, version, state, deadline_at_millis, record_json FROM ${relation("effect_agent_message_deliveries")} WHERE owner_thread_id = ${input.ownerThreadId} AND message_id = ${input.messageId}`,
      );

      return (yield* decodeRows(rows))[0] ?? null;
    },
  );

  const insert: MessageDeliveryStore["Service"]["insert"] = Effect.fn(
    "SqlMessageDeliveryStore.insert",
  )(function* (record) {
    const text = yield* encode(record);

    const input = yield* Schema.decodeEffect(codec)(text).pipe(
      Effect.mapError((cause) => corrupt("insert", cause)),
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

    const result = yield* transaction(
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
          sql`SELECT COUNT(*) AS retained, COALESCE(SUM(CASE WHEN state IN ('pending', 'accepted', 'parked') THEN 1 ELSE 0 END), 0) AS pending FROM ${relation("effect_agent_message_deliveries")} WHERE owner_thread_id = ${input.key.ownerThreadId} AND ${sql.onDialectOrElse({ orElse: () => sql`COALESCE(${sqliteJsonText(sql, "record_json", ["envelope", "messageAdmission", "_tag"])}, '') ${update ? sql`= 'WorkerUpdate'` : sql`<> 'WorkerUpdate'`}`, pg: () => sql`(read_metadata ->> 'workerUpdate') = ${String(update)}` })}`,
        );

        const count = (yield* Schema.decodeUnknownEffect(Schema.Array(Count))(counts).pipe(
          Effect.mapError((cause) => corrupt("count", cause)),
        ))[0];

        if (count === undefined) return yield* corrupt("count");
        if (count.retained >= capacity.retained || count.pending >= capacity.pending)
          return yield* MessageDeliveryError.make({ reason: "capacity", operation: "insert" });
        yield* query(
          "insert",
          sql`INSERT INTO ${relation("effect_agent_message_deliveries")} (owner_thread_id, message_id, version, state, deadline_at_millis, record_json ${sql.onDialectOrElse({ orElse: () => sql``, pg: () => sql`, read_metadata` })}) VALUES (${input.key.ownerThreadId}, ${input.key.messageId}, ${input.version}, ${input.status}, ${messageDeliveryDeadline(input)}, ${text} ${sql.onDialectOrElse({ orElse: () => sql``, pg: () => sql`, ${deliveryMetadata(input)}::jsonb` })})`,
        );

        if (lifecycle !== undefined)
          yield* lifecycle
            .retain({
              id: JSON.stringify([
                input.key.ownerThreadId,
                "delivery",
                input.key.messageId,
                input.version,
              ]),
              ownerThreadId: input.key.ownerThreadId,
              createdAt: DateTime.makeUnsafe(input.createdAtMillis),
              fact: {
                _tag: "DeliveryRetained",
                key: input.key,
                envelope: input.envelope,
                createdAtMillis: input.createdAtMillis,
              },
            })
            .pipe(Effect.mapError((cause) => storage("retain lifecycle", cause)));

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

    const result = yield* transaction(
      Effect.gen(function* () {
        const current = yield* get(decodedKey);

        if (current === null)
          return yield* MessageDeliveryError.make({ reason: "not-found", operation: "change" });
        const next = yield* Effect.fromResult(applyMessageDeliveryChange(current, input));
        const text = yield* encode(next);

        const updated = yield* query(
          "change",
          sql`UPDATE ${relation("effect_agent_message_deliveries")} SET version = ${next.version}, state = ${next.status}, deadline_at_millis = ${messageDeliveryDeadline(next)}, record_json = ${text} ${sql.onDialectOrElse({ orElse: () => sql``, pg: () => sql`, read_metadata = ${deliveryMetadata(next)}::jsonb` })} WHERE owner_thread_id = ${decodedKey.ownerThreadId} AND message_id = ${decodedKey.messageId} AND version = ${input.expectedVersion} RETURNING owner_thread_id, message_id, version, state, deadline_at_millis, record_json`,
        );

        const rows = yield* decodeRows(updated);

        if (rows.length !== 1 || rows[0] === undefined)
          return yield* MessageDeliveryError.make({ reason: "conflict", operation: "change" });

        if (
          lifecycle !== undefined &&
          (current.status !== next.status ||
            current.receipt !== next.receipt ||
            current.settlement !== next.settlement)
        )
          yield* lifecycle
            .retain({
              id: JSON.stringify([
                next.key.ownerThreadId,
                "delivery",
                next.key.messageId,
                next.version,
              ]),
              ownerThreadId: next.key.ownerThreadId,
              createdAt: yield* DateTime.now,
              fact: {
                _tag: "DeliveryChanged",
                key: next.key,
                envelope: next.envelope,
                status: next.status,
                receipt: next.receipt,
                settlement: next.settlement,
                version: next.version,
              },
            })
            .pipe(Effect.mapError((cause) => storage("retain lifecycle", cause)));

        return rows[0];
      }),
    );

    yield* failpoint.hit(`${point}:after`);

    return result;
  });

  return MessageDeliveryStore.of({
    ...(lifecycle === undefined ? {} : { lifecyclePublications: lifecycle.storage }),
    limits: config,
    maxStoredValueBytes,
    insert,
    get,
    change,
    list: Effect.fn("SqlMessageDeliveryStore.list")(function* (request) {
      const input = yield* validateMessageDelivery(MessageDeliveryPageRequest, request, "list");

      const rows = yield* query(
        "list",
        sql`SELECT owner_thread_id, message_id, version, state, deadline_at_millis, record_json FROM ${relation("effect_agent_message_deliveries")} WHERE owner_thread_id = ${input.ownerThreadId} ${input.after === undefined ? sql`` : sql`AND message_id > ${input.after}`} ${input.pendingOnly ? sql`AND state NOT IN ('processed', 'refused')` : sql``}
        ${
          input.workerStarts === undefined
            ? sql``
            : sql`AND ${workerField(sql, "delegationId")} = ${queryIdentifier(sql, input.workerStarts.delegationId)}
          AND ${workerField(sql, "targetAgentId")} = ${queryIdentifier(sql, input.workerStarts.targetAgentId)}
          AND ${workerStart(sql)}`
        }
        ${
          input.pendingWorker === undefined
            ? sql``
            : sql`AND ${workerField(sql, "threadId")} = ${queryIdentifier(sql, input.pendingWorker)}
          AND state IN ('pending', 'parked') AND ${withoutReceipt(sql)}`
        }
        ORDER BY message_id LIMIT ${input.limit + 1}`,
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
        sql`SELECT owner_thread_id, message_id, version, state, deadline_at_millis, record_json FROM ${relation("effect_agent_message_deliveries")} WHERE deadline_at_millis IS NOT NULL AND deadline_at_millis <= ${input.nowMillis} ${input.ownerThreadId === undefined ? sql`` : sql`AND owner_thread_id = ${input.ownerThreadId}`} ORDER BY deadline_at_millis, owner_thread_id, message_id LIMIT ${input.limit}`,
      );

      return (yield* decodeRows(rows)).map((record) => record.key);
    }),
    nextDeadline: Effect.fn("SqlMessageDeliveryStore.nextDeadline")(function* (ownerThreadId) {
      if (ownerThreadId !== undefined)
        yield* validateMessageDelivery(ThreadId, ownerThreadId, "nextDeadline");

      const rows = yield* query(
        "next-deadline",
        sql`SELECT MIN(deadline_at_millis) AS deadline FROM ${relation("effect_agent_message_deliveries")} ${ownerThreadId === undefined ? sql`` : sql`WHERE owner_thread_id = ${ownerThreadId}`}`,
      );

      const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(Deadline))(rows).pipe(
        Effect.mapError((cause) => corrupt("next-deadline", cause)),
      );

      return decoded[0]?.deadline ?? null;
    }),
  });
});

/** Add the pending-only index within the adapter's format transaction. */
export const createMessageDeliveryPendingIndex = (namespace?: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const { table: relation, execute } = yield* makeSqlQuery(namespace);

    yield* sql`CREATE INDEX effect_agent_message_deliveries_pending ON ${relation("effect_agent_message_deliveries")}(owner_thread_id, message_id) WHERE state NOT IN ('processed', 'refused')`.pipe(
      execute,
    );
  });
