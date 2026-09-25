import { Effect, Schema } from "effect";
import {
  CanonicalRecord,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ProducerEpoch,
  RecordId,
} from "effect-agent/records";
import {
  runIdForSubmission,
  subagentLineageRecordId,
  workerOriginRecordId,
} from "effect-agent/run-journal";
import type { ThreadStore } from "effect-agent/thread-store";
import {
  SelectedThreadRead,
  ThreadPeerCountRequest,
  ThreadIdentity,
  ThreadIdentityRequest,
  ThreadNotMaterialized,
  ThreadStoreError,
} from "effect-agent/thread-store";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { sqliteJsonText, nullSafeEquals, queryIdentifier } from "./internal/sql-json.ts";
import { makeSqlQuery, SqlInteger, makeSqlTransaction } from "./SqlStorage.ts";

const canonicalPaths = {
  tag: ["payload", "_tag"],
  runId: ["payload", "runId"],
  toolCallId: ["payload", "toolCallId"],
  kind: ["payload", "kind"],
  sourceSubmissionId: ["payload", "sourceSubmissionId"],
  messageId: ["payload", "admission", "messageId"],
} as const;

const canonicalField = (sql: SqlClient.SqlClient, field: keyof typeof canonicalPaths) =>
  sql.onDialectOrElse({
    orElse: () => sqliteJsonText(sql, "record_json", canonicalPaths[field]),
    pg: () => sql.literal(`(read_metadata ->> '${field}')`),
  });

const encodeMetadata = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      tag: Schema.String,
      runId: Schema.NullOr(Schema.String),
      toolCallId: Schema.NullOr(Schema.String),
      kind: Schema.NullOr(Schema.String),
      sourceSubmissionId: Schema.NullOr(Schema.String),
      messageId: Schema.NullOr(Schema.String),
    }),
  ),
);

/**
 * Postgres jsonb rejects NUL and lone surrogates in JSON strings. Index only these decoded
 * fields, escaping identifiers as JSON strings, and leave canonical payload/digest bytes intact.
 */
export const canonicalRecordMetadata = (record: CanonicalRecord): string => {
  const payload = record.payload;

  return encodeMetadata({
    tag: payload._tag,
    runId: "runId" in payload ? JSON.stringify(payload.runId) : null,
    toolCallId: "toolCallId" in payload ? JSON.stringify(payload.toolCallId) : null,
    kind: "kind" in payload ? payload.kind : null,
    sourceSubmissionId:
      "sourceSubmissionId" in payload && payload.sourceSubmissionId !== undefined
        ? JSON.stringify(payload.sourceSubmissionId)
        : null,
    messageId:
      payload._tag === "WorkerInputRequested" ? JSON.stringify(payload.admission.messageId) : null,
  });
};

const failure = (operation: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation,
    message: "Native canonical read is incomplete or corrupt",
    ...(cause === undefined ? {} : { cause }),
  });

/** Adapter-owned metadata on canonical rows, never a second copy of execution records. */
export const createNativeReadIndexes = (namespace?: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const { table: relation, execute } = makeSqlQuery(sql, namespace);

    yield* sql.onDialectOrElse({
      orElse: () => Effect.void,
      pg: () =>
        sql`ALTER TABLE ${relation("effect_agent_canonical_records")} ADD COLUMN read_metadata JSONB NOT NULL`.pipe(
          execute,
        ),
    });
    yield* sql`ALTER TABLE ${relation("effect_agent_canonical_records")} ADD COLUMN outstanding INTEGER NOT NULL DEFAULT 0`.pipe(
      execute,
    );
    yield* sql`CREATE INDEX effect_agent_records_outstanding ON ${relation("effect_agent_canonical_records")}(thread_id, sequence) WHERE outstanding <> 0`.pipe(
      execute,
    );
    yield* sql`CREATE INDEX effect_agent_records_call ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "tag")}, ${canonicalField(sql, "runId")}, ${canonicalField(sql, "toolCallId")})`.pipe(
      execute,
    );
    yield* sql`CREATE INDEX effect_agent_records_run_input ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "runId")}) WHERE ${canonicalField(sql, "tag")} = 'UserInputRecorded' AND ${canonicalField(sql, "kind")} = 'user'`.pipe(
      execute,
    );
    yield* sql`CREATE INDEX effect_agent_records_subtree ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "sourceSubmissionId")}, sequence) WHERE ${canonicalField(sql, "tag")} = 'SubtreeBudgetReserved'`.pipe(
      execute,
    );
    yield* sql`CREATE INDEX effect_agent_records_worker_input ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "messageId")}) WHERE ${canonicalField(sql, "tag")} = 'WorkerInputRequested'`.pipe(
      execute,
    );
    yield* sql.onDialectOrElse({
      orElse: () => Effect.void,
      pg: () =>
        sql`CREATE INDEX effect_agent_worker_execution ON ${relation("effect_agent_canonical_records")}(thread_id, ${canonicalField(sql, "tag")}, sequence) WHERE ${canonicalField(sql, "runId")} IS NOT NULL`.pipe(
          execute,
        ),
    });
  });

/** Must run in the canonical append/upgrade transaction, after inserting this record. */
export const indexCanonicalRecord = Effect.fnUntraced(function* (
  threadId: string,
  record: CanonicalRecord,
  namespace?: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const { table: relation, execute } = makeSqlQuery(sql, namespace);
  const payload = record.payload;

  switch (payload._tag) {
    case "ToolCallPrepared":
    case "ToolCallUnknown": {
      if (payload._tag === "ToolCallUnknown")
        yield* sql`
        UPDATE ${relation("effect_agent_canonical_records")} SET outstanding = 0
        WHERE thread_id = ${threadId} AND ${canonicalField(sql, "tag")} = 'ToolCallPrepared'
          AND ${canonicalField(sql, "runId")} = ${queryIdentifier(sql, payload.runId)}
          AND ${canonicalField(sql, "toolCallId")} = ${queryIdentifier(sql, payload.toolCallId)}`.pipe(
          execute,
        );
      yield* sql`UPDATE ${relation("effect_agent_canonical_records")} SET outstanding = ${payload._tag === "ToolCallPrepared" ? 1 : 2}
        WHERE thread_id = ${threadId} AND record_id = ${record.recordId}`.pipe(execute);
      break;
    }
    case "ToolCallSettled":
      for (const tag of ["ToolCallPrepared", "ToolCallUnknown"])
        yield* sql`
        UPDATE ${relation("effect_agent_canonical_records")} SET outstanding = 0
        WHERE thread_id = ${threadId} AND ${canonicalField(sql, "tag")} = ${tag}
          AND ${canonicalField(sql, "runId")} = ${queryIdentifier(sql, payload.runId)}
          AND ${canonicalField(sql, "toolCallId")} = ${queryIdentifier(sql, payload.toolCallId)}`.pipe(
          execute,
        );
      break;
    case "WorkerInputRequested":
      yield* sql`UPDATE ${relation("effect_agent_canonical_records")} SET outstanding = 3 WHERE thread_id = ${threadId} AND record_id = ${record.recordId}`.pipe(
        execute,
      );
      break;
    case "WorkerInputCompleted":
      yield* sql`
        UPDATE ${relation("effect_agent_canonical_records")} SET outstanding = ${payload.effectsResolved ? 0 : 4}
        WHERE thread_id = ${threadId} AND ${canonicalField(sql, "tag")} = 'WorkerInputRequested'
          AND ${canonicalField(sql, "messageId")} = ${queryIdentifier(sql, payload.messageId)}`.pipe(
        execute,
      );
      break;
  }
});

/** One-time native index construction during the atomic supported-format upgrade. */
export const seedNativeReadIndexes = (namespace?: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const { table: relation, execute } = makeSqlQuery(sql, namespace);

    // This is the one supported-format upgrade, in the adapter transaction. Decode in
    // bounded pages; malformed rows or gaps roll back both metadata and version.
    const gaps =
      yield* sql`SELECT t.thread_id FROM ${relation("effect_agent_threads")} t LEFT JOIN ${relation("effect_agent_canonical_records")} r ON r.thread_id = t.thread_id
    GROUP BY t.thread_id HAVING count(r.sequence) <> t.tail_sequence OR coalesce(max(r.sequence), 0) <> t.tail_sequence
    UNION ALL SELECT r.thread_id FROM ${relation("effect_agent_canonical_records")} r LEFT JOIN ${relation("effect_agent_threads")} t ON t.thread_id = r.thread_id WHERE t.thread_id IS NULL LIMIT 1`.pipe(
        execute,
      );

    if (gaps.length > 0) return yield* failure("native index upgrade canonical gap");
    let afterThread = "";
    let afterSequence = 0;

    while (true) {
      const rows =
        yield* sql`SELECT thread_id, record_id, sequence, record_json FROM ${relation("effect_agent_canonical_records")}
      WHERE (thread_id, sequence) > (${afterThread}, ${afterSequence}) ORDER BY thread_id, sequence LIMIT 100`.pipe(
          execute,
          Effect.flatMap(
            Schema.decodeUnknownEffect(
              Schema.Array(
                Schema.Struct({
                  thread_id: Schema.String,
                  record_id: Schema.String,
                  sequence: SqlInteger,
                  record_json: Schema.String,
                }),
              ),
            ),
          ),
        );

      if (rows.length === 0) break;
      for (const row of rows) {
        const record = yield* Schema.decodeEffect(Schema.fromJsonString(CanonicalRecord))(
          row.record_json,
        );

        if (
          record.recordId !== row.record_id ||
          row.sequence !== (row.thread_id === afterThread ? afterSequence : 0) + 1
        )
          return yield* failure("native index upgrade canonical identity");
        yield* indexCanonicalRecord(row.thread_id, record, namespace);
        afterThread = row.thread_id;
        afterSequence = row.sequence;
      }
    }
  });

const Row = Schema.Struct({
  thread_id: SelectedThreadRead.fields.threadId,
  sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
  record_id: RecordId,
  batch_id: CanonicalRecordEnvelope.fields.batchId,
  record_json: Schema.String,
  outstanding: Schema.optionalKey(SqlInteger),
});

export const makeSelectedReads = Effect.fnUntraced(function* (
  envelope: (row: typeof Row.Type) => Effect.Effect<CanonicalRecordEnvelope, ThreadStoreError>,
  namespace?: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const { table: relation, execute } = makeSqlQuery(sql, namespace);

  const snapshot = sql.onDialectOrElse({
    orElse: () => sql.withTransaction,
    pg: () => makeSqlTransaction(sql, { begin: "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" }),
  });

  const requireThread = Effect.fnUntraced(function* (threadId: SelectedThreadRead["threadId"]) {
    const rows =
      yield* sql`SELECT tail_sequence, tail_digest FROM ${relation("effect_agent_threads")} WHERE thread_id = ${threadId}`.pipe(
        execute,
      );

    if (rows.length === 0) return yield* ThreadNotMaterialized.make({ threadId });

    const decoded = yield* Schema.decodeUnknownEffect(
      Schema.Array(
        Schema.Struct({
          tail_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
          tail_digest: Digest,
        }),
      ),
    )(rows);

    if (decoded.length !== 1 || decoded[0] === undefined)
      return yield* failure("native thread tail");

    return decoded[0];
  });

  const read = Effect.fnUntraced(
    function* (request: SelectedThreadRead) {
      return yield* snapshot(
        Effect.gen(function* () {
          const tail = yield* requireThread(request.threadId);
          const selection = request.selection;

          if (
            "expectedTailSequence" in selection &&
            (tail.tail_sequence !== selection.expectedTailSequence ||
              tail.tail_digest !== selection.expectedTailDigest)
          )
            return yield* failure("selected read tail changed");
          const after = request.page.afterSequence ?? 0;
          let rows: unknown;

          switch (selection._tag) {
            case "RecordId":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND record_id = ${selection.recordId} AND sequence > ${after}`.pipe(
                  execute,
                );
              break;
            case "RunInput":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "tag")} = 'UserInputRecorded' AND ${canonicalField(sql, "kind")} = 'user' AND ${canonicalField(sql, "runId")} = ${queryIdentifier(sql, selection.runId)} LIMIT 2`.pipe(
                  execute,
                );
              break;
            case "Outstanding":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json, outstanding FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND outstanding <> 0 AND sequence > ${after} ORDER BY sequence LIMIT ${request.page.limit}`.pipe(
                  execute,
                );
              break;
            case "WorkerExecution":
              rows = (yield* Effect.forEach(["UserInputRecorded", "RunStarted"], (tag) =>
                sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")}
                  WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "tag")} = ${tag}
                    AND ${canonicalField(sql, "runId")} IS NOT NULL
                  ORDER BY sequence DESC LIMIT 1`.pipe(execute),
              )).flat();
              break;
            case "WorkerState": {
              const runId =
                selection.sourceSubmissionId === undefined
                  ? null
                  : runIdForSubmission(selection.sourceSubmissionId);

              rows = yield* sql`
            SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")}
            WHERE thread_id = ${request.threadId} AND sequence > ${after} AND ${canonicalField(sql, "tag")} IN ('ThreadCreated', 'WorkerOriginRecorded', 'SubagentLineageRecorded', 'WorkerInputRequested', 'WorkerInputCompleted', 'WorkerStopRequested')
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")}
            WHERE thread_id = ${request.threadId} AND sequence > ${after} AND ${canonicalField(sql, "tag")} = 'SubtreeBudgetReserved' AND ${nullSafeEquals(sql, canonicalField(sql, "sourceSubmissionId"), queryIdentifier(sql, selection.sourceSubmissionId ?? null))}
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json FROM ${relation("effect_agent_canonical_records")}
            WHERE thread_id = ${request.threadId} AND sequence > ${after} AND ${canonicalField(sql, "tag")} = 'SubagentJoined' AND ${canonicalField(sql, "runId")} = ${queryIdentifier(sql, runId)}
            ORDER BY sequence LIMIT ${request.page.limit}`.pipe(execute);
              break;
            }
          }
          const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(Row))(rows);

          if (selection._tag === "RunInput" && decoded.length > 1)
            return yield* failure("ambiguous original Run input");

          const remaining = decoded.filter((row) => row.sequence > after);

          return yield* Effect.forEach(
            selection._tag === "WorkerExecution"
              ? remaining.sort((a, b) => a.sequence - b.sequence).slice(0, request.page.limit)
              : remaining,
            (row) =>
              Effect.gen(function* () {
                const value = yield* envelope(row);

                if (
                  value.record.recordId !== row.record_id ||
                  row.thread_id !== request.threadId ||
                  (selection._tag === "Outstanding" &&
                    !(
                      (row.outstanding === 1 && value.record.payload._tag === "ToolCallPrepared") ||
                      (row.outstanding === 2 && value.record.payload._tag === "ToolCallUnknown") ||
                      (row.outstanding === 3 &&
                        value.record.payload._tag === "WorkerInputRequested")
                    ))
                )
                  return yield* failure("selected record incomplete or corrupt");

                return value;
              }),
          );
        }),
      );
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("selected read", cause),
    ),
  );

  const readIdentity: ThreadStore["Service"]["readIdentity"] = Effect.fnUntraced(
    function* (request) {
      yield* Schema.decodeEffect(Schema.toType(ThreadIdentityRequest))(request);

      return yield* snapshot(
        Effect.gen(function* () {
          const tails = yield* sql`SELECT tail_sequence, tail_digest, producer_epoch
            FROM ${relation("effect_agent_threads")} WHERE thread_id = ${request.threadId}`.pipe(
            execute,
          );

          if (tails.length === 0)
            return yield* ThreadNotMaterialized.make({ threadId: request.threadId });

          const decoded = yield* Schema.decodeUnknownEffect(
            Schema.Array(
              Schema.Struct({
                tail_sequence: SqlInteger.pipe(Schema.decodeTo(CanonicalSequence)),
                tail_digest: Digest,
                producer_epoch: SqlInteger.pipe(Schema.decodeTo(ProducerEpoch)),
              }),
            ),
          )(tails);

          const tail = decoded[0];

          if (decoded.length !== 1 || tail === undefined) return yield* failure("identity tail");
          const origin = workerOriginRecordId(request.threadId);
          const lineage = subagentLineageRecordId(request.threadId);

          const rows =
            yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM (
            SELECT thread_id, sequence, record_id, batch_id, record_json, 0 AS identity_order
            FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND sequence = 1
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json, 1 AS identity_order
            FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND record_id = ${origin} AND sequence <> 1
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json, 2 AS identity_order
            FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND record_id = ${lineage} AND sequence <> 1
          ) ORDER BY identity_order`.pipe(execute);

          const records = yield* Schema.decodeUnknownEffect(Schema.Array(Row))(rows).pipe(
            Effect.flatMap(
              Effect.forEach((row) =>
                envelope(row).pipe(
                  Effect.filterOrFail(
                    (value) => value.record.recordId === row.record_id,
                    () => failure("identity record locator"),
                  ),
                ),
              ),
            ),
          );

          return yield* ThreadIdentity.makeEffect({
            threadId: request.threadId,
            tailSequence: tail.tail_sequence,
            tailDigest: tail.tail_digest,
            producerEpoch: tail.producer_epoch,
            records,
          });
        }),
      );
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("readIdentity", cause),
    ),
  );

  const countPeerMessages: NonNullable<ThreadStore["Service"]["countPeerMessages"]> =
    Effect.fnUntraced(
      function* (request) {
        yield* Schema.decodeEffect(ThreadPeerCountRequest)(request);
        yield* requireThread(request.threadId);

        const rows =
          yield* sql`SELECT 1 FROM ${relation("effect_agent_canonical_records")} WHERE thread_id = ${request.threadId} AND ${canonicalField(sql, "tag")} = 'PeerMessagePrepared' LIMIT ${request.limit}`.pipe(
            execute,
          );

        return rows.length;
      },
      Effect.mapError((cause) =>
        cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
          ? cause
          : failure("countPeerMessages", cause),
      ),
    );

  return { read, countPeerMessages, readIdentity };
});
