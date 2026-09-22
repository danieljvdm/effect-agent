import { Effect, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  CanonicalRecord,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ProducerEpoch,
  RecordId,
} from "./Records.ts";
import { runIdForSubmission, subagentLineageRecordId, workerOriginRecordId } from "./RunJournal.ts";
import type { ThreadStore } from "./ThreadStore.ts";
import {
  SelectedThreadRead,
  ThreadPeerCountRequest,
  ThreadIdentity,
  ThreadIdentityRequest,
  ThreadNotMaterialized,
  ThreadStoreError,
} from "./ThreadStore.ts";

const failure = (operation: string, cause?: unknown) =>
  ThreadStoreError.make({
    operation,
    message: "Native canonical read is incomplete or corrupt",
    ...(cause === undefined ? {} : { cause }),
  });

/** Adapter-owned metadata on canonical rows, never a second copy of execution records. */
export const createNativeReadIndexes = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`ALTER TABLE effect_agent_canonical_records ADD COLUMN outstanding INTEGER NOT NULL DEFAULT 0`;
  yield* sql`CREATE INDEX effect_agent_records_outstanding ON effect_agent_canonical_records(thread_id, sequence) WHERE outstanding <> 0`;
  yield* sql`CREATE INDEX effect_agent_records_call ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload._tag'), json_extract(record_json, '$.payload.runId'), json_extract(record_json, '$.payload.toolCallId'))`;
  yield* sql`CREATE INDEX effect_agent_records_run_input ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.runId')) WHERE json_extract(record_json, '$.payload._tag') = 'UserInputRecorded' AND json_extract(record_json, '$.payload.kind') = 'user'`;
  yield* sql`CREATE INDEX effect_agent_records_subtree ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.sourceSubmissionId'), sequence) WHERE json_extract(record_json, '$.payload._tag') = 'SubtreeBudgetReserved'`;
  yield* sql`CREATE INDEX effect_agent_records_worker_input ON effect_agent_canonical_records(thread_id, json_extract(record_json, '$.payload.admission.messageId')) WHERE json_extract(record_json, '$.payload._tag') = 'WorkerInputRequested'`;
});

/** Must run in the canonical append/upgrade transaction, after inserting this record. */
export const indexCanonicalRecord = Effect.fnUntraced(function* (
  threadId: string,
  record: CanonicalRecord,
) {
  const sql = yield* SqlClient.SqlClient;
  const payload = record.payload;

  switch (payload._tag) {
    case "ToolCallPrepared":
    case "ToolCallUnknown": {
      if (payload._tag === "ToolCallUnknown")
        yield* sql`
        UPDATE effect_agent_canonical_records SET outstanding = 0
        WHERE thread_id = ${threadId} AND json_extract(record_json, '$.payload._tag') = 'ToolCallPrepared'
          AND json_extract(record_json, '$.payload.runId') = ${payload.runId}
          AND json_extract(record_json, '$.payload.toolCallId') = ${payload.toolCallId}`;
      yield* sql`UPDATE effect_agent_canonical_records SET outstanding = ${payload._tag === "ToolCallPrepared" ? 1 : 2}
        WHERE thread_id = ${threadId} AND record_id = ${record.recordId}`;
      break;
    }
    case "ToolCallSettled":
      for (const tag of ["ToolCallPrepared", "ToolCallUnknown"])
        yield* sql`
        UPDATE effect_agent_canonical_records SET outstanding = 0
        WHERE thread_id = ${threadId} AND json_extract(record_json, '$.payload._tag') = ${tag}
          AND json_extract(record_json, '$.payload.runId') = ${payload.runId}
          AND json_extract(record_json, '$.payload.toolCallId') = ${payload.toolCallId}`;
      break;
    case "WorkerInputRequested":
      yield* sql`UPDATE effect_agent_canonical_records SET outstanding = 3 WHERE thread_id = ${threadId} AND record_id = ${record.recordId}`;
      break;
    case "WorkerInputCompleted":
      yield* sql`
        UPDATE effect_agent_canonical_records SET outstanding = ${payload.effectsResolved ? 0 : 4}
        WHERE thread_id = ${threadId} AND json_extract(record_json, '$.payload._tag') = 'WorkerInputRequested'
          AND json_extract(record_json, '$.payload.admission.messageId') = ${payload.messageId}`;
      break;
  }
});

/** One-time native index construction during the atomic supported-format upgrade. */
export const seedNativeReadIndexes = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // This is the one supported-format upgrade, in the adapter transaction. Decode in
  // bounded pages; malformed rows or gaps roll back both metadata and version.
  const gaps =
    yield* sql`SELECT t.thread_id FROM effect_agent_threads t LEFT JOIN effect_agent_canonical_records r ON r.thread_id = t.thread_id
    GROUP BY t.thread_id HAVING count(r.sequence) <> t.tail_sequence OR coalesce(max(r.sequence), 0) <> t.tail_sequence
    UNION ALL SELECT r.thread_id FROM effect_agent_canonical_records r LEFT JOIN effect_agent_threads t ON t.thread_id = r.thread_id WHERE t.thread_id IS NULL LIMIT 1`;

  if (gaps.length > 0) return yield* failure("native index upgrade canonical gap");
  let afterThread = "";
  let afterSequence = 0;

  while (true) {
    const rows = yield* sql<{
      thread_id: string;
      record_id: string;
      sequence: number;
      record_json: string;
    }>`SELECT thread_id, record_id, sequence, record_json FROM effect_agent_canonical_records
      WHERE (thread_id, sequence) > (${afterThread}, ${afterSequence}) ORDER BY thread_id, sequence LIMIT 100`;

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
      yield* indexCanonicalRecord(row.thread_id, record);
      afterThread = row.thread_id;
      afterSequence = row.sequence;
    }
  }
});

const Row = Schema.Struct({
  thread_id: SelectedThreadRead.fields.threadId,
  sequence: CanonicalSequence,
  record_id: RecordId,
  batch_id: CanonicalRecordEnvelope.fields.batchId,
  record_json: Schema.String,
  outstanding: Schema.optionalKey(Schema.Int),
});

export const makeSelectedReads = Effect.fnUntraced(function* (
  envelope: (row: typeof Row.Type) => Effect.Effect<CanonicalRecordEnvelope, ThreadStoreError>,
) {
  const sql = yield* SqlClient.SqlClient;

  const requireThread = Effect.fnUntraced(function* (threadId: SelectedThreadRead["threadId"]) {
    const rows =
      yield* sql`SELECT tail_sequence, tail_digest FROM effect_agent_threads WHERE thread_id = ${threadId}`;

    if (rows.length === 0) return yield* ThreadNotMaterialized.make({ threadId });

    const decoded = yield* Schema.decodeUnknownEffect(
      Schema.Array(Schema.Struct({ tail_sequence: CanonicalSequence, tail_digest: Digest })),
    )(rows);

    if (decoded.length !== 1 || decoded[0] === undefined)
      return yield* failure("native thread tail");

    return decoded[0];
  });

  const read = Effect.fnUntraced(
    function* (request: SelectedThreadRead) {
      return yield* sql.withTransaction(
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
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND record_id = ${selection.recordId} AND sequence > ${after}`;
              break;
            case "RunInput":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND json_extract(record_json, '$.payload._tag') = 'UserInputRecorded' AND json_extract(record_json, '$.payload.kind') = 'user' AND json_extract(record_json, '$.payload.runId') = ${selection.runId} LIMIT 2`;
              break;
            case "Outstanding":
              rows =
                yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json, outstanding FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND outstanding <> 0 AND sequence > ${after} ORDER BY sequence LIMIT ${request.page.limit}`;
              break;
            case "WorkerExecution":
              rows = (yield* Effect.forEach(
                ["UserInputRecorded", "RunStarted"],
                (tag) =>
                  sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records
                  WHERE thread_id = ${request.threadId} AND json_extract(record_json, '$.payload._tag') = ${tag}
                    AND json_extract(record_json, '$.payload.runId') IS NOT NULL
                  ORDER BY sequence DESC LIMIT 1`,
              ))
                .flat()
                .filter((row) => Number(row.sequence) > after)
                .sort((a, b) => Number(a.sequence) - Number(b.sequence))
                .slice(0, request.page.limit);
              break;
            case "WorkerState": {
              const runId =
                selection.sourceSubmissionId === undefined
                  ? null
                  : runIdForSubmission(selection.sourceSubmissionId);

              rows = yield* sql`
            SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records
            WHERE thread_id = ${request.threadId} AND sequence > ${after} AND json_extract(record_json, '$.payload._tag') IN ('ThreadCreated', 'WorkerOriginRecorded', 'SubagentLineageRecorded', 'WorkerInputRequested', 'WorkerInputCompleted', 'WorkerStopRequested')
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records
            WHERE thread_id = ${request.threadId} AND sequence > ${after} AND json_extract(record_json, '$.payload._tag') = 'SubtreeBudgetReserved' AND json_extract(record_json, '$.payload.sourceSubmissionId') IS ${selection.sourceSubmissionId ?? null}
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records
            WHERE thread_id = ${request.threadId} AND sequence > ${after} AND json_extract(record_json, '$.payload._tag') = 'SubagentJoined' AND json_extract(record_json, '$.payload.runId') = ${runId}
            ORDER BY sequence LIMIT ${request.page.limit}`;
              break;
            }
          }
          const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(Row))(rows);

          if (selection._tag === "RunInput" && decoded.length > 1)
            return yield* failure("ambiguous original Run input");

          return yield* Effect.forEach(
            decoded.filter((row) => row.sequence > after),
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

      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const tails = yield* sql`SELECT tail_sequence, tail_digest, producer_epoch
            FROM effect_agent_threads WHERE thread_id = ${request.threadId}`;

          if (tails.length === 0)
            return yield* ThreadNotMaterialized.make({ threadId: request.threadId });

          const decoded = yield* Schema.decodeUnknownEffect(
            Schema.Array(
              Schema.Struct({
                tail_sequence: CanonicalSequence,
                tail_digest: Digest,
                producer_epoch: ProducerEpoch,
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
            FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND sequence = 1
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json, 1 AS identity_order
            FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND record_id = ${origin} AND sequence <> 1
            UNION ALL
            SELECT thread_id, sequence, record_id, batch_id, record_json, 2 AS identity_order
            FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND record_id = ${lineage} AND sequence <> 1
          ) ORDER BY identity_order`;

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
          yield* sql`SELECT 1 FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND json_extract(record_json, '$.payload._tag') = 'PeerMessagePrepared' LIMIT ${request.limit}`;

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
