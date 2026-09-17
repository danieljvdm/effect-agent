import { Effect, Option, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  CanonicalRecord,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  RecordId,
} from "./Records.ts";
import { runIdForSubmission } from "./RunJournal.ts";
import {
  type ThreadOutstanding,
  type ThreadNativeReads,
  ThreadNotMaterialized,
  ThreadOutstandingRequest,
  ThreadRecordRequest,
  ThreadWorkerInputsPageRequest,
  ThreadWorkerStateRequest,
  ThreadPeerCountRequest,
  ThreadRunInputRequest,
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
  thread_id: ThreadRecordRequest.fields.threadId,
  sequence: CanonicalSequence,
  record_id: RecordId,
  batch_id: CanonicalRecordEnvelope.fields.batchId,
  record_json: Schema.String,
  outstanding: Schema.optionalKey(Schema.Int),
});

export const makeNativeReads = Effect.fnUntraced(function* (
  envelope: (row: typeof Row.Type) => Effect.Effect<CanonicalRecordEnvelope, ThreadStoreError>,
): Effect.fn.Return<ThreadNativeReads, never, SqlClient.SqlClient> {
  const sql = yield* SqlClient.SqlClient;

  const decodeRows = (rows: unknown) =>
    Schema.decodeUnknownEffect(Schema.Array(Row))(rows).pipe(
      Effect.mapError((cause) => failure("native read", cause)),
    );

  const requireThread = Effect.fnUntraced(function* (threadId: ThreadRecordRequest["threadId"]) {
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

  const one = Effect.fnUntraced(function* (rows: unknown) {
    const decoded = yield* decodeRows(rows);

    if (decoded.length > 1) return yield* failure("native unique lookup");
    if (decoded[0] === undefined) return Option.none();
    const value = yield* envelope(decoded[0]);

    if (value.record.recordId !== decoded[0].record_id)
      return yield* failure("native record identity");

    return Option.some(value);
  });

  const getRecord: ThreadNativeReads["getRecord"] = Effect.fn("ThreadStore.getRecord")(
    function* (request) {
      yield* Schema.decodeUnknownEffect(ThreadRecordRequest)(request);
      yield* requireThread(request.threadId);

      return yield* one(
        yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND record_id = ${request.recordId}`,
      );
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("getRecord", cause),
    ),
  );

  const getRunInput: ThreadNativeReads["getRunInput"] = Effect.fn("ThreadStore.getRunInput")(
    function* (request) {
      yield* Schema.decodeUnknownEffect(ThreadRunInputRequest)(request);
      yield* requireThread(request.threadId);

      return yield* one(
        yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND json_extract(record_json, '$.payload._tag') = 'UserInputRecorded' AND json_extract(record_json, '$.payload.kind') = 'user' AND json_extract(record_json, '$.payload.runId') = ${request.runId} LIMIT 2`,
      );
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("getRunInput", cause),
    ),
  );

  const readOutstanding: ThreadNativeReads["readOutstanding"] = Effect.fn(
    "ThreadStore.readOutstanding",
  )(
    function* (request) {
      yield* Schema.decodeUnknownEffect(ThreadOutstandingRequest)(request);

      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const tail = yield* requireThread(request.threadId);

          const rows = yield* decodeRows(
            yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json, outstanding FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND outstanding <> 0 ORDER BY sequence LIMIT ${request.limit + 1}`,
          );

          if (rows.length > request.limit) return yield* failure("readOutstanding limit");
          const operations: Array<ThreadOutstanding["operations"][number]> = [];
          const workerInputs: Array<ThreadOutstanding["workerInputs"][number]> = [];

          for (const row of rows) {
            const payload = (yield* envelope(row)).record.payload;

            if (payload._tag === "WorkerInputRequested") {
              workerInputs.push(payload);
              continue;
            }
            if (payload._tag !== "ToolCallPrepared" && payload._tag !== "ToolCallUnknown")
              return yield* failure("readOutstanding record");

            const preparedRecord =
              payload._tag === "ToolCallPrepared"
                ? payload
                : Option.getOrUndefined(
                    yield* one(
                      yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records WHERE thread_id = ${request.threadId} AND json_extract(record_json, '$.payload._tag') = 'ToolCallPrepared' AND json_extract(record_json, '$.payload.runId') = ${payload.runId} AND json_extract(record_json, '$.payload.toolCallId') = ${payload.toolCallId} LIMIT 2`,
                    ),
                  )?.record.payload;

            const input = Option.getOrUndefined(
              yield* getRunInput({ threadId: request.threadId, runId: payload.runId }),
            )?.record.payload;

            if (
              preparedRecord?._tag !== "ToolCallPrepared" ||
              input?._tag !== "UserInputRecorded" ||
              input.submissionId === undefined
            )
              return yield* failure("readOutstanding canonical ownership");
            operations.push({
              submissionId: input.submissionId,
              prepared: preparedRecord,
              state: payload._tag === "ToolCallUnknown" ? "unknown" : "prepared",
            });
          }

          return {
            complete: rows.every((row) => row.outstanding !== 4),
            threadId: request.threadId,
            throughSequence: tail.tail_sequence,
            operations,
            workerInputs,
          };
        }),
      );
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("readOutstanding", cause),
    ),
  );

  const readWorkerInputsPage: ThreadNativeReads["readWorkerInputsPage"] = Effect.fn(
    "ThreadStore.readWorkerInputsPage",
  )(
    function* (request) {
      yield* Schema.decodeUnknownEffect(ThreadWorkerInputsPageRequest)(request);
      yield* requireThread(request.threadId);

      const rows = yield* decodeRows(
        yield* sql`SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records
        WHERE thread_id = ${request.threadId} AND outstanding <> 0 AND outstanding IN (3, 4)
          AND sequence > ${request.afterSequence ?? 0} ORDER BY sequence LIMIT ${request.limit + 1}`,
      );

      const page = rows.slice(0, request.limit);
      const inputs = [];

      for (const row of page) {
        const payload = (yield* envelope(row)).record.payload;

        if (payload._tag !== "WorkerInputRequested")
          return yield* failure("readWorkerInputsPage record");
        inputs.push(payload);
      }

      return { inputs, next: rows.length > request.limit ? (page.at(-1)?.sequence ?? null) : null };
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("readWorkerInputsPage", cause),
    ),
  );

  const readWorkerState: ThreadNativeReads["readWorkerState"] = Effect.fn(
    "ThreadStore.readWorkerState",
  )(
    function* (request) {
      yield* Schema.decodeUnknownEffect(ThreadWorkerStateRequest)(request);

      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const tail = yield* requireThread(request.threadId);

          const runId =
            request.sourceSubmissionId === undefined
              ? null
              : runIdForSubmission(request.sourceSubmissionId);

          const rows = yield* decodeRows(
            yield* sql`
          SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records
          WHERE thread_id = ${request.threadId} AND json_extract(record_json, '$.payload._tag') IN ('ThreadCreated', 'WorkerOriginRecorded', 'SubagentLineageRecorded', 'WorkerInputRequested', 'WorkerInputCompleted')
          UNION ALL
          SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records
          WHERE thread_id = ${request.threadId} AND json_extract(record_json, '$.payload._tag') = 'SubtreeBudgetReserved' AND json_extract(record_json, '$.payload.sourceSubmissionId') IS ${request.sourceSubmissionId ?? null}
          UNION ALL
          SELECT thread_id, sequence, record_id, batch_id, record_json FROM effect_agent_canonical_records
          WHERE thread_id = ${request.threadId} AND json_extract(record_json, '$.payload._tag') = 'SubagentJoined' AND json_extract(record_json, '$.payload.runId') = ${runId}
          ORDER BY sequence LIMIT ${request.limit + 1}`,
          );

          if (rows.length > request.limit) return yield* failure("readWorkerState limit");
          const records = yield* Effect.forEach(rows, envelope);

          return {
            threadId: request.threadId,
            tailSequence: tail.tail_sequence,
            tailDigest: tail.tail_digest,
            records,
          };
        }),
      );
    },
    Effect.mapError((cause) =>
      cause._tag === "ThreadNotMaterialized" || cause._tag === "ThreadStoreError"
        ? cause
        : failure("readWorkerState", cause),
    ),
  );

  const countPeerMessages: ThreadNativeReads["countPeerMessages"] = Effect.fn(
    "ThreadStore.countPeerMessages",
  )(
    function* (request) {
      yield* Schema.decodeUnknownEffect(ThreadPeerCountRequest)(request);
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

  return {
    getRecord,
    getRunInput,
    readOutstanding,
    readWorkerInputsPage,
    readWorkerState,
    countPeerMessages,
  };
});
