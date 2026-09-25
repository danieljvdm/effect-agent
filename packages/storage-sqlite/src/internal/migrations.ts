import { createStorageSchema } from "@effect-agent/storage-sql/sql-storage-schema";
import { SqliteMigrator } from "@effect/sql-sqlite-node";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export const CurrentSqliteStorageVersion = 14;

/** One permanent destination inbox fence, including workers stopped before admission. */
export const createWorkerStops = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE effect_agent_worker_stops (thread_id TEXT PRIMARY KEY NOT NULL, terminal TEXT)`;
  yield* sql`CREATE INDEX effect_agent_worker_starts ON effect_agent_message_deliveries(owner_thread_id,
    json_extract(record_json, '$.envelope.workerAdmission.origin.worker.delegationId'),
    json_extract(record_json, '$.envelope.workerAdmission.origin.worker.targetAgentId'), message_id)
    WHERE message_id = json_extract(record_json, '$.envelope.workerAdmission.origin.firstMessageId')`;
  yield* sql`CREATE INDEX effect_agent_worker_pending ON effect_agent_message_deliveries(owner_thread_id,
    json_extract(record_json, '$.envelope.workerAdmission.origin.worker.threadId'), message_id)
    WHERE state IN ('pending', 'parked') AND json_extract(record_json, '$.receipt') IS NULL`;
  yield* sql`CREATE INDEX effect_agent_worker_execution ON effect_agent_canonical_records(thread_id,
    json_extract(record_json, '$.payload._tag'), sequence) WHERE json_extract(record_json, '$.payload.runId') IS NOT NULL`;
});

/** Index only outstanding obligations, ordered by the recovery scan's stable cursor. */
export const createNonterminalIndex = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE INDEX effect_agent_submissions_nonterminal ON effect_agent_submissions (thread_id, queue_sequence) WHERE state <> 'settled'`
    .withoutTransform;
});

/** Initialize empty storage with the complete current schema. */
export const sqliteMigrations = SqliteMigrator.fromRecord({
  "1_current_thread_storage": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    yield* createStorageSchema();
    yield* createWorkerStops;
    yield* sql`PRAGMA user_version = 14`.withoutTransform;
  }),
});
