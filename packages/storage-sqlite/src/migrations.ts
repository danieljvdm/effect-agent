import { SqliteMigrator } from "@effect/sql-sqlite-node";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export const CurrentSqliteStorageVersion = 1;

export const sqliteMigrations = SqliteMigrator.fromRecord({
  "1_current_persistent_conversation_foundation": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    yield* sql`
      CREATE TABLE effect_agent_conversations (
        conversation_id TEXT PRIMARY KEY NOT NULL,
        created_at TEXT NOT NULL,
        tail_sequence INTEGER NOT NULL,
        tail_digest TEXT NOT NULL,
        producer_epoch INTEGER NOT NULL
      )
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_canonical_batches (
        conversation_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        first_sequence INTEGER NOT NULL,
        last_sequence INTEGER NOT NULL,
        batch_digest TEXT NOT NULL,
        tail_digest TEXT NOT NULL,
        batch_json TEXT NOT NULL,
        PRIMARY KEY (conversation_id, batch_id),
        FOREIGN KEY (conversation_id)
          REFERENCES effect_agent_conversations(conversation_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_canonical_records (
        conversation_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        record_id TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        record_json TEXT NOT NULL,
        PRIMARY KEY (conversation_id, sequence),
        UNIQUE (conversation_id, record_id),
        FOREIGN KEY (conversation_id, batch_id)
          REFERENCES effect_agent_canonical_batches(conversation_id, batch_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    yield* sql`
      CREATE INDEX effect_agent_canonical_records_batch
        ON effect_agent_canonical_records (conversation_id, batch_id, sequence)
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_checkpoints (
        conversation_id TEXT NOT NULL,
        through_sequence INTEGER NOT NULL,
        tail_digest TEXT NOT NULL,
        checkpoint_json TEXT NOT NULL,
        PRIMARY KEY (conversation_id, through_sequence),
        FOREIGN KEY (conversation_id)
          REFERENCES effect_agent_conversations(conversation_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    yield* sql`PRAGMA user_version = 1`.withoutTransform;
  }),
});
