import { SqliteMigrator } from "@effect/sql-sqlite-node";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export const CurrentSqliteStorageVersion = 3;

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
  "2_durable_submission_ledger": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    // Admission rows exist before Conversation materialization (durability §4), so
    // conversation_id intentionally carries no foreign key into effect_agent_conversations.
    yield* sql`
      CREATE TABLE effect_agent_submissions (
        submission_id TEXT PRIMARY KEY NOT NULL,
        conversation_id TEXT NOT NULL,
        queue_sequence INTEGER NOT NULL,
        principal TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        agent_digests_json TEXT NOT NULL,
        deployment_id TEXT NOT NULL,
        input_json TEXT NOT NULL,
        input_digest TEXT NOT NULL,
        receipt_id TEXT NOT NULL,
        state TEXT NOT NULL,
        settled_outcome TEXT,
        created_at TEXT NOT NULL,
        ready_at TEXT,
        input_applied_record_id TEXT,
        input_applied_sequence INTEGER,
        UNIQUE (conversation_id, principal, idempotency_key),
        UNIQUE (conversation_id, queue_sequence)
      )
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_submission_ownership (
        submission_id TEXT PRIMARY KEY NOT NULL,
        attempt_id TEXT NOT NULL,
        ownership_token TEXT NOT NULL,
        producer_epoch INTEGER NOT NULL,
        owner_producer_id TEXT NOT NULL,
        lease_expires_at TEXT NOT NULL,
        FOREIGN KEY (submission_id)
          REFERENCES effect_agent_submissions(submission_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_attempts (
        attempt_id TEXT PRIMARY KEY NOT NULL,
        submission_id TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        owner_producer_id TEXT NOT NULL,
        producer_epoch INTEGER NOT NULL,
        claimed_at TEXT NOT NULL,
        FOREIGN KEY (submission_id)
          REFERENCES effect_agent_submissions(submission_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_settlement_reservations (
        submission_id TEXT PRIMARY KEY NOT NULL,
        settlement_id TEXT NOT NULL,
        outcome TEXT NOT NULL,
        record_id TEXT NOT NULL,
        record_json TEXT NOT NULL,
        record_digest TEXT NOT NULL,
        reserved_at TEXT NOT NULL,
        finalized_at TEXT,
        FOREIGN KEY (submission_id)
          REFERENCES effect_agent_submissions(submission_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_abort_intents (
        submission_id TEXT PRIMARY KEY NOT NULL,
        author TEXT NOT NULL,
        reason TEXT NOT NULL,
        requested_at TEXT NOT NULL,
        canonical_record_id TEXT,
        FOREIGN KEY (submission_id)
          REFERENCES effect_agent_submissions(submission_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    yield* sql`PRAGMA user_version = 2`.withoutTransform;
  }),
  "3_durable_tools_and_joined_input": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    // Joined queued input (plan §2.5, DUR-016): a joining/joined Submission records which host
    // Run claimed it. The canonical input marker reuses input_applied_record_id/_sequence
    // because the joined input IS the Submission's own canonical `input:{sid}` record.
    yield* sql`
      ALTER TABLE effect_agent_submissions
        ADD COLUMN joined_host_submission_id TEXT
    `.withoutTransform;

    // Durable approval suspension (plan §2.6): the reason is the Schema-encoded
    // SuspensionReason union so later suspension families stay additive.
    yield* sql`
      ALTER TABLE effect_agent_submissions
        ADD COLUMN suspended_reason_json TEXT
    `.withoutTransform;
    yield* sql`
      ALTER TABLE effect_agent_submissions
        ADD COLUMN suspended_at TEXT
    `.withoutTransform;

    // Unknown Outcome marking (DUR-009/DUR-017): the marked open Tool Call identities are
    // stored so `recordUnknownResolution` can reopen the lane only when every marked call
    // has a durable resolution intent.
    yield* sql`
      ALTER TABLE effect_agent_submissions
        ADD COLUMN unknown_reason TEXT
    `.withoutTransform;
    yield* sql`
      ALTER TABLE effect_agent_submissions
        ADD COLUMN unknown_tool_call_ids_json TEXT
    `.withoutTransform;

    yield* sql`
      CREATE INDEX effect_agent_submissions_joined_host
        ON effect_agent_submissions (joined_host_submission_id)
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_approval_decisions (
        submission_id TEXT NOT NULL,
        tool_call_id TEXT NOT NULL,
        decision TEXT NOT NULL,
        resolver TEXT NOT NULL,
        reason TEXT NOT NULL,
        decided_at TEXT NOT NULL,
        PRIMARY KEY (submission_id, tool_call_id),
        FOREIGN KEY (submission_id)
          REFERENCES effect_agent_submissions(submission_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_unknown_resolutions (
        submission_id TEXT NOT NULL,
        tool_call_id TEXT NOT NULL,
        author TEXT NOT NULL,
        reason TEXT NOT NULL,
        resolution_json TEXT NOT NULL,
        resolved_at TEXT NOT NULL,
        PRIMARY KEY (submission_id, tool_call_id),
        FOREIGN KEY (submission_id)
          REFERENCES effect_agent_submissions(submission_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    yield* sql`PRAGMA user_version = 3`.withoutTransform;
  }),
});
