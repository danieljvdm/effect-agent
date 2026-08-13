import { SqliteMigrator } from "@effect/sql-sqlite-do";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * The exact-or-fresh storage version recorded in `effect_agent_meta`. Cloudflare is a fresh
 * platform, so there is exactly ONE migration carrying the complete current schema — no
 * v1→v4 history to replay (deployment spec §9: no rolling data-version promise during
 * private development).
 */
export const CurrentDoStorageVersion = 1;

/**
 * The Conversation Durable Object schema. Table names and columns mirror the Node/SQLite v4
 * schema byte-for-byte (`packages/storage-sqlite/src/migrations.ts`, migrations 1–4 collapsed
 * into their final shape) so the shared conformance suites and crash-matrix rows address
 * identical durable state. Two DC-specific additions:
 *
 * 1. `effect_agent_meta` replaces `PRAGMA user_version` as the exact-or-fresh version gate —
 *    a meta table is portable regardless of which PRAGMAs Durable Object SQL storage allows.
 * 2. `effect_agent_child_settlements` is the durable cross-store notification marker the
 *    SubmissionLedger port contract mandates for cross-store adapters (`suspend`'s covering
 *    check and `recordChildSettled`'s wake both consult it): parent and child Conversations
 *    live in different Durable Objects, so a child settlement reported before the parent's
 *    suspend commits must be observable from the PARENT's own storage.
 */
export const doMigrations = SqliteMigrator.fromRecord({
  "1_current_cloudflare_conversation_object": Effect.gen(function* () {
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
        joined_host_submission_id TEXT,
        suspended_reason_json TEXT,
        suspended_at TEXT,
        unknown_reason TEXT,
        unknown_tool_call_ids_json TEXT,
        parent_submission_id TEXT,
        parent_tool_call_id TEXT,
        UNIQUE (conversation_id, principal, idempotency_key),
        UNIQUE (conversation_id, queue_sequence)
      )
    `.withoutTransform;

    yield* sql`
      CREATE INDEX effect_agent_submissions_joined_host
        ON effect_agent_submissions (joined_host_submission_id)
    `.withoutTransform;

    yield* sql`
      CREATE INDEX effect_agent_submissions_parent
        ON effect_agent_submissions (parent_submission_id)
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

    yield* sql`
      CREATE TABLE effect_agent_child_reservations (
        reservation_id TEXT PRIMARY KEY NOT NULL,
        parent_submission_id TEXT NOT NULL,
        parent_tool_call_id TEXT NOT NULL,
        child_submission_id TEXT,
        status TEXT NOT NULL,
        allocation_json TEXT NOT NULL,
        allocation_digest TEXT NOT NULL,
        accounting_json TEXT,
        reserved_at TEXT NOT NULL,
        release_began_at TEXT,
        released_at TEXT,
        UNIQUE (parent_submission_id, parent_tool_call_id),
        FOREIGN KEY (parent_submission_id)
          REFERENCES effect_agent_submissions(submission_id)
          ON DELETE RESTRICT
      )
    `.withoutTransform;

    // Durable cross-store child-settlement notification marker (parent-side; the child's row
    // lives in ANOTHER Durable Object). child_outcome is nullable: the notification command
    // carries identities only, and the child's canonical Settlement stays the outcome
    // authority (DUR-015). No foreign keys: the parent row is checked by the operation, and
    // the child row is intentionally foreign.
    yield* sql`
      CREATE TABLE effect_agent_child_settlements (
        parent_submission_id TEXT NOT NULL,
        child_submission_id TEXT NOT NULL,
        child_outcome TEXT,
        recorded_at TEXT NOT NULL,
        PRIMARY KEY (parent_submission_id, child_submission_id)
      )
    `.withoutTransform;

    yield* sql`
      CREATE TABLE effect_agent_meta (
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT NOT NULL
      )
    `.withoutTransform;

    yield* sql`
      INSERT INTO effect_agent_meta (key, value)
      VALUES ('storage_version', ${String(CurrentDoStorageVersion)})
    `.withoutTransform;
  }),
});
