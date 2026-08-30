import { SqliteMigrator } from "@effect/sql-sqlite-node";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export const CurrentSqliteStorageVersion = 6;

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
  "4_durable_subagents": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    // Durable attached children (spec §12, SUB-004): a child Submission records its immutable
    // parent linkage at admission; the parent-side index serves the recovery attachment view.
    yield* sql`
      ALTER TABLE effect_agent_submissions
        ADD COLUMN parent_submission_id TEXT
    `.withoutTransform;
    yield* sql`
      ALTER TABLE effect_agent_submissions
        ADD COLUMN parent_tool_call_id TEXT
    `.withoutTransform;
    yield* sql`
      CREATE INDEX effect_agent_submissions_parent
        ON effect_agent_submissions (parent_submission_id)
    `.withoutTransform;

    // Parent-owned child budget reservations (spec §12 steps 2 and 6, SUB-010): generic
    // opaque-payload state-machine rows (D8) — allocation and accounting are Schema-encoded
    // JSON documents the adapter never interprets; status moves
    // reserved → releasePending → released, applied exactly once.
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

    yield* sql`PRAGMA user_version = 4`.withoutTransform;
  }),
  "5_durable_schedules": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    // record_json is authoritative. The remaining columns support owner keyset paging and
    // deadline queries without decoding unrelated future schedules.
    yield* sql`
      CREATE TABLE effect_agent_schedules (
        tenant_id TEXT NOT NULL,
        owner_id TEXT NOT NULL,
        schedule_id TEXT NOT NULL,
        deadline_at_millis INTEGER,
        record_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, owner_id, schedule_id)
      )
    `.withoutTransform;

    yield* sql`
      CREATE INDEX effect_agent_schedules_deadline
        ON effect_agent_schedules (deadline_at_millis, tenant_id, owner_id, schedule_id)
        WHERE deadline_at_millis IS NOT NULL
    `.withoutTransform;

    yield* sql`
      CREATE INDEX effect_agent_schedules_owner_deadline
        ON effect_agent_schedules (tenant_id, owner_id, deadline_at_millis, schedule_id)
        WHERE deadline_at_millis IS NOT NULL
    `.withoutTransform;

    yield* sql`PRAGMA user_version = 5`.withoutTransform;
  }),
  "6_durable_subscriptions": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      CREATE TABLE effect_agent_subscription_sequences (
        tenant_id TEXT NOT NULL,
        source_address TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        event_scan_cursor TEXT NOT NULL,
        delivery_scan_cursor TEXT NOT NULL,
        recovery_scan_cursor INTEGER NOT NULL,
        PRIMARY KEY (tenant_id, source_address)
      )
    `.withoutTransform;
    yield* sql`
      CREATE TABLE effect_agent_subscriptions (
        tenant_id TEXT NOT NULL,
        source_address TEXT NOT NULL,
        owner_id TEXT NOT NULL,
        subscription_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        source_name TEXT NOT NULL,
        source_version TEXT NOT NULL,
        matching_key TEXT NOT NULL,
        state TEXT NOT NULL,
        expires_at_millis INTEGER NOT NULL,
        recovery_at_millis INTEGER,
        record_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id),
        UNIQUE (tenant_id, source_address, ordinal)
      )
    `.withoutTransform;
    yield* sql`CREATE INDEX effect_agent_subscriptions_owner ON effect_agent_subscriptions (tenant_id, source_address, owner_id, ordinal)`
      .withoutTransform;
    yield* sql`CREATE INDEX effect_agent_subscriptions_candidates ON effect_agent_subscriptions (tenant_id, source_address, source_name, source_version, matching_key, ordinal)`
      .withoutTransform;
    yield* sql`CREATE INDEX effect_agent_subscriptions_recovery ON effect_agent_subscriptions (tenant_id, source_address, recovery_at_millis, ordinal) WHERE recovery_at_millis IS NOT NULL`
      .withoutTransform;
    yield* sql`
      CREATE TABLE effect_agent_subscription_events (
        tenant_id TEXT NOT NULL,
        source_address TEXT NOT NULL,
        event_id TEXT NOT NULL,
        source_name TEXT NOT NULL,
        source_version TEXT NOT NULL,
        matching_key TEXT NOT NULL,
        payload_digest TEXT NOT NULL,
        cutoff INTEGER NOT NULL,
        cursor INTEGER NOT NULL,
        routing_complete INTEGER NOT NULL,
        next_attempt_at_millis INTEGER NOT NULL,
        record_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, source_address, event_id)
      )
    `.withoutTransform;
    yield* sql`CREATE INDEX effect_agent_subscription_events_pending ON effect_agent_subscription_events (tenant_id, source_address, routing_complete, next_attempt_at_millis, event_id)`
      .withoutTransform;
    yield* sql`
      CREATE TABLE effect_agent_subscription_deliveries (
        tenant_id TEXT NOT NULL,
        source_address TEXT NOT NULL,
        owner_id TEXT NOT NULL,
        subscription_id TEXT NOT NULL,
        event_id TEXT NOT NULL,
        delivery_key TEXT NOT NULL,
        state TEXT NOT NULL,
        next_attempt_at_millis INTEGER NOT NULL,
        record_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id, event_id),
        UNIQUE (tenant_id, source_address, delivery_key)
      )
    `.withoutTransform;
    yield* sql`CREATE INDEX effect_agent_subscription_deliveries_pending ON effect_agent_subscription_deliveries (tenant_id, source_address, state, next_attempt_at_millis, delivery_key)`
      .withoutTransform;
    yield* sql`CREATE INDEX effect_agent_subscription_deliveries_registration ON effect_agent_subscription_deliveries (tenant_id, source_address, owner_id, subscription_id, delivery_key)`
      .withoutTransform;
    yield* sql`PRAGMA user_version = 6`.withoutTransform;
  }),
});
