import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { createMessageDeliveryPendingIndex } from "./SqlMessageDeliveryStore.ts";
import { createNativeReadIndexes } from "./SqlThreadNativeReads.ts";

/**
 * The canonical storage schema. Every SQL adapter stores the same tables, indexes and
 * constraints; only the scalar type names differ, and `SqlClient` already knows which dialect it
 * is connected to. The version marker stays with each adapter, because dialects disagree about
 * where a database records its own format.
 */
export const createStorageSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // A bound parameter cannot name a column type, so the dialect's spelling is inlined.
  const integer = sql.literal(sql.onDialectOrElse({ orElse: () => "INTEGER", pg: () => "BIGINT" }));

  yield* sql`
    CREATE TABLE effect_agent_threads (
      thread_id TEXT PRIMARY KEY NOT NULL,
      created_at TEXT NOT NULL,
      tail_sequence ${integer} NOT NULL,
      tail_digest TEXT NOT NULL,
      producer_epoch ${integer} NOT NULL
    )
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_canonical_batches (
      thread_id TEXT NOT NULL,
      batch_id TEXT NOT NULL,
      first_sequence ${integer} NOT NULL,
      last_sequence ${integer} NOT NULL,
      batch_digest TEXT NOT NULL,
      tail_digest TEXT NOT NULL,
      batch_json TEXT NOT NULL,
      PRIMARY KEY (thread_id, batch_id),
      FOREIGN KEY (thread_id)
        REFERENCES effect_agent_threads(thread_id)
        ON DELETE RESTRICT
    )
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_canonical_records (
      thread_id TEXT NOT NULL,
      sequence ${integer} NOT NULL,
      record_id TEXT NOT NULL,
      batch_id TEXT NOT NULL,
      record_json TEXT NOT NULL,
      PRIMARY KEY (thread_id, sequence),
      UNIQUE (thread_id, record_id),
      FOREIGN KEY (thread_id, batch_id)
        REFERENCES effect_agent_canonical_batches(thread_id, batch_id)
        ON DELETE RESTRICT
    )
  `.withoutTransform;

  yield* sql`
    CREATE INDEX effect_agent_canonical_records_batch
      ON effect_agent_canonical_records (thread_id, batch_id, sequence)
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_checkpoints (
      thread_id TEXT NOT NULL,
      through_sequence ${integer} NOT NULL,
      tail_digest TEXT NOT NULL,
      checkpoint_json TEXT NOT NULL,
      PRIMARY KEY (thread_id, through_sequence),
      FOREIGN KEY (thread_id)
        REFERENCES effect_agent_threads(thread_id)
        ON DELETE RESTRICT
    )
  `.withoutTransform;

  // Admission rows exist before Thread materialization (durability §4), so
  // thread_id intentionally carries no foreign key into effect_agent_threads.
  yield* sql`
    CREATE TABLE effect_agent_submissions (
      submission_id TEXT PRIMARY KEY NOT NULL,
      thread_id TEXT NOT NULL,
      queue_sequence ${integer} NOT NULL,
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
      input_applied_sequence ${integer},
      joined_host_submission_id TEXT,
      suspended_reason_json TEXT,
      suspended_at TEXT,
      unknown_reason TEXT,
      unknown_tool_call_ids_json TEXT,
      parent_submission_id TEXT,
      parent_tool_call_id TEXT,
      admission_group TEXT,
      admission_fence_json TEXT,
      worker_admission_json TEXT,
      message_admission_json TEXT,
      UNIQUE (thread_id, principal, idempotency_key),
      UNIQUE (thread_id, queue_sequence)
    )
  `.withoutTransform;

  yield* sql`
    CREATE INDEX effect_agent_submissions_group
      ON effect_agent_submissions (thread_id, admission_group, state)
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_submission_ownership (
      submission_id TEXT PRIMARY KEY NOT NULL,
      attempt_id TEXT NOT NULL,
      ownership_token TEXT NOT NULL,
      producer_epoch ${integer} NOT NULL,
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
      thread_id TEXT NOT NULL,
      owner_producer_id TEXT NOT NULL,
      producer_epoch ${integer} NOT NULL,
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

  // Durable attached children (spec §12, SUB-004): a child Submission records its immutable
  // parent linkage at admission; the parent-side index serves the recovery attachment view.
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

  // record_json is authoritative. The remaining columns support owner keyset paging and
  // deadline queries without decoding unrelated future schedules.
  yield* sql`
    CREATE TABLE effect_agent_schedules (
      tenant_id TEXT NOT NULL,
      owner_id TEXT NOT NULL,
      schedule_id TEXT NOT NULL,
      deadline_at_millis ${integer},
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

  yield* sql`
    CREATE TABLE effect_agent_subscription_sequences (
      tenant_id TEXT NOT NULL,
      source_address TEXT NOT NULL,
      sequence ${integer} NOT NULL,
      event_scan_cursor TEXT NOT NULL,
      delivery_scan_cursor TEXT NOT NULL,
      recovery_scan_cursor ${integer} NOT NULL,
      PRIMARY KEY (tenant_id, source_address)
    )
  `.withoutTransform;
  yield* sql`
    CREATE TABLE effect_agent_subscriptions (
      tenant_id TEXT NOT NULL,
      source_address TEXT NOT NULL,
      owner_id TEXT NOT NULL,
      subscription_id TEXT NOT NULL,
      ordinal ${integer} NOT NULL,
      source_name TEXT NOT NULL,
      source_version TEXT NOT NULL,
      matching_key TEXT NOT NULL,
      state TEXT NOT NULL,
      expires_at_millis ${integer},
      recovery_at_millis ${integer},
      recovery_present ${integer} NOT NULL DEFAULT 0,
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
      cutoff ${integer} NOT NULL,
      cursor ${integer} NOT NULL,
      routing_complete ${integer} NOT NULL,
      tombstone ${integer} NOT NULL DEFAULT 0,
      next_attempt_at_millis ${integer} NOT NULL,
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
      next_attempt_at_millis ${integer} NOT NULL,
      record_json TEXT NOT NULL,
      PRIMARY KEY (tenant_id, source_address, owner_id, subscription_id, event_id),
      UNIQUE (tenant_id, source_address, delivery_key)
    )
  `.withoutTransform;
  yield* sql`CREATE INDEX effect_agent_subscription_deliveries_pending ON effect_agent_subscription_deliveries (tenant_id, source_address, state, next_attempt_at_millis, delivery_key)`
    .withoutTransform;
  yield* sql`CREATE INDEX effect_agent_subscription_deliveries_registration ON effect_agent_subscription_deliveries (tenant_id, source_address, owner_id, subscription_id, delivery_key)`
    .withoutTransform;
  yield* sql`
    CREATE TABLE effect_agent_message_deliveries (
      owner_thread_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      version ${integer} NOT NULL,
      state TEXT NOT NULL,
      deadline_at_millis ${integer},
      record_json TEXT NOT NULL,
      PRIMARY KEY (owner_thread_id, message_id)
    )
  `.withoutTransform;
  yield* sql`
    CREATE INDEX effect_agent_message_deliveries_due
    ON effect_agent_message_deliveries (deadline_at_millis, owner_thread_id, message_id)
    WHERE deadline_at_millis IS NOT NULL
  `.withoutTransform;
  yield* sql`
    CREATE TABLE effect_agent_recovery_checkpoints (
      thread_id TEXT PRIMARY KEY NOT NULL,
      through_sequence ${integer} NOT NULL,
      tail_digest TEXT NOT NULL,
      checkpoint_json TEXT NOT NULL,
      FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT
    )
  `.withoutTransform;
  yield* sql`CREATE INDEX effect_agent_submissions_nonterminal ON effect_agent_submissions (thread_id, queue_sequence) WHERE state <> 'settled'`
    .withoutTransform;
  yield* createNativeReadIndexes;
  yield* createMessageDeliveryPendingIndex;
});
