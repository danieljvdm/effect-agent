import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  createMessageDeliveryPendingIndex,
  createWorkerControlIndexes,
} from "./SqlMessageDeliveryStore.ts";
import { createNativeReadIndexes } from "./SqlThreadNativeReads.ts";

/**
 * Current logical storage schema shared by SQL adapters. Native scalar types and query-only
 * metadata follow the SqlClient dialect; format markers and supported upgrades stay adapter-owned.
 */
export const createStorageSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // A bound parameter cannot name a column type, so the dialect's spelling is inlined.
  const integer = sql.literal(sql.onDialectOrElse({ orElse: () => "INTEGER", pg: () => "BIGINT" }));

  const text = sql.literal(
    sql.onDialectOrElse({ orElse: () => "TEXT", pg: () => 'TEXT COLLATE "C"' }),
  );

  yield* sql`
    CREATE TABLE effect_agent_threads (
      thread_id ${text} PRIMARY KEY NOT NULL,
      created_at ${text} NOT NULL,
      tail_sequence ${integer} NOT NULL,
      tail_digest ${text} NOT NULL,
      producer_epoch ${integer} NOT NULL
    )
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_canonical_batches (
      thread_id ${text} NOT NULL,
      batch_id ${text} NOT NULL,
      first_sequence ${integer} NOT NULL,
      last_sequence ${integer} NOT NULL,
      batch_digest ${text} NOT NULL,
      tail_digest ${text} NOT NULL,
      batch_json ${text} NOT NULL,
      PRIMARY KEY (thread_id, batch_id),
      FOREIGN KEY (thread_id)
        REFERENCES effect_agent_threads(thread_id)
        ON DELETE RESTRICT
    )
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_canonical_records (
      thread_id ${text} NOT NULL,
      sequence ${integer} NOT NULL,
      record_id ${text} NOT NULL,
      batch_id ${text} NOT NULL,
      record_json ${text} NOT NULL,
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
      thread_id ${text} NOT NULL,
      through_sequence ${integer} NOT NULL,
      tail_digest ${text} NOT NULL,
      checkpoint_json ${text} NOT NULL,
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
      submission_id ${text} PRIMARY KEY NOT NULL,
      thread_id ${text} NOT NULL,
      queue_sequence ${integer} NOT NULL,
      principal ${text} NOT NULL,
      idempotency_key ${text} NOT NULL,
      agent_id ${text} NOT NULL,
      agent_digests_json ${text} NOT NULL,
      deployment_id ${text} NOT NULL,
      input_json ${text} NOT NULL,
      input_digest ${text} NOT NULL,
      receipt_id ${text} NOT NULL,
      state ${text} NOT NULL,
      settled_outcome ${text},
      created_at ${text} NOT NULL,
      ready_at ${text},
      input_applied_record_id ${text},
      input_applied_sequence ${integer},
      joined_host_submission_id ${text},
      suspended_reason_json ${text},
      suspended_at ${text},
      unknown_reason ${text},
      unknown_tool_call_ids_json ${text},
      parent_submission_id ${text},
      parent_tool_call_id ${text},
      admission_group ${text},
      admission_fence_json ${text},
      worker_admission_json ${text},
      message_admission_json ${text},
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
      submission_id ${text} PRIMARY KEY NOT NULL,
      attempt_id ${text} NOT NULL,
      ownership_token ${text} NOT NULL,
      producer_epoch ${integer} NOT NULL,
      owner_producer_id ${text} NOT NULL,
      lease_expires_at ${text} NOT NULL,
      FOREIGN KEY (submission_id)
        REFERENCES effect_agent_submissions(submission_id)
        ON DELETE RESTRICT
    )
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_attempts (
      attempt_id ${text} PRIMARY KEY NOT NULL,
      submission_id ${text} NOT NULL,
      thread_id ${text} NOT NULL,
      owner_producer_id ${text} NOT NULL,
      producer_epoch ${integer} NOT NULL,
      claimed_at ${text} NOT NULL,
      FOREIGN KEY (submission_id)
        REFERENCES effect_agent_submissions(submission_id)
        ON DELETE RESTRICT
    )
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_settlement_reservations (
      submission_id ${text} PRIMARY KEY NOT NULL,
      settlement_id ${text} NOT NULL,
      outcome ${text} NOT NULL,
      record_id ${text} NOT NULL,
      record_json ${text} NOT NULL,
      record_digest ${text} NOT NULL,
      reserved_at ${text} NOT NULL,
      finalized_at ${text},
      FOREIGN KEY (submission_id)
        REFERENCES effect_agent_submissions(submission_id)
        ON DELETE RESTRICT
    )
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_abort_intents (
      submission_id ${text} PRIMARY KEY NOT NULL,
      author ${text} NOT NULL,
      reason ${text} NOT NULL,
      requested_at ${text} NOT NULL,
      canonical_record_id ${text},
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
      submission_id ${text} NOT NULL,
      tool_call_id ${text} NOT NULL,
      decision ${text} NOT NULL,
      resolver ${text} NOT NULL,
      reason ${text} NOT NULL,
      decided_at ${text} NOT NULL,
      PRIMARY KEY (submission_id, tool_call_id),
      FOREIGN KEY (submission_id)
        REFERENCES effect_agent_submissions(submission_id)
        ON DELETE RESTRICT
    )
  `.withoutTransform;

  yield* sql`
    CREATE TABLE effect_agent_unknown_resolutions (
      submission_id ${text} NOT NULL,
      tool_call_id ${text} NOT NULL,
      author ${text} NOT NULL,
      reason ${text} NOT NULL,
      resolution_json ${text} NOT NULL,
      resolved_at ${text} NOT NULL,
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
      reservation_id ${text} PRIMARY KEY NOT NULL,
      parent_submission_id ${text} NOT NULL,
      parent_tool_call_id ${text} NOT NULL,
      child_submission_id ${text},
      status ${text} NOT NULL,
      allocation_json ${text} NOT NULL,
      allocation_digest ${text} NOT NULL,
      accounting_json ${text},
      reserved_at ${text} NOT NULL,
      release_began_at ${text},
      released_at ${text},
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
      tenant_id ${text} NOT NULL,
      owner_id ${text} NOT NULL,
      schedule_id ${text} NOT NULL,
      deadline_at_millis ${integer},
      record_json ${text} NOT NULL${sql.onDialectOrElse({ pg: () => sql`, uses_capacity BOOLEAN NOT NULL`, orElse: () => sql`` })},
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
      tenant_id ${text} NOT NULL,
      source_address ${text} NOT NULL,
      sequence ${integer} NOT NULL,
      event_scan_cursor ${text} NOT NULL,
      delivery_scan_cursor ${text} NOT NULL,
      recovery_scan_cursor ${integer} NOT NULL,
      PRIMARY KEY (tenant_id, source_address)
    )
  `.withoutTransform;
  yield* sql`
    CREATE TABLE effect_agent_subscriptions (
      tenant_id ${text} NOT NULL,
      source_address ${text} NOT NULL,
      owner_id ${text} NOT NULL,
      subscription_id ${text} NOT NULL,
      ordinal ${integer} NOT NULL,
      source_name ${text} NOT NULL,
      source_version ${text} NOT NULL,
      matching_key ${text} NOT NULL,
      state ${text} NOT NULL,
      expires_at_millis ${integer},
      recovery_at_millis ${integer},
      recovery_present ${integer} NOT NULL DEFAULT 0,
      record_json ${text} NOT NULL,
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
      tenant_id ${text} NOT NULL,
      source_address ${text} NOT NULL,
      event_id ${text} NOT NULL,
      source_name ${text} NOT NULL,
      source_version ${text} NOT NULL,
      matching_key ${text} NOT NULL,
      payload_digest ${text} NOT NULL,
      cutoff ${integer} NOT NULL,
      cursor ${integer} NOT NULL,
      routing_complete ${integer} NOT NULL,
      tombstone ${integer} NOT NULL DEFAULT 0,
      next_attempt_at_millis ${integer} NOT NULL,
      record_json ${text} NOT NULL,
      PRIMARY KEY (tenant_id, source_address, event_id)
    )
  `.withoutTransform;
  yield* sql`CREATE INDEX effect_agent_subscription_events_pending ON effect_agent_subscription_events (tenant_id, source_address, routing_complete, next_attempt_at_millis, event_id)`
    .withoutTransform;
  yield* sql`
    CREATE TABLE effect_agent_subscription_deliveries (
      tenant_id ${text} NOT NULL,
      source_address ${text} NOT NULL,
      owner_id ${text} NOT NULL,
      subscription_id ${text} NOT NULL,
      event_id ${text} NOT NULL,
      delivery_key ${text} NOT NULL,
      state ${text} NOT NULL,
      next_attempt_at_millis ${integer} NOT NULL,
      record_json ${text} NOT NULL${sql.onDialectOrElse({ pg: () => sql`, retry_parked BOOLEAN NOT NULL, observe_settlement BOOLEAN NOT NULL`, orElse: () => sql`` })},
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
      owner_thread_id ${text} NOT NULL,
      message_id ${text} NOT NULL,
      version ${integer} NOT NULL,
      state ${text} NOT NULL,
      deadline_at_millis ${integer},
      record_json ${text} NOT NULL${sql.onDialectOrElse({ pg: () => sql`, read_metadata JSONB NOT NULL`, orElse: () => sql`` })},
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
      thread_id ${text} PRIMARY KEY NOT NULL,
      through_sequence ${integer} NOT NULL,
      tail_digest ${text} NOT NULL,
      checkpoint_json ${text} NOT NULL,
      FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT
    )
  `.withoutTransform;
  yield* sql`CREATE INDEX effect_agent_submissions_nonterminal ON effect_agent_submissions (thread_id, queue_sequence) WHERE state <> 'settled'`
    .withoutTransform;
  yield* sql.onDialectOrElse({
    pg: () =>
      Effect.gen(function* () {
        yield* sql`CREATE TABLE effect_agent_worker_stops (thread_id ${text} PRIMARY KEY NOT NULL, terminal ${text})`;
        yield* createWorkerControlIndexes;
      }),
    orElse: () => Effect.void,
  });
  yield* createNativeReadIndexes;
  yield* createMessageDeliveryPendingIndex;
});
