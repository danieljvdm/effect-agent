import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import { CanonicalSequence, ProducerEpoch } from "@effect-agent/thread/Records";
import { ScheduleFailpoint, ScheduleFailpointError } from "@effect-agent/thread/Schedule";
import {
  checkV2ThreadLayout,
  upgradeV2Schedules,
  upgradeV2Subscriptions,
} from "@effect-agent/thread/SqlStorageV2Upgrade";
import {
  SubscriptionFailpoint,
  SubscriptionFailpointError,
} from "@effect-agent/thread/Subscription";
import {
  MAX_THREAD_EXPORT_RECORDS,
  CheckpointRejected,
  FenceRejected,
  ThreadNotMaterialized,
  type SaveRecoveryCheckpointRequest,
} from "@effect-agent/thread/ThreadStore";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteMigrator } from "@effect/sql-sqlite-node";
import { Effect, Exit, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { SqliteStorageConfig } from "../SqliteStorageConfig.ts";
import type { SqliteStorageFailpointError } from "../SqliteStorageError.ts";
import {
  SqliteAppendConflict,
  SqliteCheckpointConflict,
  SqliteFenceRejected,
  SqliteStorageCompatibilityError,
  SqliteStorageFailpointLocation,
  SqliteStorageCorruptionError,
  SqliteStorageError,
  SqliteWriteContention,
} from "../SqliteStorageError.ts";
import { SqliteStorageFailpoint } from "../SqliteStorageFailpoint.ts";
import { createMessageDeliveryTables } from "./message-delivery-schema.ts";
import {
  CurrentSqliteStorageVersion,
  createNonterminalIndex,
  sqliteMigrations,
} from "./migrations.ts";
import { createRecoveryCheckpointTable } from "./recovery-checkpoint-schema.ts";

const BoundedStoredText = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const MAX_RECORDS_PER_THREAD = MAX_THREAD_EXPORT_RECORDS;
const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
const MAX_STORED_TEXT_BYTES = 16 * 1024 * 1024;
const MAX_IDENTIFIER_LENGTH = 1_024;

const storedTextBytes = (value: string): number => new TextEncoder().encode(value).byteLength;

class SqliteVersionRow extends Schema.Class<SqliteVersionRow>("SqliteVersionRow")({
  user_version: NonNegativeInt,
}) {}

class SqliteJournalModeRow extends Schema.Class<SqliteJournalModeRow>("SqliteJournalModeRow")({
  journal_mode: Schema.NonEmptyString.check(Schema.isMaxLength(32)),
}) {}

class SqliteNameRow extends Schema.Class<SqliteNameRow>("SqliteNameRow")({
  name: BoundedIdentifier,
}) {}

class ThreadRow extends Schema.Class<ThreadRow>("ThreadRow")({
  thread_id: BoundedIdentifier,
  created_at: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  producer_epoch: ProducerEpoch,
  tail_digest: BoundedStoredText,
  tail_sequence: CanonicalSequence,
}) {}

class BatchRow extends Schema.Class<BatchRow>("BatchRow")({
  batch_digest: BoundedStoredText,
  batch_id: BoundedIdentifier,
  batch_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  first_sequence: CanonicalSequence,
  last_sequence: CanonicalSequence,
  tail_digest: BoundedStoredText,
}) {}

class RecordRow extends Schema.Class<RecordRow>("RecordRow")({
  batch_id: BoundedIdentifier,
  thread_id: BoundedIdentifier,
  record_id: BoundedIdentifier,
  record_json: BoundedStoredText,
  sequence: CanonicalSequence,
}) {}

class CheckpointRow extends Schema.Class<CheckpointRow>("CheckpointRow")({
  checkpoint_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  tail_digest: BoundedStoredText,
  through_sequence: CanonicalSequence,
}) {}

export class RawRecord extends Schema.Class<RawRecord>("@effect-agent/storage-sqlite/RawRecord")({
  recordId: BoundedIdentifier,
  recordJson: BoundedStoredText,
}) {}

export class RawAppendRequest extends Schema.Class<RawAppendRequest>(
  "@effect-agent/storage-sqlite/RawAppendRequest",
)({
  batchDigest: BoundedStoredText,
  batchId: BoundedIdentifier,
  batchJson: BoundedStoredText,
  threadId: BoundedIdentifier,
  expectedTailDigest: BoundedStoredText,
  expectedTailSequence: CanonicalSequence,
  producerEpoch: ProducerEpoch,
  records: Schema.NonEmptyArray(RawRecord).check(Schema.isMaxLength(256)),
  tailDigest: BoundedStoredText,
}) {}

export class RawAppendResult extends Schema.Class<RawAppendResult>(
  "@effect-agent/storage-sqlite/RawAppendResult",
)({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  replayed: Schema.Boolean,
  tailDigest: BoundedStoredText,
}) {}

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-sqlite/RawReadRequest",
)({
  threadId: BoundedIdentifier,
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class RawCheckpoint extends Schema.Class<RawCheckpoint>(
  "@effect-agent/storage-sqlite/RawCheckpoint",
)({
  checkpointJson: BoundedStoredText,
  threadId: BoundedIdentifier,
  tailDigest: BoundedStoredText,
  throughSequence: CanonicalSequence,
}) {}

export class RawThreadExport extends Schema.Class<RawThreadExport>(
  "@effect-agent/storage-sqlite/RawThreadExport",
)({
  thread: ThreadRow,
  records: Schema.Array(RecordRow),
}) {}

type AppendError =
  | SqliteAppendConflict
  | SqliteFenceRejected
  | SqliteStorageCorruptionError
  | SqliteStorageError
  | SqliteStorageFailpointError
  | SqliteWriteContention;

type CheckpointError =
  | SqliteCheckpointConflict
  | SqliteStorageCorruptionError
  | SqliteStorageError
  | SqliteWriteContention;

const storageError =
  (operation: string) =>
  (error: SqlError): SqliteStorageError =>
    SqliteStorageError.make({
      cause: error,
      operation,
      message: error.message,
    });

/** Decode raw SQLite rows against a Schema, reporting failures as typed corruption. */
export const decodeRows = Effect.fn("SqliteJournal.decodeRows")(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<ReadonlyArray<A>, SqliteStorageCorruptionError> =>
    Schema.decodeUnknownEffect(schema)(rows).pipe(
      Effect.mapError((error) =>
        SqliteStorageCorruptionError.make({
          table,
          rowKey,
          message: String(error),
        }),
      ),
    ),
);

/** Decode exactly one raw SQLite row against a Schema, reporting failures as typed corruption. */
export const decodeSingleRow = Effect.fn("SqliteJournal.decodeSingleRow")(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<A, SqliteStorageCorruptionError> =>
    decodeRows(schema, table, rowKey, rows).pipe(
      Effect.flatMap((decoded) =>
        decoded.length === 1
          ? Effect.succeed(decoded[0])
          : Effect.fail(
              SqliteStorageCorruptionError.make({
                table,
                rowKey,
                message: `Expected exactly one row but found ${decoded.length}.`,
              }),
            ),
      ),
    ),
);

/** Column inventory of the supported v8 predecessor, independent of physical column order. */
const predecessorColumns = {
  effect_agent_threads: [
    "thread_id",
    "created_at",
    "tail_sequence",
    "tail_digest",
    "producer_epoch",
  ],
  effect_agent_canonical_batches: [
    "thread_id",
    "batch_id",
    "first_sequence",
    "last_sequence",
    "batch_digest",
    "tail_digest",
    "batch_json",
  ],
  effect_agent_canonical_records: ["thread_id", "sequence", "record_id", "batch_id", "record_json"],
  effect_agent_checkpoints: ["thread_id", "through_sequence", "tail_digest", "checkpoint_json"],
  effect_agent_submissions: [
    "submission_id",
    "thread_id",
    "queue_sequence",
    "principal",
    "idempotency_key",
    "agent_id",
    "agent_digests_json",
    "deployment_id",
    "input_json",
    "input_digest",
    "receipt_id",
    "state",
    "settled_outcome",
    "created_at",
    "ready_at",
    "input_applied_record_id",
    "input_applied_sequence",
    "joined_host_submission_id",
    "suspended_reason_json",
    "suspended_at",
    "unknown_reason",
    "unknown_tool_call_ids_json",
    "parent_submission_id",
    "parent_tool_call_id",
    "admission_group",
    "admission_fence_json",
  ],
  effect_agent_submission_ownership: [
    "submission_id",
    "attempt_id",
    "ownership_token",
    "producer_epoch",
    "owner_producer_id",
    "lease_expires_at",
  ],
  effect_agent_attempts: [
    "attempt_id",
    "submission_id",
    "thread_id",
    "owner_producer_id",
    "producer_epoch",
    "claimed_at",
  ],
  effect_agent_settlement_reservations: [
    "submission_id",
    "settlement_id",
    "outcome",
    "record_id",
    "record_json",
    "record_digest",
    "reserved_at",
    "finalized_at",
  ],
  effect_agent_abort_intents: [
    "submission_id",
    "author",
    "reason",
    "requested_at",
    "canonical_record_id",
  ],
  effect_agent_approval_decisions: [
    "submission_id",
    "tool_call_id",
    "decision",
    "resolver",
    "reason",
    "decided_at",
  ],
  effect_agent_unknown_resolutions: [
    "submission_id",
    "tool_call_id",
    "author",
    "reason",
    "resolution_json",
    "resolved_at",
  ],
  effect_agent_child_reservations: [
    "reservation_id",
    "parent_submission_id",
    "parent_tool_call_id",
    "child_submission_id",
    "status",
    "allocation_json",
    "allocation_digest",
    "accounting_json",
    "reserved_at",
    "release_began_at",
    "released_at",
  ],
  effect_agent_schedules: [
    "tenant_id",
    "owner_id",
    "schedule_id",
    "deadline_at_millis",
    "record_json",
  ],
  effect_agent_subscription_sequences: [
    "tenant_id",
    "source_address",
    "sequence",
    "event_scan_cursor",
    "delivery_scan_cursor",
    "recovery_scan_cursor",
  ],
  effect_agent_subscriptions: [
    "tenant_id",
    "source_address",
    "owner_id",
    "subscription_id",
    "ordinal",
    "source_name",
    "source_version",
    "matching_key",
    "state",
    "expires_at_millis",
    "recovery_at_millis",
    "recovery_present",
    "record_json",
  ],
  effect_agent_subscription_events: [
    "tenant_id",
    "source_address",
    "event_id",
    "source_name",
    "source_version",
    "matching_key",
    "payload_digest",
    "cutoff",
    "cursor",
    "routing_complete",
    "next_attempt_at_millis",
    "record_json",
    "tombstone",
  ],
  effect_agent_subscription_deliveries: [
    "tenant_id",
    "source_address",
    "owner_id",
    "subscription_id",
    "event_id",
    "delivery_key",
    "state",
    "next_attempt_at_millis",
    "record_json",
  ],
} as const;

const checkPredecessorLayout = Effect.fn("SqliteJournal.checkPredecessorLayout")(function* (
  version: 8 | 9 | 10,
) {
  const sql = yield* SqlClient.SqlClient;

  const messageColumns =
    version === 8
      ? predecessorColumns
      : {
          ...predecessorColumns,
          effect_agent_submissions: [
            ...predecessorColumns.effect_agent_submissions,
            "worker_admission_json",
            "message_admission_json",
          ],
          effect_agent_message_deliveries: [
            "owner_thread_id",
            "message_id",
            "version",
            "state",
            "deadline_at_millis",
            "record_json",
          ],
        };

  const expectedColumns = {
    ...messageColumns,
    ...(version === 10
      ? {
          effect_agent_recovery_checkpoints: [
            "thread_id",
            "through_sequence",
            "tail_digest",
            "checkpoint_json",
          ],
        }
      : {}),
  };

  for (const [table, expected] of Object.entries(expectedColumns)) {
    const columns = yield* decodeRows(
      Schema.Array(Schema.Struct({ name: BoundedIdentifier })),
      table,
      "schema",
      yield* sql.unsafe(`PRAGMA table_info(${table})`),
    );

    const names = new Set<string>(expected);

    if (columns.length !== names.size || columns.some((column) => !names.has(column.name)))
      return yield* SqliteStorageCompatibilityError.make({
        actualVersion: version,
        supportedVersion: CurrentSqliteStorageVersion,
        message: `The v${version} ${table} columns do not match the supported predecessor; no upgrade was committed.`,
      });
  }
});

export const initializeSqliteJournal = Effect.fn("SqliteJournal.initialize")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const { hit: failpoint } = yield* SqliteStorageFailpoint;
  const { busyTimeout } = yield* SqliteStorageConfig;

  yield* sql`PRAGMA foreign_keys = ON`.pipe(Effect.mapError(storageError("enable foreign keys")));
  // PRAGMA statements do not accept bound parameters; the value is a schema-validated
  // non-negative integer, never caller-controlled text.
  yield* sql
    .unsafe(`PRAGMA busy_timeout = ${busyTimeout}`)
    .pipe(Effect.mapError(storageError("configure busy timeout")));

  const journalModeRows = yield* sql<Record<string, unknown>>`PRAGMA journal_mode`.pipe(
    Effect.mapError(storageError("read journal mode")),
  );

  const journalMode = yield* decodeSingleRow(
    Schema.Array(SqliteJournalModeRow),
    "pragma_journal_mode",
    "singleton",
    journalModeRows,
  );

  if (journalMode.journal_mode.toLowerCase() !== "wal") {
    return yield* SqliteStorageCompatibilityError.make({
      actualVersion: 0,
      supportedVersion: CurrentSqliteStorageVersion,
      message: `SQLite WAL mode is required; the database reported ${journalMode.journal_mode}.`,
    });
  }

  const versionRows = yield* sql<Record<string, unknown>>`PRAGMA user_version`.pipe(
    Effect.mapError(storageError("read storage version")),
  );

  const version = yield* decodeSingleRow(
    Schema.Array(SqliteVersionRow),
    "pragma_user_version",
    "singleton",
    versionRows,
  );

  // Support the known beta49/beta50 and immediate predecessor formats atomically.
  if (
    version.user_version !== 0 &&
    version.user_version !== 7 &&
    version.user_version !== 8 &&
    version.user_version !== 9 &&
    version.user_version !== 10 &&
    version.user_version !== CurrentSqliteStorageVersion
  ) {
    return yield* SqliteStorageCompatibilityError.make({
      actualVersion: version.user_version,
      supportedVersion: CurrentSqliteStorageVersion,
      message:
        `The SQLite file uses unsupported storage version ${version.user_version}; ` +
        `this build supports exactly version ${CurrentSqliteStorageVersion}. ` +
        "Only supported v7, v8, v9 and v10 can be upgraded automatically. Keep the original file and use a compatible library version.",
    });
  }

  if (
    version.user_version === 7 ||
    version.user_version === 8 ||
    version.user_version === 9 ||
    version.user_version === 10
  ) {
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const current = yield* sql<{ user_version: number }>`PRAGMA user_version`;

          if (current.length === 1 && current[0].user_version === CurrentSqliteStorageVersion)
            return;
          if (
            current.length !== 1 ||
            (current[0].user_version !== 7 &&
              current[0].user_version !== 8 &&
              current[0].user_version !== 9 &&
              current[0].user_version !== 10)
          )
            return yield* SqliteStorageCompatibilityError.make({
              actualVersion: -1,
              supportedVersion: CurrentSqliteStorageVersion,
              message: "Storage version changed while acquiring the upgrade transaction.",
            });

          const required = yield* sql<{
            name: string;
          }>`SELECT name FROM sqlite_master WHERE type='table' AND name IN (
            'effect_agent_threads', 'effect_agent_canonical_batches', 'effect_agent_canonical_records',
            'effect_agent_checkpoints', 'effect_agent_submissions', 'effect_agent_submission_ownership',
            'effect_agent_attempts', 'effect_agent_settlement_reservations', 'effect_agent_abort_intents',
            'effect_agent_approval_decisions', 'effect_agent_unknown_resolutions', 'effect_agent_schedules'
          )`;

          if (required.length !== 12)
            return yield* SqliteStorageCompatibilityError.make({
              actualVersion: current[0].user_version,
              supportedVersion: CurrentSqliteStorageVersion,
              message:
                "The predecessor store is missing required tables; no upgrade was committed.",
            });

          const recoveryTables =
            yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name='effect_agent_recovery_checkpoints'`;

          if (recoveryTables.length !== (current[0].user_version === 10 ? 1 : 0))
            return yield* SqliteStorageCompatibilityError.make({
              actualVersion: current[0].user_version,
              supportedVersion: CurrentSqliteStorageVersion,
              message:
                "The predecessor recovery checkpoint storage does not match its version; refusing ambiguous data without mutation.",
            });

          const indexes =
            yield* sql`SELECT name FROM sqlite_master WHERE name='effect_agent_submissions_nonterminal'`;

          if (indexes.length !== 0)
            return yield* SqliteStorageCompatibilityError.make({
              actualVersion: current[0].user_version,
              supportedVersion: CurrentSqliteStorageVersion,
              message:
                "The predecessor already contains the nonterminal index; refusing ambiguous storage without mutation.",
            });
          if (current[0].user_version === 7) {
            yield* checkV2ThreadLayout();
            for (const statement of [
              sql`ALTER TABLE effect_agent_submissions ADD COLUMN admission_group TEXT`,
              sql`ALTER TABLE effect_agent_submissions ADD COLUMN admission_fence_json TEXT`,
              sql`CREATE INDEX effect_agent_submissions_group ON effect_agent_submissions (thread_id, admission_group, state)`,
            ]) {
              yield* failpoint("upgrade:before-mutation");
              yield* statement;
              yield* failpoint("upgrade:after-mutation");
            }
            yield* upgradeV2Schedules(16 * 1024 * 1024).pipe(
              Effect.provideService(ScheduleFailpoint, {
                hit: (point) =>
                  Schema.decodeUnknownEffect(SqliteStorageFailpointLocation)(point).pipe(
                    Effect.flatMap(failpoint),
                    Effect.mapError(() => ScheduleFailpointError.make({ point })),
                  ),
              }),
            );
            yield* upgradeV2Subscriptions(16 * 1024 * 1024).pipe(
              Effect.provideService(SubscriptionFailpoint, {
                hit: (point) =>
                  Schema.decodeUnknownEffect(SqliteStorageFailpointLocation)(point).pipe(
                    Effect.flatMap(failpoint),
                    Effect.mapError(() => SubscriptionFailpointError.make({ point })),
                  ),
              }),
            );
          }
          if (
            current[0].user_version === 8 ||
            current[0].user_version === 9 ||
            current[0].user_version === 10
          )
            yield* checkPredecessorLayout(current[0].user_version);
          if (current[0].user_version === 7 || current[0].user_version === 8) {
            yield* failpoint("upgrade:before-mutation");
            yield* sql`ALTER TABLE effect_agent_submissions ADD COLUMN worker_admission_json TEXT`;
            yield* failpoint("upgrade:after-mutation");
            yield* failpoint("upgrade:before-mutation");
            yield* sql`ALTER TABLE effect_agent_submissions ADD COLUMN message_admission_json TEXT`;
            yield* failpoint("upgrade:after-mutation");
            yield* failpoint("upgrade:before-mutation");
            yield* createMessageDeliveryTables;
            yield* failpoint("upgrade:after-mutation");
          }
          if (current[0].user_version !== 10) {
            yield* failpoint("upgrade:before-mutation");
            yield* createRecoveryCheckpointTable;
            yield* failpoint("upgrade:after-mutation");
          }
          yield* failpoint("upgrade:before-mutation");
          yield* createNonterminalIndex;
          yield* failpoint("upgrade:after-mutation");
          yield* failpoint("upgrade:before-version");
          yield* sql`PRAGMA user_version = 11`;
          yield* failpoint("upgrade:after-version");
        }),
      )
      .pipe(
        Effect.provide(NodeCrypto.layer),
        Effect.catchTag("SqliteStorageFailpointError", (error) =>
          SqliteStorageError.make({
            cause: error,
            operation: "upgrade storage",
            message: error.message,
          }),
        ),
        Effect.catchTag(["ScheduleFailpointError", "SubscriptionFailpointError"], (error) =>
          SqliteStorageError.make({
            cause: error,
            operation: "upgrade storage",
            message: "Injected storage upgrade failure",
          }),
        ),
        Effect.catchTag("StorageUpgradeError", (error) =>
          SqliteStorageCorruptionError.make({
            table: error.table,
            rowKey: error.rowKey,
            message: error.message,
          }),
        ),
        Effect.catchTag("SqlError", storageError("upgrade supported storage")),
        Effect.catchTag("SchemaError", (error) =>
          SqliteStorageCorruptionError.make({
            table: "upgrade",
            rowKey: "v7",
            message: error.message,
          }),
        ),
      );
  }

  if (version.user_version === 0) {
    const existingRows = yield* sql<Record<string, unknown>>`
      SELECT name
      FROM sqlite_master
      WHERE type = 'table'
        AND name LIKE 'effect_agent_%'
      ORDER BY name
    `.pipe(Effect.mapError(storageError("inspect unversioned storage")));

    const existing = yield* decodeRows(
      Schema.Array(SqliteNameRow),
      "sqlite_master",
      "effect_agent_%",
      existingRows,
    );

    if (existing.length > 0) {
      return yield* SqliteStorageCompatibilityError.make({
        actualVersion: 0,
        supportedVersion: CurrentSqliteStorageVersion,
        message:
          "The SQLite file contains unversioned Effect Agent tables. Refusing to mutate ambiguous stored data; retain it for inspection with its original writer.",
      });
    }

    yield* SqliteMigrator.run({ loader: sqliteMigrations }).pipe(
      Effect.mapError((error) =>
        SqliteStorageError.make({
          cause: error,
          operation: "initialize current storage",
          message: error.message,
        }),
      ),
    );
  }

  const requiredRows = yield* sql<Record<string, unknown>>`
    SELECT name
    FROM sqlite_master
    WHERE (type = 'table'
      AND name IN (
        'effect_agent_threads',
        'effect_agent_canonical_batches',
        'effect_agent_canonical_records',
        'effect_agent_checkpoints',
        'effect_agent_submissions',
        'effect_agent_submission_ownership',
        'effect_agent_attempts',
        'effect_agent_settlement_reservations',
        'effect_agent_abort_intents',
        'effect_agent_approval_decisions',
        'effect_agent_unknown_resolutions',
        'effect_agent_schedules',
        'effect_agent_message_deliveries',
        'effect_agent_recovery_checkpoints'
      )) OR (type = 'index' AND name = 'effect_agent_submissions_nonterminal')
    ORDER BY name
  `.pipe(Effect.mapError(storageError("verify storage tables")));

  const required = yield* decodeRows(
    Schema.Array(SqliteNameRow),
    "sqlite_master",
    "required_tables",
    requiredRows,
  );

  if (required.length !== 15) {
    return yield* SqliteStorageCompatibilityError.make({
      actualVersion: CurrentSqliteStorageVersion,
      supportedVersion: CurrentSqliteStorageVersion,
      message:
        "The SQLite file claims the current format but is missing required tables or its nonterminal index. Retain the original store for inspection.",
    });
  }

  const classifyWriteFailure =
    (operation: string) =>
    (error: SqlError): SqliteStorageError | SqliteWriteContention =>
      error.reason._tag === "LockTimeoutError"
        ? SqliteWriteContention.make({
            cause: error,
            operation,
            message: `Another producer holds the SQLite write lock; ${operation} is safe to retry.`,
          })
        : storageError(operation)(error);

  /**
   * Runs one journal write transaction under `BEGIN IMMEDIATE`. SQLite's deferred `BEGIN`
   * would let a read-then-write transaction start as a reader and fail with
   * SQLITE_BUSY_SNAPSHOT on upgrade, which `busy_timeout` never retries. Taking the write
   * lock up front keeps cross-owner contention inside the bounded busy retry; a lock
   * timeout is classified as the retryable SqliteWriteContention. A failed `BEGIN` leaves
   * no transaction, so no rollback is attempted for it.
   *
   * Journal write transactions are always top level. Nesting one inside another would
   * deadlock the single-connection client, so new journal operations must not wrap this
   * helper inside another transaction.
   */
  const withWriteTransaction =
    (operation: string) =>
    <A, E>(
      effect: Effect.Effect<A, E>,
    ): Effect.Effect<A, E | SqliteStorageError | SqliteWriteContention> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.scoped(
          Effect.gen(function* () {
            const connection = yield* sql.reserve.pipe(
              Effect.mapError(classifyWriteFailure(operation)),
            );

            yield* connection
              .executeUnprepared("BEGIN IMMEDIATE", [], undefined)
              .pipe(Effect.mapError(classifyWriteFailure(operation)));

            const exit = yield* restore(
              Effect.provideService(effect, sql.transactionService, [connection, 0] as const),
            ).pipe(Effect.exit);

            if (Exit.isSuccess(exit)) {
              yield* connection
                .executeUnprepared("COMMIT", [], undefined)
                .pipe(Effect.mapError(classifyWriteFailure(operation)));

              return exit.value;
            }
            yield* Effect.orDie(connection.executeUnprepared("ROLLBACK", [], undefined));

            return yield* exit;
          }),
        ).pipe(
          Effect.withSpan("SqliteJournal.withWriteTransaction", { attributes: { operation } }),
        ),
      );

  /**
   * Runs a read-only snapshot under a deferred transaction. Effect's SQLite client now
   * starts every writable-client `withTransaction` with `BEGIN IMMEDIATE`, which is the
   * right default for mutations but would make exports take the write lock and block a
   * concurrent append. Reserving the connection and beginning explicitly preserves the
   * adapter's snapshot-with-concurrent-writer contract. As with the write helper, a failed
   * `BEGIN` is reported directly because there is no transaction to roll back.
   */
  const withReadTransaction =
    (operation: string) =>
    <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E | SqliteStorageError> =>
      Effect.uninterruptibleMask((restore) =>
        Effect.scoped(
          Effect.gen(function* () {
            const connection = yield* sql.reserve.pipe(Effect.mapError(storageError(operation)));

            yield* connection
              .executeUnprepared("BEGIN", [], undefined)
              .pipe(Effect.mapError(storageError(operation)));

            const exit = yield* restore(
              Effect.provideService(effect, sql.transactionService, [connection, 0] as const),
            ).pipe(Effect.exit);

            if (Exit.isSuccess(exit)) {
              yield* connection
                .executeUnprepared("COMMIT", [], undefined)
                .pipe(Effect.mapError(storageError(operation)));

              return exit.value;
            }
            yield* Effect.orDie(connection.executeUnprepared("ROLLBACK", [], undefined));

            return yield* exit;
          }),
        ).pipe(Effect.withSpan("SqliteJournal.withReadTransaction", { attributes: { operation } })),
      );

  const materialize = Effect.fn("SqliteJournal.materialize")(function* (
    threadId: string,
    createdAt: string,
    emptyTailDigest: string,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<
    void,
    SqliteFenceRejected | SqliteStorageCorruptionError | SqliteStorageError | SqliteWriteContention
  > {
    if (
      threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(emptyTailDigest) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* SqliteStorageError.make({
        operation: "materialize thread",
        message: "Thread identity or initial digest exceeds the SQLite storage bounds.",
      });
    }
    yield* withWriteTransaction("materialize transaction")(
      Effect.gen(function* () {
        const existingRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            tail_digest,
            producer_epoch
          FROM effect_agent_threads
          WHERE thread_id = ${threadId}
        `.pipe(Effect.mapError(storageError("read materialized thread")));

        const existing = yield* decodeRows(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          threadId,
          existingRows,
        );

        if (existing.length > 1) {
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_threads",
            rowKey: threadId,
            message: "A thread primary key returned more than one row.",
          });
        }
        if (existing.length === 0) {
          yield* sql`
            INSERT INTO effect_agent_threads (
              thread_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            ) VALUES (
              ${threadId},
              ${createdAt},
              0,
              ${emptyTailDigest},
              ${producerEpoch}
            )
          `.pipe(Effect.mapError(storageError("materialize thread")));

          return;
        }
        if (producerEpoch < existing[0].producer_epoch) {
          return yield* SqliteFenceRejected.make({
            producerEpoch,
            actualEpoch: existing[0].producer_epoch,
            message: `Producer epoch ${producerEpoch} is stale; current epoch is ${existing[0].producer_epoch}.`,
          });
        }
        if (producerEpoch > existing[0].producer_epoch) {
          yield* sql`
            UPDATE effect_agent_threads
            SET producer_epoch = ${producerEpoch}
            WHERE thread_id = ${threadId}
          `.pipe(Effect.mapError(storageError("advance materialization epoch")));
        }
      }),
    );
  });

  const getThread = Effect.fn("SqliteJournal.getThread")(function* (threadId: string) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        created_at,
        tail_sequence,
        tail_digest,
        producer_epoch
      FROM effect_agent_threads
      WHERE thread_id = ${threadId}
    `.pipe(Effect.mapError(storageError("read thread")));

    return yield* decodeRows(Schema.Array(ThreadRow), "effect_agent_threads", threadId, rows);
  });

  const append = Effect.fn("SqliteJournal.append")(function* (
    request: RawAppendRequest,
  ): Effect.fn.Return<RawAppendResult, AppendError> {
    if (
      request.threadId.length > MAX_IDENTIFIER_LENGTH ||
      request.batchId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(request.batchJson) > MAX_STORED_TEXT_BYTES ||
      storedTextBytes(request.batchDigest) > MAX_STORED_TEXT_BYTES ||
      storedTextBytes(request.tailDigest) > MAX_STORED_TEXT_BYTES ||
      request.records.some(
        (record) =>
          record.recordId.length > MAX_IDENTIFIER_LENGTH ||
          storedTextBytes(record.recordJson) > MAX_STORED_TEXT_BYTES,
      )
    ) {
      return yield* SqliteStorageError.make({
        operation: "append canonical batch",
        message: "Canonical identifiers or encoded JSON exceed the SQLite storage bounds.",
      });
    }

    return yield* withWriteTransaction("append transaction")(
      Effect.gen(function* () {
        const recordIds = request.records.map((record) => record.recordId);

        if (new Set(recordIds).size !== recordIds.length) {
          return yield* SqliteAppendConflict.make({
            message: `Batch ${request.batchId} contains duplicate canonical record IDs.`,
            reason: "record-identity",
          });
        }

        const threadRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            tail_digest,
            producer_epoch
          FROM effect_agent_threads
          WHERE thread_id = ${request.threadId}
        `.pipe(Effect.mapError(storageError("read append tail")));

        const thread = yield* decodeSingleRow(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          request.threadId,
          threadRows,
        );

        if (request.producerEpoch !== thread.producer_epoch) {
          return yield* SqliteFenceRejected.make({
            producerEpoch: request.producerEpoch,
            actualEpoch: thread.producer_epoch,
            message: `Producer epoch ${request.producerEpoch} is not the current epoch ${thread.producer_epoch}.`,
          });
        }

        const batchRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            batch_id,
            first_sequence,
            last_sequence,
            batch_digest,
            tail_digest,
            batch_json
          FROM effect_agent_canonical_batches
          WHERE thread_id = ${request.threadId}
            AND batch_id = ${request.batchId}
        `.pipe(Effect.mapError(storageError("read idempotent batch")));

        const batches = yield* decodeRows(
          Schema.Array(BatchRow),
          "effect_agent_canonical_batches",
          `${request.threadId}/${request.batchId}`,
          batchRows,
        );

        if (batches.length > 1) {
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_canonical_batches",
            rowKey: `${request.threadId}/${request.batchId}`,
            message: "A canonical batch primary key returned more than one row.",
          });
        }
        if (batches.length === 1) {
          const existing = batches[0];

          if (existing.batch_digest !== request.batchDigest) {
            return yield* SqliteAppendConflict.make({
              message: `Batch ${request.batchId} already exists with different canonical content.`,
              reason: "batch-digest",
            });
          }

          return RawAppendResult.make({
            firstSequence: existing.first_sequence,
            lastSequence: existing.last_sequence,
            replayed: true,
            tailDigest: existing.tail_digest,
          });
        }

        if (
          request.expectedTailSequence !== thread.tail_sequence ||
          request.expectedTailDigest !== thread.tail_digest
        ) {
          return yield* SqliteAppendConflict.make({
            message:
              `Expected tail ${request.expectedTailSequence}/${request.expectedTailDigest} ` +
              `but found ${thread.tail_sequence}/${thread.tail_digest}.`,
            reason: "tail",
            actualTailSequence: thread.tail_sequence,
            actualTailDigest: thread.tail_digest,
          });
        }
        if (thread.tail_sequence + request.records.length > MAX_RECORDS_PER_THREAD) {
          return yield* SqliteStorageError.make({
            operation: "append canonical batch",
            message: `Thread record limit ${MAX_RECORDS_PER_THREAD} would be exceeded.`,
          });
        }

        const existingRecordRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            sequence,
            record_id,
            batch_id,
            record_json
          FROM effect_agent_canonical_records
          WHERE thread_id = ${request.threadId}
            AND record_id IN ${sql.in(recordIds)}
          ORDER BY sequence
        `.pipe(Effect.mapError(storageError("check canonical record identities")));

        const existingRecords = yield* decodeRows(
          Schema.Array(RecordRow),
          "effect_agent_canonical_records",
          `${request.threadId}/record_ids`,
          existingRecordRows,
        );

        if (existingRecords.length > 0) {
          return yield* SqliteAppendConflict.make({
            message: `Canonical record ID ${existingRecords[0].record_id} already exists.`,
            reason: "record-identity",
          });
        }

        const firstSequence = yield* Schema.decodeEffect(CanonicalSequence)(
          thread.tail_sequence + 1,
        ).pipe(
          Effect.mapError((error) =>
            SqliteStorageError.make({
              cause: error,
              operation: "append canonical batch",
              message: error.message,
            }),
          ),
        );

        const lastSequence = yield* Schema.decodeEffect(CanonicalSequence)(
          firstSequence + request.records.length - 1,
        ).pipe(
          Effect.mapError((error) =>
            SqliteStorageError.make({
              cause: error,
              operation: "append canonical batch",
              message: error.message,
            }),
          ),
        );

        yield* sql`
          INSERT INTO effect_agent_canonical_batches (
            thread_id,
            batch_id,
            first_sequence,
            last_sequence,
            batch_digest,
            tail_digest,
            batch_json
          ) VALUES (
            ${request.threadId},
            ${request.batchId},
            ${firstSequence},
            ${lastSequence},
            ${request.batchDigest},
            ${request.tailDigest},
            ${request.batchJson}
          )
        `.pipe(Effect.mapError(storageError("insert canonical batch")));
        yield* failpoint("append:after-batch-insert");

        yield* Effect.forEach(
          request.records,
          (record, index) =>
            Effect.gen(function* () {
              yield* sql`
                  INSERT INTO effect_agent_canonical_records (
                    thread_id,
                    sequence,
                    record_id,
                    batch_id,
                    record_json
                  ) VALUES (
                    ${request.threadId},
                    ${firstSequence + index},
                    ${record.recordId},
                    ${request.batchId},
                    ${record.recordJson}
                  )
                `.pipe(Effect.mapError(storageError("insert canonical record")));
              yield* failpoint("append:after-record-insert");
            }),
          { discard: true },
        );

        yield* sql`
          UPDATE effect_agent_threads
          SET
            tail_sequence = ${lastSequence},
            tail_digest = ${request.tailDigest},
            producer_epoch = ${request.producerEpoch}
          WHERE thread_id = ${request.threadId}
        `.pipe(Effect.mapError(storageError("advance thread tail")));
        yield* failpoint("append:after-tail-update");

        return RawAppendResult.make({
          firstSequence,
          lastSequence,
          replayed: false,
          tailDigest: request.tailDigest,
        });
      }),
    );
  });

  const read = Effect.fn("SqliteJournal.read")(function* (request: RawReadRequest) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        sequence,
        record_id,
        batch_id,
        record_json
      FROM effect_agent_canonical_records
      WHERE thread_id = ${request.threadId}
        AND sequence > ${request.fromSequenceExclusive}
      ORDER BY sequence
      LIMIT ${request.limit}
    `.pipe(Effect.mapError(storageError("read canonical records")));

    return yield* decodeRows(
      Schema.Array(RecordRow),
      "effect_agent_canonical_records",
      `${request.threadId}>${request.fromSequenceExclusive}`,
      rows,
    );
  });

  const exportThread = Effect.fn("SqliteJournal.exportThread")(function* (threadId: string) {
    return yield* withReadTransaction("export transaction")(
      Effect.gen(function* () {
        const threadRows = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            FROM effect_agent_threads
            WHERE thread_id = ${threadId}
          `.pipe(Effect.mapError(storageError("export thread")));

        const thread = yield* decodeSingleRow(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          threadId,
          threadRows,
        );

        yield* failpoint("export:after-thread-read");

        if (thread.tail_sequence > MAX_RECORDS_PER_THREAD)
          return yield* SqliteStorageError.make({
            operation: "export thread",
            message: "The thread exceeds the current export record limit.",
          });
        const records: Array<RecordRow> = [];
        let afterSequence = ZERO_SEQUENCE;

        while (afterSequence < thread.tail_sequence) {
          const limit = Math.min(1_024, thread.tail_sequence - afterSequence);

          const request = RawReadRequest.make({
            threadId,
            fromSequenceExclusive: afterSequence,
            limit,
          });

          const page = yield* read(request);

          if (
            page.length !== limit ||
            page.some((record, index) => record.sequence !== afterSequence + index + 1)
          ) {
            return yield* SqliteStorageCorruptionError.make({
              table: "effect_agent_canonical_records",
              rowKey: threadId,
              message: "The exported canonical prefix is not contiguous through its captured tail.",
            });
          }
          records.push(...page);
          afterSequence = page[page.length - 1].sequence;
        }

        const beyondTail =
          yield* sql`SELECT sequence FROM effect_agent_canonical_records WHERE thread_id=${threadId} AND sequence > ${thread.tail_sequence} LIMIT 1`.pipe(
            Effect.mapError(storageError("verify export tail")),
          );

        if (beyondTail.length !== 0)
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_canonical_records",
            rowKey: threadId,
            message: "Canonical records exist beyond the captured thread tail.",
          });

        return RawThreadExport.make({ thread, records });
      }),
    );
  });

  const saveCheckpoint = Effect.fn("SqliteJournal.saveCheckpoint")(function* (
    checkpoint: RawCheckpoint,
  ): Effect.fn.Return<void, CheckpointError> {
    if (
      checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(checkpoint.checkpointJson) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* SqliteStorageError.make({
        operation: "save checkpoint",
        message: "Checkpoint identity or encoded JSON exceeds the SQLite storage bounds.",
      });
    }
    yield* withWriteTransaction("checkpoint transaction")(
      Effect.gen(function* () {
        const threadRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            created_at,
            tail_sequence,
            tail_digest,
            producer_epoch
          FROM effect_agent_threads
          WHERE thread_id = ${checkpoint.threadId}
        `.pipe(Effect.mapError(storageError("read checkpoint tail")));

        const thread = yield* decodeSingleRow(
          Schema.Array(ThreadRow),
          "effect_agent_threads",
          checkpoint.threadId,
          threadRows,
        );

        if (checkpoint.throughSequence > thread.tail_sequence) {
          return yield* SqliteCheckpointConflict.make({
            message:
              `Checkpoint sequence ${checkpoint.throughSequence} is after canonical tail ` +
              `${thread.tail_sequence}.`,
          });
        }

        const checkpointRows = yield* sql<Record<string, unknown>>`
          SELECT
            thread_id,
            through_sequence,
            tail_digest,
            checkpoint_json
          FROM effect_agent_checkpoints
          WHERE thread_id = ${checkpoint.threadId}
            AND through_sequence = ${checkpoint.throughSequence}
        `.pipe(Effect.mapError(storageError("read idempotent checkpoint")));

        const existing = yield* decodeRows(
          Schema.Array(CheckpointRow),
          "effect_agent_checkpoints",
          `${checkpoint.threadId}/${checkpoint.throughSequence}`,
          checkpointRows,
        );

        if (existing.length > 1) {
          return yield* SqliteStorageCorruptionError.make({
            table: "effect_agent_checkpoints",
            rowKey: `${checkpoint.threadId}/${checkpoint.throughSequence}`,
            message: "A checkpoint primary key returned more than one row.",
          });
        }
        if (existing.length === 1) {
          if (
            existing[0].tail_digest !== checkpoint.tailDigest ||
            existing[0].checkpoint_json !== checkpoint.checkpointJson
          ) {
            return yield* SqliteCheckpointConflict.make({
              message: "A different checkpoint already exists at this canonical sequence.",
            });
          }

          return;
        }

        yield* sql`
          INSERT INTO effect_agent_checkpoints (
            thread_id,
            through_sequence,
            tail_digest,
            checkpoint_json
          ) VALUES (
            ${checkpoint.threadId},
            ${checkpoint.throughSequence},
            ${checkpoint.tailDigest},
            ${checkpoint.checkpointJson}
          )
        `.pipe(Effect.mapError(storageError("insert checkpoint")));
      }),
    );
  });

  const saveRecoveryCheckpoint = Effect.fn("SqliteJournal.saveRecoveryCheckpoint")(function* (
    request: SaveRecoveryCheckpointRequest,
    checkpointJson: string,
  ) {
    const { checkpoint } = request;

    if (
      checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH ||
      storedTextBytes(checkpointJson) > MAX_STORED_TEXT_BYTES
    ) {
      return yield* SqliteStorageError.make({
        operation: "save recovery checkpoint",
        message: "Checkpoint identity or encoded JSON exceeds the SQLite storage bounds.",
      });
    }
    yield* withWriteTransaction("recovery checkpoint transaction")(
      Effect.gen(function* () {
        const threads = yield* getThread(checkpoint.threadId);
        const thread = threads[0];

        if (thread === undefined)
          return yield* ThreadNotMaterialized.make({ threadId: checkpoint.threadId });
        if (request.producerEpoch !== thread.producer_epoch)
          return yield* FenceRejected.make({
            threadId: checkpoint.threadId,
            actualEpoch: thread.producer_epoch,
            attemptedEpoch: request.producerEpoch,
          });
        if (checkpoint.throughSequence > thread.tail_sequence)
          return yield* CheckpointRejected.make({
            threadId: checkpoint.threadId,
            reason: "ahead-of-tail",
          });

        const digests =
          checkpoint.throughSequence === 0
            ? [EMPTY_TAIL_DIGEST]
            : yield* getTailDigestAt(checkpoint.threadId, checkpoint.throughSequence);

        if (digests.length !== 1 || digests[0] !== checkpoint.tailDigest)
          return yield* CheckpointRejected.make({
            threadId: checkpoint.threadId,
            reason: "digest-mismatch",
          });

        yield* failpoint("save-recovery-checkpoint:before");
        yield* sql`
          INSERT INTO effect_agent_recovery_checkpoints (thread_id, through_sequence, tail_digest, checkpoint_json)
          VALUES (${checkpoint.threadId}, ${checkpoint.throughSequence}, ${checkpoint.tailDigest}, ${checkpointJson})
          ON CONFLICT (thread_id) DO UPDATE SET
            through_sequence = excluded.through_sequence,
            tail_digest = excluded.tail_digest,
            checkpoint_json = excluded.checkpoint_json
          WHERE excluded.through_sequence >= effect_agent_recovery_checkpoints.through_sequence
        `.pipe(Effect.mapError(storageError("save recovery checkpoint")));
      }),
    );
    yield* failpoint("save-recovery-checkpoint:after");
  });

  const loadRecoveryCheckpoint = Effect.fn("SqliteJournal.loadRecoveryCheckpoint")(function* (
    threadId: string,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT thread_id, through_sequence, tail_digest, checkpoint_json
      FROM effect_agent_recovery_checkpoints
      WHERE thread_id = ${threadId}
    `.pipe(Effect.mapError(storageError("load recovery checkpoint")));

    return yield* decodeRows(
      Schema.Array(CheckpointRow),
      "effect_agent_recovery_checkpoints",
      threadId,
      rows,
    );
  });

  const loadCheckpoint = Effect.fn("SqliteJournal.loadCheckpoint")(function* (
    threadId: string,
    atOrBeforeSequence: CanonicalSequence,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        through_sequence,
        tail_digest,
        checkpoint_json
      FROM effect_agent_checkpoints
      WHERE thread_id = ${threadId}
        AND through_sequence <= ${atOrBeforeSequence}
      ORDER BY through_sequence DESC
      LIMIT 1
    `.pipe(Effect.mapError(storageError("load checkpoint")));

    return yield* decodeRows(
      Schema.Array(CheckpointRow),
      "effect_agent_checkpoints",
      `${threadId}<=${atOrBeforeSequence}`,
      rows,
    );
  });

  const getTailDigestAt = Effect.fn("SqliteJournal.getTailDigestAt")(function* (
    threadId: string,
    sequence: CanonicalSequence,
  ) {
    if (sequence === 0) {
      const threads = yield* getThread(threadId);

      return threads.length === 0
        ? []
        : [threads[0].tail_sequence === 0 ? threads[0].tail_digest : undefined].filter(
            (value): value is string => value !== undefined,
          );
    }

    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        thread_id,
        batch_id,
        first_sequence,
        last_sequence,
        batch_digest,
        tail_digest,
        batch_json
      FROM effect_agent_canonical_batches
      WHERE thread_id = ${threadId}
        AND last_sequence = ${sequence}
    `.pipe(Effect.mapError(storageError("read canonical digest at sequence")));

    const batches = yield* decodeRows(
      Schema.Array(BatchRow),
      "effect_agent_canonical_batches",
      `${threadId}/${sequence}`,
      rows,
    );

    return batches.map((batch) => batch.tail_digest);
  });

  const scanStoredPayloads = Effect.fn("SqliteJournal.scanStoredPayloads")(function* () {
    return yield* withReadTransaction("startup scan transaction")(
      Effect.gen(function* () {
        const threads = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              created_at,
              tail_sequence,
              tail_digest,
              producer_epoch
            FROM effect_agent_threads
            ORDER BY thread_id
          `.pipe(Effect.mapError(storageError("scan threads")));

        const batches = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              batch_id,
              first_sequence,
              last_sequence,
              batch_digest,
              tail_digest,
              batch_json
            FROM effect_agent_canonical_batches
            ORDER BY thread_id, first_sequence
          `.pipe(Effect.mapError(storageError("scan canonical batches")));

        const records = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              sequence,
              record_id,
              batch_id,
              record_json
            FROM effect_agent_canonical_records
            ORDER BY thread_id, sequence
          `.pipe(Effect.mapError(storageError("scan canonical records")));

        const checkpoints = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              through_sequence,
              tail_digest,
              checkpoint_json
            FROM effect_agent_checkpoints
            ORDER BY thread_id, through_sequence
          `.pipe(Effect.mapError(storageError("scan checkpoints")));

        return {
          threads: yield* decodeRows(
            Schema.Array(ThreadRow),
            "effect_agent_threads",
            "startup_scan",
            threads,
          ),
          batches: yield* decodeRows(
            Schema.Array(BatchRow),
            "effect_agent_canonical_batches",
            "startup_scan",
            batches,
          ),
          records: yield* decodeRows(
            Schema.Array(RecordRow),
            "effect_agent_canonical_records",
            "startup_scan",
            records,
          ),
          checkpoints: yield* decodeRows(
            Schema.Array(CheckpointRow),
            "effect_agent_checkpoints",
            "startup_scan",
            checkpoints,
          ),
        };
      }),
    );
  });

  return {
    append,
    exportThread,
    getThread,
    getTailDigestAt,
    loadCheckpoint,
    loadRecoveryCheckpoint,
    saveRecoveryCheckpoint,
    materialize,
    read,
    saveCheckpoint,
    scanStoredPayloads,
    withWriteTransaction,
    withReadTransaction,
  } as const;
});

export type SqliteJournal = Effect.Success<ReturnType<typeof initializeSqliteJournal>>;
