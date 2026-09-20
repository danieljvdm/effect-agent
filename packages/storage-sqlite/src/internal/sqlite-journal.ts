import { makeSqlJournal } from "@effect-agent/storage-sql/sql-journal";
import { createMessageDeliveryPendingIndex } from "@effect-agent/storage-sql/sql-message-delivery-store";
import { makeRowDecoder, makeSqlTransaction } from "@effect-agent/storage-sql/sql-storage";
import {
  checkV2ThreadLayout,
  upgradeV2Schedules,
  upgradeV2Subscriptions,
} from "@effect-agent/storage-sql/sql-storage-v2-upgrade";
import {
  createNativeReadIndexes,
  seedNativeReadIndexes,
} from "@effect-agent/storage-sql/sql-thread-native-reads";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteMigrator } from "@effect/sql-sqlite-node";
import { Effect, Schema } from "effect";
import { ScheduleFailpoint, ScheduleFailpointError } from "effect-agent/schedule";
import { SubscriptionFailpoint, SubscriptionFailpointError } from "effect-agent/subscription";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { isSqlError, type SqlError } from "effect/unstable/sql/SqlError";

import { SqliteStorageConfig } from "../SqliteStorageConfig.ts";
import {
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
  createWorkerStops,
  createNonterminalIndex,
  sqliteMigrations,
} from "./migrations.ts";
import { createRecoveryCheckpointTable } from "./recovery-checkpoint-schema.ts";

const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

class SqliteVersionRow extends Schema.Class<SqliteVersionRow>("SqliteVersionRow")({
  user_version: NonNegativeInt,
}) {}

class SqliteJournalModeRow extends Schema.Class<SqliteJournalModeRow>("SqliteJournalModeRow")({
  journal_mode: Schema.NonEmptyString.check(Schema.isMaxLength(32)),
}) {}

class SqliteNameRow extends Schema.Class<SqliteNameRow>("SqliteNameRow")({
  name: BoundedIdentifier,
}) {}

const storageError =
  (operation: string) =>
  (error: SqlError): SqliteStorageError =>
    SqliteStorageError.make({
      cause: error,
      operation,
      message: error.message,
    });

export const sqliteErrors = {
  storage: SqliteStorageError.make,
  corruption: SqliteStorageCorruptionError.make,
  isCorruption: Schema.is(SqliteStorageCorruptionError),
};

const { decodeRows, decodeSingleRow } = makeRowDecoder(SqliteStorageCorruptionError.make);

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
  version: 8 | 9 | 10 | 12,
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
    ...(version === 12
      ? {
          effect_agent_canonical_records: [
            ...predecessorColumns.effect_agent_canonical_records,
            "outstanding",
          ],
        }
      : {}),
    ...(version >= 10
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

  const verifyWorkerPredecessor = Effect.fnUntraced(function* (workerContract: boolean) {
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
      )) OR (type = 'index' AND name IN ('effect_agent_submissions_nonterminal', 'effect_agent_records_subtree', 'effect_agent_message_deliveries_pending', 'effect_agent_records_outstanding', 'effect_agent_records_call', 'effect_agent_records_run_input', 'effect_agent_records_worker_input'))
    OR (${workerContract ? 1 : 0} = 1 AND name IN ('effect_agent_worker_stops', 'effect_agent_worker_starts', 'effect_agent_worker_pending', 'effect_agent_worker_execution'))
    ORDER BY name
  `.pipe(Effect.mapError(storageError("verify storage tables")));

    const required = yield* decodeRows(
      Schema.Array(SqliteNameRow),
      "sqlite_master",
      "required_tables",
      requiredRows,
    );

    if (required.length !== 21 + (workerContract ? 4 : 0)) {
      return yield* SqliteStorageCompatibilityError.make({
        actualVersion: CurrentSqliteStorageVersion,
        supportedVersion: CurrentSqliteStorageVersion,
        message:
          "The SQLite file claims the current format but is missing required tables or its nonterminal index. Retain the original store for inspection.",
      });
    }
  });

  // Support the known beta49/beta50 and immediate predecessor formats atomically.
  if (
    version.user_version !== 0 &&
    version.user_version !== 7 &&
    version.user_version !== 8 &&
    version.user_version !== 9 &&
    version.user_version !== 10 &&
    version.user_version !== 11 &&
    version.user_version !== 12 &&
    version.user_version !== 13 &&
    version.user_version !== CurrentSqliteStorageVersion
  ) {
    return yield* SqliteStorageCompatibilityError.make({
      actualVersion: version.user_version,
      supportedVersion: CurrentSqliteStorageVersion,
      message:
        `The SQLite file uses unsupported storage version ${version.user_version}; ` +
        `this build supports exactly version ${CurrentSqliteStorageVersion}. ` +
        "Only supported v7, v8, v9, v10, v11, v12 and v13 can be upgraded automatically. Keep the original file and use a compatible library version.",
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
          yield* createNativeReadIndexes;
          yield* createMessageDeliveryPendingIndex;
          yield* seedNativeReadIndexes.pipe(
            Effect.catchTag("ThreadStoreError", (error) =>
              SqliteStorageCorruptionError.make({
                table: "effect_agent_canonical_records",
                rowKey: "upgrade",
                message: error.message,
              }),
            ),
          );
          yield* createWorkerStops;
          yield* sql`PRAGMA user_version = 14`;
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

  if (version.user_version === 11) {
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const current = yield* sql<{ user_version: number }>`PRAGMA user_version`;

          if (current[0]?.user_version === 14) return;
          if (current[0]?.user_version !== 11)
            return yield* SqliteStorageCompatibilityError.make({
              actualVersion: current[0]?.user_version ?? -1,
              supportedVersion: 14,
              message: "Storage version changed during native index upgrade",
            });
          yield* checkPredecessorLayout(10);

          const requiredIndex =
            yield* sql`SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'effect_agent_submissions_nonterminal'`;

          if (requiredIndex.length !== 1)
            return yield* SqliteStorageCompatibilityError.make({
              actualVersion: 11,
              supportedVersion: 14,
              message: "Predecessor storage is missing its required nonterminal index",
            });
          yield* failpoint("upgrade:before-mutation");
          yield* createNativeReadIndexes;
          yield* createMessageDeliveryPendingIndex;
          yield* seedNativeReadIndexes.pipe(
            Effect.catchTag("ThreadStoreError", (error) =>
              SqliteStorageCorruptionError.make({
                table: "effect_agent_canonical_records",
                rowKey: "upgrade",
                message: error.message,
              }),
            ),
          );
          yield* failpoint("upgrade:after-mutation");
          yield* failpoint("upgrade:before-version");
          yield* createWorkerStops;
          yield* sql`PRAGMA user_version = 14`;
          yield* failpoint("upgrade:after-version");
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          SqliteStorageError.make({
            operation: "upgrade native indexes",
            message: error.message,
            cause: error,
          }),
        ),
      );
  }

  if (version.user_version === 12) {
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const current = yield* sql<{ user_version: number }>`PRAGMA user_version`;

          if (current[0]?.user_version === 14) return;
          if (current[0]?.user_version !== 12)
            return yield* SqliteStorageCompatibilityError.make({
              actualVersion: current[0]?.user_version ?? -1,
              supportedVersion: 14,
              message: "Storage version changed during worker stop upgrade",
            });
          yield* checkPredecessorLayout(12);
          yield* verifyWorkerPredecessor(false);
          yield* failpoint("upgrade:before-mutation");
          yield* createWorkerStops;
          yield* failpoint("upgrade:after-mutation");
          yield* failpoint("upgrade:before-version");
          yield* sql`PRAGMA user_version = 14`;
          yield* failpoint("upgrade:after-version");
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          SqliteStorageError.make({
            operation: "upgrade worker stop",
            message: "Worker stop upgrade failed",
            cause,
          }),
        ),
      );
  }

  if (version.user_version === 13) {
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const current = yield* sql<{ user_version: number }>`PRAGMA user_version`;

          if (current[0]?.user_version === 14) return;
          if (current[0]?.user_version !== 13)
            return yield* SqliteStorageCompatibilityError.make({
              actualVersion: current[0]?.user_version ?? -1,
              supportedVersion: 14,
              message: "Storage version changed during assignment seal upgrade",
            });
          yield* checkPredecessorLayout(12);
          yield* verifyWorkerPredecessor(true);
          const columns = yield* sql`PRAGMA table_info(effect_agent_worker_stops)`;

          yield* Schema.decodeUnknownEffect(
            Schema.Tuple([
              Schema.Struct({
                cid: Schema.Literal(0),
                name: Schema.Literal("thread_id"),
                type: Schema.Literal("TEXT"),
                notnull: Schema.Literal(1),
                dflt_value: Schema.Null,
                pk: Schema.Literal(1),
              }),
            ]),
          )(columns).pipe(
            Effect.mapError(() =>
              SqliteStorageCompatibilityError.make({
                actualVersion: 13,
                supportedVersion: 14,
                message: "Unsupported worker seal layout; no upgrade was committed",
              }),
            ),
          );
          yield* failpoint("upgrade:before-mutation");
          yield* sql`ALTER TABLE effect_agent_worker_stops ADD COLUMN terminal TEXT`;
          yield* failpoint("upgrade:after-mutation");
          yield* failpoint("upgrade:before-version");
          yield* sql`PRAGMA user_version = 14`;
          yield* failpoint("upgrade:after-version");
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          SqliteStorageError.make({
            operation: "upgrade assignment seals",
            message: "Assignment seal upgrade failed",
            cause,
          }),
        ),
      );
  }

  yield* verifyWorkerPredecessor(true);

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

  return yield* makeSqlJournal({
    errors: sqliteErrors,
    hitFailpoint: failpoint,
    transactions: {
      withWriteTransaction:
        (operation) =>
        <A, E, R>(body: Effect.Effect<A, E, R>) =>
          makeSqlTransaction(sql, { begin: "BEGIN IMMEDIATE" })(body).pipe(
            Effect.mapError((error) =>
              isSqlError(error) ? classifyWriteFailure(operation)(error) : error,
            ),
          ),
      withReadTransaction:
        (operation) =>
        <A, E, R>(body: Effect.Effect<A, E, R>) =>
          makeSqlTransaction(sql, { begin: "BEGIN" })(body).pipe(
            Effect.mapError((error) =>
              isSqlError(error) ? storageError(operation)(error) : error,
            ),
          ),
      isTransactionFailure: Schema.is(Schema.Union([SqliteStorageError, SqliteWriteContention])),
    },
  });
});

export type SqliteJournal = Effect.Success<ReturnType<typeof initializeSqliteJournal>>;
