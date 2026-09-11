import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import { CanonicalSequence, ProducerEpoch } from "@effect-agent/thread/Records";
import { checkV2ThreadLayout } from "@effect-agent/thread/SqlStorageV2Upgrade";
import {
  MAX_THREAD_EXPORT_RECORDS,
  CheckpointRejected,
  FenceRejected,
  ThreadNotMaterialized,
  type SaveRecoveryCheckpointRequest,
} from "@effect-agent/thread/ThreadStore";
import { SqliteMigrator } from "@effect/sql-sqlite-do";
import { Effect, Schema, Stream } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlError } from "effect/unstable/sql/SqlError";

import {
  type DoStorageFailpointError,
  DoAppendConflict,
  DoCheckpointConflict,
  DoFenceRejected,
  DoStorageCompatibilityError,
  DoStorageCorruptionError,
  DoStorageError,
  DoValueBoundExceeded,
  type DoStorageFailpointLocation,
} from "../DoStorageError.ts";
import { createMessageDeliveryTables } from "./message-delivery-schema.ts";
import { CurrentDoStorageVersion, createNonterminalIndex, doMigrations } from "./migrations.ts";
import { createRecoveryCheckpointTable } from "./recovery-checkpoint-schema.ts";

/**
 * Static schema ceiling for stored text columns. Writes are bounded in BYTES by the
 * configured `maxStoredValueBytes` (always ≤ 2,000,000); UTF-8 byte length is never smaller
 * than UTF-16 string length, so any value that passed the byte bound also passes this
 * decode-side character ceiling.
 */
const BoundedStoredText = Schema.String.check(Schema.isMaxLength(2_000_000));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const MAX_RECORDS_PER_THREAD = MAX_THREAD_EXPORT_RECORDS;
const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
const MAX_IDENTIFIER_LENGTH = 1_024;
const MAX_READ_PAGE_JSON_BYTES = 4 * 1024 * 1024;
/** Durable Object SQL storage allows at most 100 bound parameters per statement. */
const MAX_BOUND_PARAMETERS = 100;
const isSqlError = Schema.is(SqlError);

const storedTextBytes = (value: string): number => new TextEncoder().encode(value).byteLength;

const chunked = <A>(values: ReadonlyArray<A>, size: number): Array<ReadonlyArray<A>> => {
  const chunks: Array<ReadonlyArray<A>> = [];

  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }

  return chunks;
};

class DoMetaRow extends Schema.Class<DoMetaRow>("DoMetaRow")({
  value: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
}) {}

class DoNameRow extends Schema.Class<DoNameRow>("DoNameRow")({
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

const ReadPlanRow = Schema.Struct({
  sequence: CanonicalSequence,
  record_json_bytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_READ_PAGE_JSON_BYTES)),
});

type ReadPage = [typeof ReadPlanRow.Type, ...Array<typeof ReadPlanRow.Type>];

class CheckpointRow extends Schema.Class<CheckpointRow>("CheckpointRow")({
  checkpoint_json: BoundedStoredText,
  thread_id: BoundedIdentifier,
  tail_digest: BoundedStoredText,
  through_sequence: CanonicalSequence,
}) {}

export class RawRecord extends Schema.Class<RawRecord>(
  "@effect-agent/storage-cloudflare/RawRecord",
)({
  recordId: BoundedIdentifier,
  recordJson: BoundedStoredText,
}) {}

export class RawAppendRequest extends Schema.Class<RawAppendRequest>(
  "@effect-agent/storage-cloudflare/RawAppendRequest",
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
  "@effect-agent/storage-cloudflare/RawAppendResult",
)({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  replayed: Schema.Boolean,
  tailDigest: BoundedStoredText,
}) {}

export class RawReadRequest extends Schema.Class<RawReadRequest>(
  "@effect-agent/storage-cloudflare/RawReadRequest",
)({
  threadId: BoundedIdentifier,
  fromSequenceExclusive: CanonicalSequence,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

export class RawCheckpoint extends Schema.Class<RawCheckpoint>(
  "@effect-agent/storage-cloudflare/RawCheckpoint",
)({
  checkpointJson: BoundedStoredText,
  threadId: BoundedIdentifier,
  tailDigest: BoundedStoredText,
  throughSequence: CanonicalSequence,
}) {}

export class RawThreadExport extends Schema.Class<RawThreadExport>(
  "@effect-agent/storage-cloudflare/RawThreadExport",
)({
  thread: ThreadRow,
  records: Schema.Array(RecordRow),
}) {}

type AppendError =
  | DoAppendConflict
  | DoFenceRejected
  | DoStorageCorruptionError
  | DoStorageError
  | DoStorageFailpointError
  | DoValueBoundExceeded;

type CheckpointError =
  | DoCheckpointConflict
  | DoStorageCorruptionError
  | DoStorageError
  | DoValueBoundExceeded;

type DoJournalFailpoint = (
  location: DoStorageFailpointLocation,
) => Effect.Effect<void, DoStorageFailpointError>;

const noFailpoint: DoJournalFailpoint = () => Effect.void;

const storageError =
  (operation: string) =>
  (error: SqlError): DoStorageError =>
    DoStorageError.make({
      cause: error,
      operation,
      message: error.message,
    });

/** Decode raw Durable Object SQLite rows against a Schema, reporting failures as typed corruption. */
export const decodeRows = Effect.fn(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<ReadonlyArray<A>, DoStorageCorruptionError> =>
    Schema.decodeUnknownEffect(schema)(rows).pipe(
      Effect.mapError((error) =>
        DoStorageCorruptionError.make({
          table,
          rowKey,
          message: String(error),
        }),
      ),
    ),
);

/** Decode exactly one raw row against a Schema, reporting failures as typed corruption. */
export const decodeSingleRow = Effect.fn(
  <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<A, DoStorageCorruptionError> =>
    decodeRows(schema, table, rowKey, rows).pipe(
      Effect.flatMap((decoded) =>
        decoded.length === 1
          ? Effect.succeed(decoded[0])
          : Effect.fail(
              DoStorageCorruptionError.make({
                table,
                rowKey,
                message: `Expected exactly one row but found ${decoded.length}.`,
              }),
            ),
      ),
    ),
);

/** Column inventory of the supported v3 predecessor, independent of physical column order. */
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
  effect_agent_meta: ["key", "value"],
  effect_agent_child_settlements: [
    "parent_submission_id",
    "child_submission_id",
    "child_outcome",
    "recorded_at",
  ],
} as const;

const checkPredecessorLayout = Effect.fn("DoJournal.checkPredecessorLayout")(function* (
  version: 3 | 4 | 5,
) {
  const sql = yield* SqlClient.SqlClient;

  const messageColumns =
    version === 3
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
    ...(version === 5
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
      return yield* DoStorageCompatibilityError.make({
        actualVersion: version,
        supportedVersion: CurrentDoStorageVersion,
        message: `The v${version} ${table} columns do not match the supported predecessor; no upgrade was committed.`,
      });
  }
});

const REQUIRED_TABLES = [
  "effect_agent_abort_intents",
  "effect_agent_approval_decisions",
  "effect_agent_attempts",
  "effect_agent_canonical_batches",
  "effect_agent_canonical_records",
  "effect_agent_checkpoints",
  "effect_agent_child_reservations",
  "effect_agent_child_settlements",
  "effect_agent_threads",
  "effect_agent_meta",
  "effect_agent_settlement_reservations",
  "effect_agent_submission_ownership",
  "effect_agent_submissions",
  "effect_agent_unknown_resolutions",
] as const;

/**
 * Supported-predecessor or fresh storage gate (DEPLOY-008) over `effect_agent_meta` instead of
 * `PRAGMA user_version` (unverified on Durable Object SQL storage; a meta table is portable
 * regardless). No WAL check (Durable Object storage owns durability and confirms writes
 * through output gates) and no busy timeout (a Durable Object has exactly one writer): the
 * Node machinery those served has no DC analogue and is deliberately absent.
 */
const ensureCurrentStorage = Effect.fn("DoJournal.ensureCurrentStorage")(function* (
  sql: SqlClient.SqlClient,
  failpoint: DoJournalFailpoint = noFailpoint,
  maxStoredValueBytes: number,
) {
  const metaTableRows = yield* sql<Record<string, unknown>>`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name = 'effect_agent_meta'
  `.pipe(Effect.mapError(storageError("read storage version table")));

  const metaTables = yield* decodeRows(
    Schema.Array(DoNameRow),
    "sqlite_master",
    "effect_agent_meta",
    metaTableRows,
  );

  if (metaTables.length === 0) {
    const existingRows = yield* sql<Record<string, unknown>>`
      SELECT name
      FROM sqlite_master
      WHERE type = 'table'
        AND name LIKE 'effect_agent_%'
      ORDER BY name
    `.pipe(Effect.mapError(storageError("inspect unversioned storage")));

    const existing = yield* decodeRows(
      Schema.Array(DoNameRow),
      "sqlite_master",
      "effect_agent_%",
      existingRows,
    );

    if (existing.length > 0) {
      return yield* DoStorageCompatibilityError.make({
        actualVersion: 0,
        supportedVersion: CurrentDoStorageVersion,
        message:
          "The Durable Object contains unversioned Effect Agent tables. Refusing to mutate ambiguous stored data; retain it for inspection with its original writer.",
      });
    }

    yield* SqliteMigrator.run({
      loader: doMigrations,
      // An application can share this SQL client and own its own migration history.
      // Keep bookkeeping outside effect_agent_% so interrupted unversioned schemas
      // still fail the ambiguity check above.
      table: "effect_sql_migrations_agent_threads",
    }).pipe(
      // SqliteMigrator depends on the generic client supplied by this adapter. The concrete
      // Durable Object client is kept at the outer Layer boundary.
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.mapError((error) =>
        DoStorageError.make({
          cause: error,
          operation: "initialize current storage",
          message: error.message,
        }),
      ),
    );
  } else {
    const versionRows = yield* sql<Record<string, unknown>>`
      SELECT value
      FROM effect_agent_meta
      WHERE key = 'storage_version'
    `.pipe(Effect.mapError(storageError("read storage version")));

    const version = yield* decodeSingleRow(
      Schema.Array(DoMetaRow),
      "effect_agent_meta",
      "storage_version",
      versionRows,
    );

    if (
      version.value === "2" ||
      version.value === "3" ||
      version.value === "4" ||
      version.value === "5"
    ) {
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const current = yield* sql<{
              value: string;
            }>`SELECT value FROM effect_agent_meta WHERE key='storage_version'`;

            if (current.length === 1 && current[0].value === String(CurrentDoStorageVersion))
              return;
            if (
              current.length !== 1 ||
              (current[0].value !== "2" &&
                current[0].value !== "3" &&
                current[0].value !== "4" &&
                current[0].value !== "5")
            )
              return yield* DoStorageCompatibilityError.make({
                actualVersion: -1,
                supportedVersion: CurrentDoStorageVersion,
                message: "Storage version changed while acquiring the upgrade transaction.",
              });

            const required = yield* decodeRows(
              Schema.Array(DoNameRow),
              "sqlite_master",
              "required_tables",
              yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name IN ${sql.in([...REQUIRED_TABLES])}`,
            );

            if (required.length !== REQUIRED_TABLES.length)
              return yield* DoStorageCompatibilityError.make({
                actualVersion: Number(current[0].value),
                supportedVersion: CurrentDoStorageVersion,
                message:
                  "The predecessor store is missing required tables. Retain the original store for inspection; no upgrade was committed.",
              });

            const recoveryTables =
              yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name='effect_agent_recovery_checkpoints'`;

            if (recoveryTables.length !== (current[0].value === "5" ? 1 : 0))
              return yield* DoStorageCompatibilityError.make({
                actualVersion: Number(current[0].value),
                supportedVersion: CurrentDoStorageVersion,
                message:
                  "The predecessor recovery checkpoint storage does not match its version; refusing ambiguous data without mutation.",
              });

            const indexes =
              yield* sql`SELECT name FROM sqlite_master WHERE name='effect_agent_submissions_nonterminal'`;

            if (indexes.length !== 0)
              return yield* DoStorageCompatibilityError.make({
                actualVersion: Number(current[0].value),
                supportedVersion: CurrentDoStorageVersion,
                message:
                  "The predecessor already contains the nonterminal index; refusing ambiguous storage without mutation.",
              });
            if (current[0].value === "2") {
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
            }
            if (current[0].value === "3") yield* checkPredecessorLayout(3);
            if (current[0].value === "4") yield* checkPredecessorLayout(4);
            if (current[0].value === "5") yield* checkPredecessorLayout(5);
            if (current[0].value === "2" || current[0].value === "3") {
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
            if (current[0].value !== "5") {
              yield* failpoint("upgrade:before-mutation");
              yield* createRecoveryCheckpointTable;
              yield* failpoint("upgrade:after-mutation");
            }
            yield* failpoint("upgrade:before-mutation");
            yield* createNonterminalIndex;
            yield* failpoint("upgrade:after-mutation");
            yield* failpoint("upgrade:before-version");
            yield* sql`UPDATE effect_agent_meta SET value='6' WHERE key='storage_version'`;
            yield* failpoint("upgrade:after-version");
          }),
        )
        .pipe(
          Effect.catchTag("DoStorageFailpointError", (error) =>
            DoStorageError.make({
              cause: error,
              operation: "upgrade storage",
              message: error.message,
            }),
          ),
          Effect.catchTag("StorageUpgradeError", (error) =>
            DoStorageCorruptionError.make({
              table: error.table,
              rowKey: error.rowKey,
              message: error.message,
            }),
          ),
          Effect.catchTag("SqlError", storageError("upgrade supported thread storage")),
        );
    } else if (version.value !== String(CurrentDoStorageVersion)) {
      const actualVersion = Number.parseInt(version.value, 10);

      return yield* DoStorageCompatibilityError.make({
        actualVersion: Number.isSafeInteger(actualVersion) ? actualVersion : -1,
        supportedVersion: CurrentDoStorageVersion,
        message:
          `The Durable Object uses unsupported storage version ${version.value}; ` +
          `this build supports exactly version ${CurrentDoStorageVersion}. ` +
          "Only supported v2, v3, v4 and v5 can be upgraded automatically. Keep the original store and use a compatible library version.",
      });
    }
  }

  const requiredRows = yield* sql<Record<string, unknown>>`
    SELECT name
    FROM sqlite_master
    WHERE (type = 'table'
      AND name IN ${sql.in([...REQUIRED_TABLES, "effect_agent_message_deliveries", "effect_agent_recovery_checkpoints"])}
    ) OR (type = 'index' AND name = 'effect_agent_submissions_nonterminal')
    ORDER BY name
  `.pipe(Effect.mapError(storageError("verify storage tables")));

  const required = yield* decodeRows(
    Schema.Array(DoNameRow),
    "sqlite_master",
    "required_tables",
    requiredRows,
  );

  if (required.length !== REQUIRED_TABLES.length + 3) {
    return yield* DoStorageCompatibilityError.make({
      actualVersion: CurrentDoStorageVersion,
      supportedVersion: CurrentDoStorageVersion,
      message:
        "The Durable Object claims the current format but is missing required tables or its nonterminal index. Retain the original store for inspection.",
    });
  }

  return makeJournal(sql, failpoint, maxStoredValueBytes);
});

const makeJournal = (
  sql: SqlClient.SqlClient,
  failpoint: DoJournalFailpoint,
  maxStoredValueBytes: number,
) => {
  /** Typed pre-write refusal for any single value over the configured byte bound. */
  const checkValueBound = (
    operation: string,
    value: string,
  ): Effect.Effect<void, DoValueBoundExceeded> => {
    const actualBytes = storedTextBytes(value);

    return actualBytes > maxStoredValueBytes
      ? Effect.fail(
          DoValueBoundExceeded.make({
            actualBytes,
            maxBytes: maxStoredValueBytes,
            operation,
          }),
        )
      : Effect.void;
  };

  /**
   * Runs one journal write transaction on the Durable Object storage-backed
   * `withTransaction` (`ctx.storage.transaction()` under the hood). Within one Durable
   * Object there is exactly ONE writer, so the Node `BEGIN IMMEDIATE` + busy-retry +
   * `SqliteWriteContention` machinery has no analogue here and is deliberately absent.
   * Ownership-token and epoch checks still run INSIDE the transaction, so fencing atomicity
   * (DUR-006) is preserved identically.
   *
   * Journal write transactions are always top level: the Durable Object client rejects
   * nested transactions, so new journal operations must not wrap this helper inside another
   * transaction.
   */
  const withWriteTransaction =
    (operation: string) =>
    <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E | DoStorageError> =>
      sql.withTransaction(effect).pipe(
        Effect.mapError((error) => (isSqlError(error) ? storageError(operation)(error) : error)),
        Effect.withSpan("DoJournal.withWriteTransaction", { attributes: { operation } }),
      );

  const materialize = Effect.fn("DoJournal.materialize")(function* (
    threadId: string,
    createdAt: string,
    emptyTailDigest: string,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<
    void,
    DoFenceRejected | DoStorageCorruptionError | DoStorageError | DoValueBoundExceeded
  > {
    if (threadId.length > MAX_IDENTIFIER_LENGTH) {
      return yield* DoStorageError.make({
        operation: "materialize thread",
        message: "Thread identity exceeds the Durable Object storage bounds.",
      });
    }
    yield* checkValueBound("materialize thread", emptyTailDigest);
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
          return yield* DoStorageCorruptionError.make({
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
          return yield* DoFenceRejected.make({
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

  const getThread = Effect.fn("DoJournal.getThread")(function* (threadId: string) {
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

  const append = Effect.fn("DoJournal.append")(function* (
    request: RawAppendRequest,
  ): Effect.fn.Return<RawAppendResult, AppendError> {
    if (
      request.threadId.length > MAX_IDENTIFIER_LENGTH ||
      request.batchId.length > MAX_IDENTIFIER_LENGTH ||
      request.records.some((record) => record.recordId.length > MAX_IDENTIFIER_LENGTH)
    ) {
      return yield* DoStorageError.make({
        operation: "append canonical batch",
        message: "Canonical identifiers exceed the Durable Object storage bounds.",
      });
    }
    // The platform's ~2 MB per-value limit, enforced typed BEFORE any write (plan §1.2).
    yield* checkValueBound("append canonical batch", request.batchJson);
    yield* checkValueBound("append canonical batch", request.batchDigest);
    yield* checkValueBound("append canonical batch", request.tailDigest);
    yield* Effect.forEach(
      request.records,
      (record) => checkValueBound("append canonical record", record.recordJson),
      { discard: true },
    );

    return yield* withWriteTransaction("append transaction")(
      Effect.gen(function* () {
        const recordIds = request.records.map((record) => record.recordId);

        if (new Set(recordIds).size !== recordIds.length) {
          return yield* DoAppendConflict.make({
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
          return yield* DoFenceRejected.make({
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
          return yield* DoStorageCorruptionError.make({
            table: "effect_agent_canonical_batches",
            rowKey: `${request.threadId}/${request.batchId}`,
            message: "A canonical batch primary key returned more than one row.",
          });
        }
        if (batches.length === 1) {
          const existing = batches[0];

          if (existing.batch_digest !== request.batchDigest) {
            return yield* DoAppendConflict.make({
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
          return yield* DoAppendConflict.make({
            message:
              `Expected tail ${request.expectedTailSequence}/${request.expectedTailDigest} ` +
              `but found ${thread.tail_sequence}/${thread.tail_digest}.`,
            reason: "tail",
            actualTailSequence: thread.tail_sequence,
            actualTailDigest: thread.tail_digest,
          });
        }
        if (thread.tail_sequence + request.records.length > MAX_RECORDS_PER_THREAD) {
          return yield* DoStorageError.make({
            operation: "append canonical batch",
            message: `Thread record limit ${MAX_RECORDS_PER_THREAD} would be exceeded.`,
          });
        }

        // Chunked to respect the Durable Object platform's 100-bound-parameter statement
        // limit: a batch may carry up to 256 records.
        const existingRecords: Array<RecordRow> = [];

        for (const chunk of chunked(recordIds, MAX_BOUND_PARAMETERS - 10)) {
          const existingRecordRows = yield* sql<Record<string, unknown>>`
            SELECT
              thread_id,
              sequence,
              record_id,
              batch_id,
              record_json
            FROM effect_agent_canonical_records
            WHERE thread_id = ${request.threadId}
              AND record_id IN ${sql.in([...chunk])}
            ORDER BY sequence
          `.pipe(Effect.mapError(storageError("check canonical record identities")));

          existingRecords.push(
            ...(yield* decodeRows(
              Schema.Array(RecordRow),
              "effect_agent_canonical_records",
              `${request.threadId}/record_ids`,
              existingRecordRows,
            )),
          );
        }
        if (existingRecords.length > 0) {
          return yield* DoAppendConflict.make({
            message: `Canonical record ID ${existingRecords[0].record_id} already exists.`,
            reason: "record-identity",
          });
        }

        const firstSequence = yield* Schema.decodeUnknownEffect(CanonicalSequence)(
          thread.tail_sequence + 1,
        ).pipe(
          Effect.mapError((error) =>
            DoStorageError.make({
              cause: error,
              operation: "append canonical batch",
              message: error.message,
            }),
          ),
        );

        const lastSequence = yield* Schema.decodeUnknownEffect(CanonicalSequence)(
          firstSequence + request.records.length - 1,
        ).pipe(
          Effect.mapError((error) =>
            DoStorageError.make({
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

  const read = Effect.fn("DoJournal.read")(function* (request: RawReadRequest) {
    // Capture membership without retaining payloads. Append-only sequences keep each later
    // payload query inside this snapshot, even when new records arrive during consumption.
    const planRows = yield* sql<Record<string, unknown>>`
      SELECT
        sequence,
        length(CAST(record_json AS BLOB)) AS record_json_bytes
      FROM effect_agent_canonical_records
      WHERE thread_id = ${request.threadId}
        AND sequence > ${request.fromSequenceExclusive}
      ORDER BY sequence
      LIMIT ${request.limit}
    `.pipe(Effect.mapError(storageError("read canonical records")));

    const plan = yield* decodeRows(
      Schema.Array(ReadPlanRow),
      "effect_agent_canonical_records",
      `${request.threadId}>${request.fromSequenceExclusive}`,
      planRows,
    );

    const mismatch = () =>
      DoStorageCorruptionError.make({
        table: "effect_agent_canonical_records",
        rowKey: `${request.threadId}>${request.fromSequenceExclusive}`,
        message: "Canonical read membership, sequence, or payload size changed from its read plan.",
      });

    if (plan.length > request.limit) return yield* mismatch();

    const pages: Array<ReadPage> = [];
    let page: ReadPage | undefined;
    let pageBytes = 0;
    let previousSequence = request.fromSequenceExclusive;

    for (const row of plan) {
      if (row.sequence !== previousSequence + 1) return yield* mismatch();
      previousSequence = row.sequence;
      if (page === undefined || pageBytes + row.record_json_bytes > MAX_READ_PAGE_JSON_BYTES) {
        page = [row];
        pages.push(page);
        pageBytes = row.record_json_bytes;
      } else {
        page.push(row);
        pageBytes += row.record_json_bytes;
      }
    }

    const readPage = Effect.fn("DoJournal.readPage")(function* (page: ReadPage) {
      const rows = yield* sql<Record<string, unknown>>`
        SELECT thread_id, sequence, record_id, batch_id, record_json
        FROM effect_agent_canonical_records
        WHERE thread_id = ${request.threadId}
          AND sequence >= ${page[0].sequence}
          AND sequence <= ${page[page.length - 1].sequence}
        ORDER BY sequence
      `.pipe(Effect.mapError(storageError("read canonical records")));

      const decoded = yield* decodeRows(
        Schema.Array(RecordRow),
        "effect_agent_canonical_records",
        `${request.threadId}/${page[0].sequence}`,
        rows,
      );

      if (
        decoded.length !== page.length ||
        decoded.some(
          (row, index) =>
            row.thread_id !== request.threadId ||
            row.sequence !== page[index].sequence ||
            storedTextBytes(row.record_json) !== page[index].record_json_bytes,
        )
      ) {
        return yield* mismatch();
      }

      return decoded;
    });

    return {
      count: plan.length,
      records: Stream.fromIterable(pages).pipe(
        Stream.flatMap((page) => Stream.fromIterableEffect(readPage(page))),
      ),
    };
  });

  const exportThread = Effect.fn("DoJournal.exportThread")(function* (threadId: string) {
    return yield* sql
      .withTransaction(
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
            return yield* DoStorageError.make({
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

            const plan = yield* read(request);
            const page = yield* Stream.runCollect(plan.records);

            if (
              page.length !== limit ||
              page.some((record, index) => record.sequence !== afterSequence + index + 1)
            ) {
              return yield* DoStorageCorruptionError.make({
                table: "effect_agent_canonical_records",
                rowKey: threadId,
                message:
                  "The exported canonical prefix is not contiguous through its captured tail.",
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
            return yield* DoStorageCorruptionError.make({
              table: "effect_agent_canonical_records",
              rowKey: threadId,
              message: "Canonical records exist beyond the captured thread tail.",
            });

          return RawThreadExport.make({ thread, records });
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", (error) =>
          Effect.fail(storageError("export transaction")(error)),
        ),
      );
  });

  const saveCheckpoint = Effect.fn("DoJournal.saveCheckpoint")(function* (
    checkpoint: RawCheckpoint,
  ): Effect.fn.Return<void, CheckpointError> {
    if (checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH) {
      return yield* DoStorageError.make({
        operation: "save checkpoint",
        message: "Checkpoint identity exceeds the Durable Object storage bounds.",
      });
    }
    yield* checkValueBound("save checkpoint", checkpoint.checkpointJson);
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
          return yield* DoCheckpointConflict.make({
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
          return yield* DoStorageCorruptionError.make({
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
            return yield* DoCheckpointConflict.make({
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

  const saveRecoveryCheckpoint = Effect.fn("DoJournal.saveRecoveryCheckpoint")(function* (
    request: SaveRecoveryCheckpointRequest,
    checkpointJson: string,
  ) {
    const { checkpoint } = request;

    if (checkpoint.threadId.length > MAX_IDENTIFIER_LENGTH) {
      return yield* DoStorageError.make({
        operation: "save recovery checkpoint",
        message: "Checkpoint identity exceeds the Durable Object storage bounds.",
      });
    }
    yield* checkValueBound("save recovery checkpoint", checkpointJson);
    // Keep injected waits outside the storage-backed transaction callback.
    yield* failpoint("save-recovery-checkpoint:before");
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

  const loadRecoveryCheckpoint = Effect.fn("DoJournal.loadRecoveryCheckpoint")(function* (
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

  const loadCheckpoint = Effect.fn("DoJournal.loadCheckpoint")(function* (
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

  const getTailDigestAt = Effect.fn("DoJournal.getTailDigestAt")(function* (
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

  const scanStoredPayloads = Effect.fn("DoJournal.scanStoredPayloads")(function* () {
    return yield* sql
      .withTransaction(
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
      )
      .pipe(
        Effect.catchTag("SqlError", (error) =>
          Effect.fail(storageError("startup scan transaction")(error)),
        ),
      );
  });

  return {
    append,
    checkValueBound,
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
  } as const;
};

export type DoJournal = ReturnType<typeof makeJournal>;

export const initializeDoJournal = ensureCurrentStorage;
