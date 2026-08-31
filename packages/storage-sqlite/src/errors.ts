import { CanonicalSequence, ProducerEpoch } from "@effect-agent/thread";
import { Schema } from "effect";

/** The SQLite file uses a private-development format this adapter cannot read. */
export class SqliteStorageCompatibilityError extends Schema.TaggedError<SqliteStorageCompatibilityError>()(
  "SqliteStorageCompatibilityError",
  {
    actualVersion: Schema.Int,
    message: Schema.String,
    supportedVersion: Schema.Int,
  },
) {}

/** Stored bytes failed the current Schema and cannot be used as recovery truth. */
export class SqliteStorageCorruptionError extends Schema.TaggedError<SqliteStorageCorruptionError>()(
  "SqliteStorageCorruptionError",
  {
    message: Schema.String,
    rowKey: Schema.String,
    table: Schema.String,
  },
) {}

/** SQLite infrastructure failed while opening or operating the store. */
export class SqliteStorageError extends Schema.TaggedError<SqliteStorageError>()(
  "SqliteStorageError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/**
 * SQLite infrastructure failed while operating the Submission Ledger. Surfaces at the
 * SubmissionLedger port as the typed `LedgerError` with this error preserved as its cause,
 * so the adapter-level tag is never erased.
 */
export class SqliteLedgerError extends Schema.TaggedError<SqliteLedgerError>()(
  "SqliteLedgerError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/**
 * A canonical batch retry conflicts with existing append state. Tail conflicts carry the
 * actual committed tail as a diagnostic resume hint.
 */
export class SqliteAppendConflict extends Schema.TaggedError<SqliteAppendConflict>()(
  "SqliteAppendConflict",
  {
    message: Schema.String,
    reason: Schema.Literals(["batch-digest", "record-identity", "tail"]),
    actualTailSequence: Schema.optionalKey(CanonicalSequence),
    actualTailDigest: Schema.optionalKey(Schema.String),
  },
) {}

/**
 * A producer epoch does not match the Thread's current writer registration. Appends
 * require the exact registered epoch, so both older and newer unregistered epochs are fenced;
 * a newer epoch takes over by materializing first.
 */
export class SqliteFenceRejected extends Schema.TaggedError<SqliteFenceRejected>()(
  "SqliteFenceRejected",
  {
    actualEpoch: ProducerEpoch,
    message: Schema.String,
    producerEpoch: ProducerEpoch,
  },
) {}

/**
 * A write transaction could not acquire the SQLite write lock within the configured busy
 * timeout (SQLITE_BUSY / SQLITE_LOCKED). Another transiently coexisting owner is writing;
 * the operation did not mutate canonical state and is safe to retry.
 */
export class SqliteWriteContention extends Schema.TaggedError<SqliteWriteContention>()(
  "SqliteWriteContention",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/** A checkpoint conflicts with a previously stored checkpoint at the same offset. */
export class SqliteCheckpointConflict extends Schema.TaggedError<SqliteCheckpointConflict>()(
  "SqliteCheckpointConflict",
  {
    message: Schema.String,
  },
) {}

export const SqliteStorageFailpointLocation = Schema.Literals([
  "materialize:before",
  "materialize:after",
  "append:before",
  "append:after-batch-insert",
  "append:after-record-insert",
  "append:after-tail-update",
  "append:after",
  "export:after-thread-read",
  "save-checkpoint:before",
  "save-checkpoint:after",
  "ledger:admit:before",
  "ledger:admit:after",
  "ledger:mark-ready:before",
  "ledger:mark-ready:after",
  "ledger:claim:before",
  "ledger:claim:after",
  "ledger:mark-input-applied:before",
  "ledger:mark-input-applied:after",
  "ledger:renew:before",
  "ledger:renew:after",
  "ledger:reserve-settlement:before",
  "ledger:reserve-settlement:after",
  "ledger:finalize-settlement:before",
  "ledger:finalize-settlement:after",
  "ledger:request-abort:before",
  "ledger:request-abort:after",
  "ledger:release:before",
  "ledger:release:after",
  "ledger:claim-joining:before",
  "ledger:claim-joining:after",
  "ledger:mark-joined:before",
  "ledger:mark-joined:after",
  "ledger:revert-joining:before",
  "ledger:revert-joining:after",
  "ledger:suspend:before",
  "ledger:suspend:after",
  "ledger:approval-decision:before",
  "ledger:approval-decision:after",
  "ledger:mark-unknown:before",
  "ledger:mark-unknown:after",
  "ledger:unknown-resolution:before",
  "ledger:unknown-resolution:after",
  "ledger:child-reservation:before",
  "ledger:child-reservation:after",
  "ledger:child-attach:before",
  "ledger:child-attach:after",
  "ledger:child-release-pending:before",
  "ledger:child-release-pending:after",
  "ledger:child-release:before",
  "ledger:child-release:after",
  "ledger:child-settled:before",
  "ledger:child-settled:after",
]);
export type SqliteStorageFailpointLocation = typeof SqliteStorageFailpointLocation.Type;

/** Deterministic test-only fault or pause injected at a SQLite operation boundary. */
export class SqliteStorageFailpointError extends Schema.TaggedError<SqliteStorageFailpointError>()(
  "SqliteStorageFailpointError",
  {
    location: SqliteStorageFailpointLocation,
  },
) {
  override get message() {
    return `Injected SQLite storage failure at ${this.location}.`;
  }
}
