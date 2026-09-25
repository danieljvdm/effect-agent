import { SqlStorageFailpointLocation as SqliteStorageFailpointLocation } from "@effect-agent/storage-sql/sql-storage-failpoint";
import { Schema } from "effect";
import { CanonicalSequence, ProducerEpoch } from "effect-agent/records";

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

export { SqlStorageFailpointLocation as SqliteStorageFailpointLocation } from "@effect-agent/storage-sql/sql-storage-failpoint";

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
