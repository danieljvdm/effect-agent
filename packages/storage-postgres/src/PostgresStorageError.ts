import { Schema } from "effect";
import { CanonicalSequence, ProducerEpoch } from "effect-agent/records";

/** The database uses a private-development storage format this adapter cannot read. */
export class PostgresStorageCompatibilityError extends Schema.TaggedError<PostgresStorageCompatibilityError>()(
  "PostgresStorageCompatibilityError",
  {
    actualVersion: Schema.Int,
    message: Schema.String,
    supportedVersion: Schema.Int,
  },
) {}

/** Stored bytes failed the current Schema and cannot be used as recovery truth. */
export class PostgresStorageCorruptionError extends Schema.TaggedError<PostgresStorageCorruptionError>()(
  "PostgresStorageCorruptionError",
  {
    message: Schema.String,
    rowKey: Schema.String,
    table: Schema.String,
  },
) {}

/** Postgres infrastructure failed while opening or operating the store. */
export class PostgresStorageError extends Schema.TaggedError<PostgresStorageError>()(
  "PostgresStorageError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/**
 * Postgres infrastructure failed while operating the Submission Ledger. Surfaces at the
 * SubmissionLedger port as the typed `LedgerError` with this error preserved as its cause,
 * so the adapter-level tag is never erased.
 */
export class PostgresLedgerError extends Schema.TaggedError<PostgresLedgerError>()(
  "PostgresLedgerError",
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
export class PostgresAppendConflict extends Schema.TaggedError<PostgresAppendConflict>()(
  "PostgresAppendConflict",
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
export class PostgresFenceRejected extends Schema.TaggedError<PostgresFenceRejected>()(
  "PostgresFenceRejected",
  {
    actualEpoch: ProducerEpoch,
    message: Schema.String,
    producerEpoch: ProducerEpoch,
  },
) {}

/**
 * A write transaction lost a concurrency race and was rolled back by Postgres: a
 * serialization failure (40001), a deadlock (40P01), or a lock timeout (55P03). The
 * transaction mutated no canonical state and is safe to retry.
 */
export class PostgresWriteContention extends Schema.TaggedError<PostgresWriteContention>()(
  "PostgresWriteContention",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/** A checkpoint conflicts with a previously stored checkpoint at the same offset. */
export class PostgresCheckpointConflict extends Schema.TaggedError<PostgresCheckpointConflict>()(
  "PostgresCheckpointConflict",
  {
    message: Schema.String,
  },
) {}

export const PostgresStorageFailpointLocation = Schema.Literals([
  "upgrade:before-mutation",
  "upgrade:after-mutation",
  "upgrade:before-version",
  "upgrade:after-version",
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
  "save-recovery-checkpoint:before",
  "save-recovery-checkpoint:after",
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

export type PostgresStorageFailpointLocation = typeof PostgresStorageFailpointLocation.Type;

/** Deterministic test-only fault or pause injected at a Postgres operation boundary. */
export class PostgresStorageFailpointError extends Schema.TaggedError<PostgresStorageFailpointError>()(
  "PostgresStorageFailpointError",
  {
    location: PostgresStorageFailpointLocation,
  },
) {
  override get message() {
    return `Injected Postgres storage failure at ${this.location}.`;
  }
}
