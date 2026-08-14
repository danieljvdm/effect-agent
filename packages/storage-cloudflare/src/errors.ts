import { CanonicalSequence, ProducerEpoch } from "@effect-agent/session";
import { Schema } from "effect";

/** The Durable Object's SQLite storage uses a private-development format this adapter cannot read. */
export class DoStorageCompatibilityError extends Schema.TaggedError<DoStorageCompatibilityError>()(
  "DoStorageCompatibilityError",
  {
    actualVersion: Schema.Int,
    message: Schema.String,
    supportedVersion: Schema.Int,
  },
) {}

/** Stored bytes failed the current Schema and cannot be used as recovery truth. */
export class DoStorageCorruptionError extends Schema.TaggedError<DoStorageCorruptionError>()(
  "DoStorageCorruptionError",
  {
    message: Schema.String,
    rowKey: Schema.String,
    table: Schema.String,
  },
) {}

/** Durable Object SQLite infrastructure failed while opening or operating the store. */
export class DoStorageError extends Schema.TaggedError<DoStorageError>()("DoStorageError", {
  cause: Schema.optionalKey(Schema.Defect()),
  message: Schema.String,
  operation: Schema.String,
}) {}

/**
 * Durable Object SQLite infrastructure failed while operating the Submission Ledger. Surfaces
 * at the SubmissionLedger port as the typed `LedgerError` with this error preserved as its
 * cause, so the adapter-level tag is never erased.
 */
export class DoLedgerError extends Schema.TaggedError<DoLedgerError>()("DoLedgerError", {
  cause: Schema.optionalKey(Schema.Defect()),
  message: Schema.String,
  operation: Schema.String,
}) {}

/**
 * A value to be stored exceeds the configured Durable Object per-value bound
 * (`DoStorageConfigValue.maxStoredValueBytes`, kept under the platform's 2 MB SQLite value
 * limit). The refusal happens typed BEFORE any durable mutation; no partial state is written.
 * Payloads of this size are the designed overflow case for a future R2-backed AttachmentStore
 * (deployment spec §3.1, deferred until a real attachment requirement exists).
 */
export class DoValueBoundExceeded extends Schema.TaggedError<DoValueBoundExceeded>()(
  "DoValueBoundExceeded",
  {
    actualBytes: Schema.Int,
    maxBytes: Schema.Int,
    operation: Schema.String,
  },
) {
  override get message() {
    return (
      `A stored value of ${this.actualBytes} bytes exceeds the Durable Object per-value bound ` +
      `of ${this.maxBytes} bytes during ${this.operation}. Nothing was written. Values of this ` +
      "size are the designed R2 AttachmentStore overflow path (deferred, deployment spec §3.1)."
    );
  }
}

/**
 * A canonical batch retry conflicts with existing append state. Tail conflicts carry the
 * actual committed tail as a diagnostic resume hint.
 */
export class DoAppendConflict extends Schema.TaggedError<DoAppendConflict>()("DoAppendConflict", {
  message: Schema.String,
  reason: Schema.Literals(["batch-digest", "record-identity", "tail"]),
  actualTailSequence: Schema.optionalKey(CanonicalSequence),
  actualTailDigest: Schema.optionalKey(Schema.String),
}) {}

/**
 * A producer epoch does not match the Conversation's current writer registration. Appends
 * require the exact registered epoch, so both older and newer unregistered epochs are fenced;
 * a newer epoch takes over by materializing first.
 */
export class DoFenceRejected extends Schema.TaggedError<DoFenceRejected>()("DoFenceRejected", {
  actualEpoch: ProducerEpoch,
  message: Schema.String,
  producerEpoch: ProducerEpoch,
}) {}

/** A checkpoint conflicts with a previously stored checkpoint at the same offset. */
export class DoCheckpointConflict extends Schema.TaggedError<DoCheckpointConflict>()(
  "DoCheckpointConflict",
  {
    message: Schema.String,
  },
) {}

/**
 * Deterministic fault-injection locations at Durable Object storage operation boundaries.
 *
 * The string list is copied VERBATIM from `SqliteStorageFailpointLocation`
 * (`packages/storage-sqlite/src/errors.ts`) so every crash-matrix row keeps the same name on
 * both platforms — the DN process-kill evidence and the DC eviction evidence address identical
 * locations. There is intentionally no Cloudflare-only location.
 */
export const DoStorageFailpointLocation = Schema.Literals([
  "materialize:before",
  "materialize:after",
  "append:before",
  "append:after-batch-insert",
  "append:after-record-insert",
  "append:after-tail-update",
  "append:after",
  "export:after-conversation-read",
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
export type DoStorageFailpointLocation = typeof DoStorageFailpointLocation.Type;

/** Deterministic test-only fault or pause injected at a Durable Object storage boundary. */
export class DoStorageFailpointError extends Schema.TaggedError<DoStorageFailpointError>()(
  "DoStorageFailpointError",
  {
    location: DoStorageFailpointLocation,
  },
) {
  override get message() {
    return `Injected Durable Object storage failure at ${this.location}.`;
  }
}
