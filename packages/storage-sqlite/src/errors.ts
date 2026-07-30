import { Schema } from "effect";

/** The SQLite file uses a private-development format this adapter cannot read. */
export class SqliteStorageCompatibilityError extends Schema.TaggedErrorClass<SqliteStorageCompatibilityError>()(
  "SqliteStorageCompatibilityError",
  {
    actualVersion: Schema.Int,
    message: Schema.String,
    supportedVersion: Schema.Int,
  },
) {}

/** Stored bytes failed the current Schema and cannot be used as recovery truth. */
export class SqliteStorageCorruptionError extends Schema.TaggedErrorClass<SqliteStorageCorruptionError>()(
  "SqliteStorageCorruptionError",
  {
    message: Schema.String,
    rowKey: Schema.String,
    table: Schema.String,
  },
) {}

/** SQLite infrastructure failed while opening or operating the store. */
export class SqliteStorageError extends Schema.TaggedErrorClass<SqliteStorageError>()(
  "SqliteStorageError",
  {
    message: Schema.String,
    operation: Schema.String,
  },
) {}

/** A canonical batch retry conflicts with existing append state. */
export class SqliteAppendConflict extends Schema.TaggedErrorClass<SqliteAppendConflict>()(
  "SqliteAppendConflict",
  {
    message: Schema.String,
  },
) {}

/** A producer epoch is older than the current conversation writer. */
export class SqliteFenceRejected extends Schema.TaggedErrorClass<SqliteFenceRejected>()(
  "SqliteFenceRejected",
  {
    actualEpoch: Schema.Int,
    message: Schema.String,
    producerEpoch: Schema.Int,
  },
) {}

/** A checkpoint conflicts with a previously stored checkpoint at the same offset. */
export class SqliteCheckpointConflict extends Schema.TaggedErrorClass<SqliteCheckpointConflict>()(
  "SqliteCheckpointConflict",
  {
    message: Schema.String,
  },
) {}

export const SqliteStorageFailpointLocation = Schema.Literals([
  "materialize:before",
  "materialize:after",
  "append:before",
  "append:after",
  "save-checkpoint:before",
  "save-checkpoint:after",
]);
export type SqliteStorageFailpointLocation = typeof SqliteStorageFailpointLocation.Type;

/** Deterministic test-only fault injected at a durable SQLite mutation boundary. */
export class SqliteStorageFailpointError extends Schema.TaggedErrorClass<SqliteStorageFailpointError>()(
  "SqliteStorageFailpointError",
  {
    location: SqliteStorageFailpointLocation,
  },
) {
  override get message() {
    return `Injected SQLite storage failure at ${this.location}.`;
  }
}
