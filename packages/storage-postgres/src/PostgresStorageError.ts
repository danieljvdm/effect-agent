import { SqlStorageFailpointLocation } from "@effect-agent/storage-sql/sql-storage-failpoint";
import { Schema } from "effect";

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

export const PostgresStorageFailpointLocation = SqlStorageFailpointLocation;

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

/** Every way opening Postgres storage can fail before it serves a request. */
export type PostgresStorageInitializationError =
  | PostgresStorageCompatibilityError
  | PostgresStorageCorruptionError
  | PostgresStorageError
  | PostgresWriteContention;
