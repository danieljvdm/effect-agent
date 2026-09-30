import { Context, Effect, Schema } from "effect";

/** Failure to enroll newly committed execution work. The source transaction must roll back. */
export class SqlStorageProgressError extends Schema.TaggedError<SqlStorageProgressError>()(
  "SqlStorageProgressError",
  {
    operation: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

export type SqlStorageProgressKind =
  | "canonical"
  | "submission"
  | "control"
  | "lifecycle"
  | "delivery";

/**
 * Source-transaction enrollment for durable maintenance. Call only after a new fact was
 * written, before its transaction commits. Replays, claims, leases and retry clocks are not
 * progress. Platforms may atomically replenish their finite maintenance budget and alarm;
 * ordinary SQL consumers do not need an alarm implementation.
 */
export const SqlStorageProgress = Context.Reference<{
  readonly committed: (
    kind: SqlStorageProgressKind,
  ) => Effect.Effect<void, SqlStorageProgressError>;
}>("@effect-agent/storage-sql/SqlStorageProgress", {
  defaultValue: () => ({ committed: () => Effect.void }),
});
