import { Context, Schema } from "effect";

const ObservationPollInterval = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** Validated construction configuration consumed by the SQLite storage Layer. */
export class SqliteStorageConfigValue extends Schema.Class<SqliteStorageConfigValue>(
  "@effect-agent/storage-sqlite/SqliteStorageConfigValue",
)({
  filename: Schema.NonEmptyString,
  observationPollInterval: ObservationPollInterval,
}) {}

/** Explicit SQLite storage configuration authority. */
export class SqliteStorageConfig extends Context.Service<
  SqliteStorageConfig,
  SqliteStorageConfigValue
>()("@effect-agent/storage-sqlite/SqliteStorageConfig") {}
