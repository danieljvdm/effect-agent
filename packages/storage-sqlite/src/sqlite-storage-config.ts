import { Context, Schema } from "effect";

const ObservationPollInterval = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const BusyTimeoutMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/**
 * Validated construction configuration consumed by the SQLite storage Layer. The database
 * identity itself belongs to the SqlClient Layer; duplicating it here could silently diverge
 * from the connection actually in use.
 */
export class SqliteStorageConfigValue extends Schema.Class<SqliteStorageConfigValue>(
  "@effect-agent/storage-sqlite/SqliteStorageConfigValue",
)({
  observationPollInterval: ObservationPollInterval,
  /** Bounded SQLITE_BUSY retry window for write-lock acquisition, in milliseconds. */
  busyTimeout: BusyTimeoutMillis,
  /**
   * Re-verify every stored payload and digest chain while opening the store. Per-operation
   * Schema decoding and the digest chain already fail clearly on corrupt rows, so the full
   * scan is an explicit opt-in integrity audit rather than a startup requirement.
   */
  verifyOnOpen: Schema.Boolean,
}) {}

/** Explicit SQLite storage configuration authority. */
export class SqliteStorageConfig extends Context.Service<
  SqliteStorageConfig,
  SqliteStorageConfigValue
>()("@effect-agent/storage-sqlite/SqliteStorageConfig") {}
