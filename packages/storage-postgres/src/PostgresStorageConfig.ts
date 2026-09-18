import { Context, Schema } from "effect";

const ObservationPollInterval = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const LockTimeoutMillis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const OwnershipLeaseMillis = Schema.Int.check(Schema.isGreaterThan(0));

const SchemaName = Schema.NonEmptyString.check(
  Schema.isMaxLength(63),
  Schema.isPattern(/^[a-z_][a-z0-9_]*$/),
);

/**
 * Validated construction configuration consumed by the Postgres storage Layer. The database
 * identity itself belongs to the SqlClient Layer; duplicating it here could silently diverge
 * from the connection actually in use.
 */
export class PostgresStorageConfigValue extends Schema.Class<PostgresStorageConfigValue>(
  "@effect-agent/storage-postgres/PostgresStorageConfigValue",
)({
  observationPollInterval: ObservationPollInterval,
  /**
   * Bounded `lock_timeout` applied to write transactions, in milliseconds. A transaction that
   * cannot take a contended row lock inside this window fails with `PostgresWriteContention`
   * instead of blocking a connection indefinitely.
   */
  lockTimeout: LockTimeoutMillis,
  /**
   * Submission ownership lease duration in milliseconds (D5). The lease is a liveness hint
   * that makes an abandoned claim reclaimable; correctness never depends on it because every
   * canonical append is fenced by producer epoch. Convenience layers default this to
   * `DEFAULT_OWNERSHIP_LEASE_DURATION` from `effect-agent/submission-ledger`.
   */
  ownershipLeaseDuration: OwnershipLeaseMillis,
  /**
   * Re-verify every stored payload and digest chain while opening the store. Per-operation
   * Schema decoding and the digest chain already fail clearly on corrupt rows, so the full
   * scan is an explicit opt-in integrity audit rather than a startup requirement.
   */
  verifyOnOpen: Schema.Boolean,
  /**
   * Postgres schema holding the adapter's tables. A dedicated schema keeps agent storage
   * separable from application tables in the same database; it is created if absent.
   */
  schema: SchemaName,
}) {}

/** Explicit Postgres storage configuration authority. */
export class PostgresStorageConfig extends Context.Service<
  PostgresStorageConfig,
  PostgresStorageConfigValue
>()("@effect-agent/storage-postgres/PostgresStorageConfig") {}
