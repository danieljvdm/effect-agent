import { Duration, Effect, Layer, Schema, Context } from "effect";
import { DEFAULT_OWNERSHIP_LEASE_DURATION } from "effect-agent/submission-ledger";

import type { PostgresClientOptions } from "./PostgresStorageClient.ts";
import { PostgresStorageError } from "./PostgresStorageError.ts";
import type { PostgresStorageFailpointHandler } from "./PostgresStorageFailpoint.ts";

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
   * Bounded wait on the writer lock, in milliseconds. Exceeding it fails with the retryable
   * `PostgresWriteContention` rather than holding a pooled connection indefinitely.
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
  /** Postgres schema holding the adapter's tables, verified against the connection at startup. */
  schema: SchemaName,
}) {}

/** Explicit Postgres storage configuration authority. */
export class PostgresStorageConfig extends Context.Service<
  PostgresStorageConfig,
  PostgresStorageConfigValue
>()("@effect-agent/storage-postgres/PostgresStorageConfig") {}

export interface PostgresStorageOptions {
  readonly client: PostgresClientOptions;
  /**
   * Postgres schema holding the adapter's tables, created if absent. Defaults to `public`.
   *
   * Selecting any other schema requires it to be the *connection's* default, because
   * `search_path` binds per connection and this driver exposes no way to set one for a pool.
   * Set it with `ALTER ROLE ... SET search_path` or `ALTER DATABASE ... SET search_path`; the
   * adapter verifies the effective schema at startup and refuses to run if it disagrees.
   */
  readonly schema?: string | undefined;
  readonly observationPollInterval?: number | undefined;
  readonly lockTimeout?: number | undefined;
  readonly ownershipLeaseDuration?: number | undefined;
  readonly verifyOnOpen?: boolean | undefined;
  readonly failpoint?: PostgresStorageFailpointHandler | undefined;
}

/**
 * Validated Postgres storage configuration Layer with the documented defaults applied. Shared
 * by the ThreadStore and SubmissionLedger convenience layers so their defaults cannot
 * drift.
 */
export const layerConfig = (
  options: PostgresStorageOptions,
): Layer.Layer<PostgresStorageConfig, PostgresStorageError> =>
  Layer.effect(PostgresStorageConfig)(
    Schema.decodeEffect(PostgresStorageConfigValue)({
      observationPollInterval: options.observationPollInterval ?? 25,
      lockTimeout: options.lockTimeout ?? 5_000,
      schema: options.schema ?? "public",
      ownershipLeaseDuration:
        options.ownershipLeaseDuration ?? Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION),
      verifyOnOpen: options.verifyOnOpen ?? false,
    }).pipe(
      Effect.mapError((error) =>
        PostgresStorageError.make({
          cause: error,
          operation: "configure Postgres storage",
          message: error.message,
        }),
      ),
    ),
  );
