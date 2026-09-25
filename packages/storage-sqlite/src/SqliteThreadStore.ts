import { makeSqlThreadStore } from "@effect-agent/storage-sql/sql-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import type { Crypto } from "effect";
import { Duration, Effect, Layer, Schema } from "effect";
import { DEFAULT_OWNERSHIP_LEASE_DURATION } from "effect-agent/submission-ledger";
import type { ThreadStore } from "effect-agent/thread-store";
import type * as SqlClientService from "effect/unstable/sql/SqlClient";

import { initializeSqliteJournal, sqliteErrors } from "./internal/sqlite-journal.ts";
import { SqliteStorageConfig, SqliteStorageConfigValue } from "./SqliteStorageConfig.ts";
import type {
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
} from "./SqliteStorageError.ts";
import { SqliteStorageError } from "./SqliteStorageError.ts";
import {
  SqliteStorageFailpoint,
  type SqliteStorageFailpointHandler,
} from "./SqliteStorageFailpoint.ts";

export interface SqliteStorageOptions {
  readonly filename: string;
  readonly observationPollInterval?: number | undefined;
  /** Bounded SQLITE_BUSY retry window for write-lock acquisition, in milliseconds. */
  readonly busyTimeout?: number | undefined;
  /**
   * Submission ownership lease duration in milliseconds (D5). Defaults to
   * `DEFAULT_OWNERSHIP_LEASE_DURATION` from `effect-agent/submission-ledger`.
   */
  readonly ownershipLeaseDuration?: number | undefined;
  /**
   * Re-verify every stored payload and digest chain while opening the store. Defaults to
   * off: per-operation Schema decoding and the digest chain already fail clearly on corrupt
   * rows without scanning the whole database on every open.
   */
  readonly verifyOnOpen?: boolean | undefined;
  readonly failpoint?: SqliteStorageFailpointHandler | undefined;
}

export type SqliteStorageInitializationError =
  | SqliteStorageCompatibilityError
  | SqliteStorageCorruptionError
  | SqliteStorageError;

const makeServices = Effect.fn("SqliteThreadStore.makeServices")(function* () {
  const config = yield* SqliteStorageConfig;
  const failpoint = yield* SqliteStorageFailpoint;
  const journal = yield* initializeSqliteJournal();

  return yield* makeSqlThreadStore(journal, {
    ...config,
    errors: sqliteErrors,
    hitFailpoint: failpoint.hit,
    offsetPrefix: "effect-agent-sqlite@1:",
  });
});

/**
 * SQLite Thread Store implementation with configuration, failpoint, SQL, and Crypto
 * authority kept visible in its input channel.
 */
export const threadStoreLayer: Layer.Layer<
  ThreadStore,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(makeServices());

/**
 * Validated SQLite storage configuration Layer with the documented defaults applied. Shared
 * by the ThreadStore and SubmissionLedger convenience layers so their defaults cannot
 * drift.
 */
export const storageConfigLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SqliteStorageConfig, SqliteStorageError> =>
  Layer.effect(SqliteStorageConfig)(
    Schema.decodeEffect(SqliteStorageConfigValue)({
      observationPollInterval: options.observationPollInterval ?? 25,
      busyTimeout: options.busyTimeout ?? 5_000,
      ownershipLeaseDuration:
        options.ownershipLeaseDuration ?? Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION),
      verifyOnOpen: options.verifyOnOpen ?? false,
    }).pipe(
      Effect.mapError((error) =>
        SqliteStorageError.make({
          cause: error,
          operation: "configure SQLite storage",
          message: error.message,
        }),
      ),
    ),
  );

/** The failpoint Layer selected by convenience options: explicit handler or the no-op default. */
export const storageFailpointLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SqliteStorageFailpoint> =>
  options.failpoint === undefined
    ? SqliteStorageFailpoint.layer
    : Layer.succeed(SqliteStorageFailpoint)({ hit: options.failpoint });

/**
 * A composition-root convenience Layer for canonical Threads. Durable accepted work is
 * served by the separate SubmissionLedger port.
 */
export const layer = (
  options: SqliteStorageOptions,
): Layer.Layer<ThreadStore, SqliteStorageInitializationError> =>
  Layer.unwrap(
    Effect.map(SqliteStorageConfig, (config) =>
      threadStoreLayer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqliteStorageConfig)(config),
            storageFailpointLayer(options),
            SqliteClient.layer({ filename: options.filename }),
            NodeCrypto.layer,
          ),
        ),
      ),
    ),
  ).pipe(Layer.provide(storageConfigLayer(options)));
