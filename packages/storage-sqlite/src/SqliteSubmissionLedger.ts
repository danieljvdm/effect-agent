import { makeSqlSubmissionLedger } from "@effect-agent/storage-sql/sql-submission-ledger";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import type { Crypto } from "effect";
import { Effect, Layer } from "effect";
import { LedgerError, SubmissionLedger } from "effect-agent/submission-ledger";
import type * as SqlClientService from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { initializeSqliteJournal, sqliteErrors } from "./internal/sqlite-journal.ts";
import { SqliteStorageConfig } from "./SqliteStorageConfig.ts";
import { SqliteLedgerError, SqliteWriteContention } from "./SqliteStorageError.ts";
import { SqliteStorageFailpoint } from "./SqliteStorageFailpoint.ts";
import {
  storageConfigLayer,
  storageFailpointLayer,
  type SqliteStorageInitializationError,
  type SqliteStorageOptions,
} from "./SqliteThreadStore.ts";

/** Wrap an adapter-internal failure into the port's LedgerError without erasing its tag. */
const internalFailure =
  (operation: string) =>
  (error: { readonly message: string }): LedgerError =>
    LedgerError.make({ operation, message: error.message, cause: error });

/** Classify raw SQL failures: write-lock timeouts stay retryable typed contention. */
const sqlFailure =
  (operation: string) =>
  (error: SqlError): LedgerError => {
    const internal =
      error.reason._tag === "LockTimeoutError"
        ? SqliteWriteContention.make({
            cause: error,
            operation,
            message: `Another producer holds the SQLite write lock; ${operation} is safe to retry.`,
          })
        : SqliteLedgerError.make({
            cause: error,
            operation,
            message: error.message,
          });

    return internalFailure(operation)(internal);
  };

const makeServices = Effect.fn("SqliteSubmissionLedger.makeServices")(function* () {
  const config = yield* SqliteStorageConfig;
  const failpoint = yield* SqliteStorageFailpoint;
  const journal = yield* initializeSqliteJournal();

  return yield* makeSqlSubmissionLedger(journal, {
    errors: sqliteErrors,
    hitFailpoint: failpoint.hit,
    ownershipLeaseDuration: config.ownershipLeaseDuration,
    sqlFailure,
  });
});

/**
 * SQLite SubmissionLedger implementation sharing the journal's database file, write
 * transaction discipline, and producer-epoch fencing substrate. Configuration, failpoint,
 * SQL, and Crypto authority stay visible in the input channel.
 */
export const submissionLedgerLayer: Layer.Layer<
  SubmissionLedger,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effect(SubmissionLedger, makeServices());

/**
 * A composition-root convenience Layer for the durable Submission Ledger. Point it at the
 * same database file as the ThreadStore so claims fence the same producer epochs.
 */
export const ledgerLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SubmissionLedger, SqliteStorageInitializationError> =>
  Layer.unwrap(
    Effect.map(SqliteStorageConfig, (config) =>
      submissionLedgerLayer.pipe(
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
