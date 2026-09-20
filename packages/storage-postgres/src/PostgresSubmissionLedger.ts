import { makeSqlSubmissionLedger } from "@effect-agent/storage-sql/sql-submission-ledger";
import { NodeCrypto } from "@effect/platform-node";
import type { Crypto } from "effect";
import { Effect, Layer } from "effect";
import type { SubmissionLedger } from "effect-agent/submission-ledger";
import { LedgerError } from "effect-agent/submission-ledger";
import type * as SqlClientService from "effect/unstable/sql/SqlClient";

import { initializePostgresJournal, postgresStorageErrors } from "./internal/postgres-journal.ts";
import * as PostgresStorageClient from "./PostgresStorageClient.ts";
import {
  layerConfig,
  PostgresStorageConfig,
  type PostgresStorageOptions,
} from "./PostgresStorageConfig.ts";
import {
  type PostgresStorageInitializationError,
  PostgresLedgerError,
  PostgresWriteContention,
} from "./PostgresStorageError.ts";
import { layerFailpoint, PostgresStorageFailpoint } from "./PostgresStorageFailpoint.ts";

const makeServices = Effect.fn("PostgresSubmissionLedger.makeServices")(function* () {
  const config = yield* PostgresStorageConfig;
  const failpoint = yield* PostgresStorageFailpoint;
  const journal = yield* initializePostgresJournal();

  return yield* makeSqlSubmissionLedger(journal, {
    errors: postgresStorageErrors,
    hitFailpoint: failpoint.hit,
    ownershipLeaseDuration: config.ownershipLeaseDuration,
    sqlFailure: (operation) => (cause) => {
      const contention =
        cause.reason._tag === "LockTimeoutError" ||
        cause.reason._tag === "DeadlockError" ||
        cause.reason._tag === "SerializationError";

      const internal = contention
        ? PostgresWriteContention.make({
            operation,
            cause,
            message: `Another producer won the Postgres write race; ${operation} is safe to retry.`,
          })
        : PostgresLedgerError.make({ operation, cause, message: cause.message });

      return LedgerError.make({ operation, message: internal.message, cause: internal });
    },
  });
});

/**
 * Postgres SubmissionLedger implementation sharing the journal's database, write
 * transaction discipline, and producer-epoch fencing substrate. Configuration, failpoint,
 * SQL, and Crypto authority stay visible in the input channel.
 */
export const layerWithServices: Layer.Layer<
  SubmissionLedger,
  PostgresStorageInitializationError,
  PostgresStorageConfig | PostgresStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(makeServices());

/**
 * A composition-root convenience Layer for the durable Submission Ledger. Point it at the
 * same database and schema as the ThreadStore so claims fence the same producer epochs.
 */
export const layer = (
  options: PostgresStorageOptions,
): Layer.Layer<SubmissionLedger, PostgresStorageInitializationError> =>
  Layer.unwrap(
    Effect.map(PostgresStorageConfig, (config) =>
      layerWithServices.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(PostgresStorageConfig)(config),
            layerFailpoint(options),
            PostgresStorageClient.layer(options.client, config.schema),
            NodeCrypto.layer,
          ),
        ),
      ),
    ),
  ).pipe(Layer.provide(layerConfig(options)));
