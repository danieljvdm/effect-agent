import { makeSqlThreadStore } from "@effect-agent/storage-sql/sql-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import type { Crypto } from "effect";
import { Effect, Layer } from "effect";
import type { ThreadStore } from "effect-agent/thread-store";
import type * as SqlClientService from "effect/unstable/sql/SqlClient";

import { initializePostgresJournal, postgresStorageErrors } from "./internal/postgres-journal.ts";
import * as PostgresStorageClient from "./PostgresStorageClient.ts";
import {
  layerConfig,
  PostgresStorageConfig,
  type PostgresStorageOptions,
} from "./PostgresStorageConfig.ts";
import { type PostgresStorageInitializationError } from "./PostgresStorageError.ts";
import { layerFailpoint, PostgresStorageFailpoint } from "./PostgresStorageFailpoint.ts";

const makeServices = Effect.fn("PostgresThreadStore.makeServices")(function* () {
  const config = yield* PostgresStorageConfig;
  const failpoint = yield* PostgresStorageFailpoint;
  const journal = yield* initializePostgresJournal();

  return yield* makeSqlThreadStore(journal, {
    errors: postgresStorageErrors,
    hitFailpoint: failpoint.hit,
    observationPollInterval: config.observationPollInterval,
    verifyOnOpen: config.verifyOnOpen,
    offsetPrefix: "effect-agent-postgres@1:",
  });
});

/**
 * Postgres Thread Store implementation with configuration, failpoint, SQL, and Crypto
 * authority kept visible in its input channel.
 */
export const layerWithServices: Layer.Layer<
  ThreadStore,
  PostgresStorageInitializationError,
  PostgresStorageConfig | PostgresStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(makeServices());

/**
 * A composition-root convenience Layer for canonical Threads. Durable accepted work is
 * served by the separate SubmissionLedger port.
 */
export const layer = (
  options: PostgresStorageOptions,
): Layer.Layer<ThreadStore, PostgresStorageInitializationError> =>
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
