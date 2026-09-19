import { Effect, Layer, Schema } from "effect";
import {
  makeSqlSubscriptionStore,
  SqlSubscriptionTransaction,
} from "effect-agent/sql-subscription-store";
import { SourcePartition, SubscriptionError, SubscriptionStore } from "effect-agent/subscription";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { initializePostgresJournal } from "./internal/postgres-journal.ts";
import type { PostgresStorageConfig } from "./PostgresStorageConfig.ts";
import type { PostgresStorageInitializationError } from "./PostgresStorageError.ts";
import type { PostgresStorageFailpoint } from "./PostgresStorageFailpoint.ts";

const transactionLayer = Layer.effect(
  SqlSubscriptionTransaction,
  Effect.gen(function* () {
    const sql = yield* SqlClientService.SqlClient;

    return SqlSubscriptionTransaction.of({
      run: (body) =>
        sql
          .withTransaction(body)
          .pipe(
            Effect.catchTag("SqlError", () =>
              Effect.fail(SubscriptionError.make({ reason: "storage", code: "transaction" })),
            ),
          ),
    });
  }),
);

const makeSubscriptionStore = Effect.fn("PostgresSubscriptionStore.make")(function* (
  owned: SourcePartition,
) {
  const partition = yield* Schema.decodeEffect(SourcePartition)(owned).pipe(
    Effect.mapError(() => SubscriptionError.make({ reason: "validation", code: "partition" })),
  );

  yield* initializePostgresJournal();

  return yield* makeSqlSubscriptionStore(partition, {
    maxStoredJsonLength: 16 * 1024 * 1024,
  });
});

export const layer = (
  partition: SourcePartition,
): Layer.Layer<
  SubscriptionStore,
  PostgresStorageInitializationError | SubscriptionError,
  PostgresStorageConfig | PostgresStorageFailpoint | SqlClientService.SqlClient
> =>
  Layer.effect(SubscriptionStore, makeSubscriptionStore(partition)).pipe(
    Layer.provide(transactionLayer),
  );
