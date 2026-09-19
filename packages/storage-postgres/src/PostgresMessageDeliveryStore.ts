import { Effect, Layer } from "effect";
import {
  MessageDeliveryError,
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "effect-agent/message-delivery";
import {
  makeSqlMessageDeliveryStore,
  SqlMessageDeliveryTransaction,
} from "effect-agent/sql-message-delivery-store";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { initializePostgresJournal } from "./internal/postgres-journal.ts";
import { withWriterLockTransaction } from "./internal/postgres-transactions.ts";
import { PostgresStorageConfig } from "./PostgresStorageConfig.ts";

const transactions = Layer.effect(
  SqlMessageDeliveryTransaction,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const { lockTimeout } = yield* PostgresStorageConfig;

    // The shared store reads then writes inside one transaction, which SQLite serialises with
    // `BEGIN IMMEDIATE`; on Postgres only the writer lock does.
    return SqlMessageDeliveryTransaction.of({
      run: (body) =>
        withWriterLockTransaction(
          sql,
          lockTimeout,
        )(body).pipe(
          Effect.catchTag("SqlError", () =>
            MessageDeliveryError.make({ reason: "storage", operation: "transaction" }),
          ),
        ),
    });
  }),
);

/** Source-owned message obligations in the host's existing Postgres database. */
export const layer = (limits?: MessageDeliveryStoreLimits) =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      yield* initializePostgresJournal();

      return yield* makeSqlMessageDeliveryStore(limits, { maxStoredValueBytes: 16 * 1024 * 1024 });
    }),
  ).pipe(Layer.provide(transactions));
