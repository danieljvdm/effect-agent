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

const transactions = Layer.effect(
  SqlMessageDeliveryTransaction,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    return SqlMessageDeliveryTransaction.of({
      run: (body) =>
        sql
          .withTransaction(body)
          .pipe(
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
