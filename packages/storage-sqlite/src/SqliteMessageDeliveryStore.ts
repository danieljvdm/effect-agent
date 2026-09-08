import {
  MessageDeliveryError,
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "@effect-agent/thread/MessageDelivery";
import {
  makeSqlMessageDeliveryStore,
  SqlMessageDeliveryTransaction,
} from "@effect-agent/thread/SqlMessageDeliveryStore";
import { Effect, Layer } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { initializeSqliteJournal } from "./internal/sqlite-journal.ts";

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

/** Source-owned message obligations in the host's existing SQLite database. */
export const messageDeliveryStoreLayer = (limits?: MessageDeliveryStoreLimits) =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      yield* initializeSqliteJournal();

      return yield* makeSqlMessageDeliveryStore(limits, { maxStoredValueBytes: 16 * 1024 * 1024 });
    }),
  ).pipe(Layer.provide(transactions));
