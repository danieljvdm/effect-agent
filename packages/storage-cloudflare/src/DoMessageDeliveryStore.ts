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

import { DoStorageConfig } from "./DoStorageConfig.ts";
import { DoStorageFailpoint } from "./DoStorageFailpoint.ts";
import { initializeDoJournal } from "./internal/do-journal.ts";

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

/**
 * Message obligations in a Thread Object's own SQL storage. The platform must prearm
 * maintenance before insertion and retain its alarm while nextDeadline is present.
 */
export const doMessageDeliveryStoreLayer = (limits?: MessageDeliveryStoreLimits) =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const config = yield* DoStorageConfig;
      const failpoint = yield* DoStorageFailpoint;

      yield* initializeDoJournal(sql, failpoint.hit, config.maxStoredValueBytes);

      return yield* makeSqlMessageDeliveryStore(limits, {
        maxStoredValueBytes: config.maxStoredValueBytes,
      });
    }),
  ).pipe(Layer.provide(transactions));
