import { makeSqlMessageDeliveryStore } from "@effect-agent/storage-sql/sql-message-delivery-store";
import { Effect, Layer } from "effect";
import {
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "effect-agent/message-delivery";

import { initializeSqliteJournal } from "./internal/sqlite-journal.ts";

/** Source-owned message obligations in the host's existing SQLite database. */
export const messageDeliveryStoreLayer = (limits?: MessageDeliveryStoreLimits) =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      yield* initializeSqliteJournal();

      return yield* makeSqlMessageDeliveryStore(limits);
    }),
  );
