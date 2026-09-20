import { makeSqlMessageDeliveryStore } from "@effect-agent/storage-sql/sql-message-delivery-store";
import { Effect, Layer } from "effect";
import {
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "effect-agent/message-delivery";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { DoStorageConfig } from "./DoStorageConfig.ts";
import { DoStorageFailpoint } from "./DoStorageFailpoint.ts";
import { initializeDoJournal } from "./internal/do-journal.ts";

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
  );
