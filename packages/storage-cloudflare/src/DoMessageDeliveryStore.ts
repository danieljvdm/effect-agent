import { SqlLifecycleSource } from "@yielded/agent-storage-sql/sql-lifecycle-publication";
import { makeSqlMessageDeliveryStore } from "@yielded/agent-storage-sql/sql-message-delivery-store";
import {
  MessageDeliveryStore,
  type MessageDeliveryStoreLimits,
} from "@yielded/agent/message-delivery";
import { SqlStorageOwner } from "@yielded/agent/sql-memory-store";
import { Effect, Layer } from "effect";
import * as SqlClient from "effect/sql/SqlClient";

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

      const journal = yield* initializeDoJournal(sql, failpoint.hit, config.maxStoredValueBytes);

      return yield* makeSqlMessageDeliveryStore(limits, {
        maxStoredValueBytes: config.maxStoredValueBytes,
      }).pipe(
        Effect.provideService(SqlStorageOwner, journal.owner),
        Effect.provideService(SqlLifecycleSource, {
          beforeRetain: (threadId) => journal.flushCanonical(threadId),
          beforePending: journal.flushPublications(),
        }),
      );
    }),
  );
