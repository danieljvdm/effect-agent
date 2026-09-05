import { makeSqlSubscriptionStore } from "@effect-agent/thread/SqlSubscriptionStore";
import {
  SourcePartition,
  SubscriptionError,
  SubscriptionStore,
} from "@effect-agent/thread/Subscription";
import { Effect, Layer, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { initializeSqliteJournal } from "./internal/sqlite-journal.ts";
import type { SqliteStorageConfig } from "./SqliteStorageConfig.ts";
import type { SqliteStorageFailpoint } from "./SqliteStorageFailpoint.ts";
import type { SqliteStorageInitializationError } from "./SqliteThreadStore.ts";

const makeSubscriptionStore = Effect.fn("SqliteSubscriptionStore.make")(function* (
  owned: SourcePartition,
) {
  const partition = yield* Schema.decodeUnknownEffect(SourcePartition)(owned).pipe(
    Effect.mapError(() => SubscriptionError.make({ reason: "validation", code: "partition" })),
  );

  const sql = yield* SqlClientService.SqlClient;

  yield* initializeSqliteJournal();

  return yield* makeSqlSubscriptionStore(partition, {
    maxStoredJsonLength: 16 * 1024 * 1024,
    transaction: (body) =>
      sql
        .withTransaction(body)
        .pipe(
          Effect.catchTag("SqlError", () =>
            Effect.fail(SubscriptionError.make({ reason: "storage", code: "transaction" })),
          ),
        ),
  });
});

export const subscriptionStoreLayer = (
  partition: SourcePartition,
): Layer.Layer<
  SubscriptionStore,
  SqliteStorageInitializationError | SubscriptionError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient
> => Layer.effect(SubscriptionStore, makeSubscriptionStore(partition));
