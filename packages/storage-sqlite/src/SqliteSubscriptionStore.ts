import { makeSqlSubscriptionStore } from "@effect-agent/storage-sql/sql-subscription-store";
import { Effect, Layer, Schema } from "effect";
import { SourcePartition, SubscriptionError, SubscriptionStore } from "effect-agent/subscription";
import type * as SqlClientService from "effect/unstable/sql/SqlClient";

import { initializeSqliteJournal } from "./internal/sqlite-journal.ts";
import type { SqliteStorageConfig } from "./SqliteStorageConfig.ts";
import type { SqliteStorageFailpoint } from "./SqliteStorageFailpoint.ts";
import type { SqliteStorageInitializationError } from "./SqliteThreadStore.ts";

const makeSubscriptionStore = Effect.fn("SqliteSubscriptionStore.make")(function* (
  owned: SourcePartition,
) {
  const partition = yield* Schema.decodeEffect(SourcePartition)(owned).pipe(
    Effect.mapError(() => SubscriptionError.make({ reason: "validation", code: "partition" })),
  );

  yield* initializeSqliteJournal();

  return yield* makeSqlSubscriptionStore(partition, {
    maxStoredJsonLength: 16 * 1024 * 1024,
  });
});

export const subscriptionStoreLayer = (
  partition: SourcePartition,
): Layer.Layer<
  SubscriptionStore,
  SqliteStorageInitializationError | SubscriptionError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient
> => Layer.effect(SubscriptionStore, makeSubscriptionStore(partition));
