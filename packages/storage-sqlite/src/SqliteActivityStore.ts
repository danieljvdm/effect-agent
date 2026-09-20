import { makeSqlActivityStore } from "@effect-agent/storage-sql/sql-activity-store";
import { Effect, Layer } from "effect";
import {
  ActivityMutationFailpoint,
  type ActivityMutationFailure,
  ActivityProcessorStore,
  type ActivityStoreError,
} from "effect-agent/activity-store";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

export type SqliteActivityInitializationError = ActivityStoreError | ActivityMutationFailure;

const makeActivityStore = Effect.fn("SqliteActivityStore.make")(function* () {
  const sql = yield* SqlClientService.SqlClient;

  return yield* makeSqlActivityStore(sql.withTransaction);
});

/** SQLite activity progress with mutation failpoints kept injectable for recovery tests. */
export const activityProcessorStoreLayerWithFailpoints: Layer.Layer<
  ActivityProcessorStore,
  SqliteActivityInitializationError,
  SqlClientService.SqlClient | ActivityMutationFailpoint
> = Layer.effect(ActivityProcessorStore, makeActivityStore());

/** SQLite activity progress with the production no-op mutation failpoint. */
export const activityProcessorStoreLayer: Layer.Layer<
  ActivityProcessorStore,
  SqliteActivityInitializationError,
  SqlClientService.SqlClient
> = activityProcessorStoreLayerWithFailpoints.pipe(Layer.provide(ActivityMutationFailpoint.layer));
