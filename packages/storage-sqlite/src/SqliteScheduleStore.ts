import { makeSqlScheduleStore } from "@yielded/agent-storage-sql/sql-schedule-store";
import { ScheduleStore } from "@yielded/agent/schedule";
import { Effect, Layer } from "effect";
import * as SqlClient from "effect/sql/SqlClient";

import { initializeSqliteJournal } from "./internal/sqlite-journal.ts";
import type { SqliteStorageConfig } from "./SqliteStorageConfig.ts";
import type { SqliteStorageFailpoint } from "./SqliteStorageFailpoint.ts";
import type { SqliteStorageInitializationError } from "./SqliteThreadStore.ts";

/** SQLite implementation of the atomic ScheduleStore port. */
export const scheduleStoreLayer: Layer.Layer<
  ScheduleStore,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClient.SqlClient
> = Layer.effect(
  ScheduleStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    yield* initializeSqliteJournal();

    return yield* makeSqlScheduleStore(sql.withTransaction);
  }),
);
