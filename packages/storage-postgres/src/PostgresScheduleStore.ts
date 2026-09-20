import { makeSqlScheduleStore } from "@effect-agent/storage-sql/sql-schedule-store";
import { Effect, Layer } from "effect";
import { ScheduleStore } from "effect-agent/schedule";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { initializePostgresJournal } from "./internal/postgres-journal.ts";
import { withWriterLockTransaction } from "./internal/postgres-transactions.ts";
import { PostgresStorageConfig } from "./PostgresStorageConfig.ts";
import type { PostgresStorageInitializationError } from "./PostgresStorageError.ts";
import type { PostgresStorageFailpoint } from "./PostgresStorageFailpoint.ts";

const makeScheduleStore = Effect.gen(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const { lockTimeout } = yield* PostgresStorageConfig;

  yield* initializePostgresJournal();

  return yield* makeSqlScheduleStore(withWriterLockTransaction(sql, lockTimeout));
});

export const layer: Layer.Layer<
  ScheduleStore,
  PostgresStorageInitializationError,
  PostgresStorageConfig | PostgresStorageFailpoint | SqlClientService.SqlClient
> = Layer.effect(ScheduleStore)(makeScheduleStore);
