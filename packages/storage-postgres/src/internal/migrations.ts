import { createStorageSchema } from "@effect-agent/storage-sql/sql-storage-schema";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export const CurrentPostgresStorageVersion = 1;

/** Initialize empty storage with the complete current schema. */
export const createPostgresStorageSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // The boolean primary key is what keeps the version marker single-row: no second value can
  // satisfy the constraint. SQLite records its format in `PRAGMA user_version` instead.
  yield* sql`
    CREATE TABLE effect_agent_storage_version (
      id BOOLEAN PRIMARY KEY NOT NULL,
      version BIGINT NOT NULL,
      CONSTRAINT effect_agent_storage_version_single_row CHECK (id)
    )
  `;
  yield* createStorageSchema;
  yield* sql`
    INSERT INTO effect_agent_storage_version (id, version)
    VALUES (TRUE, ${CurrentPostgresStorageVersion})
  `;
});
