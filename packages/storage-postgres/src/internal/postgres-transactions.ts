import { makeSqlTransaction } from "@effect-agent/storage-sql/sql-storage";
import { Effect } from "effect";
import type { SqlClient } from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { PostgresStorageError, PostgresWriteContention } from "../PostgresStorageError.ts";

/**
 * This exact FNV-1a hash, including its tag and UTF-8 encoding, is a persistent advisory-lock
 * wire format and must never change: a different key would let an old and a new deployment write
 * concurrently. The shape follows `SqlRunnerStorage`'s lock namespace in Effect's cluster module.
 */
const advisoryLockKey = (tag: string): number => {
  const bytes = new TextEncoder().encode(`effect-agent:${tag}`);
  let hash = 0x811c9dc5;

  for (const byte of bytes) hash = Math.imul(hash ^ byte, 0x01000193);

  return hash | 0;
};

/** One key serialises every writer, which is the scope SQLite's write lock had. */
export const WRITER_LOCK_KEY = advisoryLockKey("storage/writer");

export const storageError = (operation: string) => (cause: SqlError) =>
  PostgresStorageError.make({ operation, cause, message: cause.message });

export const classifyWriteFailure =
  (operation: string) =>
  (cause: SqlError): PostgresStorageError | PostgresWriteContention =>
    cause.reason._tag === "SerializationError" ||
    cause.reason._tag === "DeadlockError" ||
    cause.reason._tag === "LockTimeoutError"
      ? PostgresWriteContention.make({
          operation,
          cause,
          message: `Another producer won the Postgres write race; ${operation} is safe to retry.`,
        })
      : storageError(operation)(cause);

/** The lock wait is interruptible; the shared transaction rolls back before releasing its connection. */
export const withWriterLockTransaction = (sql: SqlClient, lockTimeout: number) =>
  makeSqlTransaction(sql, {
    begin: "BEGIN",
    prelude: Effect.gen(function* () {
      yield* sql`SELECT set_config('lock_timeout', ${`${lockTimeout}ms`}, true)`;
      yield* sql`SELECT pg_advisory_xact_lock(${WRITER_LOCK_KEY})`;
    }),
  });

/** Every page of a multi-query export observes the same snapshot without taking the writer lock. */
export const withReadTransaction = (sql: SqlClient) =>
  makeSqlTransaction(sql, { begin: "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" });
