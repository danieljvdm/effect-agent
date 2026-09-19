import { Effect, Exit } from "effect";
import type { SqlClient } from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

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

/**
 * Runs one body holding the adapter's writer lock, the equivalent of the `BEGIN IMMEDIATE` these
 * stores were written against. At READ COMMITTED two callers can both read the prior state and
 * the loser's compare-and-set matches zero rows. A transaction-scoped lock releases with the
 * transaction however it ends, so no finalizer has to unlock it.
 */
export const withWriterLockTransaction =
  (sql: SqlClient, lockTimeout: number) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | SqlError, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* sql.reserve;

          yield* connection.executeUnprepared("BEGIN", [], undefined);

          // The timeout precedes the lock wait it bounds, and a wait that times out has already
          // aborted the transaction, so the prelude shares the body's rollback path.
          const prelude = connection
            .executeUnprepared(`SET LOCAL lock_timeout = '${lockTimeout}ms'`, [], undefined)
            .pipe(
              Effect.andThen(
                connection.executeUnprepared(
                  `SELECT pg_advisory_xact_lock(${WRITER_LOCK_KEY})`,
                  [],
                  undefined,
                ),
              ),
            );

          const exit = yield* prelude.pipe(
            Effect.andThen(
              restore(
                Effect.provideService(effect, sql.transactionService, [connection, 0] as const),
              ),
            ),
            Effect.exit,
          );

          if (Exit.isSuccess(exit)) {
            yield* connection.executeUnprepared("COMMIT", [], undefined);

            return exit.value;
          }
          // A rollback that itself fails cannot be reported without losing the original failure.
          yield* Effect.orDie(connection.executeUnprepared("ROLLBACK", [], undefined));

          return yield* exit;
        }),
      ).pipe(Effect.withSpan("PostgresStorage.withWriterLockTransaction")),
    );
