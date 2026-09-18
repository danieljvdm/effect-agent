import { Effect, Exit } from "effect";
import type { SqlClient } from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { WRITER_LOCK_KEY } from "./postgres-journal.ts";

/**
 * Runs one body holding the adapter's writer lock.
 *
 * The SQLite adapter reaches these stores through `withTransaction`, which its client starts
 * with `BEGIN IMMEDIATE`: a database-wide write lock that makes every read-then-write sequence
 * inside trivially serialisable. Postgres would otherwise run the same body at READ COMMITTED,
 * where two callers can both read the prior state and the loser's compare-and-set silently
 * matches zero rows. The transaction-scoped advisory lock restores the invariant these stores
 * were written against, using the same key as the journal so the two cannot interleave.
 */
export const withWriterLockTransaction =
  (sql: SqlClient) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | SqlError, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* sql.reserve;

          yield* connection.executeUnprepared("BEGIN", [], undefined);
          yield* connection.executeUnprepared(
            `SELECT pg_advisory_xact_lock(${WRITER_LOCK_KEY})`,
            [],
            undefined,
          );

          const exit = yield* restore(
            Effect.provideService(effect, sql.transactionService, [connection, 0] as const),
          ).pipe(Effect.exit);

          if (Exit.isSuccess(exit)) {
            yield* connection.executeUnprepared("COMMIT", [], undefined);

            return exit.value;
          }
          // A failed BEGIN leaves no transaction, so only a started one is rolled back, and a
          // rollback that itself fails cannot be reported without losing the original failure.
          yield* Effect.orDie(connection.executeUnprepared("ROLLBACK", [], undefined));

          return yield* exit;
        }),
      ).pipe(Effect.withSpan("PostgresStorage.withWriterLockTransaction")),
    );
