import { SqliteClient } from "@effect/sql-sqlite-do";
import { Context, Effect, Exit, Predicate, Schema, Semaphore } from "effect";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { DoStorageCorruptionError, DoStorageError } from "../DoStorageError.ts";

const ActiveState = Context.Reference<OwnedState | undefined>(
  "@effect-agent/storage-cloudflare/internal/ActiveState",
  { defaultValue: () => undefined },
);

/** One disposable view per physical Object database, including separately acquired adapters. */
export class OwnedState {
  private readonly gate = Semaphore.makeUnsafe(1);
  readonly invalidators = new Set<() => void>();

  readonly invalidate = Effect.sync(() => {
    for (const invalidate of this.invalidators) invalidate();
  });

  /** Cached readers must wait for the complete transaction, including rollback/commit. */
  readonly read = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.flatMap(ActiveState, (active) =>
      active === this
        ? effect
        : this.gate.withPermits(1)(Effect.provideService(effect, ActiveState, this)),
    );

  readonly transaction = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | SqlError, R | SqlClient> =>
    this.read(
      Effect.flatMap(SqlClient, (sql) =>
        sql
          .withTransaction(effect)
          .pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? this.invalidate : Effect.void))),
      ),
    );
}

const states = new WeakMap<object, OwnedState>();

/** Explicit invalidation for maintenance that writes private tables outside the adapters. */
export const invalidateOwnedState = (storage: DurableObjectStorage): Effect.Effect<void> =>
  Effect.suspend(() => {
    const state = states.get(storage);

    return state === undefined ? Effect.void : state.read(state.invalidate);
  });

const DoClient = Schema.declare<SqliteClient.SqliteClient>(
  (value): value is SqliteClient.SqliteClient => Predicate.hasProperty(value, SqliteClient.TypeId),
);

export const ownedState = (sql: SqlClient) =>
  Schema.decodeUnknownEffect(DoClient)(sql).pipe(
    Effect.mapError(() =>
      DoStorageError.make({
        operation: "open owned state",
        message: "Owned state requires a Durable Object SQLite client.",
      }),
    ),
    Effect.map((client) => {
      // Instrumentation may return a fresh SQL wrapper on every storage.sql access.
      const key = client.config.storage ?? client.config.db ?? client;
      let state = states.get(key);

      if (state === undefined) {
        state = new OwnedState();
        states.set(key, state);
      }

      return state;
    }),
  );

/**
 * Demand-loaded row views. Writes apply SQLite's RETURNING rows to every retained view in
 * the same transaction; rollback discards them. Bounded views avoid loading a host's entire
 * retained submission history just to fence one live submission.
 */
export const ownedRows = <A, I>(
  schema: Schema.Codec<A, I>,
  table: string,
  key: (row: A) => string,
  identityColumn?: keyof A & string,
) => {
  type View = {
    readonly id: string;
    readonly matches: (row: A) => boolean;
    readonly maxRows: number;
    readonly rows: ReadonlyArray<A>;
    readonly bytes: number;
  };
  const caches = new WeakMap<OwnedState, Map<string, View>>();

  return (state: OwnedState, sql: SqlClient) => {
    let cache = caches.get(state);

    if (cache === undefined) {
      cache = new Map();
      caches.set(state, cache);
      const current = cache;

      state.invalidators.add(() => current.clear());
    }
    const current = cache;

    const decode = (rows: unknown) =>
      Schema.decodeUnknownEffect(Schema.Array(schema))(rows).pipe(
        Effect.mapError(() =>
          DoStorageCorruptionError.make({
            table,
            rowKey: "owned-state",
            message: "Stored rows do not satisfy the storage schema",
          }),
        ),
      );

    const retain = (
      id: string,
      matches: View["matches"],
      rows: ReadonlyArray<A>,
      maxRows: number,
    ) => {
      const bytes = new TextEncoder().encode(JSON.stringify(rows)).byteLength;

      current.delete(id);
      if (rows.length > maxRows || bytes > 4 * 1024 * 1024) return;
      current.set(id, { id, matches, maxRows, rows, bytes });
      let total = 0;

      for (const view of current.values()) total += view.bytes;
      while (current.size > 128 || total > 4 * 1024 * 1024) {
        const oldest = current.entries().next().value;

        if (oldest === undefined) break;
        total -= oldest[1].bytes;
        current.delete(oldest[0]);
      }
    };

    const matching = (
      id: string,
      matches: View["matches"],
      query: Effect.Effect<ReadonlyArray<unknown>, SqlError>,
      maxRows = Infinity,
    ) =>
      state.read(
        Effect.gen(function* () {
          const view = current.get(id);

          if (view !== undefined) return view.rows;

          const rows = yield* decode(yield* query);

          retain(id, matches, rows, maxRows);

          return rows;
        }),
      );

    const by = (field: keyof A & string, value: string) =>
      matching(
        JSON.stringify([field, value]),
        (row) => row[field] === value,
        sql`SELECT * FROM ${sql(table)} WHERE ${sql(field)} = ${value}`,
      );

    const apply =
      (remove: boolean) =>
      <E, R>(effect: Effect.Effect<ReadonlyArray<unknown>, E, R>) =>
        Effect.gen(function* () {
          const rows = yield* decode(yield* effect);
          const changed = new Set(rows.map(key));

          for (const { id, matches, maxRows, rows: prior } of Array.from(current.values())) {
            const next = prior.filter((row) => !changed.has(key(row)));

            if (!remove) next.push(...rows.filter(matches));
            retain(id, matches, next, maxRows);
          }
          // A RETURNING row completely defines its unique-key view, even on a new Thread.
          if (identityColumn !== undefined)
            for (const row of rows) {
              const value = row[identityColumn];

              if (typeof value === "string")
                retain(
                  JSON.stringify([identityColumn, value]),
                  (row) => row[identityColumn] === value,
                  remove ? [] : [row],
                  Infinity,
                );
            }

          return rows;
        });

    return { by, matching, write: apply(false), remove: apply(true) };
  };
};
