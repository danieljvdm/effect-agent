import * as PostgresStorageClient from "@effect-agent/storage-postgres/postgres-storage-client";
import { layerConfig } from "@effect-agent/storage-postgres/postgres-storage-config";
import { PgClient } from "@effect/sql-pg";
import { Effect, Redacted } from "effect";

/**
 * A live Postgres is required: this adapter's contract is its concurrency behaviour, and no
 * in-process double reproduces it. `EFFECT_AGENT_TEST_POSTGRES_URL` points CI at its own service.
 */
export const adminUrl =
  process.env.EFFECT_AGENT_TEST_POSTGRES_URL ??
  "postgres://postgres:postgres@localhost:55432/effect_agent";

export const databaseUrl = (database: string): string => {
  const url = new URL(adminUrl);

  url.pathname = `/${database}`;

  return url.toString();
};

let databaseCounter = 0;

const admin = <A, E>(effect: Effect.Effect<A, E, PgClient.PgClient>) =>
  effect.pipe(
    Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })),
    Effect.catchTag("SqlError", (error) =>
      Effect.die(
        new Error(
          `The storage-postgres tests need a reachable Postgres at ${adminUrl} ` +
            "(set EFFECT_AGENT_TEST_POSTGRES_URL, or run one with " +
            "`docker run -d -p 55432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=effect_agent postgres:17-alpine`).",
          { cause: error },
        ),
      ),
    ),
  );

/**
 * Each case owns a database. A schema would be cheaper, but selecting one needs `search_path`,
 * which binds per connection and so cannot be set for a pool through this driver; the adapter
 * rejects that configuration at startup, and a test that worked around it would not exercise
 * what callers actually run. `WITH (FORCE)` ends any pooled connection the case left open.
 */
export const withTemporaryDatabase = <A, E>(
  use: (url: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      databaseCounter = databaseCounter + 1;

      return `effect_agent_test_${process.pid}_${databaseCounter}`;
    }).pipe(
      Effect.tap((database) =>
        admin(
          Effect.flatMap(PgClient.PgClient, (sql) => sql.unsafe(`CREATE DATABASE ${database}`)),
        ),
      ),
    ),
    (database) => use(databaseUrl(database)),
    (database) =>
      admin(
        Effect.flatMap(PgClient.PgClient, (sql) =>
          sql.unsafe(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`),
        ),
      ).pipe(Effect.ignore),
  );

/** The adapter's own client over one temporary database. */
export const clientLayer = (url: string) =>
  PostgresStorageClient.layer({ url: Redacted.make(url) });

/** The adapter's validated configuration for one temporary database, with fast observation polling. */
export const configLayer = (url: string) =>
  layerConfig({ client: { url: Redacted.make(url) }, observationPollInterval: 1 });
