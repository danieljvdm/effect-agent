import * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { Effect, Layer, Redacted } from "effect";

import { WRITER_LOCK_KEY } from "../src/internal/postgres-storage.ts";

/**
 * A live Postgres is required: this adapter's contract is its concurrency behaviour, and no
 * in-process double reproduces it. `EFFECT_AGENT_TEST_POSTGRES_URL` points CI at its own service.
 */
const adminUrl =
  process.env.EFFECT_AGENT_TEST_POSTGRES_URL ??
  "postgres://postgres:postgres@localhost:55432/effect_agent";

let databaseCounter = 0;

const admin = <A, E>(effect: Effect.Effect<A, E, PgClient.PgClient>) =>
  effect.pipe(
    Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })),
    Effect.catchTag("SqlError", (error) =>
      Effect.die(
        new Error(
          "The storage-postgres tests need a reachable configured Postgres server " +
            "(set EFFECT_AGENT_TEST_POSTGRES_URL, or run one with " +
            "`docker run -d -p 55432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=effect_agent postgres:17-alpine`).",
          { cause: error },
        ),
      ),
    ),
  );

/**
 * Each case owns a database, including its database-scoped writer lock. `WITH (FORCE)` ends
 * any pooled connection the case left open.
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
    (database) => {
      const url = new URL(adminUrl);

      url.pathname = `/${database}`;

      return use(url.toString());
    },
    (database) =>
      admin(
        Effect.flatMap(PgClient.PgClient, (sql) =>
          sql.unsafe(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`),
        ),
      ).pipe(Effect.ignore),
  );

export const clientLayer = (url: string) => PgClient.layer({ url: Redacted.make(url) });

/** Supply the real native client at the test composition root. */
export const storage = (
  url: string,
  options: PostgresStorage.PostgresStorageOptions = {},
  client: Omit<PgClient.PgPoolConfig, "url"> = {},
) => {
  const stores = PostgresStorage.make({ observationPollInterval: 1, ...options });
  const clientLayer = PgClient.layer({ ...client, url: Redacted.make(url) });
  const dependencies = Layer.merge(clientLayer, NodeCrypto.layer);

  return {
    clientLayer,
    threadStore: stores.threadStore.pipe(Layer.provide(dependencies)),
    submissionLedger: stores.submissionLedger.pipe(Layer.provide(dependencies)),
    scheduleStore: stores.scheduleStore.pipe(Layer.provide(dependencies)),
    activityStore: stores.activityStore.pipe(Layer.provide(dependencies)),
    messageDeliveryStore: (...args: Parameters<typeof stores.messageDeliveryStore>) =>
      stores.messageDeliveryStore(...args).pipe(Layer.provide(dependencies)),
    subscriptionStore: (...args: Parameters<typeof stores.subscriptionStore>) =>
      stores.subscriptionStore(...args).pipe(Layer.provide(dependencies)),
  };
};

/** One pool connection makes subsequent operations verify failed-transaction cleanup. */
export const singleConnectionStorage = (url: string, lockTimeout: number) =>
  storage(url, { lockTimeout, ownershipLeaseDuration: 30_000 }, { maxConnections: 1 });

/**
 * Holds the adapter's writer lock from an unrelated client for the duration of `use`, as a
 * transiently coexisting producer would, then rolls the holding transaction back.
 */
export const whileHoldingWriterLock = <A, E>(url: string, use: Effect.Effect<A, E>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* PgClient.PgClient;
      const connection = yield* sql.reserve;

      yield* connection.executeUnprepared("BEGIN", [], undefined);
      yield* connection.executeUnprepared(
        `SELECT pg_advisory_xact_lock(${WRITER_LOCK_KEY})`,
        [],
        undefined,
      );

      const result = yield* use;

      yield* connection.executeUnprepared("ROLLBACK", [], undefined);

      return result;
    }),
  ).pipe(Effect.provide(PgClient.layer({ url: Redacted.make(url), maxConnections: 1 })));
