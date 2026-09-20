import * as PostgresStorageClient from "@effect-agent/storage-postgres/postgres-storage-client";
import {
  layerConfig,
  PostgresStorageConfig,
  PostgresStorageConfigValue,
} from "@effect-agent/storage-postgres/postgres-storage-config";
import { PostgresStorageFailpoint } from "@effect-agent/storage-postgres/postgres-storage-failpoint";
import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { Effect, Layer, Redacted } from "effect";

import { WRITER_LOCK_KEY } from "../src/internal/postgres-transactions.ts";

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
          `The storage-postgres tests need a reachable Postgres at ${adminUrl} ` +
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

export const clientLayer = (url: string) =>
  PostgresStorageClient.layer({ url: Redacted.make(url) });

export const configLayer = (url: string) =>
  layerConfig({ client: { url: Redacted.make(url) }, observationPollInterval: 1 });

/**
 * Services over exactly one connection, so a successful retry also proves the preceding
 * failed transaction rolled back and returned a usable connection to the pool.
 */
export const singleConnectionServices = (url: string, lockTimeout: number) =>
  Layer.mergeAll(
    Layer.succeed(PostgresStorageConfig)(
      PostgresStorageConfigValue.make({
        observationPollInterval: 1,
        lockTimeout,
        ownershipLeaseDuration: 30_000,
        verifyOnOpen: false,
        schema: "public",
      }),
    ),
    PostgresStorageFailpoint.layer,
    PgClient.layer({
      url: Redacted.make(url),
      maxConnections: 1,
      types: PostgresStorageClient.makeTypeRegistry(),
    }),
    NodeCrypto.layer,
  );

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
