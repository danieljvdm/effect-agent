import { layer } from "@effect-agent/storage-postgres/postgres-thread-store";
import { PgClient } from "@effect/sql-pg";
import { describe, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";
import {
  threadStoreConformanceCases,
  threadCheckpointConformanceCases,
} from "effect-agent/testing/thread-store-conformance";
import type { ThreadStore } from "effect-agent/thread-store";

/**
 * A live Postgres is required: this adapter's contract is its concurrency behaviour, and no
 * in-process double reproduces it. The URL is configurable so CI can point at its own service.
 */
const adminUrl =
  process.env.EFFECT_AGENT_TEST_POSTGRES_URL ??
  "postgres://postgres:postgres@localhost:55432/effect_agent";

const databaseUrl = (database: string) => {
  const url = new URL(adminUrl);

  url.pathname = `/${database}`;

  return url.toString();
};

let databaseCounter = 0;

/**
 * Each case owns a database rather than a schema. A schema would be cheaper, but selecting one
 * needs `search_path`, which binds per connection and so cannot be set for a pool through this
 * driver — the adapter rejects that configuration at startup, and a test that worked around it
 * would not be exercising what callers actually run.
 */
const withTemporaryDatabase = <A, E>(
  use: (url: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  Effect.gen(function* () {
    databaseCounter = databaseCounter + 1;
    const database = `effect_agent_test_${process.pid}_${databaseCounter}`;

    const admin = Effect.gen(function* () {
      const sql = yield* PgClient.PgClient;

      return yield* sql.unsafe(`CREATE DATABASE ${database}`);
    }).pipe(
      Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })),
      Effect.orDie,
    );

    yield* admin;

    return yield* use(databaseUrl(database));
  });

const withStorage = <A, E>(url: string, effect: Effect.Effect<A, E, ThreadStore>) =>
  Effect.provide(
    effect,
    layer({ client: { url: Redacted.make(url) }, observationPollInterval: 1 }),
  );

describe("PostgresThreadStore", () => {
  describe("shared ThreadStore conformance", () => {
    for (const conformanceCase of [
      ...threadStoreConformanceCases,
      ...threadCheckpointConformanceCases,
    ]) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((url) => withStorage(url, conformanceCase.run)),
      );
    }
  });
});
