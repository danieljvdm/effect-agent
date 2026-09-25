import * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import { PostgresStorageError } from "@effect-agent/storage-postgres/postgres-storage-error";
import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Redacted, Schema, String } from "effect";
import { ThreadStore } from "effect-agent/thread-store";
import { TestClock } from "effect/testing";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { withTemporaryDatabase } from "./harness.ts";

const Session = Schema.Array(Schema.Struct({ name: Schema.String, pid: Schema.Int }));

it.effect(
  "qualifies storage tables without changing the native client across concurrent and replaced connections",
  () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const configuredUrl = new URL(url);

        configuredUrl.searchParams.set("options", "-c search_path=pg_catalog");

        const client = {
          url: Redacted.make(configuredUrl.toString()),
          startupParameters: { search_path: "pg_catalog" },
          transformQueryNames: String.snakeToCamel,
          maxConnections: 2,
          connectionTTL: 1000,
        };

        // A keyword schema also exercises identifier quoting during the adapter's own DDL.
        yield* Effect.gen(function* () {
          yield* Effect.asVoid(ThreadStore).pipe(
            Effect.provide(PostgresStorage.make({ schema: "select" }).threadStore),
          );
          const sql = yield* SqlClient.SqlClient;

          const initial = yield* Effect.scoped(
            Effect.gen(function* () {
              const first = yield* sql.reserve;
              const second = yield* sql.reserve;

              return [
                yield* Schema.decodeEffect(Session)(
                  yield* first.executeUnprepared(
                    "SELECT current_schema() AS name, pg_backend_pid() AS pid",
                    [],
                    undefined,
                  ),
                ),
                yield* Schema.decodeEffect(Session)(
                  yield* second.executeUnprepared(
                    "SELECT current_schema() AS name, pg_backend_pid() AS pid",
                    [],
                    undefined,
                  ),
                ),
              ].flat();
            }),
          );

          expect(initial.map((session) => session.name)).toEqual(["pg_catalog", "pg_catalog"]);
          expect(new Set(initial.map((session) => session.pid)).size).toBe(2);

          yield* TestClock.adjust(1001);

          const replacement = yield* Schema.decodeUnknownEffect(Session)(
            yield* sql`SELECT current_schema() AS name, pg_backend_pid() AS pid`,
          );

          expect(replacement[0]?.name).toBe("pg_catalog");
          expect(initial.map((session) => session.pid)).not.toContain(replacement[0]?.pid);

          const tables =
            yield* sql`SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='effect_agent_storage_version'`;

          expect(tables).toEqual([{ nspname: "select" }]);
        }).pipe(Effect.provide([PgClient.layer(client), NodeCrypto.layer]));
      }),
    ),
);

it.effect("rejects a zero writer timeout before opening storage", () =>
  Effect.gen(function* () {
    const opened = yield* ThreadStore.pipe(
      Effect.provide(PostgresStorage.make({ lockTimeout: 0 }).threadStore),
      Effect.provide([PgClient.layer({ port: 1 }), NodeCrypto.layer]),
      Effect.exit,
    );

    expect(Exit.isFailure(opened)).toBe(true);
    if (Exit.isFailure(opened))
      expect(Cause.squash(opened.cause)).toBeInstanceOf(PostgresStorageError);
  }),
);
