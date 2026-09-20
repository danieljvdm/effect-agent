import * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import * as PostgresStorageClient from "@effect-agent/storage-postgres/postgres-storage-client";
import { PostgresStorageError } from "@effect-agent/storage-postgres/postgres-storage-error";
import { expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Redacted, Schema } from "effect";
import { ThreadStore } from "effect-agent/thread-store";
import { TestClock } from "effect/testing";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { withTemporaryDatabase } from "./harness.ts";

const Session = Schema.Array(Schema.Struct({ name: Schema.String, pid: Schema.Int }));

it.effect(
  "uses the configured schema on concurrent and replaced connections, overriding startup defaults",
  () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const configuredUrl = new URL(url);

        configuredUrl.searchParams.set("options", "-c search_path=pg_catalog");

        const client = {
          url: Redacted.make(configuredUrl.toString()),
          startupOptions: "-c search_path=public",
          startupParameters: { search_path: "public", SEARCH_PATH: "pg_catalog" },
          maxConnections: 2,
          connectionTTL: 1000,
        };

        // A keyword schema also exercises identifier quoting during the adapter's own DDL.
        yield* Effect.asVoid(ThreadStore).pipe(
          Effect.provide(PostgresStorage.make({ client, schema: "select" }).threadStore),
        );
        yield* Effect.gen(function* () {
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

          expect(initial.map((session) => session.name)).toEqual(["select", "select"]);
          expect(new Set(initial.map((session) => session.pid)).size).toBe(2);

          yield* TestClock.adjust(1001);

          const replacement = yield* Schema.decodeUnknownEffect(Session)(
            yield* sql`SELECT current_schema() AS name, pg_backend_pid() AS pid`,
          );

          expect(replacement[0]?.name).toBe("select");
          expect(initial.map((session) => session.pid)).not.toContain(replacement[0]?.pid);

          const tables =
            yield* sql`SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='effect_agent_storage_version'`;

          expect(tables).toEqual([{ nspname: "select" }]);
        }).pipe(Effect.provide(PostgresStorageClient.layer(client, "select")));
      }),
    ),
);

it.effect("rejects a zero writer timeout before opening storage", () =>
  Effect.gen(function* () {
    const opened = yield* ThreadStore.pipe(
      Effect.provide(PostgresStorage.make({ client: {}, lockTimeout: 0 }).threadStore),
      Effect.exit,
    );

    expect(Exit.isFailure(opened)).toBe(true);
    if (Exit.isFailure(opened))
      expect(Cause.squash(opened.cause)).toBeInstanceOf(PostgresStorageError);
  }),
);
