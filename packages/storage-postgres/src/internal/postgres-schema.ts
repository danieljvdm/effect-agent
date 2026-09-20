import { Effect, Schema } from "effect";
import type { SqlClient } from "effect/unstable/sql/SqlClient";

import { PostgresStorageError } from "../PostgresStorageError.ts";

/** Run while holding the writer transaction, including when this schema does not yet exist. */
export const ensurePostgresSchema = Effect.fnUntraced(function* (sql: SqlClient, schema: string) {
  yield* sql`CREATE SCHEMA IF NOT EXISTS ${sql(schema)}`.withoutTransform;
  const rows = yield* sql`SELECT current_schema() AS name`;

  const decoded = yield* Schema.decodeUnknownEffect(
    Schema.Array(Schema.Struct({ name: Schema.NullOr(Schema.String) })),
  )(rows).pipe(
    Effect.mapError((cause) =>
      PostgresStorageError.make({
        operation: "verify storage schema",
        cause,
        message: cause.message,
      }),
    ),
  );

  if (decoded.length !== 1 || decoded[0]?.name !== schema) {
    return yield* PostgresStorageError.make({
      operation: "verify storage schema",
      message: `The client must select schema ${schema} for every pooled connection. Use PostgresStorageClient.layer(client, schema).`,
    });
  }
});
