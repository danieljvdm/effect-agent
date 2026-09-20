import { PgClient, PgTypes } from "@effect/sql-pg";
import { Effect, Layer, Result, Schema } from "effect";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import { PostgresStorageConfigValue } from "./PostgresStorageConfig.ts";
import { PostgresStorageError } from "./PostgresStorageError.ts";

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);

/**
 * Postgres reports `int8` as a JS `BigInt`, while every sequence, ordinal and epoch-millisecond
 * value this adapter stores decodes through `Schema.Int`. Decoding to a safe integer keeps the
 * shared row schemas identical across adapters; a value beyond that range is corruption rather
 * than a number to round.
 */
const safeIntegerInt8Codec: PgTypes.Codec<number> = {
  encode: (value) => PgTypes.encode(BigInt(value), PgTypes.OID.int8),
  decode: (bytes) => {
    const decoded = PgTypes.decode(bytes, PgTypes.OID.int8, 1);

    if (Result.isFailure(decoded)) return Result.fail(decoded.failure);
    const value = decoded.success;

    if (typeof value !== "bigint")
      return Result.fail(new PgTypes.CodecError({ message: "int8 decoded to a non-bigint value" }));
    if (value > MAX_SAFE || value < MIN_SAFE) {
      return Result.fail(
        new PgTypes.CodecError({
          message: `Stored int8 ${value} is outside the JavaScript safe integer range.`,
        }),
      );
    }

    return Result.succeed(Number(value));
  },
};

/** The default registry with `int8` decoding replaced. */
export const makeTypeRegistry = (): PgTypes.Registry => {
  const registry = PgTypes.makeRegistry();

  registry.register(PgTypes.OID.int8, safeIntegerInt8Codec);

  return registry;
};

/** Connection configuration for the adapter's client; the type registry is not the caller's. */
export type PostgresClientOptions = Omit<PgClient.PgPoolConfig, "types">;

/**
 * The client this adapter's stores expect. Its row decoding depends on the registry above, so a
 * client built elsewhere will not decode stored values correctly. A connection that cannot be
 * established is reported in the adapter's own error vocabulary.
 * Every physical connection receives the selected schema as its startup search path, including
 * pool growth and replacement. The schema defaults to `public`.
 */
export const layer = (
  config: PostgresClientOptions,
  schema = "public",
): Layer.Layer<PgClient.PgClient | SqlClient.SqlClient, PostgresStorageError> =>
  Layer.unwrap(
    Schema.decodeEffect(PostgresStorageConfigValue.fields.schema)(schema).pipe(
      Effect.mapError((cause) =>
        PostgresStorageError.make({
          cause,
          operation: "configure Postgres schema",
          message: cause.message,
        }),
      ),
      Effect.map((selectedSchema) =>
        PgClient.layer({
          ...config,
          types: makeTypeRegistry(),
          startupParameters: {
            ...Object.fromEntries(
              Object.entries(config.startupParameters ?? {}).filter(
                ([name]) => name.toLowerCase() !== "search_path",
              ),
            ),
            search_path: `"${selectedSchema}"`,
          },
        }).pipe(
          Layer.catchTag("SqlError", (cause) =>
            Layer.effectContext(
              Effect.fail(
                PostgresStorageError.make({
                  cause,
                  operation: "connect to Postgres",
                  message: cause.message,
                }),
              ),
            ),
          ),
        ),
      ),
    ),
  );
