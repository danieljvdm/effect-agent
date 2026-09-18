import { PgClient, PgTypes } from "@effect/sql-pg";
import { Effect, Layer, Result } from "effect";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import { PostgresStorageError } from "./PostgresStorageError.ts";

/**
 * Postgres reports `bigint` columns as JS `BigInt`, while every stored sequence, ordinal, and
 * epoch-millisecond value in this schema decodes through `Schema.Int`. Registering a
 * safe-integer codec for `int8` keeps the shared row schemas identical across adapters;
 * a stored value outside the safe range is corruption rather than a number to round.
 */
export const safeIntegerInt8Codec: PgTypes.Codec<number> = {
  encode: (value) => PgTypes.encode(BigInt(value), PgTypes.OID.int8),
  decode: (bytes) => {
    const decoded = PgTypes.decode(bytes, PgTypes.OID.int8, 1);

    if (Result.isFailure(decoded)) return Result.fail(decoded.failure);
    const value = decoded.success;

    if (typeof value !== "bigint") {
      return Result.fail(
        new PgTypes.CodecError({ message: "int8 did not decode to a bigint value" }),
      );
    }
    if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) {
      return Result.fail(
        new PgTypes.CodecError({
          message: `Stored int8 ${value} is outside the JavaScript safe integer range.`,
        }),
      );
    }

    return Result.succeed(Number(value));
  },
};

/** A registry whose only difference from the default is the safe-integer `int8` decoding. */
export const makeStorageTypeRegistry = (): PgTypes.Registry => {
  const registry = PgTypes.makeRegistry();

  registry.register(PgTypes.OID.int8, safeIntegerInt8Codec);

  return registry;
};

/** Connection configuration for the adapter's client; the type registry is not the caller's. */
export type PostgresClientOptions = Omit<PgClient.PgClientConfig, "types">;

/**
 * The Postgres client this adapter expects: the storage type registry applied, and the
 * caller's remaining connection configuration untouched. A connection that cannot be
 * established is reported in the adapter's own vocabulary, so a composition root sees one
 * storage error type rather than two.
 */
export const storageClientLayer = (
  config: PostgresClientOptions,
): Layer.Layer<PgClient.PgClient | SqlClient.SqlClient, PostgresStorageError> =>
  PgClient.layer({ ...config, types: makeStorageTypeRegistry() }).pipe(
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
  );
