import { Effect, Exit, Option, Schema, SchemaTransformation } from "effect";
import type { Cause } from "effect";
import type { SqlClient } from "effect/unstable/sql/SqlClient";
import { SqlError, UnknownError } from "effect/unstable/sql/SqlError";
import type { Statement } from "effect/unstable/sql/Statement";

import type { SqlStorageFailpointLocation } from "./SqlStorageFailpoint.ts";

/** Decode finite SQL numbers and safe native bigints without rounding integer values. */
export const SqlNumber = Schema.Union([
  Schema.Finite,
  Schema.BigInt.check(
    Schema.isBetweenBigInt({
      minimum: BigInt(Number.MIN_SAFE_INTEGER),
      maximum: BigInt(Number.MAX_SAFE_INTEGER),
    }),
  ),
]).pipe(
  Schema.decodeTo(
    Schema.Finite,
    SchemaTransformation.transform<number, number | bigint>({
      decode: Number,
      encode: (value) => value,
    }),
  ),
);

/** Apply persisted integer constraints after native numeric decoding. */
export const SqlInteger = SqlNumber.pipe(Schema.decodeTo(Schema.Int));

const PostgresText = Schema.String.check(
  Schema.makeFilter((value) => !value.includes("\0") && !/[\uD800-\uDFFF]/u.test(value), {
    expected: "PostgreSQL text without NUL or unpaired UTF-16 surrogates",
  }),
);

/**
 * Qualify storage relations and execute native statements without application name transforms.
 * PostgreSQL validates text parameters after compilation; escaped canonical JSON stays intact.
 */
export const makeSqlQuery = (sql: SqlClient, namespace?: string) => {
  const postgres = sql.onDialectOrElse({ pg: () => true, orElse: () => false });

  const execute = Effect.fnUntraced(function* <A extends object>(statement: Statement<A>) {
    if (postgres) {
      for (const parameter of statement.compile(true)[1]) {
        if (typeof parameter === "string") {
          yield* Schema.decodeEffect(PostgresText)(parameter).pipe(
            Effect.mapError((cause) =>
              SqlError.make({
                reason: UnknownError.make({
                  cause,
                  operation: "encode SQL parameter",
                  message: "Storage SQL text cannot be represented in PostgreSQL.",
                }),
              }),
            ),
          );
        }
      }
    }

    return yield* statement.withoutTransform;
  });

  return {
    table: (name: string) => sql(namespace === undefined ? name : `${namespace}.${name}`),
    execute,
  };
};

/** Adapter-owned Schema errors remain concrete in shared operation error channels. */
export interface Diagnostic extends Cause.YieldableError {
  readonly _tag: string;
}

export interface StorageErrorFields {
  readonly operation: string;
  readonly message: string;
  readonly cause?: unknown;
}

export interface CorruptionErrorFields {
  readonly table: string;
  readonly rowKey: string;
  readonly message: string;
}

export interface SqlStorageErrors<S extends Diagnostic, C extends Diagnostic> {
  readonly storage: (fields: StorageErrorFields) => S;
  readonly corruption: (fields: CorruptionErrorFields) => C;
  readonly isCorruption: (error: unknown) => error is C;
}

/** The adapter owns connection reservation, isolation, locking and commit/rollback. */
export interface SqlTransactions<S extends Diagnostic, W extends Diagnostic> {
  readonly withWriteTransaction: (
    operation: string,
  ) => <A, E, R>(body: Effect.Effect<A, E, R>) => Effect.Effect<A, E | S | W, R>;
  readonly withReadTransaction: (
    operation: string,
  ) => <A, E, R>(body: Effect.Effect<A, E, R>) => Effect.Effect<A, E | S, R>;
  readonly isTransactionFailure: (error: unknown) => error is S | W;
}

export type SqlStorageFailpoint<F extends Diagnostic> = (
  location: SqlStorageFailpointLocation,
) => Effect.Effect<void, F>;

/** Native transaction shape used by stores whose errors already belong to their ports. */
export type SqlWriteTransaction = SqlClient["withTransaction"];

/** Decode database rows once, preserving the adapter's corruption error class. */
export const makeRowDecoder = <C extends Diagnostic>(
  corruption: (fields: CorruptionErrorFields) => C,
) => {
  const decodeRows = <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<ReadonlyArray<A>, C> =>
    Schema.decodeUnknownEffect(schema)(rows).pipe(
      Effect.mapError((error) => corruption({ table, rowKey, message: String(error) })),
    );

  const decodeSingleRow = <A, I>(
    schema: Schema.Codec<ReadonlyArray<A>, ReadonlyArray<I>>,
    table: string,
    rowKey: string,
    rows: unknown,
  ): Effect.Effect<A, C> =>
    decodeRows(schema, table, rowKey, rows).pipe(
      Effect.flatMap((decoded) =>
        decoded.length === 1
          ? Effect.succeed(decoded[0])
          : Effect.fail(
              corruption({
                table,
                rowKey,
                message: `Expected exactly one row but found ${decoded.length}.`,
              }),
            ),
      ),
    );

  return { decodeRows, decodeSingleRow };
};

/**
 * A top-level transaction on one reserved connection. The caller selects the native locking
 * and snapshot semantics. An ambient SQL transaction fails before reserving another connection.
 */
export const makeSqlTransaction =
  (
    sql: SqlClient,
    options: {
      readonly begin:
        | "BEGIN"
        | "BEGIN IMMEDIATE"
        | "BEGIN ISOLATION LEVEL READ COMMITTED"
        | "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";
      readonly prelude?: Effect.Effect<void, SqlError>;
    },
  ): SqlWriteTransaction =>
  <A, E, R>(body: Effect.Effect<A, E, R>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.scoped(
        Effect.gen(function* () {
          if (Option.isSome(yield* Effect.serviceOption(sql.transactionService))) {
            return yield* SqlError.make({
              reason: UnknownError.make({
                cause: undefined,
                operation: "begin storage transaction",
                message: "Storage transactions cannot run inside an existing SQL transaction.",
              }),
            });
          }
          const connection = yield* restore(sql.reserve);

          yield* connection.executeUnprepared(options.begin, [], undefined);

          const exit = yield* restore(
            Effect.provideService(
              Effect.andThen(options.prelude ?? Effect.void, body),
              sql.transactionService,
              [connection, 0] as const,
            ),
          ).pipe(Effect.exit);

          if (Exit.isFailure(exit)) {
            yield* Effect.orDie(connection.executeUnprepared("ROLLBACK", [], undefined));

            return yield* exit;
          }

          const committed = yield* connection
            .executeUnprepared("COMMIT", [], undefined)
            .pipe(Effect.exit);

          if (Exit.isFailure(committed)) {
            yield* Effect.orDie(connection.executeUnprepared("ROLLBACK", [], undefined));

            return yield* Effect.failCause(committed.cause);
          }

          return exit.value;
        }),
      ),
    );
