import { Context, Effect, Layer } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type * as Statement from "effect/unstable/sql/Statement";

/**
 * The dialect-specific SQL this package cannot express portably. Shared `Sql*` storage
 * modules build every other statement from standard SQL, so an adapter supplies only these
 * fragments to reuse them. Paths address JSON stored as text in an adapter-owned column.
 */
export class SqlDialect extends Context.Service<
  SqlDialect,
  {
    /** Scalar text at a JSON path, comparable with bound string parameters. */
    readonly jsonText: (column: string, path: ReadonlyArray<string>) => Statement.Fragment;
    /**
     * Whether a JSON boolean at this path is present and true. Dialects disagree on the
     * scalar a JSON boolean decodes to, so the comparison itself belongs here.
     */
    readonly jsonIsTrue: (column: string, path: ReadonlyArray<string>) => Statement.Fragment;
    /** Whether a text column holds syntactically valid JSON. */
    readonly jsonIsValid: (column: string) => Statement.Fragment;
    /** Equality that treats two nulls as equal rather than unknown. */
    readonly nullSafeEquals: (left: Statement.Fragment, right: string | null) => Statement.Fragment;
    /**
     * Which of these adapter-owned relations already exist, so callers can distinguish a
     * fresh database from a partially created one without parsing a dialect's catalog.
     */
    readonly existingObjects: (
      kind: "table" | "index" | "any",
      names: ReadonlyArray<string>,
    ) => Effect.Effect<ReadonlyArray<string>>;
  }
>()("@effect-agent/durable/SqlDialect") {}

/**
 * SQLite dialect. Retained as the default so existing adapters and stored databases keep
 * their exact statement text, including the expressions their indexes were built on.
 */
export const sqliteLayer: Layer.Layer<SqlDialect, never, SqlClient.SqlClient> = Layer.effect(
  SqlDialect,
)(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    const existingObjects = (
      kind: "table" | "index" | "any",
      names: ReadonlyArray<string>,
    ): Effect.Effect<ReadonlyArray<string>> =>
      names.length === 0
        ? Effect.succeed([])
        : sql<{ readonly name: string }>`SELECT name FROM sqlite_master WHERE name IN ${sql.in(
            names,
          )} ${kind === "any" ? sql`` : sql`AND type = ${kind}`}`.pipe(
            Effect.map((rows) => rows.map((row) => row.name)),
            Effect.orDie,
          );

    return {
      jsonText: (column, path) => sql.literal(`json_extract(${column}, '$.${path.join(".")}')`),
      jsonIsTrue: (column, path) =>
        sql.literal(`COALESCE(json_extract(${column}, '$.${path.join(".")}'), 0) = 1`),
      jsonIsValid: (column) => sql.literal(`json_valid(${column})`),
      nullSafeEquals: (left, right) => sql`${left} IS ${right}`,
      existingObjects,
    };
  }),
);

/**
 * Postgres dialect. `#>>` yields the same scalar text as SQLite's `json_extract`, so the
 * shared comparisons against bound string parameters stay unchanged.
 */
export const postgresLayer: Layer.Layer<SqlDialect, never, SqlClient.SqlClient> = Layer.effect(
  SqlDialect,
)(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    // `relkind` distinguishes the two kinds the callers ask about: 'r' is an ordinary
    // table and 'i' an index. The search path scopes the lookup to the adapter's schema.
    const relkinds = { table: ["r", "p"], index: ["i"], any: ["r", "p", "i"] } as const;

    const existingObjects = (
      kind: "table" | "index" | "any",
      names: ReadonlyArray<string>,
    ): Effect.Effect<ReadonlyArray<string>> =>
      names.length === 0
        ? Effect.succeed([])
        : sql<{ readonly name: string }>`
            SELECT c.relname AS name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname IN ${sql.in(names)}
              AND c.relkind IN ${sql.in(relkinds[kind])}
              AND n.nspname = ANY (current_schemas(false))
          `.pipe(
            Effect.map((rows) => rows.map((row) => row.name)),
            Effect.orDie,
          );

    return {
      jsonText: (column, path) =>
        sql.literal(`(${column}::jsonb #>> '{${path.map(quoteSegment).join(",")}}')`),
      jsonIsTrue: (column, path) =>
        sql.literal(
          `COALESCE((${column}::jsonb #> '{${path.map(quoteSegment).join(",")}}') = 'true'::jsonb, false)`,
        ),
      // `IS JSON` is a Postgres 16 predicate; the adapter requires that version.
      jsonIsValid: (column) => sql.literal(`(${column} IS JSON)`),
      nullSafeEquals: (left, right) => sql`${left} IS NOT DISTINCT FROM ${right}`,
      existingObjects,
    };
  }),
);

/**
 * A path segment reaches Postgres inside a literal `'{...}'` array, so a segment carrying
 * that array's own syntax must be quoted rather than concatenated as-is.
 */
const quoteSegment = (segment: string): string =>
  /^[A-Za-z0-9_]+$/.test(segment) ? segment : `"${segment.replace(/(["\\])/g, "\\$1")}"`;
