import { Context, Effect, Layer, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type * as Statement from "effect/unstable/sql/Statement";

/** Catalogue relations an adapter asks about; a partitioned table answers as a table. */
export const SqlObjectKind = Schema.Literals(["table", "index", "any"]);

export type SqlObjectKind = typeof SqlObjectKind.Type;

const SqlObjectRows = Schema.Array(Schema.Struct({ name: Schema.NonEmptyString }));

/**
 * The dialect-specific SQL this package cannot express portably. Shared `Sql*` storage modules
 * build every other statement from standard SQL, so an adapter supplies only these fragments to
 * reuse them. Paths address JSON stored as text in an adapter-owned column.
 */
export class SqlDialect extends Context.Service<
  SqlDialect,
  {
    /** Scalar text at a JSON path, comparable with bound string parameters. */
    readonly jsonText: (column: string, path: ReadonlyArray<string>) => Statement.Fragment;
    /** Whether a JSON boolean at this path is present and true. */
    readonly jsonIsTrue: (column: string, path: ReadonlyArray<string>) => Statement.Fragment;
    /** Whether a text column holds syntactically valid JSON. */
    readonly jsonIsValid: (column: string) => Statement.Fragment;
    /** Equality treating two nulls as equal rather than unknown. */
    readonly nullSafeEquals: (left: Statement.Fragment, right: string | null) => Statement.Fragment;
    /** Which of these adapter-owned relations exist, so a caller can tell a fresh database
     * from a partially created one without reading a dialect's catalogue itself. */
    readonly existingObjects: (
      kind: SqlObjectKind,
      names: ReadonlyArray<string>,
    ) => Effect.Effect<ReadonlyArray<string>>;
  }
>()("@effect-agent/durable/SqlDialect") {
  /**
   * Retained as the default so existing adapters and their stored databases keep their exact
   * statement text, including the expressions their indexes were built on.
   */
  static readonly layerSqlite: Layer.Layer<SqlDialect, never, SqlClient.SqlClient> = Layer.effect(
    this,
  )(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      const extract = (column: string, path: ReadonlyArray<string>) =>
        `json_extract(${column}, '$.${path.join(".")}')`;

      return {
        jsonText: (column, path) => sql.literal(extract(column, path)),
        jsonIsTrue: (column, path) => sql.literal(`COALESCE(${extract(column, path)}, 0) = 1`),
        jsonIsValid: (column) => sql.literal(`json_valid(${column})`),
        nullSafeEquals: (left, right) => sql`${left} IS ${right}`,
        existingObjects: (kind, names) =>
          names.length === 0
            ? Effect.succeed([])
            : decodeNames(
                sql`SELECT name FROM sqlite_master WHERE name IN ${sql.in(names)} ${
                  kind === "any" ? sql`` : sql`AND type = ${kind}`
                }`,
              ),
      };
    }),
  );

  /**
   * `#>>` yields the same scalar text as SQLite's `json_extract`, so the shared comparisons
   * against bound string parameters stay unchanged.
   */
  static readonly layerPostgres: Layer.Layer<SqlDialect, never, SqlClient.SqlClient> = Layer.effect(
    this,
  )(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      const extract = (column: string, path: ReadonlyArray<string>) =>
        `${column}::jsonb #> '{${jsonPath(path)}}'`;

      return {
        jsonText: (column, path) => sql.literal(`(${column}::jsonb #>> '{${jsonPath(path)}}')`),
        jsonIsTrue: (column, path) =>
          sql.literal(`COALESCE((${extract(column, path)}) = 'true'::jsonb, false)`),
        // `IS JSON` is a Postgres 16 predicate; the adapter requires that version.
        jsonIsValid: (column) => sql.literal(`(${column} IS JSON)`),
        nullSafeEquals: (left, right) => sql`${left} IS NOT DISTINCT FROM ${right}`,
        existingObjects: (kind, names) =>
          names.length === 0
            ? Effect.succeed([])
            : decodeNames(
                sql`
                  SELECT c.relname AS name
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE c.relname IN ${sql.in(names)}
                    AND c.relkind IN ${sql.in(RELKINDS[kind])}
                    AND n.nspname = ANY (current_schemas(false))
                `,
              ),
      };
    }),
  );
}

const RELKINDS = { table: ["r", "p"], index: ["i"], any: ["r", "p", "i"] } as const;

/**
 * A catalogue that answers with anything but bounded relation names is not a catalogue this
 * adapter can reason about, so decoding failure is a defect rather than a typed absence.
 */
const decodeNames = (
  query: Effect.Effect<ReadonlyArray<unknown>, unknown>,
): Effect.Effect<ReadonlyArray<string>> =>
  query.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(SqlObjectRows)),
    Effect.map((rows) => rows.map((row) => row.name)),
    Effect.orDie,
  );

/**
 * A path segment reaches Postgres inside a literal `'{...}'` array, so a segment carrying that
 * array's own syntax is quoted rather than concatenated as-is.
 */
const jsonPath = (path: ReadonlyArray<string>): string =>
  path
    .map((segment) =>
      /^[A-Za-z0-9_]+$/.test(segment) ? segment : `"${segment.replace(/(["\\])/g, "\\$1")}"`,
    )
    .join(",");
