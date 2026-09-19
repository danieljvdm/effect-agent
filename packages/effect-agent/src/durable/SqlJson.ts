import type * as SqlClient from "effect/unstable/sql/SqlClient";
import type * as Statement from "effect/unstable/sql/Statement";

/**
 * JSON access over a text column, which no two dialects spell alike. SQLite is the fallback
 * rather than one case among others because the shipped SQLite adapter's indexes are built on
 * these exact expressions.
 */
export const jsonText = (
  sql: SqlClient.SqlClient,
  column: string,
  path: ReadonlyArray<string>,
): Statement.Fragment =>
  sql.onDialectOrElse({
    orElse: () => sql.literal(sqliteExtract(column, path)),
    pg: () => sql.literal(`(${column}::jsonb #>> '{${pgPath(path)}}')`),
  });

export const jsonIsTrue = (
  sql: SqlClient.SqlClient,
  column: string,
  path: ReadonlyArray<string>,
): Statement.Fragment =>
  sql.onDialectOrElse({
    orElse: () => sql.literal(`COALESCE(${sqliteExtract(column, path)}, 0) = 1`),
    pg: () =>
      sql.literal(`COALESCE((${column}::jsonb #> '{${pgPath(path)}}') = 'true'::jsonb, false)`),
  });

/** `IS JSON` is a Postgres 16 predicate. */
export const jsonIsValid = (sql: SqlClient.SqlClient, column: string): Statement.Fragment =>
  sql.onDialectOrElse({
    orElse: () => sql.literal(`json_valid(${column})`),
    pg: () => sql.literal(`(${column} IS JSON)`),
  });

export const nullSafeEquals = (
  sql: SqlClient.SqlClient,
  left: Statement.Fragment,
  right: string | null,
): Statement.Fragment =>
  sql.onDialectOrElse({
    orElse: () => sql`${left} IS ${right}`,
    pg: () => sql`${left} IS NOT DISTINCT FROM ${right}`,
  });

const sqliteExtract = (column: string, path: ReadonlyArray<string>): string =>
  `json_extract(${column}, '$.${path.join(".")}')`;

/** A segment reaches Postgres inside a literal `'{...}'` array, so it is quoted, not concatenated. */
const pgPath = (path: ReadonlyArray<string>): string =>
  path
    .map((segment) =>
      /^[A-Za-z0-9_]+$/.test(segment) ? segment : `"${segment.replace(/(["\\])/g, "\\$1")}"`,
    )
    .join(",");
