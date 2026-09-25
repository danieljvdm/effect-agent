import type * as SqlClient from "effect/unstable/sql/SqlClient";
import type * as Statement from "effect/unstable/sql/Statement";

/** Exact SQLite expressions are part of existing expression-index layouts. */
export const sqliteJsonText = (
  sql: SqlClient.SqlClient,
  column: string,
  path: ReadonlyArray<string>,
): Statement.Fragment => sql.literal(sqliteExtract(column, path));

export const sqliteJsonIsTrue = (
  sql: SqlClient.SqlClient,
  column: string,
  path: ReadonlyArray<string>,
): Statement.Fragment => sql.literal(`COALESCE(${sqliteExtract(column, path)}, 0) = 1`);

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

/** Escape identifiers in derived Postgres JSON, including NUL and lone UTF-16 surrogates. */
export const queryIdentifier = (sql: SqlClient.SqlClient, value: string | null): string | null =>
  value === null
    ? null
    : sql.onDialectOrElse({ orElse: () => value, pg: () => JSON.stringify(value) });
