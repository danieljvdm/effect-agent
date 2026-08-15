import { D1Client } from "@effect/sql-d1";
import { Effect, Layer, Option, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * The invoice database is a plain D1 binding (`DB` in wrangler.jsonc) — an
 * ordinary Cloudflare SQL database, reached through the standard Effect
 * `SqlClient`. Only a curated `invoice_summary` table is exposed, and every
 * model-driven query goes through the read-only ALLOWLIST scan below. Nothing
 * about the binding ever reaches the isolated executor: generated code sees
 * only the brokered `invoices.query` method and its Schema-bounded result.
 */

/**
 * `SqlClient` over the raw D1 binding. Built from the binding VALUE (not a
 * config lookup) so it can be composed inside the Conversation Object's
 * binding capture, which requires an error-free Layer.
 */
export const invoiceDbSqlLayer = (db: D1Database): Layer.Layer<SqlClient.SqlClient> =>
  Layer.orDie(D1Client.layer({ db }));

const MAX_ROWS = 200;

/** A read-only query outcome the tool handler branches on. */
export interface QueryOutcome {
  readonly ok: boolean;
  readonly columns: ReadonlyArray<string>;
  readonly rows: ReadonlyArray<Record<string, unknown>>;
  readonly rowCount: number;
  readonly truncated: boolean;
  /** Present when `ok` is false: a stable denial/failure reason. */
  readonly reason?: string;
}

const seedRows = [
  { customer: "Stellar Freight", region: "emea", revenue: 48_200, created_at: "2026-07-03" },
  { customer: "Nimbus Analytics", region: "amer", revenue: 12_800, created_at: "2026-07-11" },
  { customer: "Copper Kettle Co", region: "amer", revenue: 730, created_at: "2026-07-15" },
  { customer: "Harbor Lights Ltd", region: "apac", revenue: 9_400, created_at: "2026-07-21" },
  { customer: "Vertex Robotics", region: "emea", revenue: 21_050, created_at: "2026-07-24" },
] as const;

/**
 * Idempotent schema + seed. The COUNT guard makes re-runs no-ops, so the
 * agent host can run this on every instance load.
 */
export const seedInvoices: Effect.Effect<void, unknown, SqlClient.SqlClient> = Effect.gen(
  function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE IF NOT EXISTS invoice_summary (
      customer TEXT NOT NULL,
      region TEXT NOT NULL,
      revenue INTEGER NOT NULL,
      created_at TEXT NOT NULL
    )`;
    const existing = yield* sql`SELECT COUNT(*) AS n FROM invoice_summary`.pipe(
      Effect.map((rows) => Number((rows[0] as { n?: unknown }).n ?? 0)),
    );
    if (existing === 0) {
      for (const row of seedRows) {
        yield* sql`INSERT INTO invoice_summary ${sql.insert(row)}`;
      }
    }
  },
);

const stripLiteralsAndComments = (sql: string): string => {
  let output = "";
  let index = 0;
  while (index < sql.length) {
    const char = sql[index];
    if (char === "'") {
      index += 1;
      while (index < sql.length) {
        if (sql[index] === "'" && sql[index + 1] === "'") {
          index += 2;
          continue;
        }
        if (sql[index] === "'") {
          index += 1;
          break;
        }
        index += 1;
      }
      output += " ";
      continue;
    }
    if (char === "-" && sql[index + 1] === "-") {
      while (index < sql.length && sql[index] !== "\n") index += 1;
      continue;
    }
    output += char;
    index += 1;
  }
  return output;
};

// The model-facing query path is read-only. Because a denylist of write
// keywords is bypassable (a `WITH … DELETE` common-table expression does not
// START with a write keyword), the scan is an ALLOWLIST: the statement must
// be a single read (`SELECT`, or a `WITH` whose body is a read) and must
// contain no write, DDL, transaction, or escape-hatch token ANYWHERE in the
// literal-stripped text. A production deployment should back this with a
// read-only database identity or curated read-only views rather than a text
// scan.
const readOnlyPrefix = /^\s*(select|with)\b/i;
const disallowedTokens =
  /\b(insert|update|delete|replace|drop|create|alter|truncate|reindex|analyze|pragma|attach|detach|vacuum|load_extension|savepoint|release|begin|commit|rollback)\b/i;

const scanReadOnly = (sql: string): string | undefined => {
  const stripped = stripLiteralsAndComments(sql);
  const semicolon = stripped.indexOf(";");
  if (semicolon !== -1 && stripped.slice(semicolon + 1).trim().length > 0) {
    return "exactly one statement is allowed per call";
  }
  if (!readOnlyPrefix.test(stripped)) {
    return "only read-only SELECT statements are allowed";
  }
  const disallowed = disallowedTokens.exec(stripped);
  return disallowed === null
    ? undefined
    : `${disallowed[1].toUpperCase()} is not allowed in a read-only query`;
};

const decodeRow = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Json));

const deniedOutcome = (reason: string): QueryOutcome => ({
  ok: false,
  columns: [],
  rows: [],
  rowCount: 0,
  truncated: false,
  reason,
});

/**
 * Run one bounded read-only query through the Effect `SqlClient` over D1.
 * Expected failures (denied statement, SQL error) are values the tool handler
 * branches on, never defects.
 */
export const runReadOnlyQuery = (
  text: string,
  parameters: ReadonlyArray<string | number | boolean | null>,
): Effect.Effect<QueryOutcome, never, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const denied = scanReadOnly(text);
    if (denied !== undefined) {
      return deniedOutcome(denied);
    }
    const sql = yield* SqlClient.SqlClient;
    const raw = yield* sql
      .unsafe<Record<string, unknown>>(text, parameters as Array<unknown>)
      .withoutTransform.pipe(
        Effect.map((rows) => ({ ok: true as const, rows })),
        Effect.catch((error) =>
          Effect.succeed({
            ok: false as const,
            reason: `query failed: ${String(error.message).slice(0, 500)}`,
          }),
        ),
      );
    if (!raw.ok) {
      return deniedOutcome(raw.reason);
    }
    const rows: Array<Record<string, unknown>> = [];
    let truncated = false;
    for (const row of raw.rows) {
      if (rows.length >= MAX_ROWS) {
        truncated = true;
        break;
      }
      const decoded = decodeRow(row);
      if (Option.isNone(decoded)) {
        return deniedOutcome("a result row contained a non-JSON value");
      }
      rows.push(decoded.value);
    }
    return {
      ok: true,
      columns: rows.length === 0 ? [] : Object.keys(rows[0]),
      rows,
      rowCount: rows.length,
      truncated,
    };
  });
