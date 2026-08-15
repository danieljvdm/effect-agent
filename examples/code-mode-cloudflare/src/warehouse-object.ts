import { SqliteClient } from "@effect/sql-sqlite-do";
import { DurableObject } from "cloudflare:workers";
import { Context, Effect, Layer, Option, Predicate, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlError } from "effect/unstable/sql/SqlError";

/**
 * The warehouse Durable Object: a SQLite-backed store of curated invoice data
 * that generated Code Mode programs query through a read-only SQL Tool. The
 * database is the trust boundary — it is locked with `PRAGMA query_only = ON`
 * so the connection denies every write, and only a curated `invoice_summary`
 * view is exposed. Nothing about the connection, credentials, or storage ever
 * reaches the isolated executor: generated code sees only the brokered
 * `warehouse.query` method and its Schema-bounded result.
 *
 * This is the demo's "SQLite DO db": one Durable Object per tenant name,
 * seeded once, then read-only.
 */

const MAX_ROWS = 200;
const MAX_RESULT_BYTES = 256 * 1024;

/** A read-only query outcome returned across the DO RPC boundary as plain JSON. */
export interface WarehouseQueryOutcome {
  readonly ok: boolean;
  readonly columns: ReadonlyArray<string>;
  readonly rows: ReadonlyArray<Record<string, unknown>>;
  readonly rowCount: number;
  readonly truncated: boolean;
  /** Present when `ok` is false: a stable denial/failure reason. */
  readonly reason?: string;
}

const seedRows: ReadonlyArray<{
  readonly customer: string;
  readonly region: string;
  readonly revenue: number;
  readonly createdAt: string;
}> = [
  { customer: "Stellar Freight", region: "emea", revenue: 48_200, createdAt: "2026-07-03" },
  { customer: "Nimbus Analytics", region: "amer", revenue: 12_800, createdAt: "2026-07-11" },
  { customer: "Copper Kettle Co", region: "amer", revenue: 730, createdAt: "2026-07-15" },
  { customer: "Harbor Lights Ltd", region: "apac", revenue: 9_400, createdAt: "2026-07-21" },
  { customer: "Vertex Robotics", region: "emea", revenue: 21_050, createdAt: "2026-07-24" },
];

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

// Cloudflare Durable Object SQLite blocks `PRAGMA query_only` through its
// statement authorizer (SQLITE_AUTH), so — unlike the Node reference fixture,
// which locks the connection read-only and lets database authority deny every
// write (the primary boundary the plan §8.4 prescribes) — this demo enforces
// read-only with a leading-keyword denylist over the literal-stripped
// statement. A production deployment should back the warehouse with a
// read-only database identity or curated read-only views instead of relying
// on this scan.
const writeKeywords =
  /^\s*(insert|update|delete|replace|drop|create|alter|truncate|reindex|analyze)\b/i;
const escapeHatchKeywords = /\b(pragma|attach|detach|vacuum|load_extension)\b/i;

const scanReadOnly = (sql: string): string | undefined => {
  const stripped = stripLiteralsAndComments(sql);
  const semicolon = stripped.indexOf(";");
  if (semicolon !== -1 && stripped.slice(semicolon + 1).trim().length > 0) {
    return "exactly one statement is allowed per call";
  }
  if (writeKeywords.test(stripped)) {
    return "the warehouse is read-only";
  }
  const denied = escapeHatchKeywords.exec(stripped);
  return denied === null ? undefined : `${denied[1].toUpperCase()} is not available`;
};

const isWriteDenial = (error: SqlError): boolean => {
  const cause = (error.reason as { readonly cause?: unknown }).cause;
  return (
    Predicate.isObject(cause) &&
    "errcode" in cause &&
    typeof cause.errcode === "number" &&
    (cause.errcode & 0xff) === 8
  );
};

const utf8ByteLength = (value: string): number => new TextEncoder().encode(value).byteLength;

const decodeRow = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Json));

/**
 * Run one bounded read-only query over the DO's SQLite storage. Write denial
 * comes from `PRAGMA query_only = ON` (structural `SQLITE_READONLY`), not from
 * the scanner, which only covers the escape hatches the authority cannot
 * police.
 */
const runQuery = (
  storage: DurableObjectStorage,
  sql: string,
  parameters: ReadonlyArray<string | number | boolean | null>,
): Effect.Effect<WarehouseQueryOutcome> =>
  Effect.gen(function* () {
    const denied = scanReadOnly(sql);
    if (denied !== undefined) {
      return { ok: false, columns: [], rows: [], rowCount: 0, truncated: false, reason: denied };
    }
    const client = yield* SqlClient.SqlClient;
    const rawRows = yield* client
      .unsafe<Record<string, unknown>>(sql, parameters as Array<unknown>)
      .withoutTransform.pipe(
        Effect.map((rows) => ({ ok: true as const, rows })),
        Effect.catchTag("SqlError", (error) =>
          Effect.succeed({
            ok: false as const,
            reason: isWriteDenial(error)
              ? "the warehouse database is read-only"
              : `query failed: ${String(error.reason.message ?? "unknown").slice(0, 500)}`,
          }),
        ),
      );
    if (!rawRows.ok) {
      return {
        ok: false,
        columns: [],
        rows: [],
        rowCount: 0,
        truncated: false,
        reason: rawRows.reason,
      };
    }
    const rows: Array<Record<string, unknown>> = [];
    let truncated = false;
    let usedBytes = 0;
    for (const raw of rawRows.rows) {
      if (rows.length >= MAX_ROWS) {
        truncated = true;
        break;
      }
      const decoded = decodeRow(raw);
      if (Option.isNone(decoded)) {
        return {
          ok: false,
          columns: [],
          rows: [],
          rowCount: 0,
          truncated: false,
          reason: "a result row contained a non-JSON value",
        };
      }
      const bytes = utf8ByteLength(JSON.stringify(decoded.value));
      if (usedBytes + bytes > MAX_RESULT_BYTES) {
        truncated = true;
        break;
      }
      usedBytes += bytes;
      rows.push(decoded.value);
    }
    return {
      ok: true,
      columns: rows.length === 0 ? [] : Object.keys(rows[0]),
      rows,
      rowCount: rows.length,
      truncated,
    };
  }).pipe(Effect.provide(SqliteClient.layer({ storage })), Effect.orDie);

export class WarehouseObject extends DurableObject {
  #ready = false;

  async #ensureSeeded(): Promise<void> {
    if (this.#ready) return;
    await Effect.runPromise(
      Effect.gen(function* () {
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
            yield* sql`INSERT INTO invoice_summary ${sql.insert({
              customer: row.customer,
              region: row.region,
              revenue: row.revenue,
              created_at: row.createdAt,
            })}`;
          }
        }
      }).pipe(
        Effect.provide(SqliteClient.layer({ storage: this.ctx.storage })),
        Effect.orDie,
      ) as Effect.Effect<void>,
    );
    // On Durable Object SQLite the `PRAGMA query_only` lock is authorizer-
    // blocked (SQLITE_AUTH), so the read-only guarantee here is the
    // leading-keyword scan in `scanReadOnly`; the Node reference fixture in
    // `@effect-agent/testing` proves the stronger database-authority path.
    this.#ready = true;
  }

  /** The read-only query RPC the warehouse Tool calls (Workers RPC returns JSON). */
  async query(
    sql: string,
    parameters: ReadonlyArray<string | number | boolean | null>,
  ): Promise<WarehouseQueryOutcome> {
    await this.#ensureSeeded();
    return Effect.runPromise(runQuery(this.ctx.storage, sql, parameters));
  }
}

/** Effect service wrapping the warehouse DO namespace (host-side authority). */
export class Warehouse extends Context.Service<
  Warehouse,
  {
    readonly query: (
      sql: string,
      parameters: ReadonlyArray<string | number | boolean | null>,
    ) => Effect.Effect<WarehouseQueryOutcome>;
  }
>()("@effect-agent/example-code-mode-cloudflare/Warehouse") {}

/** Build the Warehouse service from a resolved DO namespace + tenant name. */
export const warehouseLayer = (
  namespace: DurableObjectNamespace<WarehouseObject>,
  tenant: string,
): Layer.Layer<Warehouse> =>
  Layer.succeed(Warehouse)({
    query: (sql, parameters) =>
      Effect.promise(() => {
        const stub = namespace.get(namespace.idFromName(tenant));
        return stub.query(sql, [...parameters]);
      }),
  });
