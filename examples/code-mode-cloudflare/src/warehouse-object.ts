import { DurableObject } from "cloudflare:workers";
import { Context, Effect, Layer, Option, Schema } from "effect";

/**
 * The warehouse Durable Object: a SQLite-backed store of curated invoice data
 * that generated Code Mode programs query through a read-only SQL Tool. Only a
 * single curated `invoice_summary` table is exposed, and every query goes
 * through the read-only ALLOWLIST scan (`scanReadOnly`) — Durable Object
 * SQLite cannot lock the connection with `PRAGMA query_only`, so the demo
 * permits only single read statements (see the README for the production
 * guidance). Nothing about the connection, credentials, or storage ever
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
// read-only in application code. Because a denylist of write keywords is
// bypassable (e.g. a `WITH … DELETE` common-table expression does not START
// with a write keyword), the scan is an ALLOWLIST: the statement must be a
// single read (`SELECT`, or a `WITH` whose body is a read) and must contain
// no write, DDL, or escape-hatch token ANYWHERE in the literal-stripped text.
// A production deployment should still back the warehouse with a read-only
// database identity or curated read-only views rather than a text scan.
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

const utf8ByteLength = (value: string): number => new TextEncoder().encode(value).byteLength;

const decodeRow = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Json));

/**
 * Run one bounded read-only query over the DO's SQLite storage. The read-only
 * boundary is the ALLOWLIST scan (`scanReadOnly`) — Durable Object SQLite
 * cannot lock the connection with `PRAGMA query_only`, so this demo permits
 * only single read statements. The row and byte caps are enforced WHILE
 * draining the cursor (early break), so a query that would return a huge
 * result set never fully materializes.
 */
const runQuery = (
  storage: DurableObjectStorage,
  sql: string,
  parameters: ReadonlyArray<string | number | boolean | null>,
): Effect.Effect<WarehouseQueryOutcome> =>
  Effect.sync(() => {
    const denied = scanReadOnly(sql);

    if (denied !== undefined) {
      return { ok: false, columns: [], rows: [], rowCount: 0, truncated: false, reason: denied };
    }
    let cursor;

    try {
      cursor = storage.sql.exec(sql, ...parameters);
    } catch (cause) {
      return {
        ok: false,
        columns: [],
        rows: [],
        rowCount: 0,
        truncated: false,
        reason: `query failed: ${(cause instanceof Error ? cause.message : String(cause)).slice(0, 500)}`,
      };
    }
    const rows: Array<Record<string, unknown>> = [];
    let truncated = false;
    let usedBytes = 0;

    for (const raw of cursor) {
      if (rows.length >= MAX_ROWS) {
        truncated = true;
        break;
      }
      const decoded = decodeRow(raw as Record<string, unknown>);

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
  });

export class WarehouseObject extends DurableObject {
  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    ctx.storage.transactionSync(() => {
      const sql = ctx.storage.sql;

      sql.exec(`CREATE TABLE IF NOT EXISTS invoice_summary (
          customer TEXT NOT NULL,
          region TEXT NOT NULL,
          revenue INTEGER NOT NULL,
          created_at TEXT NOT NULL
        )`);

      if (sql.exec("SELECT 1 FROM invoice_summary LIMIT 1").toArray().length > 0) return;

      for (const row of seedRows) {
        sql.exec(
          "INSERT INTO invoice_summary (customer, region, revenue, created_at) VALUES (?, ?, ?, ?)",
          row.customer,
          row.region,
          row.revenue,
          row.createdAt,
        );
      }
    });
  }

  /** The read-only query RPC the warehouse Tool calls (Workers RPC returns JSON). */
  query(
    sql: string,
    parameters: ReadonlyArray<string | number | boolean | null>,
  ): Promise<WarehouseQueryOutcome> {
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

/** A safe tenant name: the DO is addressed by this, so bound it tightly. */
export const isValidTenant = (tenant: string): boolean => /^[a-z0-9-]{1,32}$/.test(tenant);

/** Build the Warehouse service from a resolved DO namespace + tenant name. */
export const warehouseLayer = (
  namespace: DurableObjectNamespace<WarehouseObject>,
  tenant: string,
): Layer.Layer<Warehouse> =>
  Layer.succeed(Warehouse)({
    query: (sql, parameters) =>
      // A Durable Object RPC failure (transport, DO exception) is an EXPECTED
      // outcome the tool handler branches on, not a defect — surface it as a
      // typed denied outcome rather than dying the pass.
      Effect.tryPromise((): Promise<WarehouseQueryOutcome> => {
        const stub = namespace.get(namespace.idFromName(tenant));

        return stub.query(sql, [...parameters]) as Promise<WarehouseQueryOutcome>;
      }).pipe(
        Effect.catch((error) =>
          Effect.succeed<WarehouseQueryOutcome>({
            ok: false,
            columns: [],
            rows: [],
            rowCount: 0,
            truncated: false,
            reason: `warehouse unavailable: ${(error.cause instanceof Error
              ? error.cause.message
              : String(error.cause)
            ).slice(0, 300)}`,
          }),
        ),
      ),
  });
