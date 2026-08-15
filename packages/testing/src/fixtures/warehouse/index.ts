import { ToolExecutionClass } from "@effect-agent/engine";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Context, Duration, Effect, Layer, Option, Predicate, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlError } from "effect/unstable/sql/SqlError";

/**
 * The C3 read-only SQL reference integration (plan §8, SEC-015): application
 * code built from native Effect AI primitives proving that generated Code
 * Mode programs can query an internal service without the executor ever
 * seeing a connection, credential, or network authority.
 *
 * The guarantee is database authority and data topology, not SQL text
 * inspection: the fixture materializes a curated, tenant-scoped copy of the
 * warehouse into a private in-memory SQLite database (cross-tenant rows are
 * physically absent), then locks the connection with `PRAGMA query_only = ON`
 * so the engine itself denies every write. The keyword scanner underneath is
 * defense in depth for the escape hatches `query_only` cannot close (PRAGMA
 * could re-enable writes; ATTACH reaches the filesystem) — it is never the
 * primary boundary.
 *
 * Statement timeout: the synchronous SQLite driver cannot cancel a running
 * statement, so a `statementTimeout` request is rejected typed at Layer
 * construction rather than silently unenforced (CAP-015 posture). Wall-clock
 * bounding of a whole pass belongs to the Run duration budget and, in Code
 * Mode, the executor's own deadline.
 */

const BoundedSqlText = Schema.NonEmptyString.check(Schema.isMaxLength(16 * 1024));
const SqlParameter = Schema.Union([Schema.String, Schema.Number, Schema.Boolean, Schema.Null]);

/** One curated multi-tenant seed row; only the configured tenant's rows are materialized. */
export class WarehouseSeedRow extends Schema.Class<WarehouseSeedRow>(
  "@effect-agent/testing/WarehouseSeedRow",
)({
  tenant: Schema.NonEmptyString,
  customer: Schema.NonEmptyString,
  region: Schema.NonEmptyString,
  revenue: Schema.Int,
  createdAt: Schema.NonEmptyString,
}) {}

export class WarehouseLimits extends Schema.Class<WarehouseLimits>(
  "@effect-agent/testing/WarehouseLimits",
)({
  maxRows: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(10_000)),
  maxResultBytes: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(4 * 1024 * 1024),
  ),
}) {}

const defaultLimits = WarehouseLimits.make({ maxRows: 256, maxResultBytes: 256 * 1024 });

/** The query was denied before or by the read-only database authority. */
export class WarehouseQueryDenied extends Schema.TaggedError<WarehouseQueryDenied>()(
  "WarehouseQueryDenied",
  {
    reason: Schema.Literals(["write-denied", "multi-statement", "denied-keyword"]),
    message: Schema.String.check(Schema.isMaxLength(2 * 1024)),
  },
) {}

/** The database rejected the query for an ordinary reason (syntax, missing view). */
export class WarehouseQueryFailed extends Schema.TaggedError<WarehouseQueryFailed>()(
  "WarehouseQueryFailed",
  {
    message: Schema.String.check(Schema.isMaxLength(2 * 1024)),
  },
) {}

/** The fixture cannot honestly enforce a requested policy (CAP-015). */
export class WarehouseConfigurationError extends Schema.TaggedError<WarehouseConfigurationError>()(
  "WarehouseConfigurationError",
  {
    message: Schema.String,
  },
) {}

export class WarehouseQuerySuccess extends Schema.Class<WarehouseQuerySuccess>(
  "@effect-agent/testing/WarehouseQuerySuccess",
)({
  columns: Schema.Array(Schema.String).check(Schema.isMaxLength(64)),
  rows: Schema.Array(Schema.Record(Schema.String, Schema.Json)).check(Schema.isMaxLength(10_000)),
  rowCount: Schema.Natural,
  truncated: Schema.Boolean,
}) {}

/**
 * The read-only warehouse query service. Tenant identity is fixed at Layer
 * construction by the host — never by model-controlled arguments (SEC-015).
 */
export class WarehouseDb extends Context.Service<
  WarehouseDb,
  {
    readonly query: (
      sql: string,
      parameters: ReadonlyArray<string | number | boolean | null>,
    ) => Effect.Effect<WarehouseQuerySuccess, WarehouseQueryDenied | WarehouseQueryFailed>;
  }
>()("@effect-agent/testing/WarehouseDb") {}

// ---------------------------------------------------------------------------
// Defense-in-depth SQL scanning: strips string literals and comments, then
// rejects multi-statements and the escape-hatch keywords the read-only
// database authority cannot police itself.
// ---------------------------------------------------------------------------

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
      while (index < sql.length && sql[index] !== "\n") {
        index += 1;
      }
      continue;
    }
    if (char === "/" && sql[index + 1] === "*") {
      index += 2;
      while (index < sql.length && !(sql[index] === "*" && sql[index + 1] === "/")) {
        index += 1;
      }
      index += 2;
      continue;
    }
    output += char;
    index += 1;
  }
  return output;
};

const deniedKeywords = /\b(pragma|attach|detach|vacuum|load_extension)\b/i;

const scanSql = (sql: string): WarehouseQueryDenied | undefined => {
  const stripped = stripLiteralsAndComments(sql);
  const semicolon = stripped.indexOf(";");
  if (semicolon !== -1 && stripped.slice(semicolon + 1).trim().length > 0) {
    return WarehouseQueryDenied.make({
      reason: "multi-statement",
      message: "Exactly one SQL statement is allowed per call",
    });
  }
  const denied = deniedKeywords.exec(stripped);
  if (denied !== null) {
    return WarehouseQueryDenied.make({
      reason: "denied-keyword",
      message: `${denied[1].toUpperCase()} is not available through the read-only warehouse`,
    });
  }
  return undefined;
};

/** Non-cryptographic FNV-1a 64-bit digest for audit correlation of SQL text. */
const queryDigest = (sql: string): string => {
  let hash = 0xcbf29ce484222325n;
  for (let index = 0; index < sql.length; index += 1) {
    hash ^= BigInt(sql.charCodeAt(index));
    hash = (hash * 0x100000001b3n) & 0xffffffffffffffffn;
  }
  return hash.toString(16).padStart(16, "0");
};

const utf8ByteLength = (value: string): number => {
  let total = 0;
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    total += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }
  return total;
};

const decodeRow = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Json));

/**
 * The read-only authority denial is structural: node-sqlite reports SQLite
 * result code 8 (`SQLITE_READONLY`; extended codes keep 8 in the low byte)
 * on the reason's cause. No error-text matching.
 */
const isWriteDenial = (error: SqlError): boolean => {
  const cause = (error.reason as { readonly cause?: unknown }).cause;
  return (
    Predicate.isObject(cause) &&
    "errcode" in cause &&
    typeof cause.errcode === "number" &&
    (cause.errcode & 0xff) === 8
  );
};

const sqlFailureMessage = (error: SqlError): string => {
  const cause = (error.reason as { readonly cause?: unknown }).cause;
  const detail =
    Predicate.isObject(cause) && "errstr" in cause && typeof cause.errstr === "string"
      ? `: ${cause.errstr}`
      : "";
  return `${error.reason.message ?? "SQL execution failed"}${detail}`.slice(0, 2_000);
};

export interface WarehouseOptions {
  /** Host-owned tenant identity; only this tenant's rows are materialized. */
  readonly tenant: string;
  readonly seed: ReadonlyArray<WarehouseSeedRow>;
  readonly limits?: WarehouseLimits | undefined;
  /**
   * Not supported: the synchronous driver cannot cancel a statement, so any
   * requested timeout fails Layer construction typed instead of being
   * silently unenforced.
   */
  readonly statementTimeout?: Duration.Duration | undefined;
}

/**
 * Build the tenant-scoped read-only warehouse. The returned Layer owns the
 * private in-memory database; nothing else ever sees the connection.
 */
export const warehouseDbLayer = (
  options: WarehouseOptions,
): Layer.Layer<WarehouseDb, WarehouseConfigurationError | SqlError> =>
  Layer.effect(WarehouseDb)(
    Effect.gen(function* () {
      if (options.statementTimeout !== undefined) {
        return yield* WarehouseConfigurationError.make({
          message:
            "The reference warehouse cannot enforce a statement timeout on the synchronous SQLite driver and refuses to pretend otherwise; bound the enclosing pass instead",
        });
      }
      const limits = options.limits ?? defaultLimits;
      const sql = yield* SqlClient.SqlClient;

      // Curated, tenant-scoped topology: only this tenant's rows exist in the
      // database the tool can reach, so cross-tenant reads are physically
      // impossible rather than merely filtered.
      yield* sql`CREATE TABLE invoice_summary (
        customer TEXT NOT NULL,
        region TEXT NOT NULL,
        revenue INTEGER NOT NULL,
        created_at TEXT NOT NULL
      )`;
      const tenantRows = options.seed.filter((row) => row.tenant === options.tenant);
      for (const row of tenantRows) {
        yield* sql`INSERT INTO invoice_summary ${sql.insert({
          customer: row.customer,
          region: row.region,
          revenue: row.revenue,
          created_at: row.createdAt,
        })}`;
      }
      // Database authority: the same connection now refuses every write.
      yield* sql`PRAGMA query_only = ON`.withoutTransform;

      const query = Effect.fn("WarehouseDb.query")(function* (
        text: string,
        parameters: ReadonlyArray<string | number | boolean | null>,
      ) {
        const denied = scanSql(text);
        if (denied !== undefined) {
          return yield* denied;
        }
        const rawRows = yield* sql
          .unsafe<Record<string, unknown>>(text, parameters as Array<unknown>)
          .withoutTransform.pipe(
            Effect.catchTag("SqlError", (error) =>
              Effect.fail<WarehouseQueryDenied | WarehouseQueryFailed>(
                isWriteDenial(error)
                  ? WarehouseQueryDenied.make({
                      reason: "write-denied",
                      message: "The warehouse database authority is read-only",
                    })
                  : WarehouseQueryFailed.make({
                      message: sqlFailureMessage(error),
                    }),
              ),
            ),
          );
        const rows: Array<Record<string, Schema.Json>> = [];
        let truncated = false;
        let usedBytes = 0;
        for (const raw of rawRows) {
          if (rows.length >= limits.maxRows) {
            truncated = true;
            break;
          }
          const decoded = decodeRow(raw);
          if (Option.isNone(decoded)) {
            return yield* WarehouseQueryFailed.make({
              message: "A result row contained a value outside the JSON surface",
            });
          }
          const bytes = utf8ByteLength(JSON.stringify(decoded.value));
          if (usedBytes + bytes > limits.maxResultBytes) {
            truncated = true;
            break;
          }
          usedBytes += bytes;
          rows.push(decoded.value);
        }
        const columns = rows.length === 0 ? [] : Object.keys(rows[0]);
        const success = WarehouseQuerySuccess.make({
          columns,
          rows,
          rowCount: rows.length,
          truncated,
        });
        // Structural audit metadata: digest and shape only — never SQL text,
        // parameter values, or row contents (SEC-015, spec §11).
        yield* Effect.logDebug("warehouse query settled").pipe(
          Effect.annotateLogs({
            queryDigest: queryDigest(text),
            parameterCount: parameters.length,
            rowCount: success.rowCount,
            truncated: success.truncated,
          }),
        );
        return success;
      });

      return WarehouseDb.of({ query });
    }),
  ).pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:" })));

/**
 * The native read-only SQL Tool over the curated warehouse (plan §8.3). The
 * curated schema is part of the model-facing description; results report
 * truncation honestly (`truncated: true` success, D-035 default).
 */
export const warehouseQueryTool = Tool.make("query_warehouse", {
  description:
    "Run one read-only SQL statement against the curated warehouse view `invoice_summary` (columns: customer TEXT, region TEXT, revenue INTEGER, created_at TEXT). Use ? placeholders with the parameters array; exactly one statement per call. Oversized results return truncated: true.",
  parameters: Schema.Struct({
    sql: BoundedSqlText,
    parameters: Schema.optionalKey(Schema.Array(SqlParameter).check(Schema.isMaxLength(32))),
  }),
  success: WarehouseQuerySuccess,
  failure: Schema.Union([WarehouseQueryDenied, WarehouseQueryFailed]),
})
  .annotate(Tool.Readonly, true)
  .annotate(ToolExecutionClass, "readonly");

export const warehouseToolkit = Toolkit.make(warehouseQueryTool);

/** Handler Layer binding the Tool to the tenant-scoped read-only service. */
export const warehouseHandlersLayer = warehouseToolkit.toLayer(
  Effect.gen(function* () {
    const db = yield* WarehouseDb;
    return {
      query_warehouse: ({
        sql,
        parameters,
      }: {
        readonly sql: string;
        readonly parameters?: ReadonlyArray<string | number | boolean | null> | undefined;
      }) => db.query(sql, parameters ?? []),
    };
  }),
);

/** A small deterministic multi-tenant seed shared by tests and the demo. */
export const warehouseDemoSeed: ReadonlyArray<WarehouseSeedRow> = [
  WarehouseSeedRow.make({
    tenant: "acme",
    customer: "Stellar Freight",
    region: "emea",
    revenue: 48_200,
    createdAt: "2026-07-03",
  }),
  WarehouseSeedRow.make({
    tenant: "acme",
    customer: "Nimbus Analytics",
    region: "amer",
    revenue: 12_800,
    createdAt: "2026-07-11",
  }),
  WarehouseSeedRow.make({
    tenant: "acme",
    customer: "Copper Kettle Co",
    region: "amer",
    revenue: 730,
    createdAt: "2026-07-15",
  }),
  WarehouseSeedRow.make({
    tenant: "acme",
    customer: "Harbor Lights Ltd",
    region: "apac",
    revenue: 9_400,
    createdAt: "2026-07-21",
  }),
  WarehouseSeedRow.make({
    tenant: "umbra",
    customer: "Shadow Cartel",
    region: "emea",
    revenue: 999_999,
    createdAt: "2026-07-01",
  }),
];
