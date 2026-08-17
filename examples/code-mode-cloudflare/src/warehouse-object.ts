import { SqliteClient } from "@effect/sql-sqlite-do";
import { DurableObject } from "cloudflare:workers";
import { Context, Effect, Layer, Option, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * The warehouse Durable Object exposes one curated invoice-list operation.
 * Callers choose typed filters, never SQL text, so the `readonly` Tool label
 * reflects real authority: every reachable statement is selected and owned by
 * this adapter. Generated code sees no connection, credentials, or storage.
 */

const MAX_ROWS = 200;

export const WarehouseRegion = Schema.Literals(["amer", "emea", "apac"]);
export type WarehouseRegion = typeof WarehouseRegion.Type;

export class WarehouseInvoice extends Schema.Class<WarehouseInvoice>(
  "@effect-agent/example-code-mode-cloudflare/WarehouseInvoice",
)({
  customer: Schema.NonEmptyString,
  region: WarehouseRegion,
  revenue: Schema.Natural,
  createdAt: Schema.NonEmptyString,
}) {}

export class WarehouseListRequest extends Schema.Class<WarehouseListRequest>(
  "@effect-agent/example-code-mode-cloudflare/WarehouseListRequest",
)({
  minimumRevenue: Schema.optionalKey(Schema.Natural),
  region: Schema.optionalKey(WarehouseRegion),
}) {}

export class WarehouseInvoices extends Schema.TaggedClass<WarehouseInvoices>(
  "@effect-agent/example-code-mode-cloudflare/WarehouseInvoices",
)("WarehouseInvoices", {
  invoices: Schema.Array(WarehouseInvoice).check(Schema.isMaxLength(MAX_ROWS)),
  truncated: Schema.Boolean,
}) {}

export class WarehouseQueryDenied extends Schema.TaggedError<WarehouseQueryDenied>()(
  "WarehouseQueryDenied",
  {
    reason: Schema.String.check(Schema.isMaxLength(500)),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

export const WarehouseListOutcome = Schema.Union([WarehouseInvoices, WarehouseQueryDenied]);
export type WarehouseListOutcome = typeof WarehouseListOutcome.Type;

const decodeListRequest = Schema.decodeUnknownEffect(WarehouseListRequest);
const decodeListOutcome = Schema.decodeUnknownEffect(WarehouseListOutcome);
const encodeListRequest = Schema.encodeEffect(WarehouseListRequest);
const encodeListOutcome = Schema.encodeEffect(WarehouseListOutcome);
const decodeInvoice = Schema.decodeUnknownOption(WarehouseInvoice);
const WarehouseCount = Schema.Struct({
  n: Schema.Union([
    Schema.Natural,
    Schema.NumberFromString.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
  ]),
});
const decodeCount = Schema.decodeUnknownEffect(WarehouseCount);

const queryFailure = (reason: string, cause?: unknown): WarehouseQueryDenied =>
  WarehouseQueryDenied.make({
    reason: reason.slice(0, 500),
    ...(cause === undefined ? {} : { cause }),
  });

const selectInvoiceRows = (
  storage: DurableObjectStorage,
  request: WarehouseListRequest,
): Effect.Effect<ReadonlyArray<unknown>, WarehouseQueryDenied> =>
  Effect.try({
    try: () => {
      const minimum = request.minimumRevenue;
      const region = request.region;
      const limit = MAX_ROWS + 1;
      const cursor =
        minimum === undefined
          ? region === undefined
            ? storage.sql.exec(
                "SELECT customer, region, revenue, created_at AS createdAt FROM invoice_summary ORDER BY revenue DESC LIMIT ?",
                limit,
              )
            : storage.sql.exec(
                "SELECT customer, region, revenue, created_at AS createdAt FROM invoice_summary WHERE region = ? ORDER BY revenue DESC LIMIT ?",
                region,
                limit,
              )
          : region === undefined
            ? storage.sql.exec(
                "SELECT customer, region, revenue, created_at AS createdAt FROM invoice_summary WHERE revenue > ? ORDER BY revenue DESC LIMIT ?",
                minimum,
                limit,
              )
            : storage.sql.exec(
                "SELECT customer, region, revenue, created_at AS createdAt FROM invoice_summary WHERE revenue > ? AND region = ? ORDER BY revenue DESC LIMIT ?",
                minimum,
                region,
                limit,
              );
      return [...cursor];
    },
    catch: (cause) => queryFailure(`warehouse query failed: ${String(cause)}`, cause),
  });

const seedRows: ReadonlyArray<WarehouseInvoice> = [
  WarehouseInvoice.make({
    customer: "Stellar Freight",
    region: "emea",
    revenue: 48_200,
    createdAt: "2026-07-03",
  }),
  WarehouseInvoice.make({
    customer: "Nimbus Analytics",
    region: "amer",
    revenue: 12_800,
    createdAt: "2026-07-11",
  }),
  WarehouseInvoice.make({
    customer: "Copper Kettle Co",
    region: "amer",
    revenue: 730,
    createdAt: "2026-07-15",
  }),
  WarehouseInvoice.make({
    customer: "Harbor Lights Ltd",
    region: "apac",
    revenue: 9_400,
    createdAt: "2026-07-21",
  }),
  WarehouseInvoice.make({
    customer: "Vertex Robotics",
    region: "emea",
    revenue: 21_050,
    createdAt: "2026-07-24",
  }),
];

const queryInvoices = (
  storage: DurableObjectStorage,
  request: WarehouseListRequest,
): Effect.Effect<WarehouseInvoices, WarehouseQueryDenied> =>
  selectInvoiceRows(storage, request).pipe(
    Effect.flatMap((rows) =>
      Effect.gen(function* () {
        const invoices: Array<WarehouseInvoice> = [];
        for (const raw of rows) {
          if (invoices.length === MAX_ROWS) {
            return WarehouseInvoices.make({ invoices, truncated: true });
          }
          const invoice = decodeInvoice(raw);
          if (Option.isNone(invoice)) {
            return yield* queryFailure("warehouse returned a row outside the invoice wire Schema");
          }
          invoices.push(invoice.value);
        }
        return WarehouseInvoices.make({ invoices, truncated: false });
      }),
    ),
  );

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
        const rows = yield* sql`SELECT COUNT(*) AS n FROM invoice_summary`;
        const decoded = yield* decodeCount(rows[0]);
        const existing = decoded.n;
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
      }).pipe(Effect.provide(SqliteClient.layer({ storage: this.ctx.storage }))),
    );
    this.#ready = true;
  }

  /** Schema-decoded RPC request and Schema-encoded response. */
  async listInvoices(encoded: unknown): Promise<unknown> {
    const outcome = await Effect.runPromise(
      decodeListRequest(encoded).pipe(
        Effect.mapError((error) =>
          queryFailure(`invalid warehouse request: ${error.message.slice(0, 450)}`, error),
        ),
        Effect.tap(() =>
          Effect.tryPromise({
            try: () => this.#ensureSeeded(),
            catch: (cause) =>
              queryFailure(`warehouse initialization failed: ${String(cause)}`, cause),
          }),
        ),
        Effect.flatMap((request) => queryInvoices(this.ctx.storage, request)),
        Effect.match({ onFailure: (failure) => failure, onSuccess: (success) => success }),
      ),
    );
    return Effect.runPromise(
      encodeListOutcome(outcome).pipe(
        Effect.catch((error) =>
          Effect.succeed({
            _tag: "WarehouseQueryDenied",
            reason: `warehouse response encoding failed: ${error.message.slice(0, 430)}`,
          }),
        ),
      ),
    );
  }
}

/** Effect service wrapping the tenant-scoped warehouse authority. */
export class Warehouse extends Context.Service<
  Warehouse,
  {
    readonly listInvoices: (
      request: WarehouseListRequest,
    ) => Effect.Effect<WarehouseInvoices, WarehouseQueryDenied>;
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
    listInvoices: (request) =>
      encodeListRequest(request).pipe(
        Effect.flatMap((encoded) =>
          Effect.tryPromise(() => {
            const stub = namespace.get(namespace.idFromName(tenant));
            return stub.listInvoices(encoded);
          }),
        ),
        Effect.flatMap(decodeListOutcome),
        Effect.mapError((error) =>
          queryFailure(
            `warehouse unavailable or returned an invalid response: ${error.message.slice(0, 390)}`,
            error,
          ),
        ),
        Effect.flatMap((outcome) =>
          outcome._tag === "WarehouseQueryDenied" ? Effect.fail(outcome) : Effect.succeed(outcome),
        ),
      ),
  });
