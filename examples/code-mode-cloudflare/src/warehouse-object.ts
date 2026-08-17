import { SqliteClient } from "@effect/sql-sqlite-do";
import { DurableObject } from "cloudflare:workers";
import { Context, Effect, Layer, Option, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  WarehouseDeniedReason,
  type WarehouseDeniedReason as WarehouseDeniedReasonType,
  WarehouseInvoice,
  WarehouseInvoices,
  WarehouseListOutcome,
  WarehouseListRequest,
  WarehouseQueryDenied,
} from "./wire.ts";

export {
  WarehouseInvoice,
  WarehouseInvoices,
  WarehouseListOutcome,
  WarehouseListRequest,
  WarehouseQueryDenied,
  WarehouseRegion,
} from "./wire.ts";

/**
 * The warehouse Durable Object exposes one curated invoice-list operation.
 * Callers choose typed filters, never SQL text, so the `readonly` Tool label
 * reflects real authority: every reachable statement is selected and owned by
 * this adapter. Generated code sees no connection, credentials, or storage.
 */

const MAX_ROWS = 200;

const decodeListRequest = Schema.decodeUnknownEffect(WarehouseListRequest);
const decodeListOutcome = Schema.decodeUnknownEffect(WarehouseListOutcome);
const encodeListRequest = Schema.encodeEffect(WarehouseListRequest);
const encodeListOutcome = Schema.encodeEffect(WarehouseListOutcome);
const decodeInvoice = Schema.decodeUnknownOption(WarehouseInvoice);
const WarehouseCount = Schema.Struct({
  n: Schema.Union([
    Schema.Natural,
    Schema.FiniteFromString.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
  ]),
});
const decodeCount = Schema.decodeUnknownEffect(WarehouseCount);

const queryFailure = (reason: WarehouseDeniedReasonType): WarehouseQueryDenied =>
  WarehouseQueryDenied.make({ reason });

class WarehouseQueryFault extends Schema.TaggedError<WarehouseQueryFault>()("WarehouseQueryFault", {
  reason: WarehouseDeniedReason,
  cause: Schema.Defect(),
}) {}

const redactFailure = (fault: WarehouseQueryFault): Effect.Effect<never, WarehouseQueryDenied> =>
  Effect.logWarning("warehouse operation failed").pipe(
    Effect.annotateLogs({ reason: fault.reason, cause: fault.cause }),
    Effect.andThen(Effect.fail(queryFailure(fault.reason))),
  );

const selectInvoiceRows = (
  storage: DurableObjectStorage,
  request: WarehouseListRequest,
): Effect.Effect<ReadonlyArray<unknown>, WarehouseQueryDenied> =>
  Effect.try({
    try: () => {
      const minimum = request.minimumRevenue;
      const region = request.region;
      const limit = (request.maximum ?? MAX_ROWS) + 1;
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
                "SELECT customer, region, revenue, created_at AS createdAt FROM invoice_summary WHERE revenue >= ? ORDER BY revenue DESC LIMIT ?",
                minimum,
                limit,
              )
            : storage.sql.exec(
                "SELECT customer, region, revenue, created_at AS createdAt FROM invoice_summary WHERE revenue >= ? AND region = ? ORDER BY revenue DESC LIMIT ?",
                minimum,
                region,
                limit,
              );
      return [...cursor];
    },
    catch: (cause) => WarehouseQueryFault.make({ reason: "query-failed", cause }),
  }).pipe(Effect.catch(redactFailure));

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
    customer: "Boundary Foods",
    region: "amer",
    revenue: 10_000,
    createdAt: "2026-07-22",
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
        const maximum = request.maximum ?? MAX_ROWS;
        if (rows.length > maximum) {
          return yield* queryFailure("result-limit");
        }
        const invoices: Array<WarehouseInvoice> = [];
        for (const raw of rows) {
          const invoice = decodeInvoice(raw);
          if (Option.isNone(invoice)) {
            return yield* queryFailure("invalid-invoice");
          }
          invoices.push(invoice.value);
        }
        return WarehouseInvoices.make({ invoices });
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
        Effect.catch((cause) =>
          redactFailure(WarehouseQueryFault.make({ reason: "invalid-request", cause })),
        ),
        Effect.tap(() =>
          Effect.tryPromise({
            try: () => this.#ensureSeeded(),
            catch: (cause) => WarehouseQueryFault.make({ reason: "initialization-failed", cause }),
          }).pipe(Effect.catch(redactFailure)),
        ),
        Effect.flatMap((request) => queryInvoices(this.ctx.storage, request)),
        Effect.match({ onFailure: (failure) => failure, onSuccess: (success) => success }),
      ),
    );
    return Effect.runPromise(
      encodeListOutcome(outcome).pipe(
        Effect.orElseSucceed(() => ({
          _tag: "WarehouseQueryDenied" as const,
          reason: "response-encoding-failed" as const,
        })),
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
          Effect.tryPromise({
            try: () => {
              const stub = namespace.get(namespace.idFromName(tenant));
              return stub.listInvoices(encoded);
            },
            catch: (cause) => WarehouseQueryFault.make({ reason: "unavailable", cause }),
          }),
        ),
        Effect.flatMap(decodeListOutcome),
        Effect.catch((cause) =>
          redactFailure(WarehouseQueryFault.make({ reason: "unavailable", cause })),
        ),
        Effect.flatMap((outcome) =>
          outcome._tag === "WarehouseQueryDenied" ? Effect.fail(outcome) : Effect.succeed(outcome),
        ),
      ),
  });
