import {
  WarehouseDb,
  WarehouseLimits,
  warehouseDbLayer,
  warehouseDemoSeed,
} from "@effect-agent/testing/warehouse";
import { expect, it } from "@effect/vitest";
import { Duration, Effect, Layer } from "effect";

const acmeLayer = (limits?: WarehouseLimits) =>
  warehouseDbLayer({ tenant: "acme", seed: warehouseDemoSeed, limits });

const withWarehouse = <A, E>(build: Effect.Effect<A, E, WarehouseDb>, limits?: WarehouseLimits) =>
  build.pipe(Effect.provide(acmeLayer(limits)));

it.effect("answers a read query over the curated tenant view", () =>
  withWarehouse(
    Effect.gen(function* () {
      const db = yield* WarehouseDb;
      const result = yield* db.query(
        "SELECT customer, revenue FROM invoice_summary WHERE revenue > ? ORDER BY revenue DESC",
        [1_000],
      );
      expect(result.columns).toEqual(["customer", "revenue"]);
      expect(result.rows.map((row) => row.customer)).toEqual([
        "Stellar Freight",
        "Nimbus Analytics",
        "Harbor Lights Ltd",
      ]);
      expect(result.truncated).toBe(false);
    }),
  ),
);

it.effect("writes and DDL are denied by the database authority, not the scanner", () =>
  withWarehouse(
    Effect.gen(function* () {
      const db = yield* WarehouseDb;
      const update = yield* db
        .query("UPDATE invoice_summary SET revenue = 0", [])
        .pipe(Effect.flip);
      expect(update).toMatchObject({ _tag: "WarehouseQueryDenied", reason: "write-denied" });
      const ddl = yield* db.query("DROP TABLE invoice_summary", []).pipe(Effect.flip);
      expect(ddl).toMatchObject({ _tag: "WarehouseQueryDenied", reason: "write-denied" });
      // The authority held: the data is still there afterwards.
      const still = yield* db.query("SELECT COUNT(*) AS n FROM invoice_summary", []);
      expect(still.rows[0]).toEqual({ n: 4 });
    }),
  ),
);

it.effect("multi-statement attempts are denied while literal semicolons pass", () =>
  withWarehouse(
    Effect.gen(function* () {
      const db = yield* WarehouseDb;
      const denied = yield* db.query("SELECT 1; DROP TABLE invoice_summary", []).pipe(Effect.flip);
      expect(denied).toMatchObject({ _tag: "WarehouseQueryDenied", reason: "multi-statement" });
      const literal = yield* db.query("SELECT ';' AS semi", []);
      expect(literal.rows[0]).toEqual({ semi: ";" });
    }),
  ),
);

it.effect("escape-hatch keywords the authority cannot police are denied closed", () =>
  withWarehouse(
    Effect.gen(function* () {
      const db = yield* WarehouseDb;
      for (const attempt of [
        "PRAGMA query_only = OFF",
        "ATTACH DATABASE ':memory:' AS other",
        "SELECT load_extension('evil')",
      ]) {
        const denied = yield* db.query(attempt, []).pipe(Effect.flip);
        expect(denied).toMatchObject({
          _tag: "WarehouseQueryDenied",
          reason: "denied-keyword",
        });
      }
    }),
  ),
);

it.effect("cross-tenant rows are physically absent from the curated database", () =>
  withWarehouse(
    Effect.gen(function* () {
      const db = yield* WarehouseDb;
      const sweep = yield* db.query(
        "SELECT customer FROM invoice_summary WHERE revenue > ?",
        [100_000],
      );
      expect(sweep.rows).toEqual([]);
      const all = yield* db.query("SELECT DISTINCT customer FROM invoice_summary", []);
      expect(all.rows.map((row) => row.customer)).not.toContain("Shadow Cartel");
    }),
  ),
);

it.effect("oversized results truncate honestly with truncated: true", () =>
  withWarehouse(
    Effect.gen(function* () {
      const db = yield* WarehouseDb;
      const result = yield* db.query("SELECT customer FROM invoice_summary", []);
      expect(result.rows).toHaveLength(2);
      expect(result.rowCount).toBe(2);
      expect(result.truncated).toBe(true);
    }),
    WarehouseLimits.make({ maxRows: 2, maxResultBytes: 256 * 1024 }),
  ),
);

it.effect("a statement timeout the driver cannot enforce is refused typed", () =>
  Effect.gen(function* () {
    const error = yield* Layer.build(
      warehouseDbLayer({
        tenant: "acme",
        seed: warehouseDemoSeed,
        statementTimeout: Duration.seconds(5),
      }),
    ).pipe(Effect.scoped, Effect.flip);
    expect(error).toMatchObject({ _tag: "WarehouseConfigurationError" });
  }),
);
