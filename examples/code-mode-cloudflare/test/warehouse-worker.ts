import { DurableObject } from "cloudflare:workers";
import { Effect } from "effect";

import { Warehouse, warehouseLayer, type WarehouseObject } from "../src/warehouse-object.ts";

/** Deliberately foreign RPC values exercise the production decoder. */
export class ForeignWarehouse extends DurableObject {
  query(scenario: string): unknown {
    const result = {
      ok: true,
      columns: ["value"],
      rows: [{ value: 42 }],
      rowCount: 1,
      truncated: false,
    };

    switch (scenario) {
      case "non-json":
        return { ...result, rows: [{ value: undefined }] };
      case "negative-count":
        return { ...result, rowCount: -1 };
      case "missing-reason":
        return { ...result, ok: false };
      case "transport":
        throw new Error("foreign warehouse is unavailable");
      default:
        return result;
    }
  }
}

export default {
  fetch(request: Request, env: { readonly WAREHOUSE: DurableObjectNamespace<WarehouseObject> }) {
    return Effect.runPromise(
      Effect.gen(function* () {
        const warehouse = yield* Warehouse;

        return yield* warehouse.query(new URL(request.url).pathname.slice(1), []);
      }).pipe(
        Effect.map((outcome) => Response.json(outcome)),
        Effect.provide(warehouseLayer(env.WAREHOUSE, "boundary-test")),
      ),
    );
  },
};
