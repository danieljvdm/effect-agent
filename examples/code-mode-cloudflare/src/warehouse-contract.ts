import { Schema } from "effect";

/** JSON query values shared by the warehouse RPC and its model-visible Tool. */
export const WarehouseQueryResult = Schema.Struct({
  columns: Schema.Array(Schema.String),
  rows: Schema.Array(Schema.Record(Schema.String, Schema.Json)),
  rowCount: Schema.Natural,
  truncated: Schema.Boolean,
});

/** A transport failure or denied query always carries a reason. */
export const WarehouseQueryOutcome = Schema.Union([
  Schema.Struct({ ok: Schema.Literal(true), ...WarehouseQueryResult.fields }),
  Schema.Struct({
    ok: Schema.Literal(false),
    ...WarehouseQueryResult.fields,
    reason: Schema.NonEmptyString,
  }),
]);

export type WarehouseQueryOutcome = typeof WarehouseQueryOutcome.Type;
