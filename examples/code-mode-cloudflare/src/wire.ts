import { Schema } from "effect";

/** Environment-neutral wire contracts shared by the HTTP Worker and warehouse DO RPC. */
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
  invoices: Schema.Array(WarehouseInvoice).check(Schema.isMaxLength(200)),
  truncated: Schema.Boolean,
}) {}

/** Stable public failure categories; diagnostic causes remain inside the Worker/DO only. */
export const WarehouseDeniedReason = Schema.Literals([
  "invalid-request",
  "initialization-failed",
  "query-failed",
  "invalid-invoice",
  "unavailable",
  "response-encoding-failed",
]);
export type WarehouseDeniedReason = typeof WarehouseDeniedReason.Type;

export class WarehouseQueryDenied extends Schema.TaggedError<WarehouseQueryDenied>()(
  "WarehouseQueryDenied",
  { reason: WarehouseDeniedReason },
) {}

export const WarehouseListOutcome = Schema.Union([WarehouseInvoices, WarehouseQueryDenied]);
export type WarehouseListOutcome = typeof WarehouseListOutcome.Type;

export class AskRequest extends Schema.Class<AskRequest>(
  "@effect-agent/example-code-mode-cloudflare/AskRequest",
)({
  question: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

export const CodeModeEvidence = Schema.Struct({
  used: Schema.Boolean,
  tool: Schema.String,
  executor: Schema.String,
  calls: Schema.Natural,
  program: Schema.optionalKey(Schema.String),
  result: Schema.optionalKey(Schema.Json),
  logs: Schema.optionalKey(Schema.Array(Schema.Json)),
});

export class AskResult extends Schema.Class<AskResult>(
  "@effect-agent/example-code-mode-cloudflare/AskResult",
)({
  answer: Schema.String,
  codeMode: CodeModeEvidence,
  profile: Schema.Literals(["scripted", "openai"]),
}) {}
