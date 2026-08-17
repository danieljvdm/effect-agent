import { CodeMode } from "@effect-agent/capabilities";
import { Agent, AgentPolicy } from "@effect-agent/core";
import { ToolExecutionClass } from "@effect-agent/engine";
import { Effect, Layer, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import {
  Warehouse,
  WarehouseInvoices,
  WarehouseListRequest,
  WarehouseQueryDenied,
} from "./warehouse-object.ts";

/**
 * The curated read-only warehouse Tool: callers provide only typed filters,
 * while the adapter owns every reachable SQL statement. The executor never
 * receives SQL authority or the connection.
 */
export const warehouseListTool = Tool.make("list_warehouse_invoices", {
  description:
    "List invoices from the curated warehouse, optionally filtering by minimum revenue and region. Results are ordered by revenue descending and bounded to 200 rows.",
  parameters: WarehouseListRequest,
  success: Schema.Struct({
    invoices: WarehouseInvoices.fields.invoices,
    truncated: Schema.Boolean,
  }),
  failure: WarehouseQueryDenied,
}).annotate(ToolExecutionClass, "readonly");

const warehouseToolkit = Toolkit.make(warehouseListTool);

/** Handler Layer binding the Tool to the tenant-scoped read-only DO service. */
export const warehouseHandlersLayer: Layer.Layer<
  Tool.HandlersFor<{ readonly list_warehouse_invoices: typeof warehouseListTool }>,
  never,
  Warehouse
> = warehouseToolkit.toLayer(
  Effect.gen(function* () {
    const warehouse = yield* Warehouse;
    return {
      list_warehouse_invoices: (request) =>
        warehouse.listInvoices(request).pipe(
          Effect.map((outcome) => ({
            invoices: outcome.invoices,
            truncated: outcome.truncated,
          })),
        ),
    };
  }),
);

/**
 * The Code Mode capability: one native `run_javascript` Tool whose
 * `warehouse.listInvoices` sandbox global routes to the curated Tool. The
 * model writes a bounded JavaScript program that queries and shapes the data;
 * the program runs in an isolated Dynamic Worker.
 */
export const codeMode = CodeMode.make("run_javascript", {
  description:
    "Write bounded JavaScript to answer questions about the invoice warehouse. Query the data, filter/aggregate it locally, and return one small JSON answer.",
  tools: { warehouse: { listInvoices: warehouseListTool } },
});

export const codeModeAgent = Agent.define("warehouse-analyst", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: [
    "You answer questions about invoice data.",
    "First call run_javascript exactly once with a program that reads invoices via warehouse.listInvoices({ minimumRevenue?, region? }) and computes the answer in code.",
    "Each invoice has customer, region, revenue, and createdAt fields. No SQL interface is exposed.",
    "After the program returns, respond with only a JSON object of exactly this shape, no prose:",
    '{"answer": "<one concise sentence answering the question>"}',
  ].join("\n"),
  toolkit: Toolkit.make(codeMode.tool),
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 6,
    maxDuration: "45 seconds",
    toolConcurrency: 1,
  }),
});

/** The composed handler Layer, minus the `CodeExecutor` (supplied by the host). */
export const codeModeHandlersLayer = codeMode.handlers.pipe(Layer.provide(warehouseHandlersLayer));
