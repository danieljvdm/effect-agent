import * as CodeMode from "@effect-agent/capabilities/CodeMode";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import type { Layer } from "effect";
import { Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { Warehouse } from "./warehouse-object.ts";

/**
 * The read-only SQL warehouse Tool (plan §8.3): a native Effect AI Tool built
 * over the application-owned `Warehouse` service, annotated `readonly`. Its
 * result is Schema-bounded JSON; the executor never sees the connection.
 */
export const warehouseQueryTool = Tool.make("query_warehouse", {
  description:
    "Run one read-only SQL statement over the curated `invoice_summary` view (columns: customer TEXT, region TEXT, revenue INTEGER, created_at TEXT). Use ? placeholders with the parameters array; one statement per call. Oversized results return truncated: true.",
  parameters: Schema.Struct({
    sql: Schema.NonEmptyString.check(Schema.isMaxLength(16 * 1024)),
    parameters: Schema.optionalKey(
      Schema.Array(Schema.Union([Schema.String, Schema.Number, Schema.Boolean, Schema.Null])).check(
        Schema.isMaxLength(32),
      ),
    ),
  }),
  success: Schema.Struct({
    columns: Schema.Array(Schema.String),
    rows: Schema.Array(Schema.Record(Schema.String, Schema.Json)),
    rowCount: Schema.Natural,
    truncated: Schema.Boolean,
  }),
  failure: Schema.Struct({
    _tag: Schema.Literal("WarehouseQueryDenied"),
    reason: Schema.String,
  }),
}).annotate(ToolExecutionClass, "readonly");

const warehouseToolkit = Toolkit.make(warehouseQueryTool);

/** Handler Layer binding the Tool to the tenant-scoped read-only DO service. */
export const warehouseHandlersLayer: Layer.Layer<
  Tool.HandlersFor<{ readonly query_warehouse: typeof warehouseQueryTool }>,
  never,
  Warehouse
> = warehouseToolkit.toLayer(
  Effect.gen(function* () {
    const warehouse = yield* Warehouse;

    return {
      query_warehouse: ({
        sql,
        parameters,
      }: {
        readonly sql: string;
        readonly parameters?: ReadonlyArray<string | number | boolean | null> | undefined;
      }) =>
        warehouse.query(sql, parameters ?? []).pipe(
          Effect.flatMap((outcome) =>
            outcome.ok
              ? Effect.succeed({
                  columns: outcome.columns,
                  rows: outcome.rows as ReadonlyArray<Record<string, Schema.Json>>,
                  rowCount: outcome.rowCount,
                  truncated: outcome.truncated,
                })
              : Effect.fail({
                  _tag: "WarehouseQueryDenied" as const,
                  reason: outcome.reason ?? "denied",
                }),
          ),
        ),
    };
  }),
);

/**
 * The Code Mode capability: one native `run_javascript` Tool whose
 * `warehouse.query` sandbox global routes to the read-only SQL Tool. The
 * model writes a bounded JavaScript program that queries and shapes the data;
 * the program runs in an isolated Dynamic Worker.
 */
export const codeMode = CodeMode.make("run_javascript", {
  description:
    "Write bounded JavaScript to answer questions about the invoice warehouse. Query the data, filter/aggregate it locally, and return one small JSON answer.",
  tools: { warehouse: { query: warehouseQueryTool } },
});

export const codeModeAgent = Agent.make("warehouse-analyst", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: [
    "You answer questions about invoice data.",
    "First call run_javascript exactly once with a program that queries the warehouse (via warehouse.query) and computes the answer in code.",
    "The warehouse exposes a read-only `invoice_summary` table (columns: customer TEXT, region TEXT, revenue INTEGER, created_at TEXT).",
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
