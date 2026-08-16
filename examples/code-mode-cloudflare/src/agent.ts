import { CodeMode } from "@effect-agent/capabilities";
import { Agent, AgentPolicy } from "@effect-agent/core";
import { ToolExecutionClass } from "@effect-agent/engine";
import { Effect, Layer, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runReadOnlyQuery } from "./db.ts";

/**
 * The read-only SQL Tool: a native Effect AI Tool whose handler runs one
 * allowlisted statement through the Effect `SqlClient` over the D1 binding.
 * Its result is Schema-bounded JSON; the executor never sees the binding.
 */
export const invoiceQueryTool = Tool.make("query_invoices", {
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
    _tag: Schema.Literal("InvoiceQueryDenied"),
    reason: Schema.String,
  }),
}).annotate(ToolExecutionClass, "readonly");

const invoiceToolkit = Toolkit.make(invoiceQueryTool);

/** Handler Layer: the tool is effect SQL over D1, denials stay typed. */
export const invoiceHandlersLayer: Layer.Layer<
  Tool.HandlersFor<{ readonly query_invoices: typeof invoiceQueryTool }>,
  never,
  SqlClient.SqlClient
> = invoiceToolkit.toLayer(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return {
      query_invoices: ({
        sql: text,
        parameters,
      }: {
        readonly sql: string;
        readonly parameters?: ReadonlyArray<string | number | boolean | null> | undefined;
      }) =>
        runReadOnlyQuery(text, parameters ?? []).pipe(
          Effect.flatMap((outcome) =>
            outcome.ok
              ? Effect.succeed({
                  columns: outcome.columns,
                  rows: outcome.rows as ReadonlyArray<Record<string, Schema.Json>>,
                  rowCount: outcome.rowCount,
                  truncated: outcome.truncated,
                })
              : Effect.fail({
                  _tag: "InvoiceQueryDenied" as const,
                  reason: outcome.reason ?? "denied",
                }),
          ),
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
    };
  }),
);

/**
 * The Code Mode capability: one native `run_javascript` Tool whose
 * `invoices.query` sandbox global routes to the read-only SQL Tool. The
 * model writes a bounded JavaScript program that queries and shapes the data;
 * the program runs in an isolated Dynamic Worker.
 */
export const codeMode = CodeMode.make("run_javascript", {
  description:
    "Write bounded JavaScript to answer questions about the invoice data. Query the data, filter/aggregate it locally, and return one small JSON answer.",
  tools: { invoices: { query: invoiceQueryTool } },
});

export const codeModeAgent = Agent.define("invoice-analyst", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: [
    "You answer questions about invoice data.",
    "Answer by calling run_javascript exactly once with a program that computes the answer in code. When the question needs invoice data, query it via invoices.query; otherwise just compute the answer directly — do not issue pointless queries.",
    "The database exposes a read-only `invoice_summary` table (columns: customer TEXT, region TEXT, revenue INTEGER, created_at TEXT). `region` values are lowercase codes: emea | amer | apac.",
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
export const codeModeHandlersLayer = codeMode.handlers.pipe(Layer.provide(invoiceHandlersLayer));
