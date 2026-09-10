---
title: Code Mode
description: Let an agent run one bounded JavaScript program through an explicit tool allowlist.
---

# Code Mode

Code Mode gives an agent one Effect AI Tool for a small JavaScript program. The program can call a
fixed set of application Tools through named globals, then return one JSON value. It fits questions
that need a bounded query followed by filtering, aggregation, or calculation.

For example, an invoice analyst can answer a question this way:

```ts
const code = `async () => {
  const result = await warehouse.query({
    sql: "SELECT region, SUM(revenue) AS total FROM invoice_summary GROUP BY region",
  });
  return result.rows.sort((a, b) => Number(b.total) - Number(a.total))[0];
}`;
```

`warehouse` is not a database client. It is a generated global that routes through the runtime's
Tool broker to an application-owned Tool handler. The handler decides what the program can read.

## Build an analyst

In your application, install Code Mode and the Cloudflare executor:

```sh
bun add @effect-agent/capabilities@beta @effect-agent/platform-cloudflare@beta
```

Requires `effect@^4.0.0-rc.112` and `effect-cf@^0.40.0`. For the example below, also install
`@effect-agent/core@beta`, `@effect-agent/engine@beta`, and `@effect/ai-openai@4.0.0-rc.112`.
Keep framework packages at the [same release](./getting-started#installation-and-compatibility).

This smaller example uses fixed invoice rows so the complete Tool and handler are visible.
Generated code calls `warehouse.invoices({ region: "emea" })` and computes its answer from those
rows. The linked warehouse example replaces the fixed data with a brokered SQL query.

```ts twoslash
// @types: @cloudflare/workers-types
import * as CodeMode from "@effect-agent/capabilities/CodeMode";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { RunContextPreparationPassthrough } from "@effect-agent/engine/RunOptions";
import { CloudflareCodeMode } from "@effect-agent/platform-cloudflare/CloudflareCodeMode";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Effect, Layer, Redacted, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import { WorkerEnvironment } from "effect-cf";

// In an application, Wrangler generates these binding types.
declare global {
  namespace Cloudflare {
    interface Env {
      readonly LOADER: WorkerLoader;
      readonly OPENAI_API_KEY: string;
    }
  }
}

const ListInvoices = Tool.make("list_invoices", {
  description: "Read invoice totals for a region: emea or americas.",
  parameters: Schema.Struct({ region: Schema.Literals(["emea", "americas"]) }),
  success: Schema.Array(Schema.Struct({ customer: Schema.String, revenue: Schema.Number })),
}).annotate(ToolExecutionClass, "readonly");

const InvoiceHandlers = Toolkit.make(ListInvoices).toLayer({
  list_invoices: ({ region }) =>
    Effect.succeed(
      [
        { customer: "Acme", region: "emea", revenue: 12_000 },
        { customer: "Atlas", region: "emea", revenue: 8_000 },
        { customer: "Beacon", region: "americas", revenue: 15_000 },
      ].filter((invoice) => invoice.region === region),
    ),
});

const codeMode = CodeMode.make("run_javascript", {
  description: "Read invoices, calculate the answer in JavaScript, and return a small JSON result.",
  tools: { warehouse: { invoices: ListInvoices } },
  maxEgressBytes: 8 * 1024,
});

const analyst = Agent.make("invoice-analyst", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Use run_javascript to calculate invoice answers. Return an answer as JSON.",
  toolkit: Toolkit.make(codeMode.tool),
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 6,
    maxDuration: "45 seconds",
    // This sandbox example runs one generated program at a time.
    toolConcurrency: 1,
  }),
});

const AnalystLive = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;
    const CodeModeLive = CloudflareCodeMode.layer(codeMode, {
      loader: env.LOADER,
      handlers: InvoiceHandlers,
    });
    const ModelLive = OpenAiLanguageModel.model("gpt-4.1-mini").pipe(
      Layer.provide(
        OpenAiClient.layer({ apiKey: Redacted.make(env.OPENAI_API_KEY) }).pipe(
          Layer.provide(FetchHttpClient.layer),
        ),
      ),
    );

    return Layer.mergeAll(
      CodeModeLive,
      ModelLive,
      IdGenerator.layer,
      ThreadHistory.layerTransient,
      RunContextPreparationPassthrough,
    );
  }),
);

export const program = AgentRuntime.run(analyst, "What is the total invoice revenue in EMEA?").pipe(
  Effect.provide(AnalystLive),
  Effect.scoped,
);
```

Only the question is input to the agent. `AnalystLive` yields `WorkerEnvironment` to obtain the
loader and provider key, leaving that service visible in the composed `program`'s requirements.
An `effect-cf` Worker supplies it. The application owns the HTTP response and authentication.

`CodeMode.make` fixes the namespaces and methods visible to generated code. Include `codeMode.tool`
in the agent's Toolkit. `CloudflareCodeMode.layer` supplies the selected Tool handlers and executor
at construction, capturing the services used by inner calls. Handler construction errors and
remaining service requirements stay visible. The runtime supplies its own Tool broker.
For a custom executor, provide it and the selected handlers directly to `codeMode.handlers`.

Code Mode accepts only Tools annotated `readonly` and without approval requirements. That annotation
does not make a database connection read-only. Enforce resource and tenant access inside handlers;
use a read-only database identity where available. The warehouse example's Durable Object uses an
application SQL allowlist because its SQLite authorizer blocks `PRAGMA query_only`. That scanner is
a demo boundary.

## Discover method documentation {#discovery}

For a large allowlist, set `includeDeclarations: false` to keep namespace inventories and full
declarations out of the initial tool description. Add `ToolDiscovery.make` beside the execution
tool. Discovery returns only matching, currently eligible methods and their encoded schemas.

```ts twoslash
import { Agent, CodeMode, ToolDiscovery } from "effect-agent";
import { ToolExecutionClass } from "effect-agent/DurableStep";
import { Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

const ListInvoices = Tool.make("list_invoices", {
  description: "Read invoice amounts for a customer.",
  parameters: Schema.Struct({ customer: Schema.String }),
  success: Schema.Array(Schema.Struct({ amountCents: Schema.Int })),
}).annotate(ToolExecutionClass, "readonly");

export const codeMode = CodeMode.make("run_javascript", {
  description: "Use discovered methods to compute invoice answers in JavaScript.",
  includeDeclarations: false,
  tools: { billing: { invoices: ListInvoices } },
});
export const discovery = ToolDiscovery.make();

export const analyst = Agent.make("invoice-discovery", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Discover invoice methods, compute the answer, then return JSON.",
  toolkit: Toolkit.make(discovery.tool, codeMode.tool),
  toolExposure: { initialToolNames: [], maxTools: 8, maxSchemaBytes: 32_768 },
});

// Host-side selective TypeScript declarations; fails with CodeModeDescriptionError.
export const invoiceDeclarations = codeMode.describe(["billing.invoices"], { maxBytes: 8_192 });
```

Provide `discovery.handlers` alongside the existing Code Mode executor/handler Layer. A discovery
call with `{ query: "invoice", namespace: "billing" }` describes `billing.invoices` and selects
the owning `run_javascript` tool for the next turn. Generated code can then call
`await billing.invoices({ customer: "Acme" })`. Common native tools can remain pinned alongside
these two tools.

Each match has a distinct ID such as `code-mode:run_javascript:billing.invoices`, its native tool
name, namespace and method. This remains unambiguous when one Tool has several aliases or is also
registered natively. Selecting a Code Mode match exposes its outer execution tool; it does not
expand the construction-time allowlist. Inner broker calls do not change native exposure.

`describe` is a host API over that fixed allowlist, not a visibility-filtered model tool. It accepts
one to 64 unique exact method paths and defaults to 16 KiB of complete UTF-8 declarations, with a
256 KiB maximum. Invalid paths and excessive output fail with `CodeModeDescriptionError`.
Declarations and discovery results use the native encoded parameter/success schemas; provider
schema transformations do not change the sandbox wire contract. The full `declarations` string
remains available to the host even when omitted from the model description.

Use `discover_tools` for model-facing documentation subject to host visibility and inherited
grants. The runtime filters the sandbox's actual namespaces and methods as well, so generated code
cannot enumerate hidden methods. If grants or host policy hide an allowlisted method while
`includeDeclarations` is still true, the runtime refuses the configuration before its full
description can leak. Use generic shared descriptions and `includeDeclarations: false` for that
case. Default eager Code Mode behavior remains available when the full allowlist is eligible.
Readonly, approval, budget and handler authorization constraints continue to apply.

## Program results and limits

Each generated namespace method returns a Promise. The program must be one
async function expression, runs once with no arguments, and returns JSON. `console.log` output
returns with the result. Expected inner Tool failures reject the Promise with a JSON failure
envelope, which generated code can catch and handle.

The Tool broker rejects calls outside the construction-time allowlist. Inner calls are strictly
sequential. The executor enforces source, wall-clock, log, result, host-call, and per-host-call
byte limits. Code Mode applies `maxEgressBytes` after optional redaction to the result, logs, and
thrown value visible to the model. The agent policy's `maxToolCalls` also includes brokered inner
calls. See [Budgets & bounded autonomy](../concepts/budgets#programmatic-calls-and-code-mode).

The default executor limits include 30 seconds and 64 host calls. Supply a `CodeExecutionLimits`
value through `limits` when constructing Code Mode to change them; `maxWallTime` takes an Effect
`Duration`. An agent's smaller remaining budget still applies. `redactEgress` can transform the
result and logs before the aggregate model-visible byte limit.
Its required services remain in the Code Mode handler Layer's requirements and are captured when
that Layer is built. Temporary redactor resources close with each invocation. The redactor must
be total: defects and interruption retain their Effect semantics.

## Run generated code on Cloudflare

`@effect-agent/platform-cloudflare` supplies `dynamicWorkerCodeExecutorLayer`. It loads each pass
into a fresh Cloudflare Dynamic Worker with `globalOutbound: null`. Generated code has no ambient
network, bindings, secrets, filesystem, or environment. Its only host authority is the scoped RPC
capability for allowlisted Tool calls.

Declare a Worker Loader binding in `wrangler.jsonc`. Cloudflare documents `worker_loaders` as the
binding that gives a Worker access to `env.LOADER`.

```jsonc
{
  "name": "warehouse-analyst",
  "main": "src/worker.ts",
  "compatibility_date": "2025-05-01",
  "worker_loaders": [{ "binding": "LOADER" }],
}
```

`CloudflareCodeMode.layer` uses that resolved binding. The lower-level
`dynamicWorkerCodeExecutorLayer({ loader })` remains available for direct `CodeExecutor` access.

See Cloudflare's [Dynamic Workers guide](https://developers.cloudflare.com/dynamic-workers/getting-started/)
for Worker Loader setup and loading modes. This adapter uses `load()` for a fresh pass.

Code Mode is ephemeral. The executor retains no pass state, and a later pass can run in another
isolate. It does not make an Agent durable, persist generated programs, reconnect a lost pass, or
replay an unresolved call. Use a Durable Object or another application store for data that must
outlive a request. A warehouse application can use a Durable Object for its invoice data.

## Application integration

Use the [canonical Cloudflare application](https://github.com/danieljvdm/effect-agent/tree/main/examples/travel-planner)
for the repository's deployment setup. A Code Mode integration additionally needs a Worker Loader
binding and a bounded Tool allowlist. The broker prevents calls to unlisted Tools, but the
application's handlers still decide which tenant, table, account, or secret may be accessed.

## Related capabilities

[Sandbox execution](./sandbox) covers trusted local commands. Its local adapter is unisolated and
does not implement the `CodeExecutor` required here. [Browser tools](./browser) cover page capture,
crawl, and interactive passes; browser capture has uncertain external effects and cannot be added
to Code Mode's readonly allowlist.
