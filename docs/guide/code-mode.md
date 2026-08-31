---
title: Code Mode
description: Let an agent run one bounded JavaScript program through an explicit tool allowlist.
---

# Code Mode

Code Mode gives an agent one Effect AI Tool for a small JavaScript program. The program can call a
fixed set of application Tools through named globals, then return one JSON value. It fits questions
that need a bounded query followed by filtering, aggregation, or calculation.

The [Cloudflare warehouse example](https://github.com/danieljvdm/effect-agent/tree/main/examples/code-mode-cloudflare)
answers invoice questions this way:

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

Requires `effect@^4.0.0-rc.111` and `effect-cf@^0.37.0`. For the example below, also install
`@effect-agent/core@beta`, `@effect-agent/engine@beta`, and `@effect/ai-openai@4.0.0-rc.111`.
Keep framework packages at the [same release](./getting-started#installation-and-compatibility).

This smaller example uses fixed invoice rows so the complete Tool and handler are visible.
Generated code calls `warehouse.invoices({ region: "emea" })` and computes its answer from those
rows. The linked warehouse example replaces the fixed data with a brokered SQL query.

```ts twoslash
// @types: @cloudflare/workers-types
import { CodeMode } from "@effect-agent/capabilities";
import { Agent, AgentPolicy, IdGenerator } from "@effect-agent/core";
import { AgentRuntime, ConversationHistory, ToolExecutionClass } from "@effect-agent/engine";
import { dynamicWorkerCodeExecutorLayer } from "@effect-agent/platform-cloudflare";
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
    toolConcurrency: 1,
  }),
});

const AnalystLive = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;
    const CodeModeLive = codeMode.handlers.pipe(
      Layer.provide(InvoiceHandlers),
      Layer.provide(dynamicWorkerCodeExecutorLayer({ loader: env.LOADER })),
    );
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
      ConversationHistory.layerTransient,
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
An `effect-cf` Worker supplies it. The application owns the HTTP response and authentication;
the [warehouse Worker](https://github.com/danieljvdm/effect-agent/blob/main/examples/code-mode-cloudflare/src/worker.ts)
shows the example's HTTP behavior.

`CodeMode.make` fixes the namespaces and methods visible to generated code. Include `codeMode.tool`
in the agent's Toolkit and provide the selected Tool handlers and executor **to `codeMode.handlers`**.
This captures the services used by the inner calls. The runtime supplies its own Tool broker.

Code Mode accepts only Tools annotated `readonly` and without approval requirements. That annotation
does not make a database connection read-only. Enforce resource and tenant access inside handlers;
use a read-only database identity where available. The warehouse example's Durable Object uses an
application SQL allowlist because its SQLite authorizer blocks `PRAGMA query_only`. That scanner is
a demo boundary.

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

The `dynamicWorkerCodeExecutorLayer({ loader })` in the example above uses that resolved binding.

See Cloudflare's [Dynamic Workers guide](https://developers.cloudflare.com/dynamic-workers/getting-started/)
for Worker Loader setup and loading modes. This adapter uses `load()` for a fresh pass.

Code Mode is ephemeral. The executor retains no pass state, and a later pass can run in another
isolate. It does not make an Agent durable, persist generated programs, reconnect a lost pass, or
replay an unresolved call. Use a Durable Object or another application store for data that must
outlive a request. The warehouse example uses a Durable Object only for its invoice data.

## Run the warehouse example

The example has an offline scripted profile, so its test needs no model credential. From the
repository root:

```sh
vp run -F @effect-agent/example-code-mode-cloudflare test
vp run -F @effect-agent/example-code-mode-cloudflare dev
```

The test bundles the Worker, starts workerd through Miniflare, loads a program through a real Worker
Loader binding, and queries the SQLite Durable Object. For a deployed Worker, optionally set
`OPENAI_API_KEY` and always set `DEMO_AUTH_TOKEN` with it, then deploy:

```sh
vp dlx wrangler secret put OPENAI_API_KEY --config examples/code-mode-cloudflare/wrangler.jsonc
vp dlx wrangler secret put DEMO_AUTH_TOKEN --config examples/code-mode-cloudflare/wrangler.jsonc
vp run -F @effect-agent/example-code-mode-cloudflare deploy
```

The example [README](https://github.com/danieljvdm/effect-agent/tree/main/examples/code-mode-cloudflare)
has the request format and full authorization behavior. It exposes one bounded, read-only warehouse
query. The broker allowlist prevents calls to unlisted Tools, but it cannot decide whether an
allowed handler should access a particular tenant, table, account, or secret.

## Related capabilities

[Sandbox execution](./sandbox) covers trusted local commands. Its local adapter is unisolated and
does not implement the `CodeExecutor` required here. [Browser tools](./browser) cover page capture,
crawl, and interactive passes; browser capture has uncertain external effects and cannot be added
to Code Mode's readonly allowlist.
