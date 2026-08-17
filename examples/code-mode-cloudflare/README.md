# Code Mode over a SQLite Durable Object warehouse

A runnable Cloudflare Worker that answers natural-language questions about
invoice data by running **Code Mode** (D-035,
[ADR-0017](../../docs/adr/0017-code-mode-executor-and-broker.md)) on **Cloudflare
Dynamic Workers**, over a **SQLite-backed Durable Object** warehouse.

## What it demonstrates

```
POST /ask { "question": "Which customers have more than $10,000 in revenue?" }
        │
        ▼
   ephemeral Code Mode Agent  ──writes──►  one bounded JavaScript program
        │                                          │
        │ native run_javascript tool               │ warehouse.listInvoices(...)  (typed sandbox global)
        ▼                                          ▼
 Dynamic Worker CodeExecutor ──loads──►  fresh Worker (globalOutbound: null)
   (@effect-agent/platform-cloudflare)        │ no network / bindings / secrets
        │                                      │ RPC back through the broker only
        ▼                                      ▼
engine-owned Tool broker  ──►  curated invoice Tool  ──►  WarehouseObject (SQLite DO)
```

The model never touches the database. It writes a program; the program runs in
an **isolated Dynamic Worker** with no ambient authority and reaches the
warehouse only through the brokered `warehouse.listInvoices` method. The
caller supplies typed filters (`minimumRevenue` and `region`), while the
adapter selects a fixed parameterized read statement. No SQL text crosses the
Tool or RPC boundary. Deployment class
**E**: the Agent runs ephemerally; the Durable Object is the warehouse data
store, not a Conversation store.

The response makes the Code Mode usage explicit — the tool, the isolated
executor, and the **actual JavaScript the model wrote**, alongside its result:

```jsonc
{
  "answer": "EMEA has the highest total revenue at 69,250.",
  "codeMode": {
    "used": true,
    "tool": "run_javascript",
    "executor": "cloudflare-dynamic-worker",
    "calls": 1,
    "program": "async () => { const result = await warehouse.listInvoices({ region: \"emea\" }); return result.invoices.reduce((sum, row) => sum + row.revenue, 0); }",
    "result": { "region": "emea", "total_revenue": 69250 },
    "logs": [],
  },
  "profile": "openai",
}
```

On the offline scripted profile `program` is a fixed demonstration program and
`profile` is `"scripted"`; on the live profile the model writes its own.

### Read-only authority on Durable Object SQLite

Durable Object SQLite does not expose a connection-level `query_only` lock to
this Worker. The example therefore does not accept arbitrary SQL and does not
claim a text scanner is database authority. `WarehouseObject.listInvoices`
Schema-decodes one closed request, selects one of four adapter-owned
parameterized `SELECT` statements, caps the cursor at 200 rows, and
Schema-encodes the response. A write statement is not representable through
the public Tool or DO RPC surface.

## Run the tests (no credentials)

Run `vp test run examples/code-mode-cloudflare/test/demo.test.ts` from the
repository root.

The test bundles the Worker and boots it in a real workerd runtime
(programmatic Miniflare) with a Worker Loader binding and the SQLite Durable
Object, then asserts that a generated program queried the real DO and computed
the answer, malformed HTTP/DO values fail Schema decoding, and SQL authority is
absent from the curated operation. The
default profile is a deterministic scripted model, so no API key is needed.

## Deploy to Cloudflare

```sh
# Optional: run the live OpenAI profile instead of the scripted one.
bunx wrangler secret put OPENAI_API_KEY
# When OPENAI_API_KEY is set, also set a shared secret so the paid /ask
# endpoint cannot be driven anonymously (callers must then send
# `Authorization: Bearer <token>`):
bunx wrangler secret put DEMO_AUTH_TOKEN

bun run --filter @effect-agent/example-code-mode-cloudflare deploy
curl -X POST https://<your-worker>/ask \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer <your DEMO_AUTH_TOKEN>' \
  -d '{"question":"Which customers have more than $10,000 in revenue?"}'
```

When `OPENAI_API_KEY` is set the Worker **requires** `DEMO_AUTH_TOKEN` and
rejects requests without a matching bearer token, so the paid path never
serves anonymous callers. The offline scripted default (no `OPENAI_API_KEY`)
needs no token.

`wrangler.jsonc` wires the `WAREHOUSE` Durable Object, the `LOADER`
(`worker_loaders`) binding the Dynamic Worker executor loads generated programs
through, and the `CODE_MODE_HOST` self service binding to
`CodeModeHostEntrypoint` (the production seam for
`ctx.exports.CodeModeHostEntrypoint()`).

Worker Loader is currently a Cloudflare beta; this example makes no hosted-
platform, cost, or durability claim beyond deployment class E. Without
`OPENAI_API_KEY` the Worker runs the deterministic scripted profile.
