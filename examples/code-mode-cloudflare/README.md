# Code Mode over a SQLite Durable Object warehouse

A runnable Cloudflare Worker that answers natural-language questions about
invoice data by running Code Mode on Cloudflare Dynamic Workers, over a
SQLite-backed Durable Object warehouse.

The [Code Mode guide](../../docs/guide/code-mode.md) explains Tool definitions, handler Layers,
executor setup, and limits.

## What it demonstrates

```
POST /ask { "question": "Which customers have more than $10,000 in revenue?" }
        │
        ▼
   ephemeral Code Mode Agent  ──writes──►  one bounded JavaScript program
        │                                          │
        │ native run_javascript tool               │ warehouse.query(...)  (typed sandbox global)
        ▼                                          ▼
 Dynamic Worker CodeExecutor ──loads──►  fresh Worker (globalOutbound: null)
   (@effect-agent/platform-cloudflare)        │ no network / bindings / secrets
        │                                      │ RPC back through the broker only
        ▼                                      ▼
 engine-owned Tool broker  ──►  read-only SQL tool  ──►  WarehouseObject (SQLite DO)
```

The model never touches the database. It writes a program; the program runs in
an **isolated Dynamic Worker** with no ambient authority and reaches the
warehouse only through the brokered `warehouse.query` method, which runs one
**read-only** SQL statement against a SQLite Durable Object. Deployment class
**E**: the Agent runs ephemerally; the Durable Object is the warehouse data
store, not a Thread store.

The response includes the Tool, isolated executor, generated JavaScript, and result:

```jsonc
{
  "answer": "EMEA has the highest total revenue at 69,250.",
  "codeMode": {
    "used": true,
    "tool": "run_javascript",
    "executor": "cloudflare-dynamic-worker",
    "calls": 1,
    "program": "async () => { const result = await warehouse.query({ sql: \"SELECT region, SUM(revenue) AS total_revenue FROM invoice_summary GROUP BY region ORDER BY total_revenue DESC LIMIT 1\" }); return result.rows[0]; }",
    "result": { "region": "emea", "total_revenue": 69250 },
    "logs": [],
  },
  "profile": "openai",
}
```

On the offline scripted profile `program` is a fixed demonstration program and
`profile` is `"scripted"`; on the live profile the model writes its own.

### Read-only enforcement on Durable Object SQLite

On Durable Object SQLite the `PRAGMA query_only` lock is blocked by the storage
authorizer (`SQLITE_AUTH`), so this demo enforces read-only in application
code. A denylist of write keywords is bypassable. For example, a `WITH … DELETE`
common-table expression does not _start_ with a write keyword. The scan therefore uses an
**allowlist**: the statement must be a single read (`SELECT`, or a `WITH` whose
body is a read) and must contain no write, DDL, transaction, or escape-hatch
token (`INSERT`/`UPDATE`/`DELETE`/`DROP`/`PRAGMA`/`ATTACH`/`load_extension`/…)
anywhere in the literal-stripped text. Row and byte caps are enforced while
draining the cursor, so a query that would return a huge result set never
fully materializes.

This text scan is a demo-grade boundary. A production warehouse should back
this with a read-only database identity or curated read-only views. The Node
private fixture in `packages/testing/test/fixtures/warehouse.ts` (`warehouseDbLayer`) proves the
stronger _database-authority_ path (`PRAGMA query_only = ON` →
`SQLITE_READONLY`) that Node SQLite allows.

## Run the tests (no credentials)

From the repository root:

```sh
vp run -F @effect-agent/example-code-mode-cloudflare test
```

The test bundles the Worker and boots it in a real workerd runtime
(programmatic Miniflare) with a Worker Loader binding and the SQLite Durable
Object, then asserts that a generated program queried the real DO and computed
the answer, and that a write is denied by the application's SQL allowlist. The
default profile is a deterministic scripted model, so no API key is needed.

## Deploy to Cloudflare

Run these commands from the repository root. Wrangler's explicit config path targets this example
when adding secrets.

```sh
# Optional: run the live OpenAI profile instead of the scripted one.
vp dlx wrangler secret put OPENAI_API_KEY --config examples/code-mode-cloudflare/wrangler.jsonc
# When OPENAI_API_KEY is set, also set a shared secret so the paid /ask
# endpoint cannot be driven anonymously (callers must then send
# `Authorization: Bearer <token>`):
vp dlx wrangler secret put DEMO_AUTH_TOKEN --config examples/code-mode-cloudflare/wrangler.jsonc

vp run -F @effect-agent/example-code-mode-cloudflare deploy
curl -X POST https://<your-worker>/ask \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer <your DEMO_AUTH_TOKEN>' \
  -d '{"question":"Which customers have more than $10,000 in revenue?"}'
```

When `OPENAI_API_KEY` is set the Worker **requires** `DEMO_AUTH_TOKEN` and
rejects requests without a matching bearer token, so the paid path never
serves anonymous callers. The offline scripted default (no `OPENAI_API_KEY`)
needs no token.

`wrangler.jsonc` wires the `WAREHOUSE` Durable Object and the `LOADER`
(`worker_loaders`) binding the Dynamic Worker executor loads generated programs
through. The executor passes a request-owned RPC capability directly to each
loaded program, so the application needs no callback entrypoint or self service
binding.

See Cloudflare's [Dynamic Workers guide](https://developers.cloudflare.com/dynamic-workers/getting-started/)
for Worker Loader setup. This example runs in deployment class E and makes no durable-execution
claim. Without `OPENAI_API_KEY` the Worker runs the deterministic scripted profile.
