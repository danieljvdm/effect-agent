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
store, not a Conversation store.

### Read-only enforcement on Durable Object SQLite

The plan (§8.4) prescribes database authority as the read-only boundary. On
Durable Object SQLite the `PRAGMA query_only` lock is blocked by the storage
authorizer (`SQLITE_AUTH`), so this demo enforces read-only with a
leading-keyword denylist over the literal-stripped statement, and denies the
escape hatches (`PRAGMA`, `ATTACH`, `load_extension`, multi-statement). A
production warehouse should back this with a read-only database identity or
curated read-only views. The Node reference fixture in `@effect-agent/testing`
(`warehouseDbLayer`) proves the stronger database-authority path (`PRAGMA
query_only = ON` → `SQLITE_READONLY`) that Node SQLite allows.

## Run the tests (no credentials)

```sh
bun run --filter @effect-agent/example-code-mode-cloudflare test
```

The test bundles the Worker and boots it in a real workerd runtime
(programmatic Miniflare) with a Worker Loader binding and the SQLite Durable
Object, then asserts that a generated program queried the real DO and computed
the answer, and that a write is denied by the read-only database authority. The
default profile is a deterministic scripted model, so no API key is needed.

## Deploy to Cloudflare

```sh
# Optional: run the live OpenAI profile instead of the scripted one.
bunx wrangler secret put OPENAI_API_KEY

bun run --filter @effect-agent/example-code-mode-cloudflare deploy
curl -X POST https://<your-worker>/ask \
  -H 'content-type: application/json' \
  -d '{"question":"Which customers have more than $10,000 in revenue?"}'
```

`wrangler.jsonc` wires the `WAREHOUSE` Durable Object, the `LOADER`
(`worker_loaders`) binding the Dynamic Worker executor loads generated programs
through, and the `CODE_MODE_HOST` self service binding to
`CodeModeHostEntrypoint` (the production seam for
`ctx.exports.CodeModeHostEntrypoint()`).

Worker Loader is currently a Cloudflare beta; this example makes no hosted-
platform, cost, or durability claim beyond deployment class E. Without
`OPENAI_API_KEY` the Worker runs the deterministic scripted profile.
