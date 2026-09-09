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

The warehouse RPC and Tool share Schema-defined JSON query results. The host
validates every RPC outcome before exposing it to the Tool; malformed responses
and transport failures become denied queries.

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

## Local tool-discovery benchmark

The separate benchmark compares eager exposure, native progressive discovery, and hybrid Code
Mode over the same 120 read-only business tools in local Miniflare/workerd. The common balance
tool remains pinned and native in every arm. The uncommon task discovers a treasury holdback;
the composed task follows invoice → fee → reserve keys, computes net cents, and preserves each
opaque read receipt. Code Mode performs those dependent reads in a real isolated Dynamic Worker.
Every arm uses the same handlers, instructions, input, limits and provider configuration.

This is a controlled exposure/composition experiment. Tools share a small key/reading schema,
and instructions provide the shared discovery term `settlement_read`. It does not evaluate
search relevance, a production application's task mix, deployed Cloudflare latency, or billing.
The harness bundles current source through public
package entry points and retains the executable bundle and its digest; it does not measure
published-package startup.

Run all nine credential-free cells through the actual isolated executor:

```sh
vp run -F @effect-agent/example-code-mode-cloudflare benchmark --validate --out-dir /tmp/tool-validation-001
```

This validates exact outputs, successful dependent reads, model receipt consumption and scoped
release. Its timings never establish a provider speedup. Ordinary tests also exercise the
OpenAI request/usage boundary with an in-process simulated response transport, including budget
rejection before dispatch; those tests make no model calls.

For live evidence, export `OPENAI_API_KEY`, select a supported model, and supply its current
input, cached-input and output prices in USD per million tokens. No model or pricing fallback
is provided. Inspect `--help`, then run the following with actual values:

```sh
vp run -F @effect-agent/example-code-mode-cloudflare benchmark --live \
  --model "$BENCHMARK_MODEL" \
  --input-usd-per-million "$BENCHMARK_INPUT_PRICE" \
  --cached-input-usd-per-million "$BENCHMARK_CACHED_PRICE" \
  --output-usd-per-million "$BENCHMARK_OUTPUT_PRICE" \
  --max-cost-usd 10 --out-dir /tmp/tool-live-001
```

The fixed provider configuration is reasoning effort `low`, service tier `default`, at most
2,048 output tokens, disabled truncation, strict tool schemas and schema-validated final output, and no response/conversation
reuse. There are no inference retries or token-count preflight requests. Before each inference,
the client reserves twice the serialized request's UTF-8 bytes plus 8,192 input tokens and the
full output allowance at the supplied prices. This deliberately conservative local estimate is
not a tokenizer or invoice. Actual usage must fit the reservation; missing or excessive usage
stops the suite and retains unresolved budget. The suite-wide ceiling is at most $10 and the
runner deadline is 25 minutes. A sample is bounded to 150 seconds and eight model turns.

One nine-cell pilot uses seed 97. Two measured cohorts each contain five blocks, using seeds
17, 29, 43, 61 and 79: 90 measured tasks. Each block includes all three workloads and all three
arms, rotates arm order, and alternates workload order. The same block input is used across
arms; each task gets a fresh Thread. Pilots, slow values, failures and active incomplete samples
are preserved. Do not run builds, tests or another benchmark during live measurement.

The thresholds are fixed before any live samples. Each candidate must reduce the equal-weight
mixed-workload median by both **15% and 500 ms** relative to eager in **each cohort**, with no
task-success loss. Common pinned regressions fail when they exceed both **10% and 500 ms**.
Native progressive and Code Mode receive separate conclusions; a Code Mode win cannot satisfy
the native gate. Missing, duplicate or failed cells cannot produce a passing comparison. The
command fails if native progressive misses its threshold, even if all tasks completed.

Each new output directory contains `worker.mjs` and an atomically updated `report.json` with
the source revision/dirty state, lockfile and bundle hashes, runtime identity, exact settings
and prices, raw samples, per-workflow medians and independent cohort conclusions. Samples
include provider rounds/finalizers, actual tool-name lists and serialized tool/request bytes,
returned model/tier, input/output/reasoning/cached tokens, estimated cost and unresolved
reservations. First-useful-action/result timestamps exclude discovery and the outer execution
tool; they refer to required business handlers. Full task time uses the runner's monotonic
clock through decoded output, while first-useful intervals use the Worker's clock. Never
subtract across those clock domains. The host is reused; provider prefix caching is uncontrolled
and observed cached-token counts must accompany interpretation. Different returned model
identities, incomplete cleanup or missing usage fail the attempt. Existing report directories
are never overwritten.
