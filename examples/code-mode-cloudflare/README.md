# Durable Code Mode on Cloudflare

A runnable Cloudflare Worker where **Code Mode** (D-035,
[ADR-0017](../../docs/adr/0017-code-mode-executor-and-broker.md)) runs on the
**real durable runtime**: every question becomes a durable Run inside a
Conversation Object, the model's generated JavaScript executes in an isolated
**Cloudflare Dynamic Worker**, and the data lives in a plain **D1** database.

## Architecture

```
POST /ask → Worker (thin durable client)
  └─ client.submit({ definition }, { question }, { digests })
       ↓
  InvoiceAgentConversationObject           ← the real Conversation Object
    ├─ bindings: ({ ctx, env }) => …       ← Code Mode wired INTERNALLY from
    │    ├─ env.LOADER        → Dynamic Worker executor     the Object's own
    │    ├─ env.CODE_MODE_HOST → host RPC seam              direct bindings
    │    └─ env.DB            → Effect SQL over D1
    ├─ append-only canonical log (Durable Object SQLite), alarm-driven
    └─ profiles registered per digest triple: scripted / probes / live OpenAI
       ↓
  awaitSettlement → readAll → the receipt is built FROM THE CANONICAL LOG
```

The Worker never touches the model, the tools, or the data: it submits a
definition plus a digest triple, and the Object resolves which registered
binding serves the claim (the framework's S2 digest resolution). The model
writes one JavaScript program; the program runs in a fresh Dynamic Worker
(`globalOutbound: null` — no network, no env, no secrets) whose only authority
is the brokered `invoices.query` tool, which is ordinary Effect SQL over the
D1 binding, guarded by a read-only allowlist scan.

Every step of the run is a durable fact: the model's program is recoverable
from the committed Turn messages (`ModelResponseRecorded`), the execution
outcome from `ToolCallSettled`, the answer from `SubmissionSettled` — and the
response includes the record trail, so the durable log is visible in every
answer.

## Run the tests (no credentials)

```sh
bun run --filter @effect-agent/example-code-mode-cloudflare test
```

The suite bundles the Worker and boots it in a real workerd runtime
(programmatic Miniflare) with a Worker Loader binding, the SQLite-backed
Conversation Object, and a local D1 database, then proves: a generated program
queried the real D1 and computed the answer through a durable, settled run; a
write-attempting program is denied with a typed envelope; a CTE-prefixed write
(which a naive denylist would miss) is rejected with the D1 rows intact; and
the canonical log streams as NDJSON while the run progresses. The default
profile is a deterministic scripted model, so no API key is needed.

## Try it locally

```sh
bunx wrangler dev

# one JSON response after settlement:
curl -X POST http://localhost:8787/ask \
  -H 'content-type: application/json' \
  -d '{"question":"Which region has the highest total revenue?"}'

# stream the canonical log record-by-record as the durable run progresses:
curl -N -X POST 'http://localhost:8787/ask?stream=1' \
  -H 'content-type: application/json' \
  -d '{"question":"What percentage of total revenue comes from EMEA?"}'
```

The streaming mode tails the Object's log with `readPage(afterSequence)` and
emits one NDJSON line per committed record, closing with the full receipt on
`SubmissionSettled`. The observer is optional by construction: kill the curl
mid-stream and the durable run still settles.

A response looks like:

```jsonc
{
  "conversationId": "conversation-…",
  "outcome": "completed",
  "answer": "EMEA accounts for 75.1% of total revenue.",
  "codeMode": {
    "used": true,
    "tool": "run_javascript",
    "executor": "cloudflare-dynamic-worker",
    "calls": 1,
    "program": "async () => { const r = await invoices.query({ sql: `…` }); … }",
    "result": { "answer": "EMEA accounts for 75.1% of total revenue." },
  },
  "records": [
    "ConversationCreated",
    "UserInputRecorded",
    "ModelResponseRecorded",
    "ToolCallSettled",
    "ModelResponseRecorded",
    "SubmissionSettled",
  ],
}
```

## Live model

```sh
# Optional: run the live OpenAI profile instead of the scripted one.
bunx wrangler secret put OPENAI_API_KEY
# REQUIRED whenever OPENAI_API_KEY is set: the Worker fails closed and rejects
# requests without a matching `Authorization: Bearer <token>` header, so the
# paid path never serves anonymous callers.
bunx wrangler secret put DEMO_AUTH_TOKEN

bun run --filter @effect-agent/example-code-mode-cloudflare deploy
```

Without `OPENAI_API_KEY` the Worker runs the deterministic scripted profile
and needs no token. The `?probe=write` / `?probe=cte` scripted profiles (used
by the tests) route by digest the same way the live profile does.

## Honest scope notes

- **Read-only enforcement is an allowlist scan**, not database authority: the
  statement must be a single read (`SELECT`, or a `WITH` whose body reads) and
  contain no write/DDL/transaction/escape-hatch token anywhere in the
  literal-stripped text (a leading-keyword denylist would miss `WITH … DELETE`).
  A production warehouse should use a read-only database identity or curated
  views. The Node reference fixture in `@effect-agent/testing` proves the
  stronger database-authority path.
- **Code Mode is specified for deployment class E** (ADR-0017); running it on
  the durable runtime here is owner-directed demo territory. Mid-pass eviction
  semantics (the deferred C5/C6 slices) remain future framework work; this
  demo's tool is `readonly`-class, so recovery may legally re-execute a pass.
- Worker Loader is a Cloudflare beta; `database_id` in `wrangler.jsonc` is a
  local-dev placeholder — point it at a real D1 database to deploy. No hosted
  Cloudflare, cost, or performance claims.
