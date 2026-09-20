# Travel planner

An Effect Agent example for planning trips through chat or voice, researching destinations
with background agents, saving itineraries, and building standalone trip websites.

It uses Cloudflare:

- **Workers** to serve the TanStack Start UI and APIs.
- **Durable Objects with SQLite** to persist accounts, conversations, trips, and agent execution.
- **Browser Run** to research travel websites.
- **Artifacts** to version generated trip-site source code.
- **Workflows and Sandbox containers** to build sites, **R2** to store builds, and
  **Dynamic Workers** to serve them.
- **Email Sending** for email sign-in, alongside GitHub OAuth.

The app consumes published Effect Agent packages and uses Effect Atom for client state.
[alchemy.run.ts](alchemy.run.ts) defines the Cloudflare resources and required configuration.

The current planner and scout exchange standard `WorkerUpdate` and `WorkerCompletion` messages.
Retired planner v8–v15 and scout v1–v3 registrations and their custom report inputs are removed.
Start fresh conversations for those demo revisions; no migration or storage reset is supplied.
Keep their old stores and original executable release for inspection or reconciliation of unfinished
external actions. Existing current-agent records, saved trips, accounts, and report evidence are retained.

Authentication uses matching versions of `@yielded/auth` and `@yielded/auth-persistence`.
The persistence package supplies the Drizzle adapters for the app-owned SQLite tables.
GitHub sign-in creates flow IDs on the server; the browser retains the returned ID for the
callback. Email verification keys are supplied through `ProofKeys` using `AUTH_PROOF_KEY`.

When upgrading from Auth beta.5 to beta.7, keep the existing database, namespaces, and key IDs.
Users with an email code requested before the upgrade should start a fresh email flow because
the proof template identifier changed. Existing accounts and sessions are retained.

The production deployment workflow enables Cloudflare traces after verifying request URL
query-string redaction, keeping authentication callback parameters out of platform telemetry.
Direct Alchemy deployments leave traces disabled because its SDK does not yet expose that setting.
Manual dispatch of **Deploy travel planner** deploys the selected branch to production, so it
requires deployment authorization even for a PR branch. Automatic deployments remain on `main`.

From the repository root:

```sh
vp install
vp run -F @effect-agent/example-travel-planner dev
```

The conversation loads in stages. `GetPlanner` returns messages, trips, and the latest
source-record overview for up to eight scouts and the trip's editor without reading child
objects. Each `GetPlannerWorker` query then fills in its own status, public progress, and recent
activity. The client shares three request permits across scouts and editor, polls active or unavailable queries
two seconds after each response, and bounds each active read to three seconds. Finished views
stop polling until a new source request, mutation invalidation, or remount. A stalled or failed
worker leaves the conversation and other workers usable. Loading updates are distinct from
starting work or unavailable updates.

Worker query identity includes the signed-in account, conversation, worker, and canonical
request sequence. Changing conversations or replacing a task releases its subscriptions,
cancels queued/in-flight client reads, and prevents late replies from replacing the current
view. Existing mutations invalidate these queries through the shared `planner` reactivity key.
Cancellation stops observation; accepted durable work continues. Native Durable Object RPCs
are finite and separately time-limited on the receiver; cancelling browser fetch does not
promise immediate cancellation of an already dispatched native RPC.

The conversation object verifies the exact `WorkerInputRequested` record and the host's read
authorization before addressing a child. The child computes the compact view locally with one
lookup for that request, a nonterminal scan, and the final 100 canonical records. Limits apply
before decoding and activity projection. Status describes that selected request plus any
active work on the worker; an older pending delivery is not reconstructed as a new task.
Activity is a recent window, not a complete audit log. Reads never admit, recover, or replay
work. Diagnostics retain the existing redaction boundary.

These queries use the exact published framework dependencies in this example. They do not
change the framework's general `Subagent.inspect` or `Subagent.observe` contracts. Any future
framework optimization belongs in a separate library PR, followed by publication and an exact
consumer dependency upgrade before integration. Local Miniflare checks establish behavior and
work budgets; deployed latency requires a separately authorized deployment and measurement.
