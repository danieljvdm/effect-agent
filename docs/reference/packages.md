---
title: Package map
description: Current private workspace packages and their boundaries.
---

# Package map

Fourteen framework packages publish to npm on the opt-in `beta` dist-tag. Provider wrapper
packages are deliberately absent: provider integration remains upstream Effect AI Models and
Layers.

## Packages

### `effect-agent` (umbrella)

Re-exports the framework's complete pure surface — schema-first authoring (core), the bounded
interpreter (engine), and operational capabilities — as one dependency-clean root package,
mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters stay scoped, and the
umbrella is version-fixed to its three constituents.

### `@effect-agent/core`

Owns Agent Definitions and Bindings, branded identity Schemas, finite policy, expected framework
errors, semantic Run Events, and the `IdGenerator` port. It depends only on Effect and Effect AI.

Key exports: `Agent`, `AgentPolicy`, identifiers, errors, `RunEvent`, `IdGenerator`.

### `@effect-agent/engine`

Owns the one ephemeral interpreter, Turn loop, Effect AI Response reduction, Tool scheduling,
policy enforcement, semantic events, and narrow `RunOptions` seams.

Key exports: `AgentRuntime`, `DetachedRun`, `RunOptions`, operational hook interfaces.

### `@effect-agent/capabilities`

Adapts richer optional services to the engine: process-local Conversations, command queues,
approval and audit, budgets, context/compaction, scheduling, MCP, structural redaction, and the
Subagent authoring surface (`Subagent.define`, `SubagentPolicy`/`SubagentGrant`, and
`SubagentRuntime.layer` with its ephemeral and durable delegation branches).

It depends outward from engine; the engine does not import it.

### `@effect-agent/sandbox`

Defines schema-first, platform-neutral sandbox requests, events, errors, and the streaming `Sandbox`
service.

### `@effect-agent/sandbox-local`

Implements the Sandbox contract with local child processes for trusted development. It identifies
itself as `unisolated` and rejects isolation policy it cannot enforce.

### `@effect-agent/session`

Owns canonical Conversation record Schemas, batches, digests, replay/checkpoints, the
`ConversationStore`, `SubmissionLedger`, and `WakeScheduler` ports, the pure recovery classifier,
the run journal, and the `DurableAgentRuntime` coordinator (Receipt, Attempt, Settlement), including
the conversation-keyed `awaitProgress` boundary that subscribes before its authoritative canonical
read. It
depends on `@effect-agent/engine` to drive the interpreter through its public seams.
It also owns the durable Subagent protocol: the requested/started/joined/lineage record
Schemas, the child budget reservation and `waitingForChild` ledger operations, and the
host-supplied `AgentBindingResolver` port for exact-digest Binding resolution.
Adapter certification reports, port runners, and the TestClock-dependent conformance case arrays
are available only from `@effect-agent/session/testing`; the package root has no transitive
test-runtime dependency.

### `@effect-agent/storage-memory`

Provides deterministic scoped in-memory `ConversationStore` and reference `SubmissionLedger`
Layers. The ledger declares `non-durable` capabilities; it exists for tests and conformance.

### `@effect-agent/storage-sqlite`

Provides the Node SQLite adapters behind the `DN` assembly: the Conversation Store and the
durable Submission Ledger in one database file, current-version (v4, exact-match) initialization,
observation, typed compatibility/corruption/conflict/contention errors, and before/after
failpoints on every durable mutation.

### `@effect-agent/platform-node`

Assembles the class `DN` Node/SQLite runtime: one shared SQLite client behind both stores,
validated typed configuration, the in-process wake scheduler with ledger-scan fallback, graceful
ownership drain, Agent Binding registration for durable workers
(`NodeDurableRuntimeOptions.bindings` behind `NodeDurableHost.runResolvedWorkers`; an empty
roster fails every claim closed), and the `NodeDurableHost` startup gates (storage compatibility,
recovery before admission) and shutdown order (close admission → release ownership → close
storage). It is a Layer-assembly library, not an application entrypoint.

### `@effect-agent/storage-cloudflare`

Provides the Durable Object SQLite adapters behind the `DC` assembly: the Conversation Store and
durable Submission Ledger against one Conversation Object's private SQLite database (the v4
table mirror plus the `effect_agent_meta` exact-or-fresh version gate and the durable
`effect_agent_child_settlements` cross-store notification marker), storage-backed transactions,
typed compatibility/corruption/bound errors, the same failpoint-location names as the Node
adapter, and the cross-Object seam — Schema port-call envelopes and routed decorator Layers over
a closed route-capable subset with adapter-minted routable Submission identities. It never
imports the `cloudflare:workers` runtime module; Durable Object handles are injected as Layer
construction values.

### `@effect-agent/platform-cloudflare`

Assembles the class `DC` Cloudflare runtime and is the only package that imports
`cloudflare:workers`: binding Layers (namespace, Object context, identity), schema-validated
configuration, the single multiplexed `DurableAlarmService` with the idempotent
`ConversationMaintenance` pass (pre-armed alarm invariant: committed nonterminal work implies a
committed alarm), the alarm/RPC wake scheduler, the Durable Object RPC port transport,
`CloudflareDurableRuntime.layer`, `makeConversationObjectClass` (local-only constructor gates
and the typed admission-limits gate before `submit`), and the Worker-side
`CloudflareConversationClient`, whose `awaitProgress` RPC retries Object resets with a fresh stub
and sends explicit scoped cancellation on interruption. The client Layer requires `Crypto.Crypto`
so its composition root owns collision-resistant cancellation identity generation instead of the
client reading ambient time or randomness. `CloudflareBindingSource` may capture registered worker
Bindings from `CloudflareBindingSourceContext` once per Object incarnation, after identity
derivation. It is a Layer-assembly library, not an application entrypoint.

### `@effect-agent/pr-review`

The packaged GitHub pull-request reviewer: schema-first review contracts, the
`PullRequestSource`/`ReviewPublisher` ports with GitHub REST adapters, fail-closed anchor
validation and publication planning, a fail-open sticky progress comment
(`ReviewProgressReporter`), flat and fan-out reviewer shapes, and the `PrReview`
configuration factory. Fan-out is scheduled entirely by host code from the deterministic
unit plan — no coordinator model, no delegation tool: independent general and specialist
discovery plus fresh candidate verification run as bounded child passes with one retry each,
and only exactly confirmed candidates publish. Its public result separates path/input
coverage from pass settlement; neither claim means exhaustive defect detection. Every
completed run that can be signed advances the authenticated incremental baseline; a pass
that stays failed carries its paths forward as retryable scope inside the state instead of
freezing the baseline, and only a fully settled state authorizes skip-unchanged. A rewritten
head that is no longer a git ancestor stays incremental when a two-dot tree comparison can
name the current PR paths whose contents changed; unchanged leftovers retry only the failed
pass and keep their stored findings. Subpath
entries: `./testing`
(fixture source, collecting publisher,
prompt-keyed scripted models), `./action` and `./cli` (platform-node host entrypoints). Consumes
the `effect-agent` umbrella — the first package-level consumer of that edge. Deployment class E
only; review posting is never claimed exactly-once.

### `@effect-agent/testing`

Provides the scripted Effect AI Model, deterministic services, cumulative Travel Planner fixtures,
and reusable conformance evidence. Production packages never depend on it (the one dev-only
exception: `platform-cloudflare`'s test suite consumes the shared fixtures to prove DN/DC
equivalence).

## Leaf examples

`examples/demo` is a local browser test bench. `examples/providers` is a compile-only proof that the
same Definition binds directly to upstream OpenAI and Anthropic Models. `examples/pr-review` is a
consumer of `@effect-agent/pr-review` demonstrating the adaptation path (guidance, an extra
read-only tool, ignore globs); `examples/pr-work-orders` is the private
trusted-local proof of a head-bound work-order implementer;
`examples/pr-work-order-ingress` is the private GitHub dispatch and isolated
publication proof; `examples/repo-ops`
is an internal repository-operations auditor. None is a framework
or deployment package. The repository root also carries `action/`, the prebuilt
node-runtime GitHub Action over `@effect-agent/pr-review` with its committed bundle.
Its normal synchronize path recovers authenticated review state and reviews only the new delta
plus carried unreviewed scope; explicit final mode performs fresh discovery over the bounded
full diff. Blocking findings fail the check first; a machinery-incomplete result fails it with
reasons that explicitly name a reviewer-side gap carried forward for retry, never a request to
change code.

## Dependency direction

```text
core ← engine ← capabilities
core ← sandbox ← sandbox-local
core ← engine ← session ← storage adapters
engine + session + adapters ← platform packages
core + engine ← testing
core + engine + capabilities ← effect-agent (umbrella) ← pr-review
```

An inward package cannot import an outward package. If a feature appears to require that, the
solution is an inward port and outward adapter—not a dependency reversal.
