---
title: Package map
description: Current private workspace packages and future phase-gated boundaries.
---

# Package map

<StatusCallout status="available" phase="Private workspace" title="Ten framework packages exist today.">

All package names are working private names with source export maps. They are not published npm
artifacts. New packages appear only when their roadmap phase begins.

</StatusCallout>

## Current packages

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
approval and audit, budgets, context/compaction, scheduling, MCP, and structural redaction.

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
the run journal, and the `DurableAgentRuntime` coordinator (Receipt, Attempt, Settlement). It
depends on `@effect-agent/engine` to drive the interpreter through its public seams (ADR-0011).

### `@effect-agent/storage-memory`

Provides deterministic scoped in-memory `ConversationStore` and reference `SubmissionLedger`
Layers. The ledger declares `non-durable` capabilities; it exists for tests and conformance.

### `@effect-agent/storage-sqlite`

Provides the Node SQLite adapters behind the `DN` assembly: the Conversation Store and the
durable Submission Ledger in one database file, current-version (v2, exact-match) initialization,
observation, typed compatibility/corruption/conflict/contention errors, and before/after
failpoints on every durable mutation.

### `@effect-agent/platform-node`

Assembles the class `DN` Node/SQLite runtime: one shared SQLite client behind both stores,
validated typed configuration, the in-process wake scheduler with ledger-scan fallback, graceful
ownership drain, and the `NodeDurableHost` startup gates (storage compatibility, recovery before
admission) and shutdown order (close admission → release ownership → close storage). It is a
Layer-assembly library, not an application entrypoint.

### `@effect-agent/testing`

Provides the scripted Effect AI Model, deterministic services, cumulative Travel Planner fixtures,
and reusable conformance evidence. Production packages never depend on it.

## Leaf examples

`examples/demo` is a local browser test bench. `examples/providers` is a compile-only proof that the
same Definition binds directly to upstream OpenAI and Anthropic Models. Neither is a framework or
deployment package.

## Future packages

| Package                             | First phase | Status                           |
| ----------------------------------- | ----------: | -------------------------------- |
| `@effect-agent/storage-cloudflare`  |          P6 | <StatusBadge status="planned" /> |
| `@effect-agent/platform-cloudflare` |          P6 | <StatusBadge status="planned" /> |

Provider wrapper packages are deliberately absent. Provider integration remains upstream Effect AI
Models and Layers.

## Dependency direction

```text
core ← engine ← capabilities
core ← sandbox ← sandbox-local
core ← engine ← session ← storage adapters
engine + session + adapters ← platform packages
core + engine ← testing
```

An inward package cannot import an outward package. If a feature appears to require that, the
solution is an inward port and outward adapter—not a dependency reversal.
