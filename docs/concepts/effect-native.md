---
title: Effect-native by construction
description: The dependency, failure, schema, and resource rules that define the framework.
---

# Effect-native by construction

<StatusCallout status="available" phase="Architectural invariant" title="These constraints shape both the current runtime and every planned phase." />

Effect Agent is not an agent framework wrapped in Effect. Its public contract is designed so an
Effect application does not surrender the properties it already relies on.

<ContractPanel
  success="what the Agent produced"
  failure="what was expected to go wrong"
  requirements="what must be provided"
/>

## Effects are the public boundary

Public asynchronous operations return `Effect` or `Stream`, never a naked `Promise`. Tool handlers,
instruction sources, approval decisions, store operations, and platform capabilities follow the
same rule.

That makes composition ordinary:

```ts
AgentRuntime.run(agent, input).pipe(
  Effect.retry(modelRetryPolicy),
  Effect.timeout("2 minutes"),
  Effect.provide(AppLive),
  Effect.scoped,
);
```

The runtime still owns the policy decision about which operations are safe to retry. Effect's
operator is mechanism, not permission to repeat an uncertain external side effect.

## Schema is canonical

Effect Schema defines:

- Agent input and structured output;
- Tool parameters, success, and declared failure;
- commands and semantic events;
- persisted record envelopes and batches;
- transport values when those boundaries arrive.

Provider-facing JSON Schema and wire codecs are derived. Applications do not maintain parallel
Zod, Valibot, or provider schema trees.

## Layers preserve architecture

Definitions contain behavior and requirements, not live infrastructure. Toolkits, Models, stores,
sandboxes, clocks, IDs, authorization, and platform services arrive through Layers.

```text
application Definitions + Effect AI Models
                    ↓
          @effect-agent/core
                    ↓
          @effect-agent/engine
              ↙           ↘
     capabilities       session ports
                            ↓
                    storage adapters
```

Dependencies point inward. Node, SQLite, Cloudflare, provider SDK, and transport types cannot enter
core domain records.

## Scope is ownership

One ephemeral Run owns one parent Scope. Model streams, Tool fibers, queues, MCP clients, sandbox
processes, event publication, and future attached children belong beneath it.

There are no daemon fibers. Interrupting the owner prevents new work, interrupts children, closes
resources, and cannot emit false success.

## Effect AI stays Effect AI

Effect Agent imports `Tool`, `Toolkit`, `LanguageModel`, `Model`, `Prompt`, and `Response` directly
from Effect AI. Provider integrations are upstream Effect AI Layers. The engine adds multi-Turn and
durability semantics around those values rather than cloning them.

This is a deliberately narrow seam: if broadly useful behavior is missing in Effect AI, the first
option is to contribute it upstream.

## The architectural test

When evaluating a proposed feature, ask:

1. Does expected failure stay typed in `E`?
2. Do acquired capabilities stay visible in `R`?
3. Is external input Schema-decoded?
4. Does every resource have one Scope owner?
5. Can the behavior be replaced with a Layer?
6. Does it add an Agent concept, or duplicate an Effect AI concept?

If the interface cannot answer those questions, the architecture is not ready.
