---
title: Effect-native by construction
description: The dependency, failure, schema, and resource rules that define the framework.
---

# Effect-native by construction

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
- transport values at every host boundary.

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
processes, event publication, and attached Subagent children belong beneath it.

There are no daemon fibers. Interrupting the owner prevents new work, interrupts children, closes
resources, and cannot emit false success.

## Effect AI stays Effect AI

Effect Agent imports `Tool`, `Toolkit`, `LanguageModel`, `Model`, `Prompt`, and `Response` directly
from Effect AI. Provider integrations are upstream Effect AI Layers. The engine adds multi-Turn and
durability semantics around those values rather than cloning them.

This is a deliberately narrow seam: if broadly useful behavior is missing in Effect AI, the first
option is to contribute it upstream.

## The architectural test

Every public surface answers the same six questions: expected failure stays typed in `E`,
acquired capabilities stay visible in `R`, external input is Schema-decoded, every resource has
one Scope owner, the behavior can be replaced with a Layer, and it adds an Agent concept rather
than duplicating an Effect AI one. If a feature you are evaluating on top of the framework can
answer those too, it composes cleanly.
