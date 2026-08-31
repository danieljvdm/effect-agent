---
title: Built on Effect
description: Use Effect schemas, typed errors, service layers, and scoped resources.
---

<a id="effect-native-by-construction"></a>

# Built on Effect

Effect Agent keeps the contracts an Effect application relies on.

<ContractPanel
  success="what the agent produced"
  failure="what was expected to go wrong"
  requirements="what must be provided"
/>

<a id="the-architectural-test"></a>

## Return Effects and Streams {#effects-are-the-public-boundary}

Public asynchronous operations return `Effect` or `Stream`. This includes tool handlers,
instructions, approval decisions, stores, and platform capabilities.

```ts
AgentRuntime.run(agent, input).pipe(Effect.timeout("2 minutes"), Effect.provide(AppLive));
```

Retrying an entire run can repeat external effects. Durable recovery follows recorded tool
outcomes and never automatically replays an uncertain ordinary tool.

## Define data with Schema {#schema-is-canonical}

Effect Schema defines agent input and output, tool parameters and results, commands, events,
persisted records, and transport values. Provider JSON Schema and wire codecs derive from these
definitions. Applications need no parallel schema tree.

## Provide services through Layers {#layers-preserve-architecture}

Definitions describe behavior and requirements. Layers provide models, toolkits, stores,
sandboxes, clocks, identifiers, authorization, and platform services.

## Scope resources {#scope-is-ownership}

An ephemeral run owns one parent Scope. Model streams, tool fibers, queues, MCP clients, sandbox
processes, event publication, and attached children live beneath it.

`run` completes cleanup before returning. `stream` closes resources when consumption completes,
fails, or is interrupted. `start` requires a caller Scope because execution and replay continue.
Requirements from application code remain visible, including any real `Scope` requirement.

The runtime creates no daemon fibers. Interrupting the owner stops new work, interrupts children,
and closes resources without reporting false success.

## Use Effect AI directly {#effect-ai-stays-effect-ai}

Effect Agent uses Effect AI's `Tool`, `Toolkit`, `LanguageModel`, `Model`, `Prompt`, and `Response`
directly. Provider integrations remain Effect AI Layers. The engine adds agent loops,
conversation history, and durable execution around those values.
