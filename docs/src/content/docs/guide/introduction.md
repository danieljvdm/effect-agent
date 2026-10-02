---
title: What is Yielded Agent?
description: An agent harness toolkit for TypeScript, built on Effect and Effect AI.
---

<a id="what-is-effect-agent"></a>

Yielded Agent (formerly Effect Agent) is an agent harness toolkit for TypeScript, built on Effect and Effect AI.
You supply a model, tools, instructions, and input/output schemas. It runs the agent loop,
executes tool calls, and validates the result.

## What it adds to Effect AI

Effect AI provides models, tools, and provider integrations. Yielded Agent uses those directly and adds:

- [Durable execution](/concepts/durability/) to recover accepted work after a crash on Node.js or Cloudflare.
- [Limits](/concepts/budgets/) on turns, tool calls, time, and token usage.
- [Streaming events and approvals](/guide/run-agents/) to observe and control a run.
- [Context management](/guide/context-management/) to prune and summarize long threads.
- [Subagents](/concepts/durability/#attached-subagents) for delegation with explicit permissions and budgets.
- [Thread history](/guide/threads/) across runs.
- [Code Mode](/guide/code-mode/) for generated JavaScript that reads and changes application data through authorized tools.
- [Sandbox execution](/guide/sandbox/) and [browser tools](/guide/browser/) for process output, rendered pages,
  crawling, and scoped browser interaction.

## It runs in Effect

`AgentRuntime.run` returns an `Effect`; `AgentRuntime.stream` returns a `Stream`.
Tool errors stay typed, required services stay visible, and interruption runs resource finalizers.
Supply dependencies through Layers, including [test models](/guide/testing/) that need no API keys.

Your application supplies credentials, tool handlers, and authorization.
The [package map](/reference/packages/) covers adapters and limitations.
