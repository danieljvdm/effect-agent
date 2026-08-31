---
title: What is Effect Agent?
description: An agent harness toolkit for TypeScript, built on Effect and Effect AI.
---

# What is Effect Agent?

Effect Agent is an agent harness toolkit for TypeScript, built on Effect and Effect AI.
You supply a model, tools, instructions, and input/output schemas. It runs the agent loop,
executes tool calls, and validates the result.

## What it adds to Effect AI

Effect AI provides models, tools, and provider integrations. Effect Agent uses those directly and adds:

- [Durable execution](../concepts/durability) to recover accepted work after a crash on Node.js or Cloudflare.
- [Limits](../concepts/budgets) on turns, tool calls, time, and token usage.
- [Streaming events and approvals](./run-agents) to observe and control a run.
- [Context management](./context-management) to prune and summarize long conversations.
- [Subagents](../concepts/durability#attached-subagents) for delegation with explicit permissions and budgets.
- [Conversation history](./conversations) across runs.
- [Code Mode](./code-mode) for generated JavaScript that queries application data through read-only tools.
- [Sandbox execution](./sandbox) and [browser tools](./browser) for process output, rendered pages,
  crawling, and scoped browser interaction.

## It runs in Effect

`AgentRuntime.run` returns an `Effect`; `AgentRuntime.stream` returns a `Stream`.
Tool errors stay typed, required services stay visible, and interruption runs resource finalizers.
Supply dependencies through Layers, including [test models](./testing) that need no API keys.

Your application supplies credentials, tool handlers, and authorization.
The [package map](../reference/packages) covers adapters and limitations.
