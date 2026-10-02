---
title: Guide
description: Build, run, and operate Effect agents with step-by-step guides and working examples.
---

<a id="guide"></a>

Build an agent, give it tools, and run it in your application. Start with
[Getting started](/guide/getting-started/) for a working example, or read
[What is Yielded Agent?](/guide/introduction/) for an introduction.

## Build your agent

- [Agent definitions](/guide/agents/): define inputs, outputs, instructions, and execution limits.
- [Tools and layers](/guide/tools/): connect models to your application services.
- [Run and stream](/guide/run-agents/): execute an agent and observe its progress.
- [Threads](/guide/threads/): keep conversation history across runs.
- [Storage](/storage/): choose in-memory, SQLite, PostgreSQL, or Durable Object storage.
- [Context management](/guide/context-management/): manage long conversations and retrieved context.

## Add capabilities

- [Subagents](/guide/subagents/): delegate work and choose whether the parent waits or continues.
- [Agent messaging](/guide/messaging/): exchange input between independent agents.
- [Effect Workflows](/guide/workflows/): drive durable execution through a workflow engine.
- [Sandbox execution](/guide/sandbox/): run commands in a controlled environment.
- [Code Mode](/guide/code-mode/): let agents compose authorized tools with generated JavaScript.
- [Browser tools](/guide/browser/): capture pages, crawl sites, and interact with browsers.

## Test and operate

Use [deterministic testing](/guide/testing/) to exercise behavior without live model calls.
The [operations guide](/guide/operations/) covers authorization, recovery, and scheduled work.
Storage adapter authors can [run the certification contracts](/guide/certify-adapters/).

When you are ready to host durable work, choose a [platform](/platforms/).
