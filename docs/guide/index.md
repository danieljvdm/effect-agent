---
title: Guide
description: Build, run, and operate Effect agents with step-by-step guides and working examples.
---

# Guide

Build an agent, give it tools, and run it in your application. Start with
[Getting started](./getting-started) for a working example, or read
[What is Effect Agent?](./introduction) for an introduction.

## Build your agent

- [Agent definitions](./agents): define inputs, outputs, instructions, and execution limits.
- [Tools and layers](./tools): connect models to your application services.
- [Run and stream](./run-agents): execute an agent and observe its progress.
- [Threads](./threads): keep conversation history across runs.
- [Context management](./context-management): manage long conversations and retrieved context.

## Add capabilities

- [Subagents](./subagents): delegate work and choose whether the parent waits or continues.
- [Agent messaging](./messaging): exchange input between independent agents.
- [Effect Workflows](./workflows): drive durable execution through a workflow engine.
- [Sandbox execution](./sandbox): run commands in a controlled environment.
- [Code Mode](./code-mode): let agents compose authorized tools with generated JavaScript.
- [Browser tools](./browser): capture pages, crawl sites, and interact with browsers.

## Test and operate

Use [deterministic testing](./testing) to exercise behavior without live model calls.
The [operations guide](./operations) covers authorization, recovery, and scheduled work.
Storage adapter authors can [run the certification contracts](./certify-adapters).

When you are ready to host durable work, choose a [platform](../platforms/).
