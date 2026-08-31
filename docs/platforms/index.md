---
title: Platforms
description: Run Effect Agent on Node.js or Cloudflare Workers.
---

# Platforms

Effect Agent runs on Node.js and Cloudflare Workers. The platform packages add storage,
work scheduling, and recovery to your Agent definitions.

|                  | [Node.js](./node)                                              | [Cloudflare](./cloudflare)                |
| ---------------- | -------------------------------------------------------------- | ----------------------------------------- |
| Package          | `@effect-agent/platform-node`                                  | `@effect-agent/platform-cloudflare`       |
| Execution        | A worker process you operate                                   | A Durable Object per Conversation         |
| Storage          | A SQLite file                                                  | Each Durable Object's SQLite database     |
| Work scheduling  | A bounded worker pool with wake notifications and ledger scans | RPC calls and Durable Object alarms       |
| Restart recovery | When the host starts and workers reclaim work                  | When an Object receives an event or alarm |

## Choose what you need

For a request that can end with its process, use [`AgentRuntime.run`](../guide/run-agents).
Supply the provider and Tool Layers plus `ConversationHistory.layerTransient`.
You do not need a platform package for that.

To keep completed conversation history between requests, provide
[`PersistentHistory.layer`](../guide/conversations#retain-completed-runs) with a storage adapter.
That retains successful Runs; it does not recover unfinished work.

Use a platform host when accepted work must survive a process restart or Object eviction.
It records a Receipt before acknowledging submission, tracks ownership of each attempt, and
records the final Settlement. The [durability guide](../concepts/durability) explains recovery,
including Tool calls whose external outcome cannot be confirmed.

## What your application supplies

Both hosts need registered Agent Bindings with provider clients and Tool handlers. Your
application also owns credentials, request authentication, and permission to submit or operate
on a Conversation. Neither package installs an HTTP API or a chat UI.

Start with [Node.js](./node) if you already operate a Node process and persistent disk.
Use [Cloudflare](./cloudflare) when your application runs on Workers and Durable Objects.
