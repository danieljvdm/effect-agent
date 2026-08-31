---
title: Platforms
description: Choose a durable host for Node.js or Cloudflare.
---

# Platforms

Run durable agents on Node.js or Cloudflare Workers. Both hosts record accepted work,
track worker ownership, and recover after restarts.

|            | [Node.js](./node)                             | [Cloudflare](./cloudflare)          |
| ---------- | --------------------------------------------- | ----------------------------------- |
| Package    | `@effect-agent/platform-node`                 | `@effect-agent/platform-cloudflare` |
| Execution  | Worker process you operate                    | Durable Object per conversation     |
| Storage    | Persistent SQLite file                        | Each Object's SQLite database       |
| Scheduling | Worker pool, wake notifications, ledger scans | RPC calls and alarms                |
| Recovery   | Host startup and worker claims                | Object events and alarms            |

Choose Node.js when you operate a process and persistent disk.
Choose Cloudflare when your application uses Workers and Durable Objects.

## Choose what you need

Use a durable host when accepted work must survive a restart.
See [durability](../concepts/durability) for recovery rules and unconfirmed tool outcomes.

For work that can end with its process, use [`AgentRuntime.run`](../guide/run-agents)
with provider and tool Layers plus `ConversationHistory.layerTransient`.

To retain completed history without recovering unfinished work, provide
[`PersistentHistory.layer`](../guide/conversations#retain-completed-runs) with a storage adapter.

## What your application supplies

Register agent bindings and supply provider clients, tool handlers, and credentials.
Authenticate callers and authorize access to each conversation.
Expose the host through your application's API or UI.
