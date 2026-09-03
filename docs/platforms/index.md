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
| Execution  | Worker process you operate                    | Durable Object per thread           |
| Storage    | Persistent SQLite file                        | Each Object's SQLite database       |
| Scheduling | Worker pool, wake notifications, ledger scans | RPC calls and alarms                |
| Recovery   | Host startup and worker claims                | Object events and alarms            |

Choose Node.js when you operate a process and persistent disk.
Choose Cloudflare when your application uses Workers and Durable Objects.

To drive the durable runtime through an injected Effect `WorkflowEngine`, see
[Effect Workflows](../guide/workflows). Platform adapters supply the runtime storage,
dispatch store, and repair trigger; the guide includes a [Node.js setup](../guide/workflows#node).

## Choose what you need

Use a durable host when accepted work must survive a restart.
See [durability](../concepts/durability) for recovery rules and unconfirmed tool outcomes.

For work that can end with its process, use [`AgentRuntime.run`](../guide/run-agents)
with provider and tool Layers plus `ThreadHistory.layerTransient`.
Provide a `RunContextPreparation` Layer only when you need host context loading.

To retain completed history without recovering unfinished work, provide
[`PersistentHistory.layer`](../guide/threads#retain-completed-runs) with a storage adapter.

## What your application supplies

Register agent bindings and supply provider clients, tool handlers, and credentials.
Authenticate callers and authorize access to each thread.
Expose the host through your application's API or UI.
