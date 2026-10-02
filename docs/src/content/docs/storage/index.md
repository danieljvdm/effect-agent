---
title: Storage
description: Choose where agent history and accepted work are stored.
---

<a id="storage"></a>

Storage adapters provide Effect services for thread history and accepted work.
Choose where that state lives, then connect the stores to your agent runtime.

| Backend                            | Use                                                                   |
| ---------------------------------- | --------------------------------------------------------------------- |
| [In-memory](/storage/memory/)      | Conversations within one application Scope; isolated stores for tests |
| [SQLite](/storage/sqlite/)         | Persistent history in a local Node.js database                        |
| [PostgreSQL](/storage/postgres/)   | A database shared by multiple processes, using your Effect SQL client |
| [Cloudflare](/storage/cloudflare/) | SQLite owned by a Durable Object                                      |

Keep `@yielded/agent` and its adapters on the same release. See
[installation and compatibility](/guide/getting-started/#installation-and-compatibility)
for the matching Effect and model provider packages.

## Connect storage to execution

Use `InMemory.layer` for conversations that live with your application.
For history that survives a restart, connect a database's `ThreadStore` to
`PersistentHistory.layer` and provide it to `AgentRuntime`:

```mermaid
flowchart LR
  accTitle: Persistent history storage
  accDescr: AgentRuntime uses PersistentHistory, which uses a ThreadStore backed by a database.
  runtime["AgentRuntime"] --> history["PersistentHistory"] --> store["ThreadStore"] --> database["database"]
```

The [SQLite](/storage/sqlite/) and [PostgreSQL](/storage/postgres/) guides show the complete Layer wiring.
Successful Runs are retained; interrupted execution is not resumed. Reuse a thread ID
to continue its conversation. See [Threads](/guide/threads/) for history behavior.

Durable execution also needs a `SubmissionLedger`, registered agents, and a host that
drives recovery and pending work. The [Node.js](/platforms/node/) and
[Cloudflare](/platforms/cloudflare/) hosts assemble those pieces with their storage.
For another host, see [custom durable runtime composition](/guide/run-agents/#assemble-a-custom-durable-runtime).
Storage alone does not start workers or recover unfinished Runs.

## Build another adapter

`@yielded/agent-storage-sql` contains shared SQL implementations for thread history,
submissions, schedules, subscriptions, message delivery, and activity progress.
SQLite and PostgreSQL use this core; Cloudflare reuses the helpers that fit Durable Objects.

An adapter supplies the database-specific initialization, transactions, and error handling
around an Effect `SqlClient`. Start with the [shared SQL package reference](/reference/packages/#effect-agent-storage-sql)
and [storage adapter contracts](/guide/certify-adapters/).
