---
title: PostgreSQL
description: Persist thread history and accepted work with an application-owned PostgreSQL client.
---

# PostgreSQL

Connect PostgreSQL storage to your agent using your application's native Effect SQL client:

```ts twoslash
import { planner as agent } from "./node-agent.ts";
import { Identifiers } from "effect-agent";
declare const input: string;
declare const threadId: Identifiers.ThreadId;
// ---cut---
import { PostgresStorage } from "@effect-agent/storage-postgres";
import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { Config, Effect, Layer } from "effect";
import { AgentRuntime, PersistentHistory } from "effect-agent";

const Database = PgClient.layerConfig({
  url: Config.Redacted("DATABASE_URL"),
});

const Persistence = PostgresStorage.layer.pipe(
  Layer.provide(Database),
  Layer.provide(NodeCrypto.layer),
);

const History = PersistentHistory.layer.pipe(Layer.provide(Persistence));

const program = AgentRuntime.run(agent, input, { threadId }).pipe(Effect.provide(History));
```

## Install and connect

```sh
bun add effect-agent@beta @effect-agent/storage-postgres@beta \
  effect@4.0.0-rc.117 @effect/sql-pg@4.0.0-rc.117 @effect/platform-node@4.0.0-rc.117
```

Requires PostgreSQL 16 or newer. Create the database and set `DATABASE_URL` to its connection URL.
Keep framework packages at one release and use compatible
[Effect and model provider packages](../guide/getting-started#installation-and-compatibility).

Here, `agent`, `input`, and `threadId` come from your application.
`PostgresStorage.layer` provides `ThreadStore` and `SubmissionLedger`;
`PersistentHistory.layer` connects the thread store to ordinary `AgentRuntime` calls.
Supply the agent's model and tool Layers at your application boundary. For a server, provide
`History` once around the application or build one `ManagedRuntime` so requests share the pool.
Reuse the same thread ID to continue a conversation, including after a process restart.

## What is retained

This setup commits each successful Run as one atomic batch. It does not recover interrupted
execution. See [retained history](../guide/threads#retain-completed-runs) for commit and
concurrency behavior, and [persistence and durability](../concepts/durability) for recovery.

## Database ownership

The adapter initializes its tables when the Layer opens. Credentials need permission to use
and initialize the selected schema. Tables default to `public`; use
`PostgresStorage.layerWith({ schema: "agent" })` for another namespace. An existing schema avoids
the need for database-wide `CREATE` permission. Storage qualifies its tables without changing
the client's search path, codecs, or application query transformations.

Call storage writes and snapshot reads outside `sql.withTransaction`: the adapter owns
its transactions and rejects nesting. It also rejects incompatible stored versions.

For individual stores, configuration, and error details, see the
[PostgreSQL package reference](../reference/packages#effect-agent-storage-postgres).
For durable execution with PostgreSQL, supply these stores to a
[custom durable runtime](../guide/run-agents#assemble-a-custom-durable-runtime) and provide its
recovery driver. The existing Node.js host owns SQLite storage.
