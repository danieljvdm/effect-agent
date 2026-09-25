---
title: SQLite
description: Retain completed agent runs in a Node.js SQLite database.
---

# SQLite

Provide `PersistentHistory.layer` with a SQLite store to retain completed runs across Node.js restarts:

```ts twoslash
import { planner } from "./node-agent.ts";
// ---cut---
import { SqliteThreadStore } from "@effect-agent/storage-sqlite";
import { AgentRuntime, PersistentHistory } from "effect-agent";
import { Effect, Layer } from "effect";

const History = PersistentHistory.layer.pipe(
  Layer.provide(SqliteThreadStore.layer({ filename: "./history.sqlite" })),
);

const conversation = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(planner, "Plan a trip to Lisbon");
  return yield* AgentRuntime.run(planner, "Make it cheaper", {
    threadId: first.threadId,
  });
}).pipe(Effect.provide(History));
```

Add the adapter to your existing Effect Agent application:

```sh
bun add @effect-agent/storage-sqlite@beta
```

Here, `planner` is an [agent definition](../guide/agents). Supply its model and tool
services around the program. The storage Layer opens a scoped Node SQLite client
and supplies Crypto. Keep framework packages at one release and use compatible
[Effect packages](../guide/getting-started#installation-and-compatibility).

Keep the database file on persistent storage. Save the returned `threadId` and pass
it to later runs to continue the conversation, including after a process restart.
Provide the history Layer around the complete program or application runtime.

## What is retained

`PersistentHistory.layer` commits each successful run's input and native messages
as one atomic batch before publishing `RunCompleted`. A failure or interruption
before commit leaves none of that run in history. A storage error after commit can
leave the whole run recorded, so inspect history before retrying.

The database retains conversation history across restarts. An interrupted run has
no automatic recovery. See [retained history](../guide/threads#retain-completed-runs)
for the commit policy, concurrency, and history inspection.

## Recover unfinished work

Use [`@effect-agent/platform-node`](../platforms/node) when accepted work must
survive a restart. It assembles SQLite storage, agent registrations, admission,
recovery, and a worker pool. Run one live host per SQLite file.

For custom durable assemblies, `SqliteSubmissionLedger.ledgerLayer(options)`
provides the separate accepted-work ledger. Point it at the same database file as
`SqliteThreadStore.layer(options)` so ownership claims fence the same thread log.

SQLite upgrades supported predecessor formats atomically and rejects incompatible
stored versions. Check the [supported storage upgrades](../guide/operations#adopting-these-contracts)
before adopting a new release.
