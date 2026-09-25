---
title: In-memory
description: Keep conversations across runs in one application Scope.
---

# In-memory

Use `InMemory.layer` from `effect-agent` to share conversation history across runs:

```ts twoslash
import { planner } from "./node-agent.ts";
// ---cut---
import { AgentRuntime, InMemory } from "effect-agent";
import { Effect } from "effect";

const conversation = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(planner, "Plan a trip to Lisbon");
  return yield* AgentRuntime.run(planner, "Make it cheaper", {
    threadId: first.threadId,
  });
}).pipe(Effect.provide(InMemory.layer));
```

Here, `planner` is an [agent definition](../guide/agents). Supply its model and tool
services around the program; see [getting started](../guide/getting-started) for
complete provider setup. No storage package is needed.

## Share the application Scope

Provide the Layer once around the conversation, or use one `ManagedRuntime` for a
long-lived application. A new Layer acquisition creates a separate store. Reuse the
returned `threadId` for follow-ups; omitting it starts a new conversation.

`InMemory.layer` shares conversation history and attached-subagent reservations.
History records complete messages and tool batches as execution advances. If a
later step fails or is interrupted, earlier recorded updates remain available.

State stays available while the application Scope remains open, within the store's
capacity limits. Closing the Scope or losing the process loses that state. See
[in-memory conversations](../guide/threads#in-memory-conversations) for limits and
history inspection.

## Canonical stores for tests and custom assemblies

`@effect-agent/storage-memory` supplies implementations of the canonical
`ThreadStore` and `SubmissionLedger` ports. These are useful for adapter tests and
custom runtime assemblies. They are separate from the conversation store supplied
by `InMemory.layer`; the submission ledger reports itself as non-durable.

`MemoryThreadStoreLive`, imported from
`@effect-agent/storage-memory/memory-thread-store`, provides `ThreadStore` and
requires a platform Crypto Layer. Providing it to `PersistentHistory.layer` uses
the [whole successful-run commit policy](../guide/threads#retain-completed-runs),
while still keeping all records in memory.

For history that survives a restart, choose [SQLite](./sqlite) or [Postgres](./postgres).
To recover unfinished work as well, use a [durable platform](../platforms/).
