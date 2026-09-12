---
title: Durable attached subagents
description: Run attached children on a durable host and recover the same child after a restart.
---

# Durable attached subagents

Run the parent and its child on a durable host:

<<< @/snippets/travel-planner/durable-delegation-host.ts{ts twoslash}

Each `{ agent, model, definitions }` entry registers an agent with the host. Register both the
coordinator and the exact child used by `Research`, so recovery can find them after a restart.
`definitions` contains the code versions; SQLite stores accepted work and recorded results.

Save this as `durable-delegation-host.ts`. It reuses the
[attached example files](./ephemeral-attached) and the provider setup from
[`node-agent.ts`](../../platforms/node#create-an-agent).

## Define the delegation

<<< @/snippets/travel-planner/delegation.ts{ts twoslash}

The declaration is the same for ephemeral and durable attached execution. Put `Research.tool`
in the parent's toolkit. The host supplies durability; no different subagent constructor is needed.

## Start the host

```ts twoslash
import { NodeDurableHost } from "@effect-agent/platform-node";
import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { HostLive } from "./durable-delegation-host.ts";

NodeRuntime.runMain(NodeDurableHost.run.pipe(Effect.provide(HostLive)));
```

Use the [Node.js setup](../../platforms/node) to submit `Coordinator` with `{ city: "Lisbon" }`.
The [Cloudflare host](../../platforms/cloudflare) supports the same attached lifecycle.

## Waiting and recovery

The parent's next model call waits until its current tool batch settles. Children can run
concurrently; the waiting parent releases its execution slot for other work.

After a restart, recovery reconnects to the same child and restores recorded results, policy,
and budget reservations. Uncertain admission does not launch a replacement child. Keep the
registered code versions needed by pending work available.

Aborting the parent propagates cancellation to its children and joins their terminal outcomes.
Cancellation cannot undo external effects. See [recovery details](../../concepts/durability#attached-subagents)
and [failure handling](../../reference/subagents#handle-failures).

To keep the parent responding while its child runs, use [durable background subagents](./background).
