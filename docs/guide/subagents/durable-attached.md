---
title: Durable attached subagents
description: Choose resumable attached children or scoped helpers inside a durable run.
---

# Durable attached subagents

Run the parent and its child on a durable host:

<<< @/snippets/travel-planner/durable-delegation-host.ts{ts twoslash}

Each `{ agent, model, definitions }` entry registers an agent with the host. Register both the
coordinator and the exact child used by `Research`, so recovery can find them after a restart.
`definitions` contains the code versions; SQLite stores accepted work and recorded results.

Save this as `durable-delegation-host.ts`. It reuses the
[attached example files](./in-memory-attached) and the provider setup from
[`node-agent.ts`](../../platforms/node#create-an-agent).

## Define the delegation

<<< @/snippets/travel-planner/delegation.ts{ts twoslash}

Put `Research.tool` in the parent's toolkit. By default, the host supplies durability; no different
subagent constructor is needed.

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
and budget reservations. Uncertain admission does not launch a replacement child. Register one
current binding per stable Agent ID. Pending delegation operations retain their original replay
contract, while child identity, lineage, and accepted delivery evidence remain unchanged.

Aborting the parent propagates cancellation to its children and joins their terminal outcomes.
Cancellation cannot undo external effects. See [recovery details](../../concepts/durability#attached-subagents)
and [failure handling](../../reference/subagents#handle-failures).

To keep the parent responding while its child runs, use [durable background subagents](./background).

## Ephemeral helpers

For short work that needs no independent recovery, set `execution: "ephemeral"`:

```ts twoslash
import { Subagent } from "effect-agent";
import { ToolExecutionClass } from "effect-agent/durable-step";
import { Toolkit } from "effect/unstable/ai";
import { Summarize } from "./subagent-basics.ts";
// ---cut---
const helper = Subagent.make("summarize", {
  target: Summarize.target,
  execution: "ephemeral",
});
const toolkit = Toolkit.make(helper.tool.annotate(ToolExecutionClass, "readonly"));
```

Provide `Subagent.layer(helper)` with the child's model as usual. Only the parent needs a durable
registration. The child runs in the parent's active Scope, including when the parent is a
background worker. It occupies that parent's execution slot until it returns; cancellation or
ownership loss interrupts it. It has no separate Submission or resumable child lifecycle.

Once committed to the parent's tool history, its result is reused after a restart. Before that
commit, ordinary tool recovery rules apply: `readonly` and `idempotent` helpers may run again;
the default `uncertain` helper waits for resolution. The annotation covers the entire helper,
including its tools and projections. Model usage still costs money on every attempt.

Helpers share the parent's delegation permissions, concurrency limits, and total allowances with
durable children. Each physical attempt reserves its full allowance, including retries. See
[budget accounting](../../reference/subagents#bound-child-work) for the recovery tradeoff.
