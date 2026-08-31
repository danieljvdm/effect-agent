---
title: Node.js
description: Run durable agents in a Node.js process with SQLite storage.
---

# Node.js

`@effect-agent/platform-node` runs durable agents in a Node.js process. It stores the
Conversation Log and Submission Ledger in one SQLite database and runs a bounded pool of workers.

## Install

```sh
npm install --save-exact @effect-agent/platform-node@beta @effect-agent/session@beta @effect/platform-node@4.0.0-rc.111 effect@4.0.0-rc.111
```

Keep framework packages on the same release, including the packages used to define your Agent.
See [installation and compatibility](../guide/getting-started#installation-and-compatibility)
for the alpha version policy and provider dependencies.

## Configure the host

`NodeDurableHost.layerStack` opens the database, checks storage compatibility, and runs recovery
before accepting new submissions. Pass the worker Bindings your application has registered:

```ts twoslash
import { NodeDurableHost } from "@effect-agent/platform-node";
import type { ResolvedBinding } from "@effect-agent/session";

export const hostLayer = (bindings: ReadonlyArray<ResolvedBinding>) =>
  NodeDurableHost.layerStack({
    filename: "./agents.sqlite",
    deploymentId: "travel-planner",
    producerId: "worker-1",
    workerConcurrency: 4,
    bindings,
  });
```

Use a persistent path for `filename` and a distinct `producerId` for each host sharing that
database. `workerConcurrency` bounds simultaneous worker loops; it defaults to one.

Use `compileRegistrations` from `@effect-agent/session` to turn Agent Bindings and version
declarations into `ResolvedBinding` values. Provide Crypto, the Agent's provider client, and
Tool-handler Layers while compiling them. The registration captures those services for later execution.
Keep those Layers in the same application Scope as the host.

Each registration's `definitions` contains application-owned JSON descriptions of the Agent,
Model, and Toolkit. Include version information for
behavior that JSON cannot represent, such as Tool implementations. Use the same digests when
submitting work and keep the matching Bindings available while work is outstanding. An empty
registration cannot execute submissions.

The submitter computes those digests with `digestDefinitions` over the same declarations.
`DurableWorkerBinding.make(agent, digests)` remains available when the digests are already computed.

## Run the workers

Constructing the host does not start its worker loops. Run `host.runResolvedWorkers` in the
Scope that owns your application:

```ts twoslash
import { NodeDurableHost } from "@effect-agent/platform-node";
import type { ResolvedBinding } from "@effect-agent/session";
import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";

export const startWorkers = (bindings: ReadonlyArray<ResolvedBinding>) =>
  Effect.gen(function* () {
    const host = yield* NodeDurableHost;
    yield* host.runResolvedWorkers;
  }).pipe(
    Effect.provide(
      NodeDurableHost.layerStack({
        filename: "./agents.sqlite",
        deploymentId: "travel-planner",
        producerId: "worker-1",
        workerConcurrency: 4,
        bindings,
      }),
    ),
    Effect.scoped,
    NodeRuntime.runMain,
  );
```

Call `startWorkers` with the registered Bindings at your process entry point. For an application
that also serves requests, share one host Layer between the server and worker effects.
`NodeDurableRuntime.layer` exposes the lower-level runtime assembly when you need to compose
the lifecycle yourself.

## Submit and follow work

From an Effect with access to `NodeDurableHost`:

1. Call `host.submit(agent, input, options)`. Options include a Conversation ID, principal,
   idempotency key, and the registered definition digests.
2. Return the Receipt to the caller. It acknowledges durable admission, not completion.
3. Use `host.awaitSettlement(receipt)` for the terminal outcome, or `host.observe(receipt)`
   to stream canonical records.

Keep the idempotency key when retrying the same request. Reusing it with different input fails
with an admission conflict. Authenticate callers before granting access to the host service.

## Shutdown and recovery

Closing the host's Scope closes admission, releases tracked ownership, and closes SQLite.
After a forced termination, replacement workers reclaim work after its ownership lease expires.
Unknown external Tool outcomes require reconciliation or an authorized resolution; recovery
does not blindly execute those calls again.

`host.startupRecovery` reports what startup repaired, deferred, or left waiting for resolution.
Use `host.explain`, `host.verify`, and `host.scanObligations` to inspect outstanding work.
The [operations guide](../guide/operations) covers recovery, approvals, scheduled input,
subscriptions, and backups.
