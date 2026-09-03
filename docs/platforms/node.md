---
title: Node.js
description: Run durable agents on Node.js with SQLite.
---

# Node.js

`@effect-agent/platform-node` stores thread history and pending work in SQLite.
A bounded worker pool executes registered agents and recovers work after a restart.

## Install

```sh
bun add @effect-agent/platform-node@beta
```

For the examples below, also install `effect@4.0.0-rc.112`, `@effect-agent/thread@beta`, and
`@effect/platform-node@4.0.0-rc.112`.
Keep framework packages at one release and use compatible [Effect and provider packages](../guide/getting-started#installation-and-compatibility).

## Configure the host

Use a persistent database path with one live host per SQLite file. Give each replacement host
incarnation a distinct `producerId`.
`workerConcurrency` limits worker loops and defaults to one.
`NodeDurableHost.layerRegistered` checks storage and runs recovery before accepting work.

Pass typed agent registrations to `NodeDurableHost.layerRegistered`. It computes definition
digests and captures worker services in the host Layer's Scope. Node supplies Crypto; provide
model clients, tool handlers, instruction services, and schema services to the returned Layer.
Keep the concrete registration tuple inferred so its requirements remain visible.
Use `NodeDurableHost.layerStack` when you already have `ResolvedBinding` values from
`compileRegistrations` and own their application Scope yourself.

Registrations include JSON descriptions and versions for the agent, model, and toolkit.
Version tool implementations and other behavior JSON cannot represent.
Submission digests must match the registered definitions. Keep matching bindings available
until their work settles; an empty registration cannot execute work.

Use `digestDefinitions` to compute submission digests.
`DurableWorkerBinding.make(agent, digests)` accepts precomputed digests.

## Run the workers

```ts twoslash
import { NodeDurableHost } from "@effect-agent/platform-node";
import type { AgentRegistration } from "@effect-agent/thread";
import { Effect } from "effect";

export const workers = <const Entries extends ReadonlyArray<AgentRegistration>>(
  registrations: Entries,
) =>
  Effect.gen(function* () {
    const host = yield* NodeDurableHost;
    yield* host.runResolvedWorkers;
  }).pipe(
    Effect.provide(
      NodeDurableHost.layerRegistered(registrations, {
        filename: "./agents.sqlite",
        deploymentId: "travel-planner",
        producerId: "worker-1",
        workerConcurrency: 4,
      }),
    ),
    Effect.scoped,
  );
```

Provide the remaining application services to `workers(registrations)`, then call
`NodeRuntime.runMain` at the process entry point. Creating the host alone does not start workers.
If the process also serves requests, share one host Layer between both effects.
Use `NodeDurableRuntime.layer` for custom lifecycle composition.

## Configure runtime services

Pass service layers through `NodeDurableHost.layerRegistered`, `NodeDurableHost.layerStack`,
or `NodeDurableRuntime.layer`:

| Option              | Service                 | Default                                                     |
| ------------------- | ----------------------- | ----------------------------------------------------------- |
| `runContext`        | `RunContextPreparation` | No prompt transform; use the available or default compactor |
| `toolAuthorization` | `RunToolAuthorization`  | Allow all tool calls                                        |

Add these options to the host assembly above. Use
`{ runContext: RunContextLive }` for [prompt preparation or compaction](../guide/context-management),
or `{ toolAuthorization: SearchOnlyLive }` for a [tool policy](../guide/tools#authorize-tool-calls).
Pass both properties when configuring both services.

The assembled layer retains each extension's construction errors and application dependencies
in its error and requirement types. The host supplies `Crypto.Crypto`. Provide the remaining
dependencies through ordinary `Layer.provide` composition before running the application.
Context preparation and authorization can each be configured independently.

Let `layer` or `layerStack` infer the types from your options. When annotating reusable options,
`NodeDurableRuntimeOptions<ContextError, ContextRequirements, AuthorizationError, AuthorizationRequirements>`
preserves the two layers' construction contracts.

The runtime captures services when the host layer is acquired. Keep their resources alive for
its Scope. Providing replacements around a later worker call does not change the captured services.
`toolFailureObserver` configures [recovered tool failure reporting](../guide/run-agents#observe-recovered-tool-failures).

## Submit and follow work

1. Authenticate the caller and call `host.submit(agent, input, options)`.
   Supply the thread ID, principal, idempotency key, and definition digests.
2. Return the receipt after admission.
3. Await completion with `host.awaitSettlement(receipt)`, or stream records with `host.observe(receipt)`.

Reuse the idempotency key when retrying the same request. Different input under that key fails
with an admission conflict.

## Shutdown and recovery

Closing the host's Scope stops admission, releases ownership, and closes SQLite.
After a crash, replacement workers reclaim work when its lease expires.
Unconfirmed external tool outcomes require reconciliation or authorized resolution before replay.

Inspect `host.startupRecovery`, `host.explain`, `host.verify`, and `host.scanObligations`
for recovery status. See [operations](../guide/operations) for approvals, schedules, and backups.
