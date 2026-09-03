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

For the examples below, also install `effect@4.0.0-rc.111`, `@effect-agent/thread@beta`, and
`@effect/platform-node@4.0.0-rc.111`.
Keep framework packages at one release and use compatible [Effect and provider packages](../guide/getting-started#installation-and-compatibility).

## Configure the host

Use a persistent database path with one live host per SQLite file. Give each replacement host
incarnation a distinct `producerId`.
`workerConcurrency` limits worker loops and defaults to one.
`NodeDurableHost.layerRegistered` checks storage and runs recovery before accepting work.

Pass typed agent registrations to `NodeDurableHost.layerRegistered`. Its runtime computes definition
digests and captures worker services in the Layer's Scope. Node supplies Crypto; provide
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
Use `NodeDurableAgentRuntime.layerRegistered` for custom lifecycle composition with executable
registrations. Its `layerWithBindings` constructor accepts previously compiled bindings; `layer`
constructs an unregistered runtime for admission, recovery, and the generic single-agent APIs.

## Use an Effect Workflow engine {#workflow}

Install `@effect-agent/workflow@beta` and `@effect/sql-sqlite-node@4.0.0-rc.111` to drive
the durable runtime through upstream Effect Workflow. Supply a `WorkflowEngine` Layer,
durable dispatch storage, and a host-owned repair trigger. The Agent definitions, registrations,
and durable recovery rules stay the same when you replace the engine Layer.

This assembly uses `ClusterWorkflowEngine` with `SingleRunner` on one Node process.
`runnerStorage: "memory"` keeps runner bookkeeping in memory; native messages and replies still
persist in SQL. The dispatch store shares that SQL connection. Canonical agent history and the
submission ledger use a separate SQLite file.

```ts twoslash
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node";
import {
  NodeWorkflowRepairTrigger,
  SqlWorkflowDispatchStore,
} from "@effect-agent/platform-node/workflow";
import type { AgentRegistration } from "@effect-agent/thread";
import { WorkflowAgentHost } from "@effect-agent/workflow";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Layer } from "effect";
import { ClusterWorkflowEngine, SingleRunner } from "effect/unstable/cluster";

const workflowDatabase = SqliteClient.layer({ filename: "./workflow.sqlite" });
const engine = ClusterWorkflowEngine.layer.pipe(
  Layer.provide(SingleRunner.layer({ runnerStorage: "memory" })),
);
const infrastructure = Layer.mergeAll(engine, SqlWorkflowDispatchStore.layer).pipe(
  Layer.provide(workflowDatabase),
);

export const workflowHost = <const Entries extends ReadonlyArray<AgentRegistration>>(
  registrations: Entries,
) =>
  WorkflowAgentHost.layer({
    deploymentId: "travel-planner",
    executionConcurrency: 4,
    repairBatchSize: 32,
    dispatchTimeoutMillis: 10_000,
  }).pipe(
    Layer.provide(
      NodeDurableAgentRuntime.layerRegistered(registrations, {
        filename: "./agents.sqlite",
        deploymentId: "travel-planner",
        producerId: "workflow-worker-1",
      }),
    ),
    Layer.provide(infrastructure),
    Layer.provide(NodeWorkflowRepairTrigger.layer({ interval: "1 second" })),
    Layer.provide(NodeCrypto.layer),
  );
```

Provide the remaining model, tool, instruction, and schema services to this Layer, then share it
with the application effects that use `WorkflowAgentHost`. Its inferred types retain
construction errors and application requirements. Acquiring it registers native execution and
starts repair. Do not also start the ordinary `NodeDurableHost` worker loop for this deployment.
The runtime owns executable registrations. The Workflow handler passes only a Thread ID to
`processThreadHead`; it cannot replace captured model or tool services on an execution.
The [compiling example](https://github.com/danieljvdm/effect-agent/blob/main/examples/providers/src/workflow.ts)
accepts the engine as a Layer parameter and retains that engine's errors and requirements.

Keep `deploymentId` identical in both runtime and Workflow host options. The optional
`workflowName` is a stable versioned prefix, defaulting to `effect-agent/Submission/v1`.
The native name appends `/deployment/<length>:<deploymentId>`. Keep one host registration per
deployment, name, and engine. Changing that identity leaves the old dispatch obligations for
their original host to repair.

### Follow and control submissions

| Operation                       | Meaning                                                                       |
| ------------------------------- | ----------------------------------------------------------------------------- |
| `submit(agent, input, options)` | Admit durable work, persist its dispatch intent, and request native execution |
| `awaitSettlement(receipt)`      | Wait for the canonical terminal outcome                                       |
| `observe(receipt)`              | Stream canonical records through the runtime's authorization policy           |
| `submissionStatus(receipt)`     | Read authorized pending or settled status without waiting                     |
| `abort(command)`                | Record authorized abort intent without replacing an existing Settlement       |
| `resolveApproval(command)`      | Record an authorized approval decision for later processing                   |
| `resolveUnknown(command)`       | Record an authorized resolution of uncertain external work                    |

A dispatch error or timeout can occur after admission. Retry the same input with the same
idempotency key; accepted work remains recoverable. A receipt identifies work and grants no
authorization by itself. Interrupting a waiter or observer only detaches that caller.
Use `abort` for cancellation; native Workflow interruption does not implement durable abort.
Approval and unknown-outcome resolutions resume through repair.

### Repair and resource ownership

`NodeWorkflowRepairTrigger` runs repair at startup and at the configured interval within the
host Scope. The shared Workflow host has no internal poll loop. Every host must provide a
trigger that continues after lost hints and process restarts. `host.repair` is also available
for explicit bounded repair.

Each repair pass selects at most `repairBatchSize` accepted submissions and that many dispatch
intents. Each scan and each item has the `dispatchTimeoutMillis` bound. Failed items leave their
obligations intact while other items can advance. Dispatch intents remain until native success
references the matching canonical Settlement, even if the submission ledger already settled.
Admission, dispatch persistence, and native Workflow persistence are separate commits. Never
enclose agent execution in a SQL transaction.

Pending status, an empty processing result, or Workflow suspension is not completion.
Infrastructure failures suspend native execution; repeated repair drives recovery and resumes.
Ordinary tools are not wrapped in Activities and retain the unknown-outcome rules described in
[durability](../concepts/durability).

`executionConcurrency` limits Attempts in this host Layer instance, not across a fleet.
Within that host, only one recovery or processing pass runs for a Thread at a time.
Each Attempt owns its resources and releases ownership and permits before native suspension.
Closing the host Scope stops its repair trigger and closes acquired resources. The SQL assembly
is certified for a single Node process. It makes no multi-runner or Cloudflare Workflow claim.

## Configure runtime services

Pass service layers through `NodeDurableHost.layerRegistered`, `NodeDurableHost.layerStack`,
or `NodeDurableAgentRuntime.layer`:

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
`NodeDurableAgentRuntimeOptions<ContextError, ContextRequirements, AuthorizationError, AuthorizationRequirements>`
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
