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

For the examples below, also install `effect@4.0.0-rc.112`, `@effect-agent/core@beta`,
`@effect-agent/thread@beta`, `@effect/ai-openai@4.0.0-rc.112`, and `@effect/platform-node@4.0.0-rc.112`.
Keep framework packages at one release and use compatible [Effect and provider packages](../guide/getting-started#installation-and-compatibility).

## Create an agent

Save this as `node-agent.ts`. The model's client reads `OPENAI_API_KEY` from the environment.
The version declarations identify the agent, model, and tools used by accepted work.

<<< @/snippets/travel-planner/node-agent.ts{ts twoslash}

## Start the host

Use a persistent database path with one live host per SQLite file. Give each replacement host
incarnation a distinct `producerId`.
`workerConcurrency` limits worker loops and defaults to one.
`NodeDurableHost.layer` checks storage, recovers pending work, and starts the worker pool when
the Layer is acquired. Save this as `node-host.ts`, replacing `producerId` for each process start:

<<< @/snippets/travel-planner/node-host.ts{ts twoslash}

TypeScript infers the registration's model, tool, instruction, and schema service requirements.
Provide those services to the Layer, as `OpenAiLive` does here. Node supplies Crypto.

Save this as `node-main.ts` and run it with Node's TypeScript transform support:

<<< @/snippets/travel-planner/node-main.ts{ts twoslash}

```sh
node --experimental-transform-types node-main.ts
```

`NodeDurableHost.run` observes the existing pool; calling it again does not start more workers.
If a worker fails, admission closes and `run` fails with the original typed error or defect.
`NodeRuntime.runMain` then closes the host. Use `run` rather than `Layer.launch(HostLive)`,
which does not observe background worker failures. Interrupting only an observer leaves the
pool running; closing the host's Scope closes admission, stops and joins workers, releases
ownership, and closes storage and application services.

If the process also serves requests, race the server Effect with `NodeDurableHost.run` using
`Effect.raceFirst`, and provide the shared `HostLive` to that combined Effect. A worker failure
then stops the server too. Creating two separate host Layers for the same SQLite file is unsupported.

## Use an Effect Workflow engine {#workflow}

Author workflows with Effect's `Workflow.make`, `toLayer`, and `execute`.
`AgentWorkflow.execute` runs a registered Agent as an Effect inside the handler and returns
its Schema-decoded output. A pending Agent suspends the handler through Effect's
`DurableDeferred`; approval resolution and settlement resume it without keeping a polling
fiber alive in the parent.

```ts twoslash
import { AgentWorkflow } from "@effect-agent/workflow";
import { Schema } from "effect";
import { Workflow } from "effect/unstable/workflow";

import { planner } from "./node-agent.ts";

const PlanTrip = Workflow.make("PlanTrip", {
  payload: { tripId: Schema.String, request: Schema.String },
  success: planner.output,
  error: AgentWorkflow.Error,
  idempotencyKey: ({ tripId }) => tripId,
});

export const PlanTripLive = PlanTrip.toLayer(({ request }) =>
  AgentWorkflow.execute(planner, request, { name: "plan-itinerary" }),
);

export const trip = PlanTrip.execute({ tripId: "kyoto", request: "Three days in Kyoto." });
```

Register `planner` and its model in the durable runtime below. Supply the resulting host Layer
to `PlanTripLive` with `Layer.provideMerge`, then provide that Layer to `trip`. Share one
WorkflowEngine Layer between the parent and host; a mismatched engine fails before admission.
Multi-step handlers use ordinary `Effect.gen` and bounded `Effect.all`.

The step `name` must be nonempty, stable across replays, and unique within a parent execution.
Use stable item IDs for repeated calls in a loop. The parent workflow identity, step name,
deployment, and configured principal determine a private Thread and submission key. Reusing a
step with changed input, Agent identity, or registered version declarations fails with an admission
conflict rather than silently starting new work. Registered admission requires exactly one
version for that Agent identity and the exact Agent Definition instance passed to registration.
A same-ID copy or replacement fails with `BindingUnavailable` before input encoding or admission,
including on replay. Import the same definition into registration and the workflow handler;
there is no object identity persisted across restarts. Explicit `host.submit` remains available
for versioned routing.

`AgentWorkflow.Error` is the Schema for the exact typed failure channel. Failed or aborted
Agent settlements become `WorkflowExecutionFailure`; invalid output becomes `AgentOutputError`.
Admission, authorization, and dispatch failures retain their original tags. Infrastructure
failure can occur after admission; retries with the same step identity reconnect to the accepted
work. Choose retry behavior with ordinary Effect combinators. Defects in the underlying durable
driver retain its existing suspension and repair behavior.

Parent interruption, timeout, or shutdown detaches the parent; it does not abort accepted Agent
work. Use the host's authorized `abort` command to cancel the Agent. Native compensation does
not undo external tool effects. Ordinary uncertain tools still require explicit resolution.

### Assemble the host

Install `@effect-agent/workflow@beta` and `@effect/sql-sqlite-node@4.0.0-rc.112` to drive
the durable runtime through upstream Effect Workflow. Supply a `WorkflowEngine` Layer,
durable dispatch storage, and a host-owned repair trigger. The Agent definitions, registrations,
and durable recovery rules stay the same when you replace the engine Layer.

This assembly uses `ClusterWorkflowEngine` with `SingleRunner` on one Node process.
`runnerStorage: "memory"` keeps runner bookkeeping in memory; native messages and replies still
persist in SQL. The dispatch store shares that SQL connection. Canonical agent history and the
submission ledger use a separate SQLite file.

```ts twoslash
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import {
  NodeWorkflowRepairTrigger,
  SqlWorkflowDispatchStore,
} from "@effect-agent/platform-node/NodeWorkflow";
import { WorkflowAgentHost } from "@effect-agent/workflow/WorkflowAgentHost";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Layer } from "effect";
import { ClusterWorkflowEngine, SingleRunner } from "effect/unstable/cluster";

import { definitions, ModelLive, OpenAiLive, planner } from "./node-agent.ts";

const workflowDatabase = SqliteClient.layer({ filename: "./workflow.sqlite" });
const engine = ClusterWorkflowEngine.layer.pipe(
  Layer.provide(SingleRunner.layer({ runnerStorage: "memory" })),
);
const infrastructure = Layer.mergeAll(engine, SqlWorkflowDispatchStore.layer).pipe(
  Layer.provide(workflowDatabase),
);

export const WorkflowLive = WorkflowAgentHost.layer({
  deploymentId: "travel-planner",
  principal: "travel-planner-service",
  executionConcurrency: 4,
  repairBatchSize: 32,
  dispatchTimeoutMillis: 10_000,
}).pipe(
  Layer.provide(
    NodeDurableAgentRuntime.layerRegistered([{ agent: planner, model: ModelLive, definitions }], {
      filename: "./agents.sqlite",
      deploymentId: "travel-planner",
      producerId: "workflow-worker-1",
    }),
  ),
  Layer.provideMerge(infrastructure),
  Layer.provide(NodeWorkflowRepairTrigger.layer({ interval: "1 second" })),
  Layer.provide(NodeCrypto.layer),
  Layer.provide(OpenAiLive),
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

The required `principal` is application-owned authority for `AgentWorkflow.execute`, never
model-supplied input. Model and tool services remain captured by runtime registration; handler
services cannot replace them. Input encoding and output decoding requirements remain visible in
the execution Effect. Each result read rechecks authorization, including on workflow replay.

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
For `AgentWorkflow.execute`, the intent also retains the parent's durable completion token.
Repair delivers the canonical reference through `DurableDeferred` before removing the intent.
Custom stores must atomically attach one token, preserve it on subsequent `put` calls, return
the retained intent, and compare the entire intent before removal. A stale cleanup must fail
instead of erasing a newly attached notification obligation.
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

## Custom runtime composition

Use `NodeDurableAgentRuntime.layerRegistered` when you own execution, as in the Workflow
assembly above. It captures registrations and acquires storage without starting workers.
`layerWithBindings` accepts precompiled `ResolvedBinding` values whose application Scope you own;
`layer` constructs an unregistered runtime for explicit admission and execution.

The service class's existing `NodeDurableHost.layerRegistered`, `layerStack`, and `layer`
constructors remain available for manually managed hosts. Import the class from
`@effect-agent/platform-node/NodeDurableHost` when using these APIs; their workers start only
when you run `host.runResolvedWorkers`. The module-level `NodeDurableHost.layer` shown above
owns worker startup and is the default for an application.

Registrations carry application version declarations. Update them when behavior changes,
including tool implementations that JSON cannot represent, and keep matching bindings available
until their work settles. `digestDefinitions` computes the digests for explicit submissions;
`DurableWorkerBinding.make(agent, digests)` accepts precomputed digests.

## Configure runtime services

Pass service layers in the options to `NodeDurableHost.layer` or `NodeDurableAgentRuntime.layer`:

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
