---
title: Effect Workflows
description: Compose durable agents inside Effect Workflows and recover pending work after restarts.
---

# Effect Workflows

`@effect-agent/workflow` drives the durable agent runtime through upstream Effect Workflow.
Supply a `WorkflowEngine` Layer, durable dispatch storage, and a host-owned repair trigger.
The agent definitions, registrations, and durable recovery rules stay the same when you
replace the engine Layer.

The shared host has no Node.js or Cloudflare dependency. Platform adapters supply storage
and repair scheduling. The [Node.js setup](#node) below uses SQLite and a single-process
Cluster runner.

## Install

```sh
bun add @effect-agent/workflow@beta
```

Install the runtime and platform adapters for your host separately. Keep framework packages
at one release and use compatible [Effect and provider packages](./getting-started#installation-and-compatibility).

## Compose agents in a Workflow

Author workflows with Effect's `Workflow.make`, `toLayer`, and `execute`.
`AgentWorkflow.execute` runs a registered Agent as an Effect inside the handler and returns
its Schema-decoded output. A pending Agent suspends the handler through Effect's
`DurableDeferred`; approval resolution and settlement resume it without keeping a polling
fiber alive in the parent.

```ts twoslash
import { Agent } from "@effect-agent/core";
import { AgentWorkflow } from "@effect-agent/workflow";
import { Schema } from "effect";
import { Toolkit } from "effect/unstable/ai";
import { Workflow } from "effect/unstable/workflow";

const triage = Agent.make("triage", {
  input: Schema.String,
  output: Schema.Struct({ severity: Schema.Literals(["low", "high", "critical"]) }),
  instructions: "Classify the severity of the bug report.",
  toolkit: Toolkit.empty,
});

const Review = Workflow.make("Review", {
  payload: { issueId: Schema.String, report: Schema.String },
  success: triage.output,
  error: AgentWorkflow.Error,
  idempotencyKey: ({ issueId }) => issueId,
});

export const ReviewLive = Review.toLayer(({ report }) =>
  AgentWorkflow.execute(triage, report, { name: "triage" }),
);

export const review = Review.execute({ issueId: "123", report: "Login is broken" });
```

Register `triage` and its model in the durable runtime used by your host. Supply the resulting host Layer
to `ReviewLive` with `Layer.provideMerge`, then provide that Layer to `review`. Share one
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

## Configure the host

`WorkflowAgentHost.layer(options)` consumes these services through Layers:

| Service                                                               | Responsibility                                                                   |
| --------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `DurableAgentRuntime`, `SubmissionLedger`, and `DurableRuntimeConfig` | Agent execution, admission, canonical history, recovery, and deployment identity |
| `WorkflowEngine`                                                      | Native Workflow execution and persistence                                        |
| `WorkflowDispatchStore`                                               | Durable dispatch intents retained until completion is verified                   |
| `WorkflowRepairTrigger`                                               | Startup and repeated repair after lost hints or host restarts                    |

The runtime Layer owns executable registrations and their model, tool, instruction, and
schema services. Supply those application services and the host's `Crypto` service through
ordinary Layer composition. The Workflow handler passes only a Thread ID to
`processThreadHead`; it cannot replace captured model or tool services on an execution.

Keep `deploymentId` identical in both runtime and Workflow host options. The optional
`workflowName` is a stable versioned prefix, defaulting to `effect-agent/Submission/v1`.
The native name appends `/deployment/<length>:<deploymentId>`. Keep one host registration per
deployment, name, and engine. Changing that identity leaves the old dispatch obligations for
their original host to repair.

The required `principal` is application-owned authority for `AgentWorkflow.execute`, never
model-supplied input. Model and tool services remain captured by runtime registration; handler
services cannot replace them. Input encoding and output decoding requirements remain visible in
the execution Effect. Each result read rechecks authorization, including on workflow replay.

## Follow and control submissions

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

## Repair and resource ownership

The shared Workflow host has no internal poll loop. Every host must provide a
`WorkflowRepairTrigger` that invokes repair at startup and continues after lost hints and
host restarts. It must stop invoking repair when its Scope closes. `host.repair` is also
available for explicit bounded repair.

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
Closing the host Scope stops its repair trigger and closes acquired resources.

## Node.js with SQLite {#node}

Install `@effect-agent/platform-node@beta`, `@effect-agent/core@beta`,
`@effect-agent/thread@beta`, `@effect/ai-openai@4.0.0-rc.112`,
`@effect/platform-node@4.0.0-rc.112`, and `@effect/sql-sqlite-node@4.0.0-rc.112` alongside
`effect@4.0.0-rc.112` and the Workflow package.

This example reuses `node-agent.ts` from the [Node.js guide](../platforms/node#create-an-agent),
including its model client and registration versions.

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
`NodeWorkflowRepairTrigger` runs repair at startup and at the configured interval within the
host Scope.

The [compiling example](https://github.com/danieljvdm/effect-agent/blob/main/examples/providers/src/workflow.ts)
accepts the engine as a Layer parameter and retains that engine's errors and requirements.
See the [Node.js guide](../platforms/node#start-the-host) for registration and storage
configuration, and [runtime services](../platforms/node#configure-runtime-services) for context
preparation and tool authorization.

This SQL assembly is certified for a single Node process. Its runner configuration and
concurrency limit do not establish multi-runner support.
