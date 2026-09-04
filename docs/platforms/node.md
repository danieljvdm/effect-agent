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

To drive the durable runtime through an injected `WorkflowEngine`, follow the
[Effect Workflows guide](../guide/workflows). Its [Node.js setup](../guide/workflows#node)
uses SQLite and a single-process Cluster runner.

## Custom runtime composition

Use `NodeDurableAgentRuntime.layerRegistered` when you own execution, as in the
[Workflow assembly](../guide/workflows#node). It captures registrations and acquires storage without starting workers.
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
