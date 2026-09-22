---
title: Package map
description: Choose packages, adapters, and providers for your application.
---

# Package map

Start with `effect-agent@beta` for agent definitions, conversations, execution, and durability.
Install storage, platform, sandbox execution, and testing packages as needed.

Keep all framework packages at the same exact release. They require `effect@^4.0.0-rc.117`;
this repository tests Effect and its OpenAI/Anthropic providers at `4.0.0-rc.117`.
The Cloudflare platform requires `effect@^4.0.0-rc.117` and `effect-cf@^0.44.1`.
Before 1.0, APIs and stored data may change without a migration path.

## Public imports

Prefer named namespace imports from package roots in application code and examples.
Namespaces use PascalCase; direct module paths use kebab-case. Agent definitions, execution,
capabilities, and durability live in one package:

```ts twoslash
import { Agent, AgentRuntime } from "effect-agent";

Agent.make;
AgentRuntime.run;
```

The same convention applies to adapters:

```ts twoslash
import { NodeDurableHost } from "@effect-agent/platform-node";

NodeDurableHost.layer;
```

For direct module access or lazy-loading boundaries, the corresponding imports are:

```ts
import * as Agent from "effect-agent/agent";
import * as AgentRuntime from "effect-agent/agent-runtime";
import * as NodeDurableHost from "@effect-agent/platform-node/node-durable-host";
```

Both forms support tree shaking. Use direct module paths at lazy-loading boundaries: mixing a
static root import with a dynamic import of that same root can pull the runtime into the initial
bundle. Also use dedicated subpaths for optional adapters and helpers intended for another
runtime, such as the Node-safe Cloudflare AI Gateway helper. The Cloudflare package root
includes Workers-specific modules. Provider, storage, platform, and testing packages remain
separate installs.

`Agent.make` and `AgentRuntime.run` have the same call shape through either import form.
Use direct imports for individual declarations, including services and Schema values, instead
of importing a namespace when only its service key is needed:

```ts
import { IdGenerator } from "effect-agent/id-generator";
import { CommandDrainPolicy, RunSchedulingOverride } from "effect-agent/run-options";
```

Root imports name module namespaces. For example, root `AgentPolicy` exposes its Schema class as
`AgentPolicy.AgentPolicy`; a named import from `effect-agent/agent-policy` selects that class
directly. Ordinary agent definitions can pass a plain `policy` object to `Agent.make`, which
validates it and fills defaults.

Operations are available directly on their module namespace: `Subagent.layer`,
`ThreadHistory.layer`, and `IdGenerator.layer`. Service keys remain inside those modules,
for example `IdGenerator.IdGenerator` when supplying a custom generator.

### Model requirements

Provide a native Effect model with `Effect.provide(model)` around an agent Run, or
`Layer.provide(model)` around `Subagent.layer(delegation)`. The [agent guide](../guide/agents#provide-native-model-services)
shows this default composition. [AutoModel](./decision-models#automodel) uses the same API.

For an explicit reusable pairing, `Agent.withModel(definition, model)` returns an optional Agent
Binding. `Subagent.layer(delegation, model)` also accepts an explicit model override.
Durable registration uses `{ agent: definition, model, definitions: versions }` so the host owns
each agent's model and version declarations; an existing Binding is also accepted.

### In-memory defaults

Import `InMemory` from `effect-agent`, or use `import * as InMemory from "effect-agent/in-memory"`.
When upgrading, replace `Ephemeral` and `effect-agent/ephemeral` imports with these names.

`InMemory.layer` supplies in-memory conversation history and a shared subagent reservation ledger.
Provide it once around the parent program and all child handler Layers. Runs with the same Thread
ID retain their conversation for that application Scope; independent builds have independent state.
Complete history updates remain after a failed or interrupted Run. Scope closure or process loss
releases the state; this layer provides no crash recovery. See [in-memory conversations](../guide/threads#in-memory-conversations)
for limits and a follow-up example.

Runtime IDs have an overridable default; no ID Layer is required. Context preparation is also
optional. `InMemory.layer` preserves custom IDs and context preparation supplied by the caller.
Models, tool handlers, credentials, and durable storage remain explicit application choices.

For storage-backed history, provide `PersistentHistory.layer` with a store and, when using subagents,
one shared `SubagentReservationsMemoryLive` instead of `InMemory.layer`. Durable hosts select
their own history and reservation services.

When upgrading, remove routine `IdGenerator.layer` provisions and `IdGenerator` from service
requirement unions. The key is now a `Context.Reference`; custom `Layer.succeed`, `Layer.effect`,
and `Effect.provideService` overrides still work. To explicitly reset an override to the default,
use the module-level `layer` export from `effect-agent/id-generator`.

Use direct module paths when you need an individual module:

| Module or declarations                           | Owning module                        |
| ------------------------------------------------ | ------------------------------------ |
| Agent constructors and inferred types            | `effect-agent/agent`                 |
| Recall composition, sources, and outcomes        | `effect-agent/memory`                |
| Memory passages and recall limits                | `effect-agent/memory-reference`      |
| Memory reader/writer contracts                   | `effect-agent/memory-store`          |
| Remembering checkpoints and persistence contract | `effect-agent/remembering-store`     |
| Durable admission and finite remembering passes  | `effect-agent/remembering`           |
| `MemoryAccess`, `revalidateMemoryLookup`         | `effect-agent/memory-revalidation`   |
| Semantic index contracts and errors              | `effect-agent/semantic-memory-index` |
| Delegation contracts and reservation amounts     | `effect-agent/subagent-contract`     |
| Runtime operations and inferred failures         | `effect-agent/agent-runtime`         |
| Native tool selection schemas and annotations    | `effect-agent/tool-exposure`         |
| Host tool visibility and eligible catalogue      | `effect-agent/tool-exposure`         |
| Bounded native and Code Mode discovery           | `effect-agent/tool-discovery`        |
| Compactor service                                | `effect-agent/context-compactor`     |
| Command-drain, scheduling, and run options       | `effect-agent/run-options`           |
| Subagent authoring and handlers                  | `effect-agent/subagent`              |
| Semantic indexing/query implementation           | `effect-agent/semantic-memory`       |

Flat root imports of individual declarations are removed. Import those declarations from the
modules above, or use the root module namespace. `CommandDrainPolicy` and
`RunSchedulingOverride` each expose a Schema and its inferred type from `RunOptions`.
Use `MemoryThreadStoreLive` from `@effect-agent/storage-memory/memory-thread-store` in place
of the removed `MemoryStorageLive` alias. SQLite memory readers and writers come directly from
`effect-agent/sql-memory-store`.

The old `/history`, `/durability`, and `/testing` aggregation paths are removed. Use the
canonical modules below, including `/testing/module` for test controls and conformance suites.
Browser adapters, fixtures, and other specialized paths use the same kebab-case convention.
Unlisted source files and implementation directories are private.

The public API does not export `initialCompactionState`, `buildCompactedView`,
`COMPACTION_INSTRUCTION`, `isContextOverflowMessage`, `formatRunStatus`, or `RunStatusView`.
These are interpreter details.
Use the `ContextCompactor` service to customize compaction and `AgentPolicy.runStatus` to configure status
messages. Token estimators and the `ContextCompactionState` type remain public for compactor
implementations.

## Find a capability {#capability-inventory}

| Need                                       | Guide                                                             | Your application supplies                                                |
| ------------------------------------------ | ----------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Run or stream an agent                     | [Execution](../guide/run-agents)                                  | Model, tool handlers, history policy                                     |
| Discover a large registered tool catalogue | [Progressive discovery](../guide/tools#progressive-discovery)     | Native toolkit, grouping metadata, optional search and visibility policy |
| Retain completed threads                   | [History](../guide/threads#retain-completed-runs)                 | Store and thread IDs                                                     |
| Recover work after a crash                 | [Durability](../concepts/durability)                              | Registered agents, workers, storage, authorization                       |
| Drive durable work with Effect Workflow    | [Effect Workflows](../guide/workflows)                            | Workflow engine, dispatch store, repair trigger                          |
| Prune, summarize, or roll over context     | [Context management](../guide/context-management)                 | Context limits and compaction policy                                     |
| Search prior context windows               | [Context windows](../guide/context-management#context-windows)    | Authorized ThreadStore or ContextHistory adapter                         |
| Keep working notes across windows          | [Context windows](../guide/context-management#context-windows)    | Memory document identity, reader, writer                                 |
| Recall application-owned sources           | [Context management](../guide/context-management#recall-memory)   | Readable passages, provenance, query policy                              |
| Remember in the background                 | [Remembering](../guide/context-management#background-remembering) | Durable jobs, source policy, extraction, merging and cleanup             |
| Require approval or limit spending         | [Run hooks](../guide/run-agents#operational-hooks)                | Approval policy, budget hooks, cost estimates                            |
| Delegate to another agent                  | [Subagents](../guide/subagents)                                   | Targets, bindings, permissions, budgets                                  |
| Schedule new input                         | [Scheduling](../guide/operations#scheduled-input)                 | Owner policy, registered inputs, driver                                  |
| React to external events                   | [Subscriptions](../guide/operations#event-subscriptions)          | Authenticated source, preparation, authorization                         |
| Run generated JavaScript                   | [Code Mode](../guide/code-mode)                                   | Authorized tools and an isolated executor                                |
| Run trusted local commands                 | [Sandbox execution](../guide/sandbox)                             | Executable, environment, output and time limits                          |
| Capture, crawl, or interact with pages     | [Browser tools](../guide/browser)                                 | Browser binding or credentials, target policy                            |
| Search the web                             | [Web search](../guide/tools#web-search)                           | Native search tool and search-model Layer                                |
| Use Cloudflare AI Gateway                  | [AI Gateway](../platforms/cloudflare#ai-gateway)                  | Account, gateway, credentials, upstream Effect client                    |
| Call tools on an MCP server                | [MCP servers](../guide/tools#mcp)                                 | Transport, `HttpClient` or process spawner, bounds                       |

### Limits and unsupported features {#compaction-and-unsupported-capabilities}

[MCP servers](../guide/tools#mcp) connect through `McpClient.layer` over Streamable HTTP or stdio;
`connectMcp` bounds discovery and returns dynamic tools. Server-initiated sampling, elicitation,
resources, and prompts are not served.

[Background subagents](../guide/subagents/background) and
[bounded nested delegation](./subagents#bound-nested-delegation) have public APIs.
Subagent handoff, runtime Skills, a framework-owned memory extraction or sharing policy,
arbitrary Thread metadata, and dynamic Turn Plans have no public APIs.
Applications own domain state. `Memory.recall` reads bounded passages from sources they select; it
does not store them. Thread history and compaction summaries do not replace application state.

Automatic compaction uses `ContextCompactor`. The separate
[artifact utilities](../guide/context-management#explicit-compaction-artifacts) validate and apply
application-managed summaries; they do not run or persist automatically.

Scheduling and subscription ownership does not isolate thread storage.
Enforce [storage separation and authorization](../guide/operations#authorization-and-isolation)
in your host.

## Packages

### `@effect-agent/ai-decision` {#decision-models}

Thread-owned automatic model selection, using the native Effect `DecisionModel` service.
The package depends only on Effect and exports `AutoModel`. Import ordinary assessments
from `effect/unstable/ai` using `Decision` and `DecisionModel`.
[`AutoModel`](./decision-models#automodel) selects a native model from described profiles on each thread's
first turn, including new subagents. A shared selection store retains choices across follow-ups.

Start with the [decision guide](../guide/tools#decision-transitions), then use the
[API reference](./decision-models) for options and results.

### `@effect/ai-typesafe` (upstream) {#typesafe-ai}

The Jev adapter: `TypeSafeDecisionModel` supplies the shared decision service, while
`TypeSafeClient` and `TypeSafeSchema` expose the native choice, score, and noul API.
This upstream Effect provider replaces `@effect-agent/ai-typesafe` and uses an Effect HttpClient.

See [client configuration](./decision-models#typesafe-client) or wrap an assessment in a
[native Effect AI tool](../guide/tools#typesafe-evaluations).

### `effect-agent` {#effect-agent-umbrella}

Agent definitions, schemas, execution, streaming, policies, subagents, memory capabilities,
MCP, durable execution, and platform-neutral sandbox contracts. It has no database driver or platform runtime dependency.
Start with `Agent`, `AgentRuntime`, and `InMemory.layer`.

`InMemory.layer` retains in-memory conversation history and shared attached-subagent reservations.
For storage-backed history, use the root namespace `PersistentHistory.layer`.
Models, provider clients, credentials, tool handlers, and durable hosts remain application choices.

Sandbox contracts including `Sandbox`, `CodeExecutor`, `PageCapture`, and `InteractiveBrowser`
are part of this package; concrete executors and browser adapters are separate. See
[sandbox execution](../guide/sandbox) and [browser tools](../guide/browser).

### Source layout

```text
packages/effect-agent/src/
├─ core/           # agent definitions, Thread, schemas, identifiers
├─ engine/         # immediate execution and history integration
├─ capabilities/   # subagents, memory, MCP, tools
├─ sandbox/        # platform-neutral execution contracts
└─ durable/        # persistence, journals, recovery, scheduling
```

These are internal directories, not separate packages or import prefixes. Storage drivers,
platform hosts, workflow integrations, sandbox execution, and testing remain separate packages.

### Migrating imports

Replace dependencies on `@effect-agent/core`, `@effect-agent/engine`,
`@effect-agent/capabilities`, `@effect-agent/sandbox`, and `@effect-agent/thread` with `effect-agent`.
Those packages are consolidated into this release; previously published versions remain on npm.
Prefer root namespaces:

```ts
import { Agent, AgentRuntime, Subagent, CodeExecutor, ThreadHistory } from "effect-agent";
```

All remaining framework packages also use kebab-case module subpaths, for example
`@effect-agent/platform-node/node-durable-host`. PascalCase namespace names remain unchanged.
Update all framework packages together. Service identities and stored formats are unchanged by
this import migration.

### `@effect-agent/sandbox-local`

Runs trusted code in local child processes. It reports `unisolated` and rejects policies
requiring isolation it cannot enforce.

Follow the [local process walkthrough](../guide/sandbox#run-a-trusted-local-process).

### Threads and durability in `effect-agent`

`Thread` describes an identified, ordered conversation. `Thread.Store` holds in-memory snapshots
and `InMemory.layer` shares it across Runs. Persistence and execution recovery are separate choices.

Versioned records, storage contracts, recovery, scheduling, and subscriptions live under
`packages/effect-agent/src/durable`. Import their public namespaces from `effect-agent`, or use
kebab-case subpaths such as `effect-agent/persistent-history` and `effect-agent/durable-agent-runtime`.
`DurableAgentRuntime.layerRegistered` hashes version declarations and captures agent services
once at construction. `layerWithBindings` accepts previously compiled registrations owned by
the application's Scope. Worker operations use those registrations without accepting services.
Optional `processCommittedActivity` runs bounded, resumable passes with separate processor
progress. The host owns record eligibility, extraction, and durable output application. See
[committed memory processing](../guide/context-management#committed-memory).

Custom drivers can advance one FIFO-head Attempt with `processThreadHead(threadId)` and apply one
submission's recovery decision with `recoverSubmission`. `submissionStatus` is the authorized
nonblocking read; `inspectSubmissionStatus` is reserved for trusted workers. Pending status and
an empty processing result do not imply completion.

| Import                                                | Use                               |
| ----------------------------------------------------- | --------------------------------- |
| `effect-agent/persistent-history`                     | Persistent history implementation |
| `effect-agent/thread-store`                           | History storage contracts         |
| `effect-agent/thread-history`                         | Interpreter history service       |
| `effect-agent/durable-agent-runtime`                  | Durable runtime                   |
| `effect-agent/submission-ledger`                      | Accepted-work storage contracts   |
| `effect-agent/git-hub-workflow-source`                | GitHub event source               |
| `effect-agent/testing/certification`                  | Adapter certification             |
| `effect-agent/testing/thread-store-conformance`       | History conformance               |
| `effect-agent/testing/submission-ledger-conformance`  | Accepted-work conformance         |
| `effect-agent/testing/durable-failpoint-test-control` | Runtime failpoint controls        |

### `@effect-agent/workflow`

`AgentWorkflow.execute(agent, input, { name })` composes registered Agents inside native
`Workflow.toLayer` handlers. Stable step names deduplicate admission across replays; Effect's
`DurableDeferred` suspends and resumes the parent. Results are decoded from canonical
settlements, and `AgentWorkflow.Error` supplies the workflow's typed error Schema.

Import `AgentWorkflow` from the package root or use the direct
`@effect-agent/workflow/agent-workflow` module. The `WorkflowExecution` module exports
the step options, Agent contract, and `WorkflowExecutionFailure` schema.

Optional `WorkflowAgentHost` over an injected upstream Effect `WorkflowEngine`. It reuses the
durable runtime's admission, journal recovery, authorization, and settlement protocol.
`WorkflowAgentHost.layer(options)` consumes a runtime whose Layer owns agent registration.
Its required `principal` supplies the identity for workflow-originated submissions.
`WorkflowDispatchStore` retains dispatch intents; `WorkflowRepairTrigger` requires the host to
schedule startup and repeated repair. The shared package starts no polling loop and imports no
Node or Cloudflare implementation.

See the [Effect Workflows guide](../guide/workflows) for host composition, engine substitution,
and cancellation semantics, including the [Node.js SQL setup](../guide/workflows#node).
Install it separately from `effect-agent`.

### `@effect-agent/storage-memory`

Scoped in-memory thread and submission stores for tests. The ledger is non-durable.
The independent `inMemorySemanticIndexLayer` supplies a bounded exact cosine derivative index.
It is disposable and must be rebuilt from authoritative sources after its Scope closes.

### `@effect-agent/storage-sqlite`

Stores thread history and pending work in one Node SQLite database.
Rejects incompatible stored versions; no migration path is promised.
`CurrentSqliteStorageVersion` identifies the supported version.
Test failpoints are in `@effect-agent/storage-sqlite/testing/sqlite-storage-failpoint-testing`.

The independent `memoryStoreLayer` from `effect-agent/sql-memory-store` supplies optional `MemoryReader` and `MemoryWriter` ports
for conditional document updates and terminal withdrawal. It initializes only memory tables.
Use `memoryReaderLayer` when the application needs no writer. See
[memory lifecycle](../guide/context-management#memory-lifecycle).

`activityProcessorStoreLayer` provides independent leases, prepared output, and per-Thread
progress for finite committed-activity passes. Its tables and fencing epochs are separate from
the Thread journal and submission ledger.

### `@effect-agent/platform-node`

`NodeDurableHost.layer(registrations, options)` acquires storage, recovers pending work, and
starts a bounded worker pool. `NodeDurableHost.run` observes that pool and propagates worker
failure to the application. Provide the host Layer once around the supervised application;
its Scope closes admission and joins workers before releasing runtime resources.

Assembles SQLite storage, recovery, and workers through `NodeDurableHost`.
Registers agent bindings before execution, recovers before admission, and releases ownership
before closing storage. See the [Node.js guide](../platforms/node).

`NodeDurableAgentRuntimeOptions.toolFailureObserver` installs a local tool-failure observer.

The optional `@effect-agent/platform-node/node-workflow` import supplies `SqlWorkflowDispatchStore`
over an injected `SqlClient` and `NodeWorkflowRepairTrigger` with scoped startup and polling.
Pair them with `NodeDurableAgentRuntime.layerRegistered` and `WorkflowAgentHost.layer` as shown
in the [Workflow guide's Node.js setup](../guide/workflows#node). This assembly does not start
the ordinary Node worker loop.

### `@effect-agent/storage-cloudflare`

Stores history and pending work in each Durable Object's SQLite database.
Accepts injected Object handles without importing `cloudflare:workers`.
Rejects incompatible stored versions; `CurrentDoStorageVersion` identifies the supported version.
Failpoints and eviction helpers are in `@effect-agent/storage-cloudflare/testing/do-storage-failpoint-testing`.

`doMemoryStoreLayer` supplies optional memory ports using storage-backed SQLite transactions.
The separate memory protocol defines bounded batch requests, responses, and typed errors.

### `@effect-agent/platform-cloudflare`

Assembles the durable host, RPC client, alarms, and Code Mode executor.
See the [Cloudflare guide](../platforms/cloudflare) for bindings, service lifetimes, and admission limits.
The [Code Mode guide](../guide/code-mode#run-generated-code-on-cloudflare) covers the independent
Dynamic Worker executor and Worker Loader binding.
`ThreadObject.Options.toolFailureObserver` installs a local tool-failure observer.

`MemoryObject.make` and `CloudflareMemoryClient` share namespace-owned memory across
Threads, with one authoritative batch RPC per recall. See [shared memory](../platforms/cloudflare#shared-memory).

Browser adapters use separate imports:

| Subpath                 | Adapter and requirements                                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `/cloudflare-browser`   | Page capture through a browser binding; structured extraction also needs explicit Workers AI authorization and accounting |
| `/browser-rest-capture` | Node-safe page capture with account credentials and `HttpClient`                                                          |
| `/browser-rest-crawl`   | Node-safe same-host Markdown crawl with bounded polling and scoped job cleanup                                            |
| `/interactive-browser`  | Bounded interactive browser and host controls with the included Puppeteer client                                          |
| `/browser-session`      | Host-owned native sessions, scoped attachments, operator controls, keepalive, and exact-session cleanup                   |
| `/browser-credentials`  | Login/card fill schemas and invocation-specific credential authority; fills the current native page                       |

Durable hosts and the stateless browser adapters do not load Puppeteer.

See [browser setup and limits](../guide/browser) for credentials, network policies,
action failures, and cleanup.

#### Browser session options

`BrowserSessions.layer({ browser, accountId, apiToken })` requires `HttpClient`.
`create(options, retain)` calls the host's retention Effect with a private
`BrowserSessionReference`; successful retention makes the host responsible for remote cleanup.
`attach(reference)` acquires a local connection in `Scope`. Its finalizer disconnects locally.

| Creation option        | Default  | Meaning                                                        |
| ---------------------- | -------- | -------------------------------------------------------------- |
| `maxElapsedMillis`     | Required | Positive safe integer; fixes the session's absolute expiry     |
| `keepAliveMillis`      | `600000` | Requested provider idle allowance, from 1 through 600,000 ms   |
| `commandTimeoutMillis` | `30000`  | Positive safe integer; bounds each authorized native operation |

The reference stores redacted session, context, and page identities, `expiresAt`, and
`commandTimeoutMillis`. Keep it in private host storage. `keepAlive(sessionId)` refreshes
provider inactivity without changing `expiresAt`; `close(sessionId)` requires confirmed
termination or exact-session absence. The owner supplies its existing expiry/cleanup trigger.
Provider expiry can happen sooner; attachment never creates a replacement session.

`session.run(authorize, action)` checks current host authority under the attachment's lock before
passing its native Puppeteer page to trusted code. The host owns network policy, output bounds,
controller fencing, and action receipts. `handoff`, `getLiveView`, and `getHandoffState` take the
same authorization Effect and use the existing `BrowserRun` request/result schemas.
Await all SDK work inside the native callback. A settled SDK rejection preserves the session for
inspection while reporting uncertain dispatch; unfinished or unsafe operations can terminate it.
Neither outcome authorizes automatic replay.

`session.fillCredential(request)` requires `BrowserCredentialAccess` for each call. Its
`FillCredentialRequest` selects 1–8 fields by explicit selector and role within one native form;
an optional `frame` path selects at most eight nested iframes. The service authorizes current
origins and resolves host-only material. The helper fills without submitting and returns only
dispatch evidence and the number of acknowledged writes. Ordinary browser observations remain
available after filling.
If a fill times out, `CredentialFillError` reports `reason: "timeout"`, the acknowledged
`filled` count, dispatch evidence, and whether browser cleanup was confirmed. A pending write
reply remains `possibly-dispatched`; its assignment is not included in `filled`.

### `@effect-agent/pr-review`

Runs a provider-neutral PR review over supplied patches and immutable base/head source.
Returns a schema-validated report, validated paths and line anchors, and token usage.
The host supplies provider configuration, pricing, GitHub access, and publication.

### `@effect-agent/testing`

Provides scripted models for offline tests.
Fixtures, certification, chaos, and CodeExecutor helpers have
[dedicated imports](../guide/testing#choose-a-testing-entry-point).
Production packages must not depend on this package.

## GitHub Action

The [review Action](https://github.com/danieljvdm/effect-agent/blob/main/action/readme.md)
adds GitHub admission, source retrieval, provider setup, and report publication to `pr-review`.

## Examples {#leaf-examples}

- [Cloudflare travel planner](https://github.com/danieljvdm/effect-agent/tree/main/examples/travel-planner): the canonical application, deployed with Alchemy.
- [Operational harnesses](https://github.com/danieljvdm/effect-agent/tree/main/tooling): release gates, performance measurements, and opt-in provider verification.

For repository layout and contribution rules, see the [toolchain guide](https://github.com/danieljvdm/effect-agent/blob/main/docs/toolchain.md).
