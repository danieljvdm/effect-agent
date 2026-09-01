---
title: Package map
description: Choose packages, adapters, and providers for your application.
---

# Package map

Start with `effect-agent@beta`. It includes core, engine, and capabilities.
Install storage, platform, sandbox, and testing packages as needed.

Keep all framework packages at the same exact release. They require `effect@^4.0.0-rc.111`;
this repository tests Effect and its OpenAI/Anthropic providers at `4.0.0-rc.111`.
Before 1.0, APIs and stored data may change without a migration path.

## Find a capability {#capability-inventory}

| Need                                   | Guide                                                           | Your application supplies                          |
| -------------------------------------- | --------------------------------------------------------------- | -------------------------------------------------- |
| Run or stream an agent                 | [Execution](../guide/run-agents)                                | Model, tool handlers, history policy               |
| Retain completed threads               | [History](../guide/threads#retain-completed-runs)               | Store and thread IDs                               |
| Recover work after a crash             | [Durability](../concepts/durability)                            | Registered agents, workers, storage, authorization |
| Prune or summarize context             | [Context management](../guide/context-management)               | Context limits and compaction policy               |
| Recall application-owned sources       | [Context management](../guide/context-management#recall-memory) | Readable passages, provenance, query policy        |
| Require approval or limit spending     | [Run hooks](../guide/run-agents#operational-hooks)              | Approval policy, budget hooks, cost estimates      |
| Delegate to another agent              | [Subagents](../guide/subagents)                                 | Targets, bindings, permissions, budgets            |
| Schedule new input                     | [Scheduling](../guide/operations#scheduled-input)               | Owner policy, registered inputs, driver            |
| React to external events               | [Subscriptions](../guide/operations#event-subscriptions)        | Authenticated source, preparation, authorization   |
| Run generated JavaScript               | [Code Mode](../guide/code-mode)                                 | Read-only tools and an isolated executor           |
| Run trusted local commands             | [Sandbox execution](../guide/sandbox)                           | Executable, environment, output and time limits    |
| Capture, crawl, or interact with pages | [Browser tools](../guide/browser)                               | Browser binding or credentials, target policy      |

### Limits and unsupported features {#compaction-and-unsupported-capabilities}

MCP validates connections and tool discovery through `McpConnector` and `connectMcp`.
Your application implements the transport and remote handlers; no stdio or HTTP client is bundled.

Nested delegation, handoff, detached subagents, runtime Skills, a framework-owned memory
extraction or sharing policy, arbitrary Thread metadata, and dynamic Turn Plans have no public APIs.
Applications own domain state. `recallMemory` reads bounded passages from sources they select; it
does not store them. Thread history and compaction summaries do not replace application state.

Automatic compaction uses `ContextCompactor`. The separate
[artifact utilities](../guide/context-management#explicit-compaction-artifacts) validate and apply
application-managed summaries; they do not run or persist automatically.

Scheduling and subscription ownership does not isolate thread storage.
Enforce [storage separation and authorization](../guide/operations#authorization-and-isolation)
in your host.

## Packages

### `effect-agent` {#effect-agent-umbrella}

Re-exports `@effect-agent/core`, `@effect-agent/engine`, and `@effect-agent/capabilities`.
Provider clients, storage, hosts, sandbox adapters, and testing remain separate installs.

### `@effect-agent/core`

Agent definitions and bindings, schemas, identifiers, errors, and run events.
Start with `Agent`, `AgentPolicy`, and `IdGenerator`.

### `@effect-agent/engine`

Runs the agent loop, schedules tool calls, enforces policy, and emits events.
Exports `AgentRuntime`, `DetachedRun`, `RunOptions`, and `ThreadHistory`.

Every entry point needs a history policy. Use `ThreadHistory.layerTransient` to retain
nothing or `PersistentHistory.layer` from `@effect-agent/thread` to retain successful runs.
Provide `RunContextPreparation` only when you need host context loading. Context service failures
retain their concrete tagged errors.
Use [`toolFailureObserverLayer`](../guide/run-agents#observe-recovered-tool-failures) to observe
recovered tool failures locally. Observations are not stored or exported automatically.

### `@effect-agent/capabilities`

Adds thread queues, approval, audit, budgets, context utilities, bounded `recallMemory`, scheduling
overrides, MCP, redaction, and subagents to the engine. `recallMemory` reads ranked
`MemoryPassage` values from host-selected sources and returns a transient `RecalledMemory` view;
it supplies no store. Optional `indexMemorySource` and `querySemanticMemory` use upstream
Effect AI `EmbeddingModel` with an application-selected index and authoritative reader. See
[semantic retrieval](../guide/context-management#semantic-memory). [`CodeMode.make`](../guide/code-mode) exposes generated
JavaScript execution over an explicit read-only Tool allowlist. `WebCapture.make`,
`WebCapture.makeScrape`, and `WebCapture.makeExtract` expose a supplied `PageCapture` service as tools.
Capture calls have uncertain external outcomes;
extraction retains its schema's service requirements.

### `@effect-agent/sandbox`

Defines `Sandbox`, `CodeExecutor`, `PageCapture`, `PageScreenshot`, `PageCrawl`, and
`InteractiveBrowser` contracts without a platform dependency.

See [sandbox execution](../guide/sandbox) to choose a contract and consume process events, or
[browser tools](../guide/browser) for page adapters, network policies, and handle lifetimes.

### `@effect-agent/sandbox-local`

Runs trusted code in local child processes. It reports `unisolated` and rejects policies
requiring isolation it cannot enforce.

Follow the [local process walkthrough](../guide/sandbox#run-a-trusted-local-process).

### `@effect-agent/thread`

Thread records, storage contracts, recovery, durable execution, scheduling, and subscriptions.
`compileRegistrations` hashes version declarations and captures agent services for workers.
Optional `processCommittedActivity` runs bounded, resumable passes with separate processor
progress. The host owns record eligibility, extraction, and durable output application. See
[committed memory processing](../guide/context-management#committed-memory).

| Import                            | Use                                                    |
| --------------------------------- | ------------------------------------------------------ |
| `@effect-agent/thread/history`    | `PersistentHistory` and history contracts              |
| `@effect-agent/thread/durability` | Durable runtime and accepted-work contracts            |
| `@effect-agent/thread/github`     | GitHub event source                                    |
| `@effect-agent/thread/testing`    | Certification, conformance runners, failpoint controls |

### `@effect-agent/storage-memory`

Scoped in-memory thread and submission stores for tests. The ledger is non-durable.
The independent `inMemorySemanticIndexLayer` supplies a bounded exact cosine derivative index.
It is disposable and must be rebuilt from authoritative sources after its Scope closes.

### `@effect-agent/storage-sqlite`

Stores thread history and pending work in one Node SQLite database.
Rejects incompatible stored versions; no migration path is promised.
`CurrentSqliteStorageVersion` identifies the supported version.
Test failpoints are in `@effect-agent/storage-sqlite/testing`.

The independent `memoryStoreLayer` supplies optional `MemoryReader` and `MemoryWriter` ports
for conditional document updates and terminal withdrawal. It initializes only memory tables.
Use `memoryReaderLayer` when the application needs no writer. See
[memory lifecycle](../guide/context-management#memory-lifecycle).

`activityProcessorStoreLayer` provides independent leases, prepared output, and per-Thread
progress for finite committed-activity passes. Its tables and fencing epochs are separate from
the Thread journal and submission ledger.

### `@effect-agent/platform-node`

Assembles SQLite storage, recovery, and workers through `NodeDurableHost`.
Registers agent bindings before execution, recovers before admission, and releases ownership
before closing storage. See the [Node.js guide](../platforms/node).

`NodeDurableRuntimeOptions.toolFailureObserver` installs a local tool-failure observer.

### `@effect-agent/storage-cloudflare`

Stores history and pending work in each Durable Object's SQLite database.
Accepts injected Object handles without importing `cloudflare:workers`.
Rejects incompatible stored versions; `CurrentDoStorageVersion` identifies the supported version.
Failpoints and eviction helpers are in `@effect-agent/storage-cloudflare/testing`.

### `@effect-agent/platform-cloudflare`

Assembles the durable host, RPC client, alarms, and Code Mode executor.
See the [Cloudflare guide](../platforms/cloudflare) for bindings, service lifetimes, and admission limits.
The [Code Mode guide](../guide/code-mode#run-generated-code-on-cloudflare) covers the independent
Dynamic Worker executor and Worker Loader binding.
`ThreadObject.Options.toolFailureObserver` installs a local tool-failure observer.

Browser adapters use separate imports:

| Subpath                 | Adapter and requirements                                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `/browser-quick-action` | Page capture through a browser binding; structured extraction also needs explicit Workers AI authorization and accounting |
| `/browser-rest-capture` | Node-safe page capture with account credentials and `HttpClient`                                                          |
| `/browser-rest-crawl`   | Node-safe same-host Markdown crawl with bounded polling and scoped job cleanup                                            |
| `/interactive-browser`  | Interactive browser and host controls; requires `@cloudflare/puppeteer@^1.1.0`                                            |

Durable hosts and the other browser adapters do not need Puppeteer.

See [browser setup and limits](../guide/browser) for credentials, network policies,
action failures, and cleanup.

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

The [review Action](https://github.com/danieljvdm/effect-agent/blob/main/action/README.md)
adds GitHub admission, source retrieval, provider setup, and report publication to `pr-review`.

## Examples {#leaf-examples}

- [Provider examples](https://github.com/danieljvdm/effect-agent/tree/main/examples/providers): OpenAI, Anthropic, and retained history.
- [Chat demo](https://github.com/danieljvdm/effect-agent/tree/main/examples/demo): steering, follow-ups, approval cards, and budget limits, with a credential-free scripted profile.
- [Code Mode warehouse](https://github.com/danieljvdm/effect-agent/tree/main/examples/code-mode-cloudflare): generated JavaScript querying a SQLite Durable Object through brokered read-only tools. Start with the [Code Mode guide](../guide/code-mode).
- [Hosted browser proof](https://github.com/danieljvdm/effect-agent/tree/main/examples/browser-run-worker-proof): an opt-in temporary Worker deployment exercising capture, screenshots, interactive actions, Live View, and handoff. See [browser setup](../guide/browser).

For repository layout and contribution rules, see the [toolchain guide](https://github.com/danieljvdm/effect-agent/blob/main/docs/TOOLCHAIN.md).
