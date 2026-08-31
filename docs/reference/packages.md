---
title: Package map
description: Public alpha packages, implemented capabilities, adapters, and host responsibilities.
---

# Package map

Fourteen framework packages publish to npm on the opt-in `beta` dist-tag. The product is a public
alpha; package versions use `X.Y.Z-beta.N`. Keep all framework packages at one exact release.
This source tree pins Effect and the OpenAI/Anthropic providers to `4.0.0-rc.111`. APIs and stored
data may change incompatibly before 1.0, with no compatibility window or migration promise.
See [installation and compatibility](../guide/getting-started#installation-and-compatibility).
Provider integration remains upstream Effect AI Models and Layers.

## Capability inventory

An exported port is an integration contract; its implementation may still belong to the host.
Optional Layers must be composed explicitly. The following entries link to their guides or APIs.

| Capability and public API                                                                                                        | Bundled behavior or adapters                                                                                      | Host requirements and limits                                                                                                                                                                                                                         |
| -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Agent execution](../guide/run-agents), `AgentRuntime.run`, `.stream`, `.start`                                                  | Bounded interpreter, finite policy, deterministic Tool-result order, scoped resources                             | Supply Model, Tool-handler, and history policy Layers. Ephemeral work ends with its Scope; it has no restart recovery.                                                                                                                               |
| [Process-local Conversations](../guide/conversations#advanced-history-integrations), `EphemeralConversations`, `RunCommandQueue` | Bounded incremental history, steering and follow-up queues, engine adapters                                       | Wire the Conversation/input hooks with `ConversationHistory.layerTransient`. Partial Runs remain visible; history and commands disappear with the process.                                                                                           |
| [Persistent history](../guide/conversations#retain-completed-runs), `ConversationHistory`, `PersistentHistory.layer`             | Successful-Run retention through the normal runtime; SQLite and Cloudflare stores; nonpersistent memory reference | Supply a store and unique identities. Concurrent Runs can both execute, but conflicting commits fail. No accepted-work recovery; checkpoints are optional.                                                                                           |
| [Durable execution](../concepts/durability), `DurableAgentRuntime`                                                               | Node/SQLite and Cloudflare Durable Object assemblies, fenced Attempts, Settlements, conservative Tool recovery    | Operate workers and storage; register exact Agent Bindings; authorize admission and operations. Unknown ordinary Tool outcomes need explicit resolution.                                                                                             |
| [Context management](../guide/context-management#compaction), `ContextCompactor`                                                 | Default pruning and metered summarization at the interpreter seam; replaceable Layer                              | Configure context limits and optional strategy/Model. Durable compaction must map to complete prior-Run records. Explicit artifact utilities are separate, as described below.                                                                       |
| [Approval and budgets](../guide/run-agents#operational-hooks), `toRunApprovalHook`, `toRunBudgetHook`                            | Approval Schemas, deny-all resolver, memory audit, structural redactor, hierarchical usage accounting             | Supply resolver, audit/redaction policy, budget hooks, and cost estimates. Local audit/budget services are not a persistent tenant billing system.                                                                                                   |
| [MCP](https://github.com/danieljvdm/effect-agent/blob/main/packages/capabilities/src/mcp.ts), `McpConnector`, `connectMcp`       | Scoped timeout and validation of discovery identity, counts, bytes, Toolkit names, and schema digests             | Implement transport, credentials, connection cleanup, and native Toolkit handlers. No bundled stdio/HTTP client transport. Remote execution is uncertain.                                                                                            |
| [Subagents](../concepts/durability#attached-subagents), `Subagent.define`, `SubagentRuntime.layer`                               | Bounded ephemeral children and durable attached child protocol                                                    | Supply targets, Bindings, grants, projections, and action policy. Nested delegation, handoff, and detachment are unsupported.                                                                                                                        |
| [Scheduled input](../guide/operations#scheduled-input), `Scheduling`                                                             | One-shot, interval, and cron delivery; memory reference, SQLite, and Cloudflare adapters/drivers                  | Supply owner policy, `ScheduleAuthorizer`, registered inputs/Bindings, and a running driver. Delivery uses ordinary admission, not a sleeping Run.                                                                                                   |
| [Event subscriptions](../guide/operations#event-subscriptions), `Subscriptions`, `SubscriptionIntake`                            | Partitioned once/continuous delivery, restricted Tools, storage/drivers, GitHub workflow-attempt source           | Supply source/preparation bindings, authorizer, authenticated intake, and recovery. Records consume lifetime quota; no automatic pruning or generic historical replay. Implemented in [#223](https://github.com/danieljvdm/effect-agent/issues/223). |
| [Sandbox and Code Mode](../guide/tools), `Sandbox`, `CodeExecutor`, `CodeMode.make`                                              | Trusted local process adapter; Cloudflare Code Mode adapter over the broker                                       | Supply execution policy and host Tool authority. Local processes are unisolated; generated programs require an isolated executor. No durable execution claim for a Code Mode pass.                                                                   |
| [Web and browser ports](#effect-agent-platform-cloudflare), `PageCapture`, `PageScreenshot`, `PageCrawl`, `InteractiveBrowser`   | Cloudflare capture, REST crawl, and interactive browser adapters; `WebCapture` Tool helpers                       | Supply browser bindings or credentials and network policy. Browser handles are ephemeral; exact-host checks do not isolate hostile networks.                                                                                                         |

### Compaction and unsupported capabilities

Automatic compaction uses the engine's `ContextCompactor` decision stream. The capabilities package
also exports `prepareModelContext`, `CompactionArtifact`, `RetainedFact`, `digestCompactionSource`,
and `applyCompaction` for application-managed views. These explicit utilities validate an artifact
against a source snapshot. They do not run automatically, store artifacts, or provide a second
interpreter compactor. See [explicit compaction artifacts](../guide/context-management#explicit-compaction-artifacts).

The following APIs are absent or deferred:

- Runtime Skills have no registration, activation, loading, or resource API. `.agents/skills` is
  contributor tooling, not a runtime capability.
- Separate persistent agent memory/state has no framework service or store. Applications own such
  domain state. Conversation history and source-bound retained compaction facts do not provide it.
- SessionStore metadata has no public `SessionStore` API. A Session is an interaction handle in
  the glossary; `ConversationStore` retains canonical history, not arbitrary session metadata.
- Generic dynamic Turn Plans have no public API. Existing prompt-preparation hooks, Tool
  authorization, and scheduling overrides are narrower contracts, not per-Turn replanning of
  Models, Toolkits, policy, and resources.

Scheduling and subscription tenant scopes apply to those registrations. They do not add tenant
fields to Conversation records or establish general storage isolation. Hosts must enforce
[database/namespace separation, addressing, and authorization](../guide/operations#authorization-and-isolation).

## Packages

### `effect-agent` (umbrella)

Re-exports schema-first authoring from core, the bounded interpreter from engine, and operational
capabilities as one platform-neutral root package,
mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters stay scoped, and the
umbrella shares the fixed release version of all framework packages. Session/history, storage,
platform hosts, sandbox adapters, and testing are separate packages.

### `@effect-agent/core`

Owns Agent Definitions and Bindings, branded identity Schemas, finite policy, expected framework
errors, semantic Run Events, and the `IdGenerator` port. It depends only on Effect and Effect AI.

Exports include `Agent`, `AgentPolicy`, identifiers, errors, `RunEvent`, and `IdGenerator`.

### `@effect-agent/engine`

Owns the one ephemeral interpreter, Turn loop, Effect AI Response reduction, Tool scheduling,
policy enforcement, semantic events, and narrow `RunOptions` seams.

Exports include `AgentRuntime`, `DetachedRun`, `RunOptions`, `ConversationHistory`,
`ConversationHistoryError`, and the operational hook interfaces. Every runtime entry point
requires a history policy Layer. The session package's `PersistentHistory.layer` implements
retained history over any ConversationStore; `ConversationHistory.layerTransient` retains none.
`CurrentToolFailureObserver`, `toolFailureObserverLayer`, `RunToolFailureObserver`, and
`ToolFailureObservation` provide the opt-in trusted local interface for non-propagating Tool
failures. The default is absent, installation is service-only, and observations are never
persisted or automatically exported. See [Tool failure observation](../guide/run-agents#observe-recovered-tool-failures).

### `@effect-agent/capabilities`

Adapts richer optional services to the engine: process-local Conversations, command queues,
approval and audit, budgets, context/compaction, Tool-batch scheduling overrides, MCP, structural redaction, web
capture through `WebCapture.make` and `WebCapture.makeExtract` over the `PageCapture` port, and
Subagent authoring through `Subagent.define`, `SubagentPolicy`, `SubagentGrant`, and
`SubagentRuntime.layer`. Capture Tools have uncertain execution class; extraction handler Layers
and Tool invocations retain their Effect Schema's decoding-service requirements.

It depends outward from engine; the engine does not import it.

### `@effect-agent/sandbox`

Defines schema-first, platform-neutral sandbox requests, events, errors, the streaming `Sandbox`
service, the callback-shaped `CodeExecutor` port, and the stateless `PageCapture` port.
It also exports the sibling `PageScreenshot` port for one bounded caller-owned PNG and the scoped
`PageCrawl` Stream port for bounded same-host Markdown records.
The scoped `InteractiveBrowser` port is a distinct programmatic capability: one ephemeral
browser/context/page pass with navigation, bounded reads, fills, clicks, PNG screenshots, viewport
scrolling, and explicit early closure. Its handle is not transportable or persistable, and its
immutable `network` policy selects `ExactHosts` for page-request URL checks, `PublicWeb` for a
connection-time public-network boundary, or `Unrestricted` for an explicit opt-out from URL/host
and private-network containment. `InteractiveBrowserTargetUrl` admits credential-free HTTP and
HTTPS navigation and observations; ExactHosts still checks HTTPS. Cloudflare rejects `PublicWeb` before acquisition with
`InteractiveBrowserUnsupportedError`, `feature: "policy"`; exact-host mode is not network
containment for hostile pages or viewers. Screenshot bytes reuse
`PageScreenshotResult` and share the handle's per-result byte limit.

### `@effect-agent/sandbox-local`

Implements the Sandbox contract with local child processes for trusted development. It identifies
itself as `unisolated` and rejects isolation policy it cannot enforce.

### `@effect-agent/session`

Owns canonical Conversation record Schemas, batches, digests, replay, optional checkpoints, the
`ConversationStore`, `SubmissionLedger`, and `WakeScheduler` ports, the pure recovery classifier,
the run journal, and the `DurableAgentRuntime` coordinator (Receipt, Attempt, Settlement), including
the conversation-keyed `awaitProgress` boundary that subscribes before its authoritative canonical
read. It
depends on `@effect-agent/engine` to drive the interpreter through its public seams.
The coordinator captures the Tool failure observer at construction and explicitly provides it to
fresh and replacement Attempts. Already-settled calls injected on resume bypass observation.
It also owns the durable Subagent protocol: the requested/started/joined/lineage record
Schemas, the child budget reservation and `waitingForChild` ledger operations, and the
exact-digest matching of explicit worker bindings. `processConversationResolved(conversationId, bindings)`
and `runResolvedWorker(bindings)` receive those bindings directly. `compileRegistrations` hashes
typed Agent version declarations and captures each Agent's required services.
Adapter certification reports, port runners, and the TestClock-dependent conformance case arrays
are available only from `@effect-agent/session/testing`; the package root has no transitive
test-runtime dependency. Mutable coordinator failpoint controls and their test Layer also live
in `@effect-agent/session/testing`. Durable `Scheduling`, `Subscriptions`, event sources, and
preparation bindings are on the production root; the GitHub source uses `@effect-agent/session/github`.

`@effect-agent/session/history` exports `PersistentHistory`, `ConversationHistory`, and the history contracts;
`@effect-agent/session/durability` exports the coordinator and accepted-work contracts. These
focused entry points do not change the distinction between retained history and durable execution.

### `@effect-agent/storage-memory`

Provides deterministic scoped in-memory `ConversationStore` and reference `SubmissionLedger`
Layers. The ledger declares `non-durable` capabilities; it exists for tests and conformance.

### `@effect-agent/storage-sqlite`

Provides the Node SQLite adapters behind the `DN` assembly: the Conversation Store and the
durable Submission Ledger in one database file, current-version (v4, exact-match) initialization,
observation, typed compatibility/corruption/conflict/contention errors, and before/after
failpoints on every durable mutation.

`@effect-agent/storage-sqlite/testing` exposes `SqliteStorageFailpointTestControl.layer` for tests.
The production root exports `CurrentSqliteStorageVersion`; schema initialization is internal.
Only the current stored version is supported, with no upgrade migration promise.

### `@effect-agent/platform-node`

See the [Node.js guide](../platforms/node) for installation, host setup, and worker lifecycle.

Assembles the class `DN` Node/SQLite runtime: one shared SQLite client behind both stores,
validated typed configuration, the in-process wake scheduler with ledger-scan fallback, graceful
ownership drain, Agent Binding registration for durable workers
(`NodeDurableHost.layer(bindings)` behind `NodeDurableHost.runResolvedWorkers`; an empty
roster fails every claim closed), and the `NodeDurableHost` startup gates (storage compatibility,
recovery before admission) and shutdown order (close admission → release ownership → close
storage). It is a Layer-assembly library, not an application entrypoint.
`NodeDurableRuntimeOptions.toolFailureObserver` installs the engine's closed trusted observer in
that durable coordinator; it is not a serialized configuration value.

### `@effect-agent/storage-cloudflare`

Provides the Durable Object SQLite adapters behind the `DC` assembly: the Conversation Store and
durable Submission Ledger against one Conversation Object's private SQLite database (the v4
table mirror plus the `effect_agent_meta` exact-or-fresh version gate and the durable
`effect_agent_child_settlements` cross-store notification marker), storage-backed transactions,
typed compatibility/corruption/bound errors, the same failpoint-location names as the Node
adapter. Its cross-Object boundary uses Schema port-call envelopes and routed decorator Layers over
a closed route-capable subset with adapter-minted routable Submission identities. It never
imports the `cloudflare:workers` runtime module; Durable Object handles are injected as Layer
construction values.

`@effect-agent/storage-cloudflare/testing` exposes `DoStorageFailpointTestControl.layer` and
`evictionFailpointHandler` for adapter tests. The production root retains
`CurrentDoStorageVersion`; schema initialization is internal. Incompatible stored versions fail
clearly, with no upgrade migration promise.

### `@effect-agent/platform-cloudflare`

See the [Cloudflare guide](../platforms/cloudflare) for Conversation Objects, Worker bindings,
and the client Layer.

The root exports the durable host and Code Mode integration. Browser Run adapters are available
only from the four browser subpaths below. Install `@cloudflare/puppeteer@^1.1.0` directly when
using `@effect-agent/platform-cloudflare/interactive-browser`; it is an optional peer dependency.
Durable hosts, Quick Actions, and REST capture/crawl do not need Puppeteer.

Assembles the class `DC` Cloudflare runtime and is the only package that imports
`cloudflare:workers`: binding Layers (namespace, Object context, identity), schema-validated
configuration, the single multiplexed `DurableAlarmService` with the idempotent
`ConversationMaintenance` pass (pre-armed alarm invariant: committed nonterminal work implies a
committed alarm), the alarm/RPC wake scheduler, the Durable Object RPC port transport,
`ConversationObject.layer`, `ConversationObject.make` (local-only constructor gates
and the typed admission-limits gate before `submit`), and the Worker-side
`CloudflareConversationClient`, whose `awaitProgress` RPC retries Object resets with a fresh stub
and sends explicit scoped cancellation on interruption. The client Layer requires `Crypto.Crypto`
so its composition root owns collision-resistant cancellation identity generation instead of the
client reading ambient time or randomness. The Conversation Object factory accepts a composed
application Layer. `ConversationObject.layer(registrations)` hashes typed version declarations and
captures each Agent's dependencies. Application Layers can yield effect-cf's `WorkerEnvironment`
and `DurableObjectState`, plus platform Crypto and `ConversationObjectIdentity`. The complete graph
acquires once per incarnation inside the constructor gate. Application requirements and
initialization errors remain visible in its type; `options.eventLayer` can consume its exposed
services. See [Cloudflare execution](../guide/run-agents#run-durably-on-cloudflare).
`BrowserQuickActionBrowserBinding.layer` lifts a host-resolved Wrangler `browser`
binding into an explicit Effect service. `browserQuickActionCaptureLayer` visibly requires that
service to adapt `quickAction()` to the `PageCapture` port without granting Workers AI authority.
`browserQuickActionWorkersAiCaptureLayer` requires both the browser-binding service and the
explicit `BrowserQuickActionWorkersAi` authorization and accounting service before permitting
structured extraction. The `./browser-quick-action` export provides these adapters without
loading Durable Object runtime modules. The Node-safe `./browser-rest-capture` export provides a
second `PageCapture` implementation through explicit account/token construction values and an
Effect `HttpClient` requirement. The Node-safe `./browser-rest-crawl` export adapts `PageCrawl` to
Cloudflare's REST crawl endpoint with explicit redacted credentials, bounded polling and lazy
pagination, and scoped remote-job cleanup. Neither REST subpath imports Worker runtime modules.
The `./interactive-browser` export supplies `browserRunInteractiveLayer` for the generic browser
port and `browserRunInteractiveHostLayer` for host-only viewport resizing, Live View, handoff, redacted session
identity, and explicit cleanup by identity. Both require `BrowserRunInteractiveBinding`; neither
adds a registry, execution reconnect, or durable browser state.

Navigation waits for DOM content loaded, with a 30-second provider timeout bounded further by
the pass deadline. `readText().text` contains JSON with `pageText`, `selectorMatchCount`,
`controls`, and `controlsTruncated`. The inventory prioritizes form controls over links, includes
visible labels for hidden native radios and options of visible native selects, and caps the list
at 64 entries. Each entry has an exact structural CSS selector, kind, optional label, and available
checked, selected, disabled, required, validity, and form-validity booleans. It never reads field
values or HTML into control diagnostics. The entire JSON text remains subject to the pass byte
limit; an oversized observation fails with the existing returned-bytes error.

Click and fill require exactly one match, including a second check on acquired element handles.
`isBrowserRunUndispatchedActionError` recognizes missing, ambiguous, or syntactically invalid CSS
refusals before a mutation is sent. The handle remains usable after these refusals; callers may
correct the target, but must not automatically replay a mutation with an unknown outcome.
Other action failures invalidate the handle. Native option observations expose labels and
selection, never option values; they do not add a new selection action to the generic port.

Actions observe only fetch/XHR requests started during the action. A 200ms quiet window after
dispatch completion is capped at two seconds. General logs contain only action names, bounded
target-kind categories, counts, status-class buckets, and control-state booleans. They omit URLs,
selectors, labels, values, headers, cookies, bodies, and provider exceptions. Post-action state
is best-effort and capped at 250ms, so losing the old document during navigation does not fail a
successful action. Listeners close on completion, failure, and abort; handle disposal waits at
most 250ms. Interruption fences any dispatch still waiting on a query, invalidates the handle,
records whether dispatch occurred before teardown, and waits at most 500ms for SDK completion.
Puppeteer cannot cancel an already dispatched mutation reliably. Its outcome remains unknown
after interruption, even if the SDK later resolves; the owning Scope closes the remote session.
These diagnostics add no durable journal records and do not replace the engine's ordinary-tool
uncertainty and recovery contract.

`BrowserRunInteractiveBinding.layer({ browser, viewport: { width: 1440, height: 900 } })`
sets Puppeteer's launch `defaultViewport`. Omitting `viewport` preserves Puppeteer's default.
The binding requires `BrowserRunSessionLifecycle.layer({ accountId, apiToken })`, backed by an
`HttpClient` and an account-scoped Browser Rendering Write token. Both `session.close` and
`host.closeSession` confirm whole-browser termination or exact-session absence. Cleanup sends at
most one DELETE and two metadata reads within ten seconds. A `closing` response remains pending;
authentication, malformed responses, and transport failures never count as absence. Local
teardown errors cannot veto confirmed termination. `BrowserRunCleanupError` carries a sanitized
reason in the close error's cause; park authorization/configuration failures until corrected.
The exported `BrowserRunViewport` Schema accepts integer CSS dimensions in `1..2048`, optional
finite `deviceScaleFactor` in `1..2`, defaulting to `1`, and requires
`max(width, height) * deviceScaleFactor <= 2048`. Hosts can impose narrower UI limits.
Invalid configuration fails Layer construction with `InteractiveBrowserPolicyDeniedError`
before launching a browser.

`session.resizeViewport({ width: 1024, height: 768, deviceScaleFactor: 2 })` changes host
presentation state through `page.setViewport`. It spends no agent action and remains available
after the action budget is exhausted. It shares the session's fail-fast operation lock,
page-policy preflight, elapsed deadline, and cleanup. Invalid requests are policy failures;
provider failures are protocol errors, remote closure is an expired-session error, and timeout
or interruption makes the session unusable without retrying the resize. Width, height, and
density are the only supported fields. Mobile, touch, and orientation emulation switches are
rejected, so resizing does not trigger an emulation-mode reload. Page scripts can still react to
resize events, subject to the existing network policy. The adapter adds no viewport telemetry
payload or persistence. The consumer must authorize resizing, including viewer ownership and
handoff fencing.

This is a Layer-assembly library, not an application entrypoint.
`ConversationObject.Options.toolFailureObserver` installs the same closed trusted observer
for the Object's coordinator. It adds no journal data or Code Mode durability claim.

### `@effect-agent/pr-review`

A provider-neutral, deployment-class-E review agent. The host supplies bounded patches and
immutable base/head repository source; one bounded, source-backed Run returns a Schema-decoded
report. The package validates paths and RIGHT-side line anchors and exposes observed cached,
uncached, cache-write, and output token usage plus an optional host-priced estimate. It contains no
GitHub adapter, provider binding, entrypoint, retry of a completed review, fan-out, continuity
state, or publication behavior.

### `@effect-agent/testing`

The root exports the scripted Effect AI Model and its supporting types and Schemas. Certification,
chaos, CodeExecutor helpers, Docs Researcher, and Travel Planner have dedicated
[testing subpaths](../guide/testing#choose-a-testing-entry-point). Production packages never depend
on this package. Shared fixtures remain available to example and adapter tests through those paths.

## GitHub Action

The private `packages/pr-review-action` workspace owns the GitHub channel over
`@effect-agent/pr-review`: webhook policy, REST decoding, bounded exact-diff admission, immutable
base/head source binding, the OpenAI Layer, one reviewer invocation, and publication. The root
`action` directory holds the consumer-facing metadata and committed node-runtime bundle. See the
[Action guide](https://github.com/danieljvdm/effect-agent/blob/main/action/README.md).

## Leaf examples

`examples/demo` is a local browser test bench. `examples/providers` is a compile-only proof that the
same Definition binds directly to upstream OpenAI and Anthropic Models.
`examples/browser-run-worker-proof` is the opt-in temporary Cloudflare deployment that proves the
native Browser Run binding through the shipped page-capture Layers. `examples/pr-work-orders` is the private
trusted-local proof of a head-bound work-order implementer;
`examples/pr-work-order-ingress` is the private GitHub dispatch and isolated
publication proof; `examples/repo-ops`
is an internal repository-operations auditor. None is a framework
or deployment package.

## Dependency direction

```text
core ← engine ← capabilities
core ← sandbox ← sandbox-local
core ← engine ← session ← storage adapters
engine + session + adapters ← platform packages
core + engine ← testing
core + engine + capabilities ← effect-agent (umbrella) ← pr-review
```

An inward package cannot import an outward package. If a feature appears to require that, the
solution is an inward port with an outward adapter, not a dependency reversal.
