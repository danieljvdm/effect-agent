# Deployment Specification

Status: Draft

This document defines supported host shapes and the conditions under which a
deployment may claim ephemeral, persistent, or durable behavior.

## 1. Deployment classes

### Class E — Ephemeral

- one process;
- in-memory queue and state;
- no recovery promise after process loss;
- suitable for libraries, scripts, tests, and request-scoped agents.

The `examples/pr-work-orders` work-order host is class E trusted-local evidence.
Its one-attempt policy is process-local, its worktree is scoped to one run, and
its branch update is locally compare-and-swap fenced. It makes no
restart-recovery, hosted publication, or untrusted-code isolation claim. See
[pull-request work orders](pr-work-orders.md). GitHub dispatch, file-backed
attempt admission, isolated checks, and network compare-and-swap live in
`examples/pr-work-order-ingress` and [work-order ingress](pr-work-order-ingress.md).
That leaf is not an enabled workflow.

### Class P — Persistent

- canonical conversation and session data survives process restart;
- no accepted-work settlement guarantee;
- suitable for interactive applications that can ask a client to retry.

### Class DN — Node/SQLite durable

- durable admission and settlement;
- process restart recovery;
- one active scheduler node;
- local SQLite storage;
- host loss outside storage recovery objectives may still lose availability.

### Class DC — Cloudflare durable

- one SQLite-backed Durable Object per Conversation;
- Cloudflare Workers provide stateless ingress;
- Durable Object storage owns Conversation state and queue order;
- alarms wake dirty or autonomously actionable work while stable external waits may quiesce;
- no PostgreSQL dependency.

No package or example may use “durable” without naming DN or DC and the tested adapter.

## 2. Node.js host

Node.js is the first production host.

Minimum host responsibilities:

- build a root Effect Layer;
- validate configuration before opening admission;
- start HTTP/transport endpoints and scheduler fibers in a Scope;
- expose readiness and liveness separately;
- stop admission during graceful shutdown;
- drain or release active Attempt ownership within a configured deadline;
- flush telemetry without blocking settlement indefinitely;
- close provider, database, sandbox, and MCP resources;
- exit nonzero on unrecoverable root-fiber failure.

The supported Node.js version is pinned in the package metadata and CI matrix.

## 3. Node process roles

A small deployment may combine roles. A larger deployment may separate:

- **API/admission**: authenticates clients and durably accepts submissions;
- **scheduler**: claims runnable submissions and starts attempts;
- **worker**: runs interpreter Attempts and renews ownership when the platform requires it;
- **projector**: builds read models and search indexes;
- **reconciler**: resolves recoverable or unknown operations;
- **operator API/UI**: exposes administrative actions and audit views.

Correctness cannot depend on role co-location or in-memory notifications.

## 3.1 Platform Effect services

The durable runtime requires capabilities rather than a platform name. The shipped inventory is
exactly three ports, all owned by `@effect-agent/session`:

```ts
class ConversationStore extends Context.Service<ConversationStore, {...}>()(
  "@effect-agent/session/ConversationStore",
) {}

class SubmissionLedger extends Context.Service<SubmissionLedger, {...}>()(
  "@effect-agent/session/SubmissionLedger",
) {}

class WakeScheduler extends Context.Service<WakeScheduler, {...}>()(
  "@effect-agent/session/WakeScheduler",
) {}
```

- `ConversationStore` owns the canonical Conversation Log: materialization, fenced atomic batch
  append, bounded reads, resumable observation, export, tail inspection, and digest-bound
  disposable checkpoints (there is no separate `CheckpointStore` port).
- `SubmissionLedger` owns operational obligations: admission, readiness, FIFO-head claims,
  ownership tokens, producer-epoch fencing, lease renewal/release, canonical-input markers,
  settlement reservation/finalization, abort intent, nonterminal scans, and recovery snapshots.
  It absorbs the earlier `AttemptOwnership` prose service; claims mint Attempt identity and
  fencing evidence atomically with queue-head selection.
- `WakeScheduler` is a pure liveness hint whose notifications may be dropped, coalesced, or
  duplicated. Workers pair the all-lanes stream with ledger scans. Public progress waits use a
  separate conversation-keyed, Scope-owned one-shot registration: subscribe first, read one
  canonical record second, then park. The canonical read is authoritative; the notification only
  tells the caller to read again.

Earlier drafts referred to a `DurableStorage` service; that was prose shorthand and no such port
exists. Two further ports are explicitly deferred: an `AttachmentStore` (digest-addressed durable
attachments) waits for a real attachment requirement, and a `RecoveryScheduler` waits for
recovery cadence needs beyond the host's startup pass and wake/scan loop.

Node Layers implement these with local SQLite transactions, process ownership, and a local
scheduler. Cloudflare Layers implement them with SQLite-backed Durable Object storage, object
ownership, and alarms. The semantic coordinator depends on these services and has no conditional
branch for `node` versus `cloudflare`.

## 4. Configuration

Configuration uses Effect Config and Effect Schema. It is resolved once during
Layer construction and exposed as typed services.

Configuration families include:

- deployment identity and environment;
- host role enablement;
- model providers and routing;
- store connections and pools;
- ownership, retry, timeout, and queue limits;
- telemetry exporters;
- security and redaction policy;
- approval providers;
- sandbox policy;
- retention;
- feature flags and compatibility gates.

Secrets are resolved through a secret provider and wrapped as redacted values.
Startup diagnostics may list missing secret names but never values.

## 5. Startup

The host performs these gates before readiness:

1. decode all configuration;
2. construct required Layers;
3. connect to durable dependencies;
4. verify store schema compatibility;
5. verify framework and adapter feature compatibility;
6. acquire or validate deployment identity;
7. start schedulers and projectors;
8. run a shallow self-check;
9. enable admission;
10. report ready.

Failure before step 9 means the process is not ready. A partially available provider
may be tolerated only if routing policy has another eligible provider.

## 6. Shutdown

On shutdown:

1. fail readiness and stop new admission;
2. stop claiming new work;
3. signal active runs to reach a safe point;
4. continue Attempt ownership renewal during the drain window where applicable;
5. commit any ready settlements;
6. release or allow expiry of unresolved ownership;
7. close resources in reverse Layer acquisition order;
8. flush bounded telemetry;
9. exit.

Forced termination is assumed possible at every step. The durability protocol, not
graceful shutdown, provides correctness.

## 7. Health

Liveness answers whether the process event loop and root supervisor are operating.
Readiness answers whether the process can fulfill its enabled role.

Readiness for admission requires:

- compatible durable store;
- ability to atomically admit;
- authorization configuration;
- at least one viable routing path if the API promises immediate execution.

Readiness for workers requires:

- compatible durable store;
- scheduler clock/ownership health;
- required model and capability Layers;
- no deployment-wide safety stop.

Provider degradation, queue saturation, and projection lag are surfaced separately
from basic process health.

## 8. Scaling and backpressure

Scaling signals include:

- runnable submission age;
- queue depth by tenant and priority;
- active attempts;
- provider concurrency and rate-limit saturation;
- database transaction latency/conflicts;
- event subscriber lag;
- sandbox capacity;
- settlement obligation age.

Admission implements configured global, tenant, conversation, and principal quotas.
Overload returns a typed rejection or a durably queued receipt according to policy.
It never accepts work only into an unbounded in-memory queue.

## 9. Version changes during private development

There is no rolling data-version or migration promise.

- Node development deployments stop, replace code, and reset incompatible SQLite data.
- Cloudflare development deployments replace incompatible development namespaces when needed.
- Stored version mismatches fail before mutation.
- Production-like durability tests use one repository version at a time.

Rolling compatibility is designed only when internal deployment needs or external release require
it.

## 10. Disaster recovery

Each durable adapter documents:

- recovery point objective;
- recovery time objective;
- backup schedule and encryption;
- restore verification frequency;
- point-in-time recovery procedure;
- reconciliation of restored ledger state;
- producer epoch invalidation after restore;
- handling of external side effects newer than the restored database.

Restore drills are part of release readiness for a durable compatibility label.

## 11. Cloudflare host

Cloudflare is a first-class target alongside Node. The mapping is:

- Workers for API and stateless orchestration;
- one SQLite-backed Durable Object for each Conversation's serialization, history, ledger, and
  alarms;
- R2 for large artifacts;
- an optional rebuildable store for cross-Conversation administration;
- platform-native observability adapters.

Cloudflare platform APIs are wrapped as Effect services and supplied through Layers. A
Conversation runtime requires storage, scheduling/alarm, clock, attachment, and observability
services rather than importing bindings in the engine.

`CloudflareDurableRuntimeOptions.bindings` accepts a resolved array, a closed Effect, or a
per-incarnation callback. The callback runs after the Object's Conversation and producer
identities are derived and receives the live `DurableObjectState`, raw Worker environment,
`conversationId`, and `producerId`. This is the host boundary for capturing environment-backed
resources such as Worker service bindings; database clients and other request-scoped resources
remain outside the cached Durable Object runtime.

`CloudflareDurableRuntimeOptions.runContext` is the generic model-context extension boundary. It
accepts either a closed `CloudflareRunContextLayer` or a per-incarnation factory receiving the same
explicit Object state, environment, Conversation identity, and producer identity. The Layer must
provide `RunContextPreparation`; its only permitted remaining requirement is `Crypto.Crypto`,
which the platform supplies with `BrowserCrypto`. A host using the capabilities compactor closes
its own service dependencies before returning the adapter:

```ts
runContext: ({ env }) =>
  contextCompactorRunContextLayer.pipe(Layer.provide(makeContextCompactorLayer(env)));
```

The factory is evaluated once and the Layer is built in the cached runtime Scope for each Durable
Object incarnation. Normal Scope closure runs finalizers, but correctness never depends on a
finalizer during eviction: Cloudflare may discard in-memory state and construct a new incarnation
without a shutdown callback. The reconstructed Layer receives canonical history again before it
prepares model context. The default is absence/pass-through, so existing Conversation Object
construction remains compatible.

`runContext`, like `bindings`, `toolReconciler`, failpoints, and authorization services, is a
non-serializable Layer option and is deliberately outside `CloudflareDurableRuntimeConfigValue`.
That Schema continues to decode only scalar identities, cadences, limits, and storage gates before
resources open.

Durable Object storage is the only correctness-critical store for that Conversation. In-memory
object state is a cache because objects may stop unexpectedly. Alarm work is idempotent because
alarms execute at least once.

Conversation maintenance is incremental rather than a perpetual nonterminal poll. Each Object
stores a versioned `dirty`/`processed` generation beside its single alarm slot:

- every public or routed mutation advances `dirty` and establishes the alarm in one storage
  transaction before its first durable effect;
- a short incarnation-local gate serializes that pre-arm boundary with pass generation snapshots
  and acknowledgements, but never spans the mutation body or cross-Object I/O;
- a pass acknowledges only the generation it observed. It cannot acknowledge while an RPC/port
  mutation remains in flight, and a later racing generation therefore retains its alarm;
- after eviction the in-memory gate/count resets, while the durable unprocessed generation and
  pre-armed alarm cause recovery to resume any committed autonomous work;
- once classified as a stable externally-driven wait (`ApprovalPending`, unresolved ordinary
  outcome, joined child, or child awaiting parent establishment), the observed generation is
  acknowledged and the alarm clears. Its resolving mutation re-establishes both generation and
  alarm before changing the wait;
- autonomous retry, indeterminate admission/establishment, lease recovery, and other locally
  actionable states leave the generation unprocessed and rearm with bounded backoff;
- a forced alarm with `processed >= dirty` reads only the maintenance record, clears the alarm,
  and returns. It performs no runtime recovery, ledger scan, or canonical-history read.

Child-to-parent settlement follows the same quiescent contract. After the exact child Settlement
record is canonical but before child ledger finalization, the child routes the idempotent durable
settlement marker to the parent. That routed mutation pre-arms and dirties the parent, so child
eviction after finalization cannot strand a quiescent parent. Same-store ledgers accept this
notification only for the exact `terminalizing` reservation; earlier states fail closed.

`CloudflareConversationClient.awaitProgress(conversationId, afterSequence)` carries that same
Effect-native boundary across RPC. A normal wait performs no periodic read and creates no alarm
loop. Every canonical append and the durable approval/unknown/abort/settlement transitions emit a
best-effort hint after their authoritative commit. RPC interruption sends a scoped cancellation;
an Object eviction rejects the old call, after which the client retries only a platform-classified
reset with a fresh stub. The reconstructed Object subscribes and rereads canonical storage before
parking, so disposable memory can neither strand the caller nor impersonate durable progress.
Each logical client wait obtains one UUID from the explicit `Crypto.Crypto` capability and reuses it
across reset attempts. That identity groups duplicate transport attempts for cancellation but
remains disposable coordination state; canonical records alone establish durable progress.
Host-supplied `CloudflareDurableRuntimeOptions.operationAuthorizer` decisions cross observation,
progress, approval, and unknown-resolution RPCs as the typed `OperationDenied`.

The target is no longer experimental: the generic durability conformance suite —
the same adapter-neutral case arrays the Node adapters run — passes inside workerd, and the
eviction (per-failpoint `ctx.abort()` with alarm-only convergence), alarm-retry (double-fire
and throw-retry), runtime-restart (Miniflare dispose/reopen over persisted storage), and
fault-injection (failpoints on every durable mutation plus routed-transport faults) scenarios
are implemented and green (see the platform-cloudflare test suites). The tested harness is
workerd/Miniflare; the hosted production service, its observability adapters, and live soak
remain explicitly unclaimed (see the certification suites), and hosted-service operation stays
outside the claims until open-source preparation revisits it.

### Dynamic Worker Code Mode executor

The first isolated `CodeExecutor` adapter is a Cloudflare Dynamic Worker Layer in
`@effect-agent/platform-cloudflare`. Each pass creates one fresh Worker through the
Worker Loader with `globalOutbound: null`, supplies only the scoped Tool-broker RPC stub and
explicitly allowed structured values, applies the configured Dynamic Worker CPU and subrequest
limits plus an executor-owned wall-clock deadline (an asynchronously suspended pass consumes no
CPU and must not outlive its deadline), invokes one fixed entrypoint, validates the returned
envelope through Effect Schema, and disposes the entrypoint and Worker handles in Scope
finalizers. Host callbacks run one at a time on retained independent root Effect fibers so their
return RPC is not coupled to the still-open guest RPC; pass teardown closes callback admission,
interrupts and awaits active work, and settles queued calls. One absolute monotonic deadline
applies to the worker RPC and every host callback. A synchronous runaway program is stopped by
platform CPU limits, not only by a JavaScript timer.

The adapter records no persistent state and adds no deployment-class claim beyond `E`: the `DN`
and `DC` assemblies make no Code Mode claim until this specification says otherwise. The tested harness is
workerd/Miniflare; hosted-platform evidence remains unclaimed. No cost or performance claim is
made before measurement — current Dynamic Workers billing counts no-ID `load()` use as a new
Dynamic Worker per invocation, and any future stable-ID Worker caching must include tenant and
binding context in cache identity.

Current platform references:

- [SQLite-backed Durable Object storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- [Durable Object alarms](https://developers.cloudflare.com/durable-objects/api/alarms/)
- [Dynamic Worker Loader API](https://developers.cloudflare.com/dynamic-workers/api-reference/)

## 12. Packaging and release

- framework and platform code live in owner-gated `packages/*`; private runnable benches may live
  in leaf `examples/*`, and there is no deployable `apps/` workspace;
- the root Bun catalog pins the exact Effect v4 version before 1.0;
- workspace manifests consume that version through `catalog:` and may not introduce another copy;
- `platform-cloudflare` publishes `effect-cf` as a compatible caret peer while the root catalog
  selects its exact development version, so the host owns one shared Effect service identity;
- `platform-node` and `platform-cloudflare` are Layer-assembly libraries, not application
  entrypoints;
- releases include generated API docs, changelog, and supported Effect/platform versions;
- canary tags precede stable tags;
- durable adapters may have a maturity label independent of the core engine;
- examples pin package versions and identify their deployment class.

## 13. Requirements

- **DEPLOY-001**: Every deployment declares E, P, DN, or DC behavior.
- **DEPLOY-002**: Node.js is the first supported host.
- **DEPLOY-003**: Configuration is schema-validated before readiness.
- **DEPLOY-004**: Readiness is role-specific and distinct from liveness.
- **DEPLOY-005**: Shutdown stops admission before draining workers.
- **DEPLOY-006**: Correctness survives forced termination; it does not depend on
  graceful shutdown.
- **DEPLOY-007**: Admission has explicit bounded quota and overload behavior.
- **DEPLOY-008**: Private development fails clearly on incompatible stored versions and makes no
  rolling compatibility promise.
- **DEPLOY-009**: Durable adapters publish and test backup/restore procedures.
- **DEPLOY-010**: Cloudflare platform bindings are supplied as Effect services/Layers and remain
  experimental until they pass the shared durability suite.
- **DEPLOY-011**: The Dynamic Worker Code Mode executor denies ambient egress, enforces platform
  CPU and executor wall-clock limits, disposes Worker and RPC handles in Scope finalizers, and
  claims deployment class `E` only.
- **DEPLOY-012**: Cloudflare Conversation maintenance is generation-incremental and quiescent for
  stable external waits; pre-armed mutations, pass acknowledgement, restart recovery, and
  autonomous rearming obey the protocol above.
- **DEPLOY-013**: A Cloudflare host context-preparation Layer is acquired once per Object
  incarnation, runs only after canonical resume reconstruction, never replaces canonical history,
  and is reconstructible without process-global state.
