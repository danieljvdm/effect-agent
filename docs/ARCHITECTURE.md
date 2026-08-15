# Technical architecture

Status: **Draft**

## 1. Architectural goals

The architecture must make these properties structural rather than conventional:

1. Agent operations are Effects and event delivery is a Stream.
2. Expected failures and requirements remain visible in types.
3. Effect Schema defines every untrusted, wire, model, and persisted value.
4. Effect AI provider Models, database, transport, and platform implementations are replaceable
   through Layers and explicit services.
5. Execution is a deterministic state machine with structured concurrency.
6. Ephemeral interruption and durable abort remain distinct.
7. Canonical state can be replayed without re-executing external effects.
8. Durability can be added without replacing the authoring model.
9. Effect AI types compose directly; provider SDK, database, transport, and platform types do not
   leak into durable domain interfaces.
10. Tests can control time, IDs, providers, crashes, and scheduling.

## 2. Concentric architecture

```text
┌─────────────────────────────────────────────────────────────┐
│ Hosts: Node, Cloudflare, CLI, HTTP/SSE                      │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Adapters: providers, stores, MCP, sandbox, telemetry  │  │
│  │  ┌─────────────────────────────────────────────────┐  │  │
│  │  │ Capabilities: tools, skills, subagents, compact│  │  │
│  │  │  ┌───────────────────────────────────────────┐ │  │  │
│  │  │  │ Engine: turns, scheduling, policies      │ │  │  │
│  │  │  │  ┌─────────────────────────────────────┐  │ │  │  │
│  │  │  │  │ Domain + ports + Effect Schema     │  │ │  │  │
│  │  │  │  └─────────────────────────────────────┘  │ │  │  │
│  │  │  └───────────────────────────────────────────┘ │  │  │
│  │  └─────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

Dependencies point inward. Inner packages define interfaces; outer packages provide Layers.

## 3. Modules

The architecture describes the intended package boundaries, but the repository creates a package
only when its roadmap phase begins. The current tree (through Phase 6) is:

```text
packages/
  effect-agent
  core
  engine
  capabilities
  sandbox
  sandbox-local
  session
  storage-memory
  storage-sqlite
  storage-cloudflare
  platform-node
  platform-cloudflare
  pr-review
  testing
examples/
  demo
  pr-review
  providers
  repo-ops
action/
```

There is no `apps/` workspace. Reusable Travel Planner fixtures live in the leaf testing package;
`examples/demo` consumes public framework packages as a local browser bench. `examples/providers`
binds the shared Travel Planner Definition directly to upstream OpenAI and Anthropic Effect AI
Models as a compile-only leaf proof. Examples are outside the framework dependency graph and do not
define a deployment boundary.
Platform hosts are library packages that applications can assemble later.

| First phase | Packages introduced                           |
| ----------: | --------------------------------------------- |
|          P0 | `core`, `engine`, `testing`                   |
|          P2 | `capabilities`, `sandbox`, `sandbox-local`    |
|          P3 | `session`, `storage-memory`, `storage-sqlite` |
|          P4 | `platform-node`                               |
|          P6 | `storage-cloudflare`, `platform-cloudflare`   |

Do not create provider wrapper packages. Do not split a capability into its own package until it
has an independently useful dependency or release boundary.

### `@effect-agent/core`

Owns:

- branded identifiers;
- Agent, policy, framework message, error, and event Schemas;
- immutable authoring constructors;
- common type utilities;
- no live provider, store, filesystem, network, or platform implementation;
- direct composition with Effect AI `Tool`, `Toolkit`, `Model`, and `Prompt`.

Allowed dependencies: the pinned Effect v4 package, including `effect/unstable/ai`.

### `@effect-agent/engine`

Owns:

- one canonical Run interpreter;
- Turn state machine;
- context preparation;
- Effect AI Response stream reduction;
- Effect AI Tool execution policy;
- stopping, retry, and interruption policies;
- semantic live event publication;
- dependency-neutral operational capability seams.

It depends only on core. The framework-owned ports needed by the interpreter are defined inward,
initially in core; a separate ports package is created only if the concrete dependency graph later
proves it necessary.

### `@effect-agent/session`

Owns:

- Conversation Log record Schemas;
- pure projections and reducers;
- Submission Ledger interface;
- durable recovery classifier;
- Receipt, reattachment, and Settlement interfaces;
- stored-version compatibility checks.

Phase 3 implemented the canonical Conversation records, pure replay/checkpoint projections,
definition digests, and `ConversationStore`. Phase 4 replaced the interim non-durable
`SubmissionStore` stub with the `SubmissionLedger` port and added the `WakeScheduler` port, the
pure recovery classifier, the run journal, and the `DurableAgentRuntime` coordinator (Receipt,
Attempt, Settlement). The session package now depends on `@effect-agent/engine` to drive the
interpreter through its public seams (ADR-0011).

### `@effect-agent/capabilities`

Owns generic optional modules:

- approvals;
- steering and follow-ups;
- model-context compaction;
- Skill activation;
- Subagent spawning;
- the Code Mode Tool builder over the sandbox `CodeExecutor` port (ADR-0017);
- ephemeral multi-Run Conversation state;
- approval, budget, context-compaction, safe-seam input, scheduling, and MCP adapters.

The package depends on engine seams and, for Code Mode, on the inward `@effect-agent/sandbox`
executor port; it must not become a grab-bag context object.
The initial package remains consolidated so the project can validate the capability boundaries
before splitting them.

### `@effect-agent/sandbox`

Owns narrow filesystem, process, path, and network capability ports plus Effect AI Tool definitions.
It also owns the callback-capable `CodeExecutor` port for Code Mode (ADR-0017), a sibling of the
command-shaped `Sandbox` service.
Implementations live in outward packages. `@effect-agent/sandbox-local` is the Phase 2 explicitly
unisolated local-process implementation: it enforces its declared request/output/time bounds and
rejects policy it cannot enforce.

### MCP

Use Effect AI MCP protocol, schema, server, and dynamic Tool facilities directly. Add a
framework package only for agent-specific policy or durability behavior that Effect AI does not
own.

### Leaf adapters

Examples:

- `@effect-agent/storage-memory`
- `@effect-agent/storage-sqlite`
- `@effect-agent/storage-cloudflare`
- `@effect-agent/sandbox-local`

The Phase 3 memory and SQLite adapters implement the same Conversation Store semantics: atomic
fenced batch append, bounded forward read, resumable observation, portable export, and
digest-bound disposable checkpoints. Neither adapter durably accepts work in this phase.

Model providers come from Effect AI provider packages and are supplied as `Model`/`LanguageModel`
Layers. The framework does not wrap them in its own provider packages.

### Hosts and transports

- `@effect-agent/platform-node`
- `@effect-agent/platform-cloudflare`
- `@effect-agent/testing`

Platform packages assemble Layers but are still libraries, not application entrypoints.
`@effect-agent/testing` owns the scripted model test kit and the leaf Travel Planner integration
fixture; production packages never depend on it.
Transport and CLI packages are deferred until a concrete internal consumer demonstrates the
boundary. Transports authenticate, authorize, decode, call runtime interfaces, and encode results;
they never contain Turn logic.

## 4. Core domain

All stable identifiers are opaque Schema brands:

```ts
const AgentId = Schema.String.pipe(Schema.brand("AgentId"));
const ConversationId = Schema.String.pipe(Schema.brand("ConversationId"));
const SubmissionId = Schema.String.pipe(Schema.brand("SubmissionId"));
const AttemptId = Schema.String.pipe(Schema.brand("AttemptId"));
const RunId = Schema.String.pipe(Schema.brand("RunId"));
const TurnId = Schema.String.pipe(Schema.brand("TurnId"));
const ToolCallId = Schema.String.pipe(Schema.brand("ToolCallId"));
const ReceiptId = Schema.String.pipe(Schema.brand("ReceiptId"));
const SettlementId = Schema.String.pipe(Schema.brand("SettlementId"));
const EventOffset = Schema.String.pipe(Schema.brand("EventOffset"));
```

IDs are not authorization capabilities and do not encode mutable domain data.

### Separate representations

The framework maintains distinct types for:

- `ConversationMessage` — canonical framework transcript;
- Effect AI `Prompt` — model-facing input;
- Effect AI `Response.StreamPart` — model-facing output stream;
- `RunEvent` — stable semantic live event;
- `CanonicalRecord` — versioned durable fact;
- `ClientProjection` — render-ready public view.

No provider SDK object crosses these seams.

### Agent Definition

Conceptual interface:

```ts
interface AgentDefinition<Input, Output, E, R, Tools> {
  readonly id: AgentId;
  readonly input: Schema.Schema<Input>;
  readonly output: Schema.Schema<Output>;
  readonly instructions: InstructionSource<Input, E, R>;
  readonly toolkit: Toolkit.Toolkit<Tools>;
  readonly policy: AgentPolicy;
  readonly context: ReadonlyArray<ContextTransform<any>>;
  readonly metadata: AgentMetadata;
}

interface AgentBinding<Definition, ModelValue> {
  readonly definition: Definition;
  readonly model: ModelValue;
}
```

An Agent Definition is model-agnostic and may be inspected or reused without acquiring resources.
`Agent.withModel` creates the immutable Agent Binding accepted by the runtime. Requirements and
failures are inferred from instruction sources, context transforms, handlers, policies, schema
encoding/decoding, and the bound Effect AI Model Layer.

### Runtime results

```ts
interface AgentResult<A> {
  readonly output: A;
  readonly conversationId: ConversationId;
  readonly runId: RunId;
  readonly turns: number;
  readonly usage: Usage;
  readonly finishReason: "completed" | "model-stop";
  readonly transcript: Transcript;
}
```

Approval suspension, budget exhaustion, interruption, and failed output decoding are not successful
finish reasons.

## 5. Ports

Framework ports are Effect Context keys whose methods return Effects or Streams.

Required core services:

- Effect AI `LanguageModel` and `Model` metadata;
- Effect AI Toolkit handler services;
- `IdGenerator`
- `Clock` through Effect's clock module
- `RunEventSink` for internal semantic publication

Durable ports (all owned by `@effect-agent/session`; see [deployment §3.1](spec/deployment.md)):

- `ConversationStore` — the canonical Conversation Log plus digest-bound disposable checkpoints
  (no separate `CheckpointStore` port exists)
- `SubmissionLedger` — admission, FIFO-head claims, Attempt identity, ownership tokens,
  producer-epoch fencing, leases, settlement, and abort intent (absorbs the earlier
  `AttemptOwnership` prose service)
- `WakeScheduler` — droppable liveness hints paired with ledger scans

Deferred durable ports: `AttachmentStore` waits for a real digest-addressed attachment
requirement; `RecoveryScheduler` waits for recovery cadence needs beyond the host startup pass
and wake/scan loop.

Optional capability ports:

- `ApprovalPolicy`
- `SkillRepository`
- `AgentSpawner`
- `SandboxFactory`
- `McpClient`
- `ContentPolicy`

Every port documents idempotency, interruption, resource, ordering, and concurrency semantics—not
only method types.

## 6. Engine shape

The interpreter is an explicit transition machine:

```text
prepare input
    ↓
prepare context/resources
    ↓
stream model response
    ↓
reduce and validate assistant response
    ↓
assistant final? ── yes ─→ decode output ─→ complete
    │ no
    ↓
validate complete Tool batch
    ↓
schedule and execute bounded Tools
    ↓
commit deterministic Tool results
    ↓
drain steering
    ↓
evaluate Stop Policy
    ↓
next Turn or terminate
```

There is one implementation. `run` drains and materializes the same stream used by `stream`.

The durable runtime adds commit seams around the same semantic transitions. It does not implement a
second agent loop.

## 7. Structured concurrency

One Run/Attempt owns one parent Scope. The model stream, Tool fibers, MCP calls, sandbox processes,
event queues, and attached Subagents are children.

- `FiberSet` tracks owned fibers.
- `Semaphore.withPermit` enforces global, per-Agent, per-Run, and Tool-group limits and releases
  permits under success, failure, and interruption.
- `Effect.acquireRelease` owns clients, sandboxes, subscriptions, and Attempt ownership.
- Scope closure interrupts children and runs finalizers.
- The engine never uses detached/daemon fibers.

Tool progress MAY reflect completion order. Canonical Tool results MUST follow declaration order.

## 8. Error model

Expected failure is represented with closed tagged unions:

- `AgentInputError`
- `InstructionError`
- Effect AI `AiError`
- `ToolInputError`
- Tool-declared expected failure
- `ToolInfrastructureError`
- `ToolOutputError`
- `PolicyDenied`
- `ApprovalRequired`
- `BudgetExceeded`
- `ConversationConflict`
- `PersistenceFailure`
- `AgentInterrupted`

Defects remain defects in `Cause`. At durable seams, defects are classified and redacted into a
stable `DurableFailure`; raw Cause values and stacks are never serialized as canonical public data.

The shipped durable seams keep their own closed typed unions: admission and ledger failures
(`AdmissionConflict`, `OwnershipLost`, `SettlementConflict`, `LedgerError`), store conflicts
(`AppendConflict`, `FenceRejected`, `ConversationStoreError`), and coordinator failures
(`RunJournalError`, failpoint errors). None of them widen to `unknown` or `Error`.

## 9. Events and backpressure

Stable semantic Run Events include:

- `RunStarted`
- `TurnStarted`
- `ModelStarted`
- `TextDelta`
- optional `ReasoningDelta`
- `ToolCallDeclared`
- `ToolCallStarted`
- `ToolProgress`
- `ToolCallSucceeded`
- `ToolCallFailed`
- `ApprovalRequested`
- `TurnCompleted`
- `RunCompleted`
- `RunFailed`
- `RunInterrupted`

Raw Effect AI Response parts and provider diagnostics are separate streams from stable
framework Run Events.

Local streams use bounded queues and backpressure by default. Detached transports journal semantic
state first and permit reconnect from an offset. A slow HTTP client must not own engine liveness.

## 10. Effect dependency policy

The framework uses these Effect foundations directly:

- `Effect`
- `Schema`
- `Layer`
- `Context`
- `Scope`
- `Stream`
- `Queue`
- `PubSub`
- `Fiber`/`FiberSet`
- `Semaphore`
- `Schedule`
- `Clock`
- `effect/unstable/ai`, including Tool, Toolkit, LanguageModel, Prompt, Response, Chat, and Model.

Effect v4 is pinned exactly during private development. An Effect upgrade is an explicit
repository-wide change with compile-time and runtime verification.

The root Bun catalog is the single version source. `scripts/sync-effect-submodule.ts` derives the
matching `effect@<version>` source tag from that catalog. Workspace packages use `catalog:` and may
not declare independent Effect ranges.

Other unstable Effect facilities may also be adopted directly when they fit the product. Do not
wrap them solely because they are marked unstable. Do keep platform and persistence choices behind
Effect services so Node and Cloudflare implementations remain interchangeable.

## 11. Package dependency graph

```text
effect + effect/unstable/ai
             ↓
           core
        /          \
     engine       sandbox
      /   \          ↑
capabilities session sandbox-local
               ↑
        storage adapters
               ↑
       platform packages

Effect AI Model Layers -> engine
sandbox <- capabilities (the CodeExecutor port consumed by Code Mode, ADR-0017)
sandbox <- platform packages (isolated CodeExecutor adapters, ADR-0017)
engine + selected adapters <- platform package
core + engine <- testing
core + engine + capabilities <- effect-agent (umbrella) <- pr-review (ADR-0016)
```

No cycle is permitted. Production packages never depend on `testing`. Package graph checks are part
of CI.

## 12. Cross-cutting invariants

1. A complete Tool input is schema-decoded before the Handler starts.
2. Truncated Tool arguments never execute.
3. Each Tool Call has at most one canonical terminal result.
4. Canonical transcript order is deterministic.
5. Tool/model/Subagent concurrency is bounded.
6. Every resource belongs to a Scope.
7. Parent interruption reaches every attached child.
8. Provider SDK types do not cross Effect AI provider Layers into canonical records.
9. Persisted and wire values are decoded, never asserted.
10. Public semantic events do not precede required durable commits.
11. Replay never executes external effects to rebuild state.
12. Secrets never enter canonical records through provider metadata.
13. A stale durable owner cannot append after replacement.
14. Every acknowledged Submission eventually has one Settlement.

## 13. Forbidden architecture patterns

- Mutable event-emitter Agent instances as the primary interface.
- React-style synchronous authoring hooks.
- Global registries for providers or Tools.
- A giant Session context object with optional capabilities.
- Callback middleware as a second effect system.
- A framework-owned duplicate of Effect AI Tool, Toolkit, LanguageModel, Prompt, Response, or
  Model.
- Independent transcript and event-store sources of truth.
- Promise-first interfaces with Effect wrappers.
- Automatic retry of unresolved ordinary Tools.
