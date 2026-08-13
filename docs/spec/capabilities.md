# Capability Specification

Status: Draft

This document specifies the capabilities surrounding the core agent interpreter. A
capability is an Effect service with an explicit contract, error channel, resource
lifetime, and event surface. Capabilities may be omitted from a runtime. The engine
must detect absence explicitly; it must not silently substitute weaker behavior.

## 1. Capability matrix

| Capability                 | First target | Required for ephemeral core |    Required for durable runtime |
| -------------------------- | -----------: | --------------------------: | ------------------------------: |
| Tools and toolkits         |           P1 |                         Yes |                             Yes |
| Sessions and conversations |           P2 |                          No |                             Yes |
| Steering and follow-up     |           P2 |                          No |                              No |
| Approval                   |           P2 |                          No |          For configured actions |
| Compaction                 |           P2 |                          No | Yes for unbounded conversations |
| Skills                     |     Deferred |                          No |                              No |
| MCP client                 |           P2 |                          No |                              No |
| Sandbox                    |           P2 |                          No |     For untrusted commands/code |
| Subagents                  |     Proposed |                          No |                              No |
| Persistent agent state     |           P4 |                          No |                              No |
| Durable steps              |           P5 |                          No |                              No |

“Required for durable runtime” means the durable host must supply the service when
the related behavior is enabled. It does not mean every deployment enables every
capability.

## 2. Tool execution

The engine owns durable Tool scheduling. Effect AI emits Tool Call Response parts,
and the engine invokes the original Effect AI Toolkit handlers.

Every tool call passes through this pipeline:

1. Reduce Effect AI Response parts into a complete Tool Call.
2. Resolve the Tool in the Effect AI Toolkit.
3. Let Effect AI decode input with the Tool's Effect Schema.
4. Evaluate authorization and Effect AI approval policy.
5. Acquire the required Effect `Semaphore` permits.
6. Execute the Effect AI Toolkit handler within a scoped child Fiber; release permits on every
   exit.
7. Encode success or a declared tool-domain failure.
8. Redact and bound the result for model context and persisted events.
9. Commit the result in deterministic order.

The framework distinguishes:

- `ToolSuccess`: a successful declared output;
- `ToolRejected`: policy or approval denied execution;
- `ToolInvalidInput`: schema decoding failed;
- `ToolFailure`: the handler failed in the typed Effect error channel;
- `ToolDefect`: the handler died, was interrupted unexpectedly, or violated its
  output schema;
- `ToolUnknownOutcome`: an external side effect may have happened but no canonical
  result was recorded.

The tool call ID is stable across logging, approval, execution, and recovery.

## 3. Sessions and conversations

A `SessionStore` manages named session records. A session may point to one or more
conversations but is not itself the canonical conversation history.

```ts
export interface SessionStore {
  readonly create: (input: SessionCreate) => Effect.Effect<Session, SessionStoreError>;

  readonly get: (id: SessionId) => Effect.Effect<Option.Option<Session>, SessionStoreError>;

  readonly update: (
    id: SessionId,
    patch: SessionPatch,
    expectedRevision: Revision,
  ) => Effect.Effect<Session, SessionConflict | SessionStoreError>;
}
```

Session metadata is schema-versioned and must use optimistic concurrency. Mutable
session metadata must never be used as the sole source of truth for accepted work.

## 4. Steering and follow-up

Steering adds input before the next model request at a defined safe point. Follow-up
adds input only when the Agent would otherwise stop.

The initial contract is:

- steering is accepted only while a run is active;
- the current model response and active Tool batch finish normally;
- accepted steering becomes canonical before the next model request;
- follow-ups enter the same admission path as ordinary submissions;
- per-conversation FIFO ordering applies;
- steering and follow-up return receipts when durable admission is enabled.

Steering is not an out-of-band mutable string. It is observable input with an ID,
timestamp, author, and admission result.

## 5. Approval

Effect AI Tool `needsApproval` defines whether approval is needed. The runtime's
approval resolver is a first-class Effect service rather than an untracked callback.

```ts
export interface Approval {
  readonly request: (request: ApprovalRequest) => Effect.Effect<ApprovalDecision, ApprovalError>;
}
```

An approval request includes:

- run, conversation, tool, and tool-call identity;
- a human-readable action summary;
- normalized resource targets;
- risk classification;
- redacted input preview;
- expiration time;
- whether denial is terminal or recoverable.

An approval provider may be interactive, policy-based, remote, or test-controlled.
Approval decisions are canonical audit events. A timeout is a denial unless the
configured policy explicitly says otherwise.

## 6. Compaction

Compaction converts a prefix of conversation history into a smaller context
representation. It does not rewrite or delete the canonical log.

A compactor returns:

```ts
interface CompactionResult {
  readonly coversThrough: ConversationSequence;
  readonly summary: ModelMessage;
  readonly retainedFacts: ReadonlyArray<RetainedFact>;
  readonly tokenEstimate: number;
  readonly sourceDigest: Digest;
  readonly compactorVersion: string;
}
```

Requirements:

- input coverage is an explicit sequence range;
- the source digest binds the result to that exact range;
- summaries are versioned;
- failed compaction cannot corrupt or advance conversation state;
- replay may reuse a valid compaction artifact;
- operators may rebuild compactions from the canonical log;
- secrets excluded by policy must not enter the summary.

Compaction quality is evaluated separately from persistence correctness.

## 7. Skills

A skill is a versioned package of instructions, schemas, assets, and optional
capability requirements. Loading a skill produces immutable agent context for a run
or turn.

Each skill manifest declares:

- stable ID and semantic version;
- human-readable description;
- instruction entry point;
- required tools and services;
- optional assets;
- compatibility range for the framework;
- integrity digest;
- trust classification.

Skill discovery and skill activation are distinct. Discovery may list metadata;
activation reads content and must pass authorization. Loaded skill versions are
recorded in run metadata for reproducibility.

The first implementation supports local, trusted, read-only skill directories.
Remote installation and executable skill hooks are out of scope.

## 8. MCP

Use Effect AI's MCP protocol/schema/server primitives where they fit. MCP remains an
external capability, not the framework's internal Tool abstraction.

An MCP client adapter:

- connects within a Scope;
- discovers server capabilities;
- maps MCP tools to Effect AI dynamic Tools;
- maps content and structured data without losing provider detail;
- validates inputs through Effect AI Tool Schema boundaries;
- enforces timeouts, size limits, and authorization;
- records server identity and advertised version;
- turns connection loss into typed failures;
- never treats remote tool execution as exactly once unless the remote protocol and
  tool explicitly support idempotency.

MCP resources and prompts may later map to separate framework capabilities. They
must not be disguised as tools solely to reduce interface count.

## 9. Sandbox

The sandbox capability executes commands or code with explicit limits:

```ts
interface Sandbox {
  readonly execute: (request: SandboxRequest) => Stream.Stream<SandboxEvent, SandboxError>;
}
```

The request declares:

- image/runtime identity;
- command and arguments as separate fields;
- working directory;
- environment allowlist;
- filesystem mounts and access modes;
- network policy;
- CPU, memory, output, and wall-clock limits;
- secret handles rather than raw secret values;
- artifact collection rules.

The result records exit status, bounded output, resource use, artifacts, and the
sandbox implementation identity. A local process runner may satisfy the interface
for trusted development, but it must identify itself as `unisolated`; it is not a
security sandbox.

## 10. Subagents

The proposed Subagent capability is specified in [subagents.md](./subagents.md). It uses declared
Effect AI Tools for attached delegation: the parent retains conversational control, each invocation
owns a fresh child Conversation, child context and authority are explicit, and the parent joins one
Schema-validated result.

An ephemeral child uses structured concurrency and belongs to its parent Scope. A durable child is
a separate accepted Submission with immutable parent linkage and independent Attempt ownership.
The proposed design is not implemented until ADR-0010 and its roadmap slices are accepted.

## 11. Persistent agent state

Persistent state is separate from conversation history. An agent state store uses
Effect Schema for value validation and optimistic revision checks.

State writes may be:

- run-local and discarded on settlement;
- committed transactionally with a canonical event;
- external and compensatable;
- external and uncertain.

The API must force the caller to select the write class. “Memory” is not a single
untyped key-value bag.

## 12. Observability integration

Every capability operation inherits tracing context and emits:

- operation name and version;
- run/conversation identity;
- duration and outcome;
- retry count;
- bounded, redacted attributes;
- relevant resource identity;
- structured failure category.

Model text, tool input/output, skill content, and sandbox output are sensitive by
default and are excluded from ordinary span attributes.

## 13. Requirements

- **CAP-001**: Every optional capability is represented by an Effect service.
- **CAP-002**: Missing capabilities fail during runtime construction or preflight,
  not midway through an unrelated operation.
- **CAP-003**: Application Tools use Effect AI definitions and handlers; the engine owns their
  durable scheduling rather than a provider SDK.
- **CAP-004**: All tool inputs and outputs cross Effect Schema boundaries.
- **CAP-005**: Tool call result commits are deterministic.
- **CAP-006**: Approval requests and decisions are canonical audit events.
- **CAP-007**: Compaction never mutates canonical history.
- **CAP-008**: Loaded skill identity and version are recorded for a run.
- **CAP-009**: Effect AI MCP integration preserves remote identity and does not imply exactly-once
  execution.
- **CAP-010**: Unisolated process execution is never labeled a security sandbox.
- **CAP-011**: When enabled, Subagents use only declared delegation Tools; attached ephemeral
  children use structured concurrency and belong to the parent Scope.
- **CAP-012**: Persistent agent state is distinct from conversation history.
- **CAP-013**: Capability telemetry is bounded and redacted by default.
