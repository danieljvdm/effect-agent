# Domain language

Use these terms consistently in code, specifications, telemetry, and user documentation.

## Product concepts

**Agent Definition**  
An immutable, schema-defined description of an agent: identity, input and output schemas,
instructions, toolkit, and execution policy. It contains no mutable conversation state, owns no
live resources, and is not executable until paired with a Model.

**Agent Binding**

An immutable pairing of one Agent Definition with one Effect AI Model. The Binding makes model
selection explicit without hiding the Model Layer's requirements and owns no acquired provider
resources.

**Agent Runtime**  
The Effect module that interprets an Agent Binding. The ephemeral runtime executes immediately;
the durable runtime admits a Submission and coordinates Attempts until Settlement.

**Run**  
One logical request to execute an Agent Binding against a Conversation. In ephemeral mode the Run
lives for one Scope. In durable mode the logical Run may span multiple process Attempts.

**Attempt**  
One ownership period in which a worker tries to advance a durable Submission. An interruption,
lost ownership, eviction, or redeploy may end an Attempt without ending the Run.

**Turn**  
One model request and its assistant response, optionally followed by one tool-call batch. A Turn
begins only after the preceding canonical state is committed.

**Conversation**  
A durable or ephemeral ordered history shared across Runs. A Conversation has exactly one
canonical transcript projection, derived from its records.

**Session**  
An addressable interaction handle for a Conversation. Avoid using Session as a synonym for process,
Run, Attempt, or model request.

**Submission**  
An immutable input accepted for durable processing in one Conversation lane. Acknowledged
Submissions create an accepted-work obligation.

**Receipt**  
The durable identity returned after ledger admission, Conversation materialization, durable
attachment storage, and readiness are committed. It is an identifier, not an authorization
capability.

**Settlement**  
The single durable terminal outcome owed to an accepted Submission: `completed`, `failed`, or
`aborted`.

## Agent capabilities

**Tool**  
An Effect AI model-visible operation defined by Effect Schemas for parameters, success, and typed
failure. A Tool definition is pure. Its Handler is provided through an Effect AI Toolkit Layer.

**Toolkit**  
An Effect AI collection of Tools plus the handler requirements needed to execute them.

**Tool Call**  
A model-declared request to execute one Tool. Its stable Tool Call ID scopes results, progress,
durable steps, approvals, and reconciliation.

**Tool Batch**  
All Tool Calls declared by one assistant message. The default scheduler executes the batch with a
finite Effect Semaphore. Canonical results commit in declaration order and the batch becomes
model-visible atomically.

**Steering**  
Input delivered to an active Run after a complete assistant response and Tool Batch, before the
next model request. Steering never mutates in-flight work.

**Follow-up**  
Input delivered only when the Agent would otherwise stop.

**Joining / Joined**  
Durable states for queued input claimed by an active Run. `joining` precedes canonical input
append; `joined` follows it and settles with the host Run.

**Ordinary Tool**  
A Tool without durable replay semantics. If ownership is lost after its effect may have happened
but before an outcome is recorded, recovery records an unknown outcome and does not replay it.

**Durable Tool**  
An Effect AI Tool whose handler requires the framework's Durable Step service and divides external
effects into named Steps. The handler may be re-entered after interruption.

**Step**  
A deterministically named sub-operation within one Durable Tool Call. Its result is
exactly-once-recorded but its external side effect is at-least-once-executed.

**Skill**  
A versioned package of instructions and bounded resources that can be activated for future Turns.
A Skill is data, not ambient executable code.

**Subagent**  
An Agent Definition invoked by another agent through a declared delegation capability. A durable
Subagent owns a child Conversation with explicit parent linkage.

**Delegation Definition**
An immutable declaration that exposes one target Agent Definition to a parent as an Effect AI
Tool, with explicit input/result projections, authority, budget, and policy bounds.

**Subagent Invocation**
One parent Tool Call that runs one declared Subagent. Its child Conversation is fresh and distinct;
its stable parent-side identity is the parent Run and Tool Call pair.

**Parent Link**
The immutable lineage from a child Conversation to the parent Conversation, Run, Tool Call, Agent,
delegation, and depth that established it.

**Attached Child**
A Subagent Invocation whose terminal outcome must be joined into its parent Tool Call before that
Tool Call settles. Detachment is a separate future capability.

**Sandbox**  
A scoped capability set for filesystem, process, and optional network operations. It is not a
generic bag of provider SDK methods.

**Approval**  
A policy decision that suspends or denies a proposed Tool Call before its Handler starts. Approval
is not inferred from model prose.

## Model concepts

**Language Model**  
Effect AI's `LanguageModel` service. It accepts Effect AI Prompts and produces typed Effects or
Response streams.

**Model**  
Effect AI's Model value: a Layer that provides a Language Model plus provider and model identity.

**Response Part**  
An Effect AI streaming Response value such as text, returned reasoning, Tool Call parameters,
usage, or completion. Provider SDK chunks remain inside the Effect AI provider implementation.

**Stop Policy**  
The bounded rules governing maximum turns, tool calls, duration, usage, cost, repeated failures,
and acceptable final output.

**Compaction**  
Creation of a model-context summary or branch that reduces future prompt size without erasing
canonical evidence. Physical record deletion is a separate retention operation. The engine
compacts natively at the pre-Turn seam when the estimated next context exceeds the Context Token
Limit — pruning old Tool results, then summarizing through one metered model call — recording
each compaction in the DN and DC assemblies as a canonical `CompactionCreated` record that
projections fold
(RUN-026). Host-supplied, digest-bound compaction artifacts remain a separate capability.

**Context Token Limit**  
The optional `AgentPolicy.contextTokenLimit` bound on one model call's live context, supplied by
the host from its model choice. Distinct from `tokenBudget` (the cumulative runaway stop) and
`costBudgetMicrousd` (spend).

**Tool Result Bounds**  
The `AgentPolicy.toolResultBounds` byte bound (default 50 KiB) applied once to every application
Tool result's encoded form at the settle seam. An oversized result becomes the canonical
`TruncatedToolResult` envelope preserving head, tail, and original size, so records and prompts
carry the same bounded value.

**Run Status Message**  
A derived message appended to each outgoing model request (policy `runStatus: "appended"`)
reflecting turns, Tool calls, tokens against budget, last-call context, and elapsed time. It is
projection-time output, never persisted as canonical history.

**Token Soft Landing**  
The token dimension's participation in the `onExhaustion: "final-answer"` resolution
(RUN-025): a token-breaching response with decodable output settles the
Run directly, and otherwise the Run takes at most one constrained grace Turn
(`toolChoice: "none"`), completing with `finishReason: "budget-exhausted"` and the
`exhausted` dimension marker instead of failing silently.

## Persistence concepts

**Canonical Record**  
An immutable, schema-versioned fact in the Conversation Log. Canonical Records are the only
recovery truth.

**Conversation Log**  
The ordered, append-only sequence of Canonical Records for one Conversation.

**Submission Ledger**  
Operational durable state for admission, FIFO readiness, ownership, Attempts, optional leases,
abort intent, and settlement obligations.

**Canonical Batch**  
An atomic append of one or more Canonical Records. Readers never observe part of a batch.

**Projection**  
A materialized view derived from Canonical Records, such as transcript, active resources, state, or
client messages. Projections are rebuildable.

**Checkpoint**  
A versioned optimization containing a Projection through a verified log offset. A Checkpoint is
never recovery truth.

**Producer Epoch**  
A fencing token that grants one owner permission to append. A stale owner with an older epoch
cannot mutate canonical state even if it resumes.

**Unknown Outcome**  
A durable Tool result stating that an external effect may have occurred but was not confirmed
canonically. It is neither success nor ordinary failure.

**Accepted-work Contract**  
Once a Submission is durably acknowledged, the runtime owes it exactly one durable Settlement.

## Architectural vocabulary

**Module**  
Anything with an interface and an implementation: package, class, function, Layer, or aggregate.

**Interface**  
Everything a caller must know: types, invariants, ordering, failure modes, resource ownership, and
performance characteristics.

**Seam**  
A location where behavior can be changed without editing the caller.

**Adapter**  
A concrete implementation at a Seam.

**Core**  
The inward domain, authoring, and engine packages that contain no provider, database, transport, or
platform implementation.

**Reference Application**  
A cumulative, package-local set of compiling fixtures and tests that exercises the public
framework through successive build-out phases. It is application-shaped evidence, not a deployable
workspace or a new product package.
