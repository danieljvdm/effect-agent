# Glossary

Use these terms consistently in code, telemetry, and user documentation.

## Product concepts

**Agent Definition**  
An immutable, schema-defined description of an agent: identity, input and output schemas,
instructions, optional model-visible input projection, toolkit, and execution policy. It contains no mutable thread state, owns no
live resources. Execution requires native model services supplied through Effect Layers.

**Agent Binding**

An immutable pairing of one Agent Definition with a Layer providing the native Language Model,
provider name, and model name. It fixes model selection for durable registration or Subagent
construction without hiding Layer requirements or acquiring provider resources.

**Agent Registration**

An Agent Definition and model Layer, or an existing Agent Binding, paired with explicit Agent,
Model, and Tool version declarations. Runtime
construction hashes the declarations and captures the Binding's required services in its Scope.
Durable workers match the registered identity and hashes before executing queued work.
An optional `attemptLayer` builds invocation-specific services for one fenced Attempt, rather than
capturing caller authority or live resources in the runtime's long-lived Scope.

**Agent Runtime**  
The Effect module that interprets an Agent Definition or Binding. The ephemeral runtime executes immediately;
the durable runtime admits a Submission and coordinates Attempts until Settlement.

**Run**  
One logical request to execute an Agent Binding against a Thread. In ephemeral mode the Run
lives for one Scope. In durable mode the logical Run may span multiple process Attempts.

**Run Disposition**<br>
An optional application-defined value selected from decoded Agent output and validated by a
Definition-owned Effect Schema. It is durable only for an ordinary completed Run and is never
inferred from prose, Tool output, or successful side effects.

**Attempt**  
One ownership period in which a worker tries to advance a durable Submission. An interruption,
lost ownership, eviction, or redeploy may end an Attempt without ending the Run.

**Turn**  
One model request and its assistant response, optionally followed by one tool-call batch. A Turn
begins only after the preceding canonical state is committed.

**Thread**
An addressable durable or ephemeral ordered history shared across Runs. An Agent executes Runs
within a Thread and retains their history there. A Thread has exactly one canonical transcript
projection, derived from its records.

**Submission**  
An immutable input accepted for durable processing in one Thread lane. Acknowledged
Submissions create an accepted-work obligation.

**Receipt**  
The durable identity returned after ledger admission, Thread materialization, durable
attachment storage, and readiness are committed. It is an identifier, not an authorization
capability.

**Schedule**
A durable, owner-scoped registration that delivers one Schema-encoded Agent input through ordinary
Submission admission at a specified time. It owns a firing until it records a Receipt or a
conclusive refusal; it does not run the Agent itself.

**Schedule Owner**
The tenant-qualified scope that owns a Schedule and authorizes its management and listing. There is
no global Schedule listing operation.

**Delivery Principal**
The stable principal recorded in a Schedule and used to authorize and admit each due occurrence.
It is distinct from the caller who manages the Schedule.

**Prepared Delivery**
An immutable pending admission envelope for one Schedule occurrence or selected event delivery.
Recovery retries that exact envelope until Receipt or conclusive refusal and never rebuilds it
from later configuration.

**Subscription**
An immutable, owner-scoped registration for Schema-defined events at one stable source partition.
A once registration is consumed when an event is atomically selected, before input preparation.
A continuous registration creates a separate delivery obligation for each selected event.

**Source Partition**
A tenant-qualified, host-defined source address with one storage owner. It contains registrations,
accepted events, routing cursors, and delivery obligations. Its identity is independent of payload,
source version, and deployment. Admission to a destination Thread is a separate operation.

**Accepted Event**
A normalized event whose identity, payload digest, registration eligibility cutoff, and remaining
routing work are durably retained. Its acknowledgement is not a Submission Receipt.

**Selected Delivery**
A durable obligation for one Subscription and Accepted Event. Selection pins the registration,
event, source version, destination, and admission identity before fallible input preparation.

**Settlement**  
The single durable terminal outcome owed to an accepted Submission: `completed`, `failed`, or
`aborted`. A failed Settlement always carries the framework's bounded generic diagnostic;
completed joined work and every aborted Settlement may legitimately have no result. An ordinary
completed Settlement may materialize the Definition-validated, Schema-encoded application run
disposition stored in its exact canonical record. A Run Settlement may also carry its canonical
aggregate model-usage and estimated-cost summary; joined Submissions do not duplicate it.

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

**Invocation kind**

How an application Tool is called: `model` for a model-declared call, or `programmatic` for an
inner broker invocation. This is independent of execution class and of whether its Handler started.

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

**Completion Tool**

The single Definition-designated Tool whose successful singleton call projects through the Agent
output Schema and completes the Run immediately. It remains an ordinary external side effect for
authorization and durability; the designation adds terminal semantics, not exactly-once execution.

**Step**  
A deterministically named sub-operation within one Durable Tool Call. Its result is
exactly-once-recorded but its external side effect is at-least-once-executed.

**Skill**  
A deferred runtime concept for a versioned package of instructions and bounded resources that
could be activated for future Turns. No runtime Skill API is implemented. Contributor skills in
`.agents/skills` are repository tooling and are unrelated to Agent execution.

**Subagent**  
An Agent Definition invoked by another agent through a declared delegation capability. A durable
Subagent owns a child Thread with explicit parent linkage.

**Delegation Definition**
An immutable declaration that exposes one target Agent Definition to a parent as an Effect AI
Tool. Input schemas and identity mapping default to the child's input. The default result wraps
its output with an exhaustion marker. Custom projections, authority, budget, and policy bounds
remain explicit when needed.

**Subagent Invocation**
One parent Tool Call that runs one declared Subagent. Its child Thread is fresh and distinct;
its stable parent-side identity is the parent Run and Tool Call pair.

**Parent Link**
The immutable lineage from a child Thread to the parent Thread, Run, Tool Call, Agent,
delegation, and depth that established it.

**Attached Child**
A Subagent Invocation whose terminal outcome must be joined into its parent Tool Call before that
Tool Call settles. Detachment is a separate future capability.

**MCP Server**
An external Model Context Protocol server reached through an application-configured transport.
Its discovered tools become dynamic Effect AI Tools that forward `tools/call`; they remain ordinary
Tools for approval, authorization, and durability, and their advertised hints are untrusted unless
the transport opts in.

**Sandbox**  
A scoped capability set for filesystem, process, and optional network operations. It is not a
generic bag of provider SDK methods.

**Page Capture**

A stateless render of one page in a managed headless browser returning exactly one bounded
output, including rendered content, links, structured extraction, or grouped selector scrape.
Its host allowlist, action set, and byte budget are immutable; the host policy governs
navigation, redirects, and subrequests. Capture results are untrusted, browser JavaScript makes
execution uncertain rather than read-only, and separately billed model inference requires
explicit host authorization and accounting.

**Page Crawl**

A scoped stream of rendered Markdown records discovered from one credential-free HTTPS URL. The
starting host and page-count, depth, byte, and deadline limits are fixed before execution; provider
job identity and pagination remain adapter-private, and a known-running job is cancelled when the
consumer's Scope exits.

**Interactive Browser**

A scoped, provider-neutral browser pass owning one browser, context, and page. Its immutable
policy fixes an explicit network mode, action count, elapsed time, and per-result byte budget.
`ExactHosts` checks page-request URLs against a fixed HTTPS host set; `PublicWeb` requires a
connection-time public-network boundary and fails unsupported where an adapter cannot enforce it.
`Unrestricted` explicitly opts out of URL/host and private-network containment while retaining
the same session limits and lifecycle.
Handles are ephemeral and uncertain: they are never persisted, replayed, reconnected for execution,
or exposed as model Tools.
Screenshots and scrolling operate on that same page, and explicit closure ends the pass early.
Provider session identity and operator controls remain private host capabilities.

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
A reduction of the model-visible prompt by pruning, summarizing, or starting a fresh context window without erasing
canonical evidence. Physical record deletion is a separate retention operation. The engine
compacts natively at the pre-Turn seam when the estimated next context exceeds the Context Token
Limit or would consume the Completion Reserve. It prunes old Tool results, summarizes through one
metered model call, and records
each compaction in the DN and DC assemblies as a canonical `CompactionCreated` record that
projections fold
(RUN-026). The engine-owned `ContextCompactor` service selects the strategy, token estimator,
summary prompt, and Model. `ContextCompactor.layer` supplies the bounded default. Cloudflare
Thread Objects install the same service through a scoped `ContextCompactor` Layer,
rebuilt after eviction. The interpreter owns metering, protected messages, events, and commits;
the durable coordinator maps actual covered messages to complete canonical records. Pruning and
summarization cover prior-Run records; rollover can cover settled batches in the current Run.

**Context Window**

The current model-visible working context within a Run. A rollover retains the original instructions
and input, carries optional untrusted continuation notes, and removes covered conversation from the
view. Its canonical boundary survives recovery. It does not reset the Run or its cumulative budgets.
`ContextTools` exposes model-directed rollover, estimated capacity, and retained history lookup;
`MemoryNotes` binds optional working notes to an application-owned Memory document.

**Context Token Limit**  
The optional `AgentPolicy.contextTokenLimit` bound on one model call's live context, supplied by
the host from its model choice. Distinct from `tokenBudget` (the cumulative runaway stop) and
`costBudgetMicrousd` (spend).

**Completion Reserve**

The `AgentPolicy.completionReserveTokens` capacity withheld from research when a cumulative token
budget is configured, so the Run can enter finalization before delivery becomes unaffordable.

**Tool Result Bounds**  
The `AgentPolicy.toolResultBounds` byte bound (default 50 KiB) applied once to every application
Tool result's encoded form at the settle seam. An oversized result becomes the canonical
`TruncatedToolResult` envelope preserving head, tail, and original size, so records and prompts
carry the same bounded value.

**Run Status Message**  
A derived message appended to each outgoing model request (policy `runStatus: "appended"`)
showing Turns, Tool Calls, tokens against budget, last-call context, and elapsed time. It is
projection-time output, never persisted as canonical history.

**Token Soft Landing**  
The token dimension's participation in the `onExhaustion: "final-answer"` resolution
(RUN-025): a token-breaching response with decodable output settles the
Run directly, and otherwise the Run takes at most one constrained grace Turn
(`toolChoice: "none"`), completing with `finishReason: "budget-exhausted"` and the
`exhausted` dimension marker instead of failing silently.

## Persistence concepts

**Memory source**

Application-owned readable content used across Threads. References preserve the source's identity,
revision or revision uncertainty, original attribution, and activity time. Search indexes and
caches are derivatives; transient recalled text is not canonical Thread history.

**Memory withdrawal**

A terminal change that excludes a source from future authoritative recall checks. Checks begun
after successful withdrawal exclude it; already captured views may finish. Original history,
past outputs, idempotency receipts, and backups have separate retention policies.

**Remembering intent**

A host-authorized request to derive a contribution from one identified source into one memory
target. Durable admission returns queued. Extraction and target updates run separately under a
host Scope; queued does not mean saved or available to recall.

**Remembering proposal**

An application-Schema value saved after extraction, with evidence bound to the admitted source.
The worker retains it across target revision conflicts. A prepared command separately saves the
complete conditional Memory Write before dispatch, so an uncertain outcome replays that command.

**Remembering suppression**

A durable source fence that prevents obsolete evidence from passing current-source recall and
retains reconciliation and cleanup obligations. Source-to-target references outlive active jobs.
Cleanup removes source-owned contributions while preserving unrelated entries and human corrections.

**Memory namespace**

An application-defined name, version, and Schema-validated identity addressing memory sources,
operation receipts, and index entries. The canonical address is portable across adapters.
Definition versions select distinct namespaces, not document revisions. Host code owns authorization.

**Memory scope**

A host-defined visibility label carried by memory access and active documents. The `MemoryScope`
brand distinguishes it from caller identity. A matching label is a recall filter, not proof of
authentication or permission to write; the host's authorization policy remains authoritative.

**Semantic memory index**

A disposable ranking derivative tied to one embedding model and chunking profile. Its candidates
carry source revisions and byte ranges; authoritative sources supply current access and attribution.
Index contents are separate from committed-activity extraction progress and make no global corpus
freshness claim.

**Canonical Record**  
An immutable, schema-versioned fact in the Thread Log. Canonical Records are the only
recovery truth.

**Activity processor**

An optional application-invoked consumer of committed Thread records. Its versioned, per-Thread
progress and prepared output are separate from canonical history, engine checkpoints, and
submission ownership. The application owns extraction, destination idempotency, and sharing.

**Thread Log**
The ordered, append-only sequence of Canonical Records for one Thread.

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
