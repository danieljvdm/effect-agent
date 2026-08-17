# Subagent Specification

Status: **Proposed**

Status note (2026-08-13): both §17 slices are implemented — S1 attached ephemeral Subagents
and S2 durable attached Subagents (proven by the capabilities and testing suites).

This document specifies how one Agent delegates bounded work to another Agent while preserving
Effect typing, structured concurrency, least authority, deterministic parent history, and the
accepted-work contract.

## 1. Scope

The first Subagent capability is **attached delegation**:

- the parent Agent retains control of its Conversation and user interaction;
- the model requests one declared delegation through an Effect AI Tool;
- the runtime starts one fresh child Run in one distinct child Conversation;
- the parent waits for the child to settle;
- a bounded, Schema-validated result becomes the parent Tool result;
- the child Conversation remains separately inspectable and auditable.

This is not a handoff. A handoff transfers conversational control to another Agent and requires a
different context, client-routing, and ownership protocol. It is outside this specification.

The first implementation also excludes:

- detached or fire-and-forget children;
- peer-to-peer child messaging;
- dynamically model-authored Agent Definitions;
- implicit inheritance of the parent transcript, tools, state, sandbox, or secrets;
- reuse of a settled child Conversation for a later delegation;
- universal exactly-once child execution or external side effects;
- treating Subagents as a generic workflow or DAG engine.

## 2. Design principles

### 2.1 Delegation is declared, not ambient

A parent may invoke only Subagents represented by Tools in its Effect AI Toolkit. A model-generated
name, prompt, URL, or package cannot register a child at runtime.

Each declaration fixes:

- stable delegation and target Agent identity;
- model-visible name and description;
- Tool parameter and result Schemas;
- the child input and parent-result projections;
- the structural child Tool-name/depth ceiling and budget requests;
- approval and risk metadata;
- target definition digest policy.

The concrete child Agent Binding is supplied through a Layer. Model selection therefore remains
explicit and cannot be expanded by model output.

### 2.2 Delegation uses Effect AI Tool semantics

Model-visible delegation is an application Tool created with Effect AI `Tool.make` and included in
an Effect AI `Toolkit`. The framework may provide a `Subagent.define` authoring helper and handler
Layer, but it MUST NOT define a competing Tool, Toolkit, Prompt, Response, or Model abstraction.
Every declared delegation Tool carries the engine `ToolExecutionKind = "delegation"` annotation;
runtime classification must not infer delegation from its name.

The normal Tool boundary remains authoritative:

1. the parent response and complete Tool batch are reduced;
2. delegation parameters are Schema-decoded;
3. authorization, approval, budget, depth, and concurrency pass preflight;
4. the Subagent handler starts;
5. the child result is Schema-encoded;
6. parent Tool results commit in original model Tool Call order (response-array order).

The parent model never receives a partial child transcript as a Tool result. A budget-exhausted
child under the default `onExhaustion: "final-answer"` policy settles as success: its
constrained grace Turn produces a real, Schema-decoded output that flows through
`projectResult` and the normal Tool boundary like any other child output, with
`SubagentCompleted.exhausted` making the degradation observable (RUN-025). The transcript
invariant stands — a grace-Turn output is a declassified Output, not a transcript.

### 2.3 A child is a normal Agent

A Subagent runs the same Agent Definition, Agent Binding, semantic interpreter, Stop Policy, model
integration, Tool scheduler, and output decoder as a top-level Agent. The framework MUST NOT
maintain a second, weaker child loop.

S1 and S2 reject every nested delegation at preflight, even if a child Toolkit contains a
delegation Tool. Enabling depth above one requires the later proposal in §17.

### 2.4 Process-local fibers are not durable identity

An ephemeral child Attempt is a supervised Effect Fiber. A durable child is a separate accepted
Submission with its own Run, Attempts, Conversation Log, and Settlement. Recovery reconstructs the
durable relationship from records; it never treats a lost Fiber as proof that the child did or did
not execute.

## 3. Domain model

**Delegation Definition**
An immutable declaration that exposes one target Agent Definition as one Effect AI Tool. It owns
Schemas, projections, policy requests, and stable identity, but no acquired resources or mutable
registry entry.

**Subagent Invocation**
One parent Tool Call that invokes one Delegation Definition. The stable invocation identity is the
pair of parent `RunId` and `ToolCallId`.

**Parent Link**
The immutable lineage connecting a child Conversation to the parent Conversation, Run, Tool Call,
Agent, delegation, and depth.

**Child Conversation**
A distinct Conversation containing only the child's canonical history. It never shares a
Conversation lane with its parent.

**Attached Child**
A child whose terminal outcome must be joined into its parent Tool Call before that Tool Call can
settle. The initial capability supports only attached children.

Conceptual Schema-owned identities and linkage:

```ts
class SubagentParentLink extends Schema.Class<SubagentParentLink>(
  "@effect-agent/SubagentParentLink",
)({
  delegationId: DelegationId,
  parentAgentId: AgentId,
  parentConversationId: ConversationId,
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
  depth: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
}) {}
```

The exact class names may change during the type proof. The following invariants do not:

- one parent invocation links to at most one child Conversation;
- every child Conversation has exactly one immutable Parent Link;
- child `ConversationId`, `SubmissionId`, `RunId`, and `AttemptId` remain ordinary framework IDs;
- the child definition, binding, policy, authority, and input digests are fixed before execution;
- linkage is explicit in both parent and child durable projections;
- IDs are references, not authorization capabilities.

## 4. Authoring model

The intended surface separates the pure declaration from the engine-owned handler Layer:

```ts
const ResearchDelegation = Subagent.define("delegate_research", {
  description: "Research one bounded question and return cited findings.",
  target: ResearchDefinition,
  parameters: ResearchRequest,
  success: ResearchFindings,
  failure: ResearchDelegationFailure,
  prepareInput: (request, context) =>
    Effect.succeed({
      question: request.question,
      evidenceRefs: context.allowedEvidenceRefs,
    }),
  projectResult: (output) => Effect.succeed(output),
  policy: SubagentPolicy.make({
    maxDepth: 1,
    maxChildren: 8,
    maxConcurrency: 4,
    maxTurns: 8,
    maxToolCalls: 12,
    maxDuration: "5 minutes",
  }),
  authority: ResearchAuthority,
});

const CoordinatorToolkit = Toolkit.make(SearchLocalDocs, ResearchDelegation.tool);

const ResearchDelegationLive = SubagentRuntime.layer(ResearchDelegation, ResearchBinding);
```

This syntax is illustrative. Before implementation, a compile-only proof MUST show:

- `ResearchDelegation.tool` is an Effect AI Tool;
- the child Definition remains model-agnostic;
- `SubagentRuntime.layer` accepts an explicit child Agent Binding and is owned outside core;
- the native Tool declares the inward-owned `AgentSpawner` service key as an explicit Tool
  dependency rather than trying to reconstruct runtime keys from `Agent.Requirements`;
- per-call handler `R` is exactly the Tool's declared dependencies, while child Model, handler,
  projection, and Schema requirements are visible as construction requirements of
  `SubagentRuntime.layer`;
- no child service is silently supplied from a root Context or erased from Layer construction `R`;
- every expected child and projection failure is total-mapped to the Tool's declared,
  Schema-backed failure before the handler returns;
- with `failureMode: "error"`, only the declared Tool failure and Effect AI's permitted AI error
  enter the parent handler `E`; with `"return"` (SUB-033), the contained failure family is encoded
  in the Tool success union and only the engine signals remain in `E`;
- interruption remains interruption and defects remain defects rather than being coerced into an
  expected Tool failure;
- no mutable global Agent registry is required;
- unsatisfied Layer requirements fail the compile/type gate, Layer acquisition failures remain in
  Layer construction `E`, and stored-digest/preflight failures are explicit runtime failures.

### 4.1 Parameter and input projection

The Tool parameter Schema and child Agent input Schema MAY be identical. When they differ,
`prepareInput` is an Effectful, typed projection from:

- Schema-decoded Tool parameters;
- bounded parent metadata;
- explicitly selected canonical evidence or artifact references;
- the authorized Principal and Tenant context.

`prepareInput` MUST NOT receive an unbounded parent transcript or a root runtime Context. Its Layer
construction requirements remain visible in `R`; its expected failures are total-mapped into the
declared Tool failure.

### 4.2 Output and failure projection

The child Agent output is decoded by its normal output Schema before `projectResult` runs.
`projectResult` produces the bounded value encoded by the delegation Tool success Schema; it also
receives the framework's bounded result context (SUB-034) — currently the honest
`budgetExhausted` marker — so a budget-truncated partial can be surfaced in the declared success
Schema for the orchestrator's extension decision.

An Agent's full inferred error channel does not automatically form a stable cross-Agent wire
contract, and native Effect AI handlers cannot leak an arbitrary child `E`. A Delegation
Definition therefore MUST provide either:

1. a Schema-backed total mapping for child expected failures that the parent may handle; or
2. the framework's bounded `SubagentExecutionFailure` projection, which contains classification
   and child references but no raw Cause, stack, provider body, or secret-bearing payload.

`failureMode: "error"` remains the default. An application may deliberately choose `"return"`
(SUB-033, implemented) when Schema-backed child failures should be model-visible: the declared
failure plus the framework delegation-failure family become result data in the Tool success
union, so one failed child cannot fail the parent Tool batch. The mechanics are deliberately NOT
Effect AI's native `failureMode: "return"` — that would convert every handler failure into a
result, including the engine-owned `ToolCallWaiting` suspension signal, silently orphaning a
durable child. Instead the underlying Effect AI Tool keeps `failureMode: "error"`, the delegation
handler contains exactly the expected failure family into the success channel, and
`ToolCallWaiting` and `SubagentDurabilityError` stay raisable, preserving durable suspension by
construction. On the durable path the settlement join records the contained failure with the
same non-failure polarity the live batch continues with (SUB-019 coherence); the child's own
failed Settlement and the `SubagentFailed` event remain the honest failure record. The mapping is
exhaustive over expected child runtime failures. Ephemeral interruption propagates as
interruption; a child defect fails the handler as a defect and is sandboxed by the ordinary Tool
batch policy.

### 4.3 Package and service ownership

The first implementation does not require a new package:

- `@effect-agent/core` owns only the branded delegation identity and shared Schema values needed
  by both runtime events and durable records;
- `@effect-agent/engine` owns the one interpreter, the `AgentSpawner` service contract, and a
  narrow execution-options seam that accepts preallocated child Conversation/Run identity and a
  non-model-visible Parent Link;
- `@effect-agent/capabilities` owns the pure `Subagent.define` helper, authority/budget policy, and
  `SubagentRuntime.layer`, which constructs the native Tool handler from an explicit Binding and
  the engine service;
- `@effect-agent/session` owns Parent Link and requested/started/joined record Schemas, reducers,
  and recovery snapshots;
- platform packages coordinate durable child admission, wakeup, ownership, and join through the
  existing storage and scheduler ports;
- storage adapters implement generic record and ledger contracts and MUST NOT invent Subagent
  policy.

The `AgentSpawner` receives only a narrow parent execution value containing immutable identity,
Principal/Tenant, budget controller, event sink, and owned Scope access. It MUST NOT expose the
engine's mutable state machine or the root Layer Context.

The engine execution seam is integrator-owned because the current interpreter allocates `RunId`
internally and Run events do not carry lineage. S1 MUST add execution options for preassigned child
identity and Parent Link rather than wrap or reimplement the loop.

`AgentSpawner` is an Effect service. Its public methods return `Effect` or `Stream`. The
composed application Layer, not a mutable registry or locally constructed runtime, supplies the
concrete Binding and all child dependencies. Static unsatisfied requirements, Layer acquisition
failures, and per-call Tool failures remain distinct.

## 5. Context isolation

Every invocation starts with a fresh child Conversation. By default the child receives:

- its own Agent Definition and instructions;
- its explicit Agent Binding and Model;
- the projected child input;
- the structural child Tool-name/depth ceiling for this invocation;
- a new child Run budget;
- Parent Link metadata that is not automatically model-visible.

It does not inherit:

- parent Conversation messages;
- parent instructions or Turn Plan;
- parent Toolkit or dynamically loaded Tools;
- parent steering or follow-up queues;
- parent mutable state;
- parent model context or compaction artifacts;
- parent sandbox mounts, working directory, environment, network, or secrets;
- sibling outputs or child Conversations.

An application may pass selected information only through the child input Schema, a bounded
context projection, or digest-addressed artifact references. Source, trust classification, and
redaction metadata MUST survive the projection. A digest proves content identity, not authority.
Every artifact reference is a capability bound to digest, immutable storage version, Tenant,
owner/source, classification, media type, size, and allowed operation. Dereference reauthorizes
the current Principal/Tenant and scans the artifact before release.

The parent receives the projected result and child provenance, not an implicit transcript merge.
Result projection is an explicit declassification boundary. Its output classification is at least
the join of child input, output, and artifact provenance unless an authorized declassifier records
a narrower classification and reason.

## 6. Authority and security

The implemented S1/S2 capabilities surface has one compatibility-named `SubagentGrant`. It carries
only allowed child Tool names and a fixed depth-one ceiling. It is not a
Principal/Tenant/resource grant and cannot authorize Tool, MCP, sandbox, secret, artifact, or model
operations.

### 6.1 Effective authority

Layer availability is not authorization, and the structural `SubagentGrant` is not authorization.
Authority is checked at the boundary that owns the protected operation:

- session's host-supplied child-admission authorizer decides whether durable establishment may
  proceed under current policy and exact parent, target, input, reservation, and child identity;
- Tool, MCP, sandbox, secret, artifact, model/provider, and observation adapters authorize their
  own operations against their current Principal/Tenant/resource policy;
- the capabilities runtime separately enforces the immutable declared child Tool-name/depth
  ceiling before establishment.

The parent need not possess each child Tool directly. Permission to establish a declared child
does not pre-authorize the child's later actions, and a broader service in the root Layer confers no
authority by itself. A future unified cross-resource grant would require a complete shared domain
model; S1/S2 do not define or claim one.

### 6.2 Fail-closed rules

- Durable child-admission authorization is evaluated immediately before establishment.
- Every child model/provider selection, Tool/MCP/Skill activation, secret resolution, artifact
  dereference, sandbox operation, administrative command, and future nested delegation is
  authorized at its action-owning adapter under current policy and normalized resource identity;
  the structural `SubagentGrant` is not an input that can expand that authority.
- Principal, Tenant membership, policy, or resource revocation after admission fails the next
  affected action with a typed, redacted denial. Durable suspension never freezes old authority.
- The model chooses only decoded task parameters; it cannot choose a Layer, Binding, Tool roster,
  Principal, Tenant, budget, depth, secret, or sandbox policy.
- The delegation Tool risk class is at least the maximum risk reachable through the declared child
  binding and host policy.
- Parent approval authorizes only establishment of the displayed, bounded delegation request.
  Every child action follows ordinary approval policy using the child Run/Tool Call, normalized
  targets, risk, expiry, policy version, approver Principal, and originating parent Tool Call.
- Parent approval is never reusable for a child action, sibling, retry, or descendant. Approval
  expiry and revocation are rechecked when durable work resumes.
- Child output and artifacts are untrusted input and are Schema-validated, bounded, classified,
  and redacted before parent use.
- A child cannot activate a Tool outside its declared Toolkit or structural ceiling, and no Skill,
  MCP server, Tool, or nested delegation may bypass its action-owning authorization boundary.
- A missing policy decision, unresolved Binding digest, or unavailable authorization dependency
  denies start.
- Delegation and all administrative intervention are audited.

### 6.3 Observation authority

Parent summaries, child transcripts, raw model/Tool events, artifacts, and provider payloads are
separate protected resources. Every subscription, reconnect, and paginated read is authorized
against the current Principal, Tenant, resource classification, and policy. Schema-driven
redaction runs before delivery, and sensitive/raw observation is audited.

Parent Link, Receipt, `ConversationId`, `RunId`, and artifact digest confer no access. Revocation
must affect later pages and reconnects even when an observer learned an identifier earlier.

## 7. Hierarchical budgets

Subagents MUST NOT create a new budget root. Every invocation has a finite child allocation and is
also charged to its ancestors.

The policy distinguishes:

- maximum delegation depth;
- total child invocations per Run;
- concurrent child invocations per Run;
- child Turns and model calls;
- child Tool Calls;
- wall-clock duration;
- reported input, output, and cache tokens;
- estimated cost;
- result and event bytes;
- artifacts and sandbox resources;
- nested child allocations.

Before a child starts, a real budget service atomically reserves its allocation from the parent's
remaining delegation budget. S1 cannot reuse today's post-response usage hook as if it were a
reservation service. Parallel children cannot each observe and spend the same remainder.

Each invocation has a stable `BudgetReservationId` derived from parent Run and Tool Call identity.
For every dimension, the reservation follows:

```text
absent → reserved → releasePending → released
```

The reservation records allocated, covered-consumed, observed-consumed, released, and overrun
amounts. At settlement:

```text
allocated = coveredConsumed + released
observedConsumed = coveredConsumed + overrun
cap + cumulativeOverrun = available + open reservations + cumulativeObservedConsumed
```

Release increases `available`; it is not another spend bucket. Descendant usage is inclusive in
each ancestor aggregate and is charged exactly once at each level. It is not charged once as child
usage and again as part of a rolled-up parent usage value.

In durable mode, reservation is a parent-owned operational ledger transition keyed by
`(parentRunId, parentToolCallId)` and committed under the parent ownership token and epoch before
`SubagentRequested`. The child admission references the immutable reservation ID and digest. This
is not an atomic transaction across parent and child stores. Join canonically records the final
accounting decision; the parent ledger then performs the idempotent
`releasePending → released` transition. Recovery repairs a reservation without a request and a
canonical join whose release is incomplete.

A child allocation MUST NOT exceed:

- the Delegation Definition cap;
- the target Agent Policy cap;
- the parent's remaining delegable budget;
- the host and tenant cap.

Structural limits such as depth, count, concurrency, Turns, Tool Calls, duration, and bytes are
hard limits. Token and cost enforcement is limited by provider reporting and MUST expose that
capability honestly. Actual usage above a reservation is recorded as an overrun, charged to every
ancestor aggregate, and prevents new work; it is never clipped to make accounting fit. Missing
provider usage conservatively consumes the reserved amount for that dimension. Late or corrected
usage is an auditable, idempotent adjustment and cannot silently create budget. Cancellation,
projection failure, compatibility failure, and unknown external outcome all settle accounting
through the same reservation state machine; an unresolved outcome retains its reservation.

Initial recommended defaults are root-relative depth `1`, at most `8` total child invocations per
parent Run, and at most `4` concurrent children. S1 and S2 reject all nested delegation, so total
descendant and cycle semantics are not executable yet. These numbers are proposed defaults, not
accepted owner decisions. All defaults remain finite.

## 8. Ephemeral lifecycle

An ephemeral attached invocation follows:

```text
declared
  → preflighted
  → child scope acquired
  → child running
  → child completed | child failed | child interrupted
  → projected
  → parent Tool Call settled
```

Execution rules:

1. The complete parent Tool batch passes ordinary Tool preflight.
2. Delegation-specific authority, approval, budget, depth, and concurrency pass preflight.
3. The runtime allocates child identities and an isolated child Conversation.
4. The executor enters a child-owned scoped region attached to the parent Run fiber.
5. The same Agent interpreter runs the explicit child Binding.
6. The parent joins the child result and waits for child finalizers.
7. The result projection crosses the Effect AI Tool Schema boundary.
8. The parent Tool result becomes eligible for deterministic batch commit.

The implementation uses one concrete Effect structured-concurrency shape:

- the handler enters an `Effect.scoped` child region and starts the interpreter with
  `Effect.forkScoped`;
- a parent-run scoped `FiberSet` tracks sibling child fibers for coordinated interruption and join;
- leaving the child region or closing the parent Run Scope interrupts the child, joins its fiber,
  and waits for all child finalizers;
- no Subagent uses `forkDetach`, a daemon Fiber, a global Scope, or an unowned Promise;
- a monotonic per-Run counter bounds total child invocations, while a separate `Semaphore` bounds
  active child execution; one semaphore cannot enforce both;
- the parent does not retain a scarce model/provider permit while merely awaiting a child.

The scheduler MUST have a documented, deadlock-free permit acquisition order. Tests cover minimum
global limits, attempted nested delegation, sibling failure, and interruption while permits are
queued.

## 9. Scheduling and parent result order

A delegation Tool participates in the parent Tool batch like any other application Tool:

- calls start in original model Tool Call order (response-array order) where permits allow;
- independent children may run concurrently;
- progress and completion may be observed in actual completion order;
- parent Tool results commit in original model Tool Call order;
- the next parent model Turn sees the complete Tool batch or none;
- a child Tool Call consumes the parent Tool Call budget and the child invocation budget;
- any future nested Tool Call would also be charged to its child and ancestor budgets; S1 and S2
  reject it.

If a default-error-mode child fails, the existing Tool batch failure policy applies. Still-running
attached siblings are interrupted in ephemeral mode or receive durable abort intent in durable
mode, and their finalizers or settlement obligations are not abandoned.

The initial API has no detached completion policy. A parent Run cannot report success, failure, or
interruption while an attached ephemeral child remains live.

## 10. Events and observability

### 10.1 Live events

The stable Run Event family adds bounded lifecycle events:

- `SubagentRequested`;
- `SubagentStarted`;
- `SubagentProgress`;
- `SubagentCompleted`;
- `SubagentFailed`;
- `SubagentInterrupted`;
- `SubagentJoined`.

Every event carries parent Run, parent Tool Call, child Run, child Conversation, target Agent,
delegation, depth, and execution provenance where available.

Parent observation receives lifecycle and bounded progress summaries. Full child model and Tool
events remain on the child stream and can be observed only through the per-read authorization and
redaction contract in §6.3. The parent stream MUST NOT duplicate every child token or Tool payload.

### 10.2 Telemetry

- An ephemeral child Run span is a child of the delegation Tool span.
- Durable Attempts use trace links when process loss or later ownership breaks one physical span
  tree.
- Logs carry parent/child correlation, attempt, target Agent, depth, and terminal class.
- Metrics cover starts, active children, queue time, duration, depth, budget use, failure,
  interruption, recovery, and join delay.
- Conversation, Run, Tool Call, and child IDs stay out of unrestricted metric labels.
- Model text, child input/output, artifact content, and failure details are sensitive by default.

## 11. Persistence model

Parent and child Conversations have separate append-only logs.

The parent log records, at minimum:

- `SubagentRequested`: exact parent Tool Call, delegation, target definition/binding digests,
  projected-input digest, grant digest, budget reservation ID/digest, and intended child identity;
- `SubagentStarted`: child Conversation, Submission, Run/Receipt identity, and establishment
  evidence;
- `SubagentJoined`: verified child Settlement identity, outcome/result digest, projected parent
  Tool result, usage summary, reservation ID, and final consumed/releasable accounting decision;
- the normal parent `ToolCallSettled` in the same canonical batch as `SubagentJoined`.

The child log starts with immutable lineage:

- Parent Link;
- child definition, model, Tool, policy, authority, and input digests;
- tenant and data-classification metadata;
- its normal input, model, Tool, approval, recovery, and terminal records.

The parent log does not copy the child's full transcript. The child log does not gain write access
to parent history.

The definition resolver used during durable recovery is an explicit Effect service supplied by
the host. It resolves an Agent Binding by stable identity and exact stored digest. Missing or
different code produces a typed compatibility failure; recovery never silently substitutes the
latest Binding.

If the child Binding is unavailable before execution, framework-owned code writes a
Schema-stable `ChildCompatibilityFailure` Settlement without application code. If the recovering
parent cannot resolve its stored Delegation Definition or projection, the framework records a
bounded `SubagentExecutionFailure` and fails the parent Tool batch; it does not invent an
application Tool result or load the latest declaration.

## 12. Durable child establishment and join

A durable child is a normal accepted Submission in a distinct child Conversation. Its lifecycle
does not share the parent's ownership token or producer epoch.

Establishment is a recoverable protocol:

1. Preflight the complete parent Tool batch.
2. Under the parent ownership token and epoch, create or read the parent-owned budget reservation.
3. Append `SubagentRequested` with the reservation ID/digest and intended child identity.
4. Derive a stable child admission idempotency key from the parent Run and Tool Call identity.
5. Authorize the exact establishment request under current host policy.
6. Admit or resolve the child Submission in the Submission Ledger.
7. Reauthorize current policy, then materialize the child Conversation and immutable Parent Link.
8. Validate the complete lineage record byte-for-field against the request, store durable
   attachments, and mark the child ready.
9. Receive the child Receipt.
10. Append `SubagentStarted` to the parent log.
11. Checkpoint the parent as `waitingForChild`, release its worker-execution permit, and schedule or
    observe the child under its own Attempt ownership.

The runtime MUST NOT emit a durable `SubagentStarted` event or rely on a child until the child
Receipt exists. A crash after child admission but before parent linkage resolves through the same
idempotency key and never creates a second child. A crash after `SubagentStarted` but before the
parent reservation attachment/checkpoint is repaired from the exact canonical
`SubagentRequested`/`SubagentStarted` pair: recovery reattaches that same child under the parent
fence before it stores `waitingForChild`. It never infers a replacement attachment from mutable
ledger state. An expected attachment failure releases the recovery claim immediately so a retry
does not wait for lease expiry; a crash after a successful attachment intentionally retains the
crash-state ownership record for fenced recovery.

Absence from an eventually consistent projection is never proof that admission did not occur.
Recovery queries the deterministically addressed child owner or ledger directly with the
idempotency key and classifies the result as `notAdmitted`, `admitted`, or `indeterminate`. Only
`notAdmitted` permits an admission attempt; `indeterminate` waits and retries rather than creating
another child.

`waitingForChild` retains the parent Conversation lane, ownership of the join obligation, and its
open budget reservation, but holds no worker, provider, model, Tool, or child-concurrency permit.
Child Settlement records an idempotent operational notification, but that notification alone never
wakes the parent. A child may route that notification to the parent owner, but it never reads the
parent's lane-local recovery snapshot; the owning lane's pre-armed maintenance pass performs the
resume decision. The coordinator first validates every stored Tool Call/child pair against exact
canonical `SubagentStarted` evidence and ensures every canonically unjoined child is still named;
these identities are compared field-by-field as structural pairs, never as delimiter-concatenated
strings that distinct identifiers could collide under. A canonical `SubagentJoined` subtracts only
its exact Tool Call/child pair; a join naming a different child is contradictory evidence and
quarantines the suspension. Only the exact-reason resume operation may clear the suspension after
all notifications cover it.
The child uses its separate Conversation lane. S2 MUST prove this suspension/wakeup path at the
smallest worker-pool size. A wake reconciles only the named Conversation through the ledger's
adapter-owned conversation-scoped nonterminal scan; it never filters the global recovery worklist.

Every public durable worker binding is registered with exact definition digests. There is no
identity-only or digest-transparent worker path. Before an admitted child can execute, both the
readiness path and the worker claim gate validate the complete immutable lineage record: parent
Submission/Conversation/Run/Tool Call, delegation and depth, child definition/input/grant digests,
and the deterministic lineage record identity. They also cross-check the admitted child's target,
definition, input, exact Principal, scoped admission idempotency key, Conversation, and parent
linkage against `SubagentRequested`; the admission authorization question separately binds the
reservation identity/digest and child identity. The same complete binding is checked when replaying
an already-admitted child and again at the worker claim gate. The child Principal must equal the
parent Principal, and its admission key must equal the deterministic parent Run/Tool Call key; a
mutually matching but forged request/row pair is not sufficient. Mere presence of a lineage-shaped
record is insufficient; any mismatch fails closed before application code runs.

Joining is also recoverable:

1. Read and verify the child's canonical Settlement.
2. Verify target, Parent Link, input, definition/binding, and result digests.
3. Schema-decode the child terminal output or mapped failure.
4. Apply the bounded result/failure projection.
5. Atomically append `SubagentJoined` and the parent `ToolCallSettled`.
6. Mark the parent reservation `releasePending` from the canonical join, idempotently apply the
   final consumed/released amounts, and mark it `released`.
7. Continue the parent Turn. A crash before step 6 leaves budget unavailable until repair, never
   available twice.

The child's canonical Settlement is authoritative. Cached parent or ledger state cannot fabricate
or rewrite it.

## 13. Durable recovery

Recovery classifies these states:

| Parent/child evidence                                           | Recovery action                                                                                                                                                                                                |
| --------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| No reservation and no `SubagentRequested`                       | No child obligation exists; normal Tool preparation rules apply                                                                                                                                                |
| Reservation exists, request absent                              | Under the parent fence, append the fixed request or release the unused reservation                                                                                                                             |
| Requested, direct admission result `notAdmitted`                | Reauthorize the exact establishment request under current policy, then idempotently admit the intended child                                                                                                   |
| Requested, direct admission result `indeterminate`              | Wait/retry direct resolution; never infer absence from a projection                                                                                                                                            |
| Child admitted, its Conversation lacks or has divergent lineage | The child lane defers or rejects readiness; the worker claim gate releases/refuses the head, and only the parent's reauthorized idempotent establishment may complete exact lineage — no child Turn runs first |
| Child admitted, parent start record missing                     | Resolve by idempotency key and append the exact `SubagentStarted` link                                                                                                                                         |
| Started, child nonterminal                                      | Enter/restore `waitingForChild`; never spawn a replacement invocation                                                                                                                                          |
| Child lost Attempt ownership before unsafe work                 | Child recovery proceeds under its own higher ownership token and epoch                                                                                                                                         |
| Child has unresolved ordinary Tool outcome                      | Child enters operator-resolution state; parent waits and no result is fabricated                                                                                                                               |
| Child terminal, parent join missing                             | Verify the child Settlement and append the parent join/result batch                                                                                                                                            |
| Parent join canonical, budget release incomplete                | Apply the canonical accounting decision idempotently, then mark the reservation released                                                                                                                       |
| Parent Attempt ownership lost while child continues             | Fence stale parent; replacement observes the same child without holding a worker permit                                                                                                                        |
| Required child Binding digest unavailable                       | Write framework `ChildCompatibilityFailure`; never run different code                                                                                                                                          |
| Required parent declaration/projection digest unavailable       | Record framework `SubagentExecutionFailure`; never invent an application result                                                                                                                                |
| Parent abort canonical, child abort absent                      | Idempotently append the child abort command and wait                                                                                                                                                           |
| Child abort canonical, parent propagation marker absent         | Repair the parent marker; do not issue a distinct command                                                                                                                                                      |
| Child terminal concurrently with abort                          | Preserve the one winning child Settlement and join that exact outcome                                                                                                                                          |

An attached durable parent does not settle while its child join obligation is unresolved.
An unresolved external outcome stops active child running time and transitions to an explicit
operator/reconciler-resolution state; it does not consume an infinite execution permit and is not
a fabricated terminal outcome. The accepted settlement obligation, parent join, and budget
reservation remain visible, aged, alerted, and auditable until an authorized resolution records
the truth needed to terminalize it.

A deployment MUST NOT claim durable Subagent liveness unless its configured durability
dependencies include an authorized, bounded resolution path and runbook for these obligations.
This is the ordinary durability contract's resolution dependency, not permission to replay an
unsafe Tool or guess its outcome.

### 13.1 Parent close behavior

The only initial parent-close policy is `request-abort-and-join`:

- parent abort intent becomes canonical;
- every active child receives durable abort intent;
- child ordinary external effects may still become unknown;
- the parent waits for each attached child terminal outcome and records the joins;
- parent Settlement never claims that child external effects were rolled back.

Terminate-without-settlement, abandon, and detached continuation are outside the initial
capability because they weaken structured ownership and complicate accepted-work accounting.

## 14. Durable crash matrix

| Crash point                                                   | Required outcome                                                                                                                                                       |
| ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Before parent budget reservation                              | No request or child obligation exists                                                                                                                                  |
| After reservation, before request append                      | Fenced recovery appends the fixed request or releases the reservation exactly once                                                                                     |
| After request append, before child admission                  | Parent-owned establishment reauthorizes current policy, then direct idempotency resolution classifies admission before one child is admitted                           |
| While admission resolution is indeterminate                   | No second admission occurs; recovery waits/retries the authoritative owner                                                                                             |
| After child admission, before readiness                       | Child-lane recovery defers; parent-owned establishment reauthorizes current policy before completing materialization/readiness                                         |
| After child readiness, before parent start append             | Parent-owned establishment reauthorizes current policy, resolves the same Receipt, and records the link                                                                |
| After parent start, before `waitingForChild` checkpoint       | Recovery checkpoints waiting state and releases execution permits                                                                                                      |
| During child model response                                   | Normal durable model recovery applies inside the child                                                                                                                 |
| During child ordinary Tool                                    | Normal prepared/settled/operator-resolution classification applies inside the child                                                                                    |
| Parent abort canonical, before child abort                    | Recovery emits the one idempotent child abort command                                                                                                                  |
| Child abort canonical, before parent propagation marker       | Recovery repairs the marker without a second command                                                                                                                   |
| Child terminal races with child abort                         | The one canonical child Settlement wins and is joined                                                                                                                  |
| After child Settlement append, before child ledger finalizes  | Cross-store parent marker/generation is durable first; same-store canonical repair atomically creates shared-row coverage; both replay notification after finalization |
| After result projection, before parent join append            | Projection is recomputed from canonical child output and fixed digests                                                                                                 |
| After join append, before budget `releasePending`             | Canonical join drives idempotent accounting repair                                                                                                                     |
| After `releasePending`, before `released`                     | The fixed consumed/released amounts are applied once                                                                                                                   |
| After release, before parent continues                        | Parent replay observes settled Tool Call and released reservation; neither is repeated                                                                                 |
| Parent and child workers stop simultaneously                  | Independent fenced recovery converges on the same link and Settlement                                                                                                  |
| Parent replacement races reservation, release, or join repair | Parent ownership token/epoch permits one transition and rejects stale writes                                                                                           |
| Stale parent resumes after replacement                        | Parent append and parent-ledger transitions are rejected by its ownership token/epoch fence                                                                            |

Every new durable mutation has before/after failpoints. Node tests kill actual processes; the
Cloudflare adapter forces eviction and alarm retries.

## 15. Failure and interruption model

The framework owns a Schema-backed discriminated failure/state family. Application mappings may
narrow details but may not erase the tag:

| Tag                              | Meaning and parent behavior                                                                                         | Retry/Settlement rule                                                                                   |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `SubagentPrestartDenied`         | Undeclared target, failed authorization/preflight, or invalid input; declared Tool failure                          | No child starts; retry only after a new authorized parent Tool Call                                     |
| `SubagentApprovalPending`        | Approval is outstanding; ephemeral run suspends in Scope or durable run releases its worker                         | Not a Tool failure or Settlement until approved, denied, expired, or aborted                            |
| `SubagentApprovalDenied`         | Approval denied, expired, or revoked; declared Tool failure                                                         | No automatic replay                                                                                     |
| `SubagentBudgetExhausted`        | Depth/count/concurrency/turn/tool/time/token/cost/byte/artifact bound reached; declared Tool failure                | No retry without a new budget decision; reservation accounting still settles                            |
| `ChildDomainFailure`             | Child expected `E` mapped exhaustively by the Delegation Definition; declared Tool failure or returned failure data | Definition declares model visibility and retryability                                                   |
| `ChildCompatibilityFailure`      | Stored child Binding/digest unavailable; framework child failure Settlement                                         | Never substitutes current code; parent receives bounded `SubagentExecutionFailure`                      |
| `SubagentProjectionFailure`      | Input/result/artifact Schema, size, integrity, classification, or declassification failure                          | Fail closed; no raw value reaches the parent model                                                      |
| `SubagentInfrastructureFailure`  | Persistence, scheduler, or recoverable service failure                                                              | Retry only at a recorded safe boundary under ownership fencing                                          |
| `SubagentUnknownExternalOutcome` | Ordinary child Tool may have executed but has no canonical result                                                   | Operator/reconciler state, not running time or terminal Settlement; no blind replay                     |
| `SubagentInterrupted`            | Ephemeral fiber interruption or durable canonical abort                                                             | Ephemeral finalizers run; durable child writes the one abort Settlement when safe                       |
| `SubagentDefectFailure`          | Durable boundary converts a child defect to a bounded framework failure record                                      | Ephemeral form remains a defect; durable form is terminal and redacted, never an application-domain `E` |

With `failureMode: "error"` (the default), every expected terminal tag is total-mapped to the
declared Tool failure and the existing Tool batch failure policy applies. With `"return"`
(SUB-033), that declared failure is result data visible to the model through the Tool success
union; the engine-signal members (`ToolCallWaiting`, `SubagentDurabilityError`) are never
contained. Pending approval and unknown outcome are nonterminal runtime states. Raw `Cause`, stack, provider response, secret, and Tool payload values
are never persisted or returned as the public parent error.

No recovery rule may blindly replay an unresolved ordinary child Tool or claim exactly-once child
model execution. Every tag defines its canonical record, retryability, redaction, parent batch
consequence, and durable Settlement mapping in the executable state-machine fixture.

## 16. Verification

### 16.1 Type and Schema evidence

- compile-time tests distinguish native per-call Tool `E`/`R` from handler-Layer construction
  `E`/`R` and prove the total child-failure mapping;
- the Tool explicitly requires `AgentSpawner`; child requirements cannot be inferred from the
  `Agent.Requirements` union or silently captured from the root Context;
- an unbound child Definition cannot be executed;
- missing child handlers, Model requirements, or projection services remain readable type errors;
- every request, link, grant, budget, event, record, result, and failure Schema has round-trip,
  invalid-input, size, and redaction tests;
- no type assertion crosses a child input, result, record, or recovery boundary.

### 16.2 Deterministic runtime evidence

- scripted parent and child LanguageModel Layers cover success, expected failure, malformed output,
  timeout, interruption, and nested denial;
- parent interruption reaches every child and all finalizers run;
- child concurrency never exceeds either child or ancestor bounds;
- varying completion order never changes parent Tool result order;
- child transcript content never leaks into parent context unless projected;
- smallest semaphore configurations do not deadlock;
- slow child observers do not determine child or parent liveness.

### 16.3 Budget and security evidence

- generated parallel-spawn sequences cannot oversubscribe one parent reservation;
- reservation/consume/release races, duplicate join replay, simultaneous parent/child settlement,
  and every crash boundary preserve conservation and idempotency;
- missing, late, corrected, or over-reservation provider usage never creates budget or hides
  ancestor consumption;
- unused budget returns once and descendant usage is charged exactly once to every ancestor;
- recursive and cyclic model requests are rejected at depth one under minimum semaphore limits;
- attempts to address undeclared Agents fail before start;
- a parent allowed to delegate but forbidden from a concrete child resource cannot reach it;
- revoking policy, Tenant membership, resource authority, or approval after admission denies the
  next affected child action and durable resume;
- a parent approval cannot be replayed for a child Tool, sibling, descendant, or recovered Attempt;
- parent/child/Receipt IDOR and unauthorized observer reconnect/page reads fail closed;
- artifact digest substitution, cross-tenant deduplication leakage, content-type confusion, and
  classification laundering fail closed;
- policy bypass through a child's Tools, MCP, Skill, sandbox, artifacts, or secrets fails closed;
- hostile child output cannot register capabilities or escape its result Schema;
- delegation approval and audit identity use the originating parent Tool Call;
- child failures, progress, events, and provenance cannot exfiltrate secrets.

### 16.4 Durability evidence

- the establishment and join crash matrices run against the executable recovery model;
- duplicate admission produces one child Receipt and Conversation;
- parent and child ownership tokens and epochs fence stale writers independently;
- a completed child is never re-executed merely because parent join acknowledgment was lost;
- authoritative admission lookup distinguishes `notAdmitted`, `admitted`, and `indeterminate`;
- waiting parents release worker/provider/Tool permits and wake durably from child Settlement;
- reservation, request, admission, abort, join, and release failpoints all converge;
- an unknown child Tool outcome blocks parent continuation while exposing an aged, alerted
  resolution obligation;
- simultaneous parent/child process loss converges;
- Node/SQLite and Cloudflare run the same Subagent conformance suite;
- no deployment claims durable Subagents while any required crash test is skipped.

## 17. Implementation slices

Subagents were introduced in two proof slices rather than mixing unproven cross-Conversation
recovery into the base durable runtime.

### S1 — Attached ephemeral Subagents

Delivers:

- pure `Subagent.define` Tool helper, explicit `AgentSpawner` dependency, and
  `SubagentRuntime.layer` in `@effect-agent/capabilities`;
- engine execution options for preallocated child Conversation/Run identity and Parent Link;
- explicit child Binding;
- fresh child Conversation per invocation;
- context and result projection;
- fail-closed structural child Tool-name/depth ceiling and non-transitive establishment approval;
- an in-memory hierarchical reservation service with conservation tests;
- depth `1`, normative nested-delegation rejection, attached join only, and no Conversation reuse;
- scoped Fiber/FiberSet ownership and bounded parallel children;
- stable live events and child observation;
- deterministic scripted Travel Planner specialist delegation.

Allowed claim: `E` Subagents, plus persisted child history under deployment class `P` when a
Conversation Store is supplied. Process loss still ends the work; no durable accepted-child claim.

### S2 — Durable attached Subagents

S2 builds on base admission, ordinary Tool uncertainty, durable Steps, approval suspension, and
joined input. An epoch-bearing Conversation Store alone is insufficient: S2 also requires the
durable ledger/ownership port, with every claim, reservation, abort, accounting, join, and
Settlement transition fenced by the current ownership token and epoch.

Delivers:

- child accepted-work admission and Receipt;
- Parent Link and exact binding resolution;
- requested/started/joined canonical records;
- parent-owned reservation state machine and recovery repair;
- authoritative cross-Conversation admission lookup by idempotency key;
- parent `waitingForChild` suspension, worker-permit release, and durable wakeup;
- child Settlement join and parent Tool settlement;
- independent parent/child ownership and fencing;
- durable abort propagation;
- establishment/join failpoints and process-kill tests;
- Node/SQLite conformance before Cloudflare.

Allowed claim: `DN` durable attached Subagents only after the full crash matrix passes. `DC`
requires the same suite under Durable Object eviction and alarms.

### Later extensions

Require separate proposals:

- any nested delegation, including depth above `1`; the proposal must define root-relative depth,
  total descendants, definition-cycle policy, shared ancestor permits/acquisition order, and
  tenant/global caps;
- follow-up submissions to an existing child Conversation;
- handoffs and user-facing control transfer;
- detached/abandoned children;
- child-to-parent questions or peer messaging;
- dynamic discovery or remote Agent registries;
- cross-tenant delegation;
- long-lived Agent teams.

## 18. Requirements

- **SUB-001**: Model-visible delegation uses Effect AI Tools and Toolkits directly.
- **SUB-002**: A parent may invoke only immutable, declared Delegation Definitions.
- **SUB-003**: Every invocation uses an explicit child Agent Binding and native Tool dependency on
  `AgentSpawner`; model output cannot select a Layer or ambient Model.
- **SUB-004**: Every invocation owns a distinct child Conversation with one immutable Parent Link.
- **SUB-005**: Child input, result, expected-failure projection, commands, events, structural
  ceilings, budgets, and records originate from Effect Schema.
- **SUB-006**: A child receives no implicit parent transcript, state, Tool, Skill, MCP, sandbox,
  secret, or artifact authority.
- **SUB-007**: Establishment authorization and approval are evaluated before child start or durable
  admission.
- **SUB-008**: `SubagentGrant` is only an immutable Tool-name/depth ceiling. Durable admission and
  every protected child operation are authorized fail-closed by their owning boundary under
  current Principal/Tenant, policy, target, and normalized resource identity.
- **SUB-009**: Every child has finite depth, count, concurrency, Turn, Tool, duration, and byte
  bounds.
- **SUB-010**: Stable parent-owned reservations conserve each budget dimension, prevent parallel
  oversubscription, charge each unit once per ancestor, and release unused allocation exactly once.
- **SUB-011**: Ephemeral children are supervised by the parent Scope; parent interruption waits for
  child interruption and finalizers.
- **SUB-012**: No Subagent execution uses a daemon Fiber, global Scope, or unowned Promise.
- **SUB-013**: Child completion may be observed in real order, while parent Tool results commit in
  original model Tool Call order (response-array order).
- **SUB-014**: A parent model Turn receives a complete child result through the Tool Schema or no
  result.
- **SUB-015**: Full child history remains in the child Conversation and is not copied into parent
  model context.
- **SUB-016**: Durable child establishment is idempotent by parent Run and Tool Call identity,
  reauthorizes current policy before replayed admission and materialization/readiness, and validates
  exact immutable lineage before readiness and execution.
- **SUB-017**: A durable child Receipt exists before `SubagentStarted` becomes canonical or
  externally durable-visible.
- **SUB-018**: Durable recovery reattaches the one existing child and never infers or respawns it
  from model prose.
- **SUB-019**: Parent join verifies the child's canonical Settlement and atomically commits
  `SubagentJoined` with the parent Tool result; a separate idempotent ledger transition applies the
  canonical accounting decision.
- **SUB-020**: Parent and child Attempts have independent ownership tokens, epochs, and fencing.
- **SUB-021**: An unresolved ordinary Tool inside a child becomes an aged operator/reconciler
  obligation and blocks parent continuation; no child result is fabricated or blindly replayed.
- **SUB-022**: Initial parent close behavior is request-abort-and-join; detached and abandon modes
  are unsupported.
- **SUB-023**: Every durable worker Binding uses exact stored definition/model/Tool/policy digests;
  no public identity-only or digest-transparent resolution exists, and unavailable or divergent
  code fails closed.
- **SUB-024**: Parent observation exposes bounded lifecycle summaries; every child read,
  subscription, reconnect, and page requires current resource authorization and redaction.
- **SUB-025**: Security, budget, structured-concurrency, state-machine, adapter, and crash tests are
  release gates for every advertised Subagent deployment class.
- **SUB-026**: Parent delegation approval authorizes only establishment and is never transitive to
  child actions, siblings, retries, or descendants.
- **SUB-027**: Artifact references bind immutable identity, Tenant, source, classification, type,
  size, and operation; dereference reauthorizes and result projection preserves classification.
- **SUB-028**: Every expected child failure is total-mapped to the declared Effect AI Tool failure;
  interruption and defects retain their distinct semantics.
- **SUB-029**: S1 and S2 reject every nested delegation at preflight; depth above one requires a
  separately accepted scheduling, cycle, authority, and budget contract.
- **SUB-030**: A durable parent waiting for a child retains its lane and join obligation but
  releases worker, provider, model, Tool, and child-concurrency permits.
- **SUB-031**: Durable admission recovery uses an authoritative idempotency-key query with
  `notAdmitted`, `admitted`, and `indeterminate` results; projection absence never proves absence.
- **SUB-032**: Child-binding and parent-declaration compatibility failures have distinct,
  framework-owned Schema records and never substitute newer code.
- **SUB-033**: Under `failureMode: "return"` every expected delegation failure — the declared
  child failure and the framework failure family — is contained as model-visible result data
  instead of failing the parent Tool batch; the engine-owned waiting signal and durability error
  always stay in the error channel, durable suspension semantics are preserved unchanged, and the
  durable settlement join records the same non-failure polarity the live batch continues with.
- **SUB-034**: A per-invocation Tool Call allowance is tightening-only and clamped fail-closed to
  the delegation's per-invocation reservation slice and the child Definition's policy;
  `projectResult` receives the framework's honest exhaustion marker (from the child result on the
  ephemeral path and the child Settlement on the durable path), and a budget extension is a fresh
  re-delegation with a raised allowance — never a mid-flight reservation top-up or child
  Conversation reuse.
