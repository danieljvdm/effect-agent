# ADR-0010: Model Subagents as declared attached delegation Tools

- Status: Proposed
- Date: 2026-07-30
- Decision owners: Project owner
- Related decisions: D-002, D-004, D-006, D-007, D-008, D-013, D-020

## Context

The architecture already reserves Subagents, child Scope ownership, bounded concurrency, child
Conversation linkage, audit events, and hierarchical budgets, but it does not define an authoring,
authority, result, or durable join contract.

Research distinguishes two materially different multi-Agent patterns:

- manager-style delegation, where the parent invokes a specialist and retains control;
- handoff, where conversational control moves to another Agent.

The project needs the first pattern sooner, without adding a second Tool abstraction, cloning
parent context and authority, creating daemon work, or weakening the durable accepted-work
contract.

## Proposed decision

Implement the first Subagent capability as **declared attached delegation**.

- Each target is exposed to the parent model as an Effect AI Tool created from an immutable
  Delegation Definition.
- A pure declaration is separate from the outward handler Layer. The native Tool explicitly
  depends on the engine-owned `AgentSpawner`; the handler Layer supplies the explicit child
  Binding and its visible construction requirements.
- The Tool handler runs the same Effect Agent interpreter through execution options that accept
  preallocated child identity and Parent Link. It does not implement a second child loop.
- Each invocation creates a fresh, distinct child Conversation with immutable linkage to the
  parent Run and Tool Call.
- Context, authority, budgets, output, and expected failures cross explicit Schema-backed
  projections. Every expected child failure is total-mapped to the declared native Tool failure;
  interruption and defects remain distinct.
- The immutable delegation grant is an authority ceiling, not a cached authorization. Every child
  action reauthorizes current Principal/Tenant, policy, target, and normalized resource.
- Parent approval authorizes only child establishment. Child Tools, Models, MCP/Skill activation,
  secrets, artifacts, sandbox operations, retries, and future descendants follow their own
  ordinary approval and authorization checks.
- Artifact references bind immutable content, Tenant, source, classification, type, size, and
  allowed operation. Result projection is an explicit declassification boundary.
- Hierarchical budgets use stable parent-owned reservations with conservation and idempotent
  consume/release transitions. Durable reservation is fenced before the request; child admission
  references its digest; canonical join fixes accounting and a repairable ledger transition
  releases unused allocation.
- The parent retains conversational control and joins one bounded child result as the Tool result.
- Ephemeral children run in a child-owned scoped region attached to the parent Run fiber and never
  use daemon Fibers.
- Durable children are separately admitted Submissions. Parent recovery reattaches them by stable
  parent Tool Call identity, authoritative admission lookup, and joins their canonical Settlement.
- A durable waiting parent checkpoints and releases execution/provider permits while retaining its
  Conversation lane, join obligation, and open reservation.
- Parent and child Attempts retain independent ownership tokens, epochs, and fencing.
- Attached children receive abort intent and are joined before parent terminalization.
- Child concurrency and parent result order follow the existing bounded Tool scheduler: execution
  may complete in real order, while parent results commit in original model Tool Call order.

The initial S1 and S2 releases have depth one and reject all nested delegation, use one-shot child
Conversations, and provide no handoff, detachment, abandonment, peer messaging, or follow-up reuse.

The complete proposed contract is [the Subagent specification](../spec/subagents.md).

## Consequences

Positive:

- delegation composes with Effect AI Tool schemas, approval, handler Layers, and provider behavior;
- the parent remains an ordinary Agent with one semantic interpreter;
- context and authority isolation are explicit and testable;
- child work has stable identity and inspectable history;
- structured concurrency supplies honest ephemeral cancellation;
- the existing durability protocol can establish and join children without pretending Fibers
  survive crashes;
- deterministic parent history is preserved under parallel specialists.

Negative:

- cross-Agent failure projection requires an explicit Schema boundary;
- hierarchical budget reservation is more complex than post-hoc usage accounting;
- durable establishment and join add cross-Conversation recovery states and failpoints;
- durable parents require a worker-permit-free suspension and wakeup path;
- attached abort may block while a child has an unresolved external outcome;
- exact child Binding resolution is required during recovery;
- a depth-one first release does not support general Agent teams.

## Alternatives considered

### Treat a Subagent as an ordinary opaque Tool handler

Rejected because an opaque handler has no child identity, Conversation linkage, authority grant,
budget lineage, durable establishment, recovery join, or child observation contract.

### Add a framework-owned Subagent call type beside Effect AI Tool

Rejected because providers already expose model-selected operations through Tools. A parallel call
protocol would duplicate schema translation, approval, batching, and result semantics.

### Clone the parent Context and Conversation

Rejected because it leaks irrelevant or privileged context, creates ambiguous state ownership,
inflates model context, and makes parent/child replay inseparable.

### Use one Conversation for parent and child

Rejected because the parent occupies the ordered lane while waiting, the child needs independent
history and ownership, and same-lane execution can deadlock durable FIFO scheduling.

### Start detached children

Rejected for the first capability because parent Scope closure would no longer own all work,
accepted-work settlement could outlive its initiating policy context, and parent-close semantics
would need explicit abandon/transfer rules.

### Implement handoff first

Rejected because handoff changes the active conversational owner, client routing, history
projection, approval routing, and user-facing lifecycle. It is not required for manager-style
specialist delegation.

### Delay all Subagents until after open-source preparation

Rejected as a recommendation because attached ephemeral delegation can be proven on the current
interpreter and Scope model, while durable delegation can be staged after the base P4/P5 recovery
protocol instead of remaining wholly unspecified.

## Validation

Before acceptance:

- type-prototype the Tool helper and handler Layer against the pinned Effect v4 release;
- prove native per-call Tool `E`/`R` is distinct from handler-Layer construction `E`/`R`, all child
  requirements remain visible, and expected child failures are total-mapped;
- model the reservation conservation equation, idempotent consume/release transitions, and
  smallest-limit concurrency;
- review per-action reauthorization, non-transitive approval, observer authorization, artifact
  declassification, and the closed failure/state family;
- assign the S1 and S2 slices in the roadmap.

Before ephemeral release:

- deterministic parent/child model tests cover parallel completion, failure, timeout,
  interruption, depth, budgets, and finalizers;
- security tests prove context/capability isolation, revocation after admission, approval replay
  resistance, observer IDOR resistance, and artifact classification integrity;
- no child Fiber outlives the parent Scope.

Before durable release:

- every reservation, request, admission, waiting/wakeup, abort, join, accounting, and release
  mutation has before/after failpoints;
- the parent releases worker/provider permits while waiting, and a child Settlement wakes it;
- authoritative child admission lookup distinguishes absence from indeterminate state;
- simultaneous parent/child process-kill tests converge;
- unresolved child ordinary Tools block rather than replay;
- the deployment has an authorized, alerted resolution path for unknown external outcomes;
- Node/SQLite passes the Subagent conformance suite before Cloudflare claims equivalence.
