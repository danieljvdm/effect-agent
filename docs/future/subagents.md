---
title: Declared Subagents
description: The proposed attached-delegation design and its unresolved owner decision.
---

# Declared Subagents

<StatusCallout status="proposed" phase="ADR-0010" title="Proposed architecture—not accepted roadmap scope or implemented API.">

The names and signatures on this page are design material. They exist to pressure-test authority,
typing, resource ownership, budget, and recovery before implementation.

</StatusCallout>

The proposal models the first Subagent capability as declared, attached delegation. A parent
retains conversational control and invokes one specialist through a native Effect AI Tool.

It is intentionally not a general Agent-team or handoff system.

## Declaration, then handler Layer

The target authoring shape separates pure delegation metadata from the outward Layer that can
construct and run the child Binding.

```ts
const Research = Subagent.define("research", {
  input: ResearchBrief,
  output: ResearchResult,
  failure: ResearchFailure,
  context: projectResearchContext,
  authority: ResearchAuthority,
  budget: {
    maxTurns: 6,
    maxToolCalls: 12,
  },
});

const ResearchLive = Research.toLayer({
  agent: ResearchAgentBinding,
  mapFailure: researchFailureFromChild,
});
```

The exact helper has not been type-prototyped. The invariant is more important than the spelling:
the exposed operation is an Effect AI Tool, child requirements remain visible, and every expected
child failure crosses a total Schema-backed projection.

## Fresh child Conversation

Each invocation creates a fresh child Conversation with an immutable Parent Link to the parent
Conversation, Run, Tool Call, Agent, delegation, and depth.

Parent and child do not share one Conversation. That avoids ambiguous ownership and a durable
same-lane deadlock while the parent waits.

## Authority does not flow transitively

The declaration is a ceiling, not cached permission. Every child action reauthorizes current
Principal, Tenant, policy, target, and normalized resource.

Parent approval authorizes only child establishment. It does not approve child Tools, Models, MCP
or Skill activation, secret access, sandbox operations, retries, or descendants.

Context and result projections are explicit declassification boundaries; the child never receives
the parent's whole Context or ambient Layer registry.

## Attached lifecycle

The proposed first release is depth one, one-shot, and attached:

- child fibers belong to the parent Scope in ephemeral execution;
- child concurrency shares existing finite Tool scheduling;
- results commit to the parent in model declaration order;
- parent completion joins every attached child;
- parent interruption reaches every child;
- nesting, detachment, handoff, peer messaging, and child reuse are rejected.

## Two proposed slices

### S1 — Ephemeral attached delegation <StatusBadge status="proposed" />

Could be proven on the current interpreter and Scope model. It still requires real budget
reservation, per-action authorization, deterministic parent ordering, context isolation, and
finalizer tests.

### S2 — Durable attached delegation <StatusBadge status="proposed" />

Depends on P4 Attempt ownership and P5 uncertainty/joining foundations. The child becomes a
separately accepted Submission with independent fencing. A waiting parent must release execution
and provider permits while retaining its lane, join obligation, and budget reservation.

The parent reattaches by stable Tool Call identity and joins the child's canonical Settlement.

## Questions the proposal must still prove

- Can native Tool `E`/`R` preserve child failures and handler-Layer construction requirements?
- Can hierarchical reservations conserve budget across every crash and retry?
- Can a parent wait without consuming a worker permit or deadlocking its Conversation lane?
- Can authorization revocation after admission prevent a later protected child action?
- Can observer and artifact APIs prevent parent/child and cross-tenant IDOR?
- Can abort converge when the child contains an unresolved ordinary external effect?

Until those are answered and the owner accepts ADR-0010, Subagents remain a proposal rather than a
feature promise.
