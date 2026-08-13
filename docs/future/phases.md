---
title: Future phases
description: A capability-led roadmap from the current persistent runtime to verified durable execution.
---

# Future phases

<StatusCallout status="next" phase="P4 is next" title="The roadmap is ordered by proof, not calendar date.">

P0–P3 have implementation evidence. P4 is the next active target. P5–P7 remain planned. The
overall specification and roadmap are Draft; “complete” means the current evidence gate passed,
not that a public compatibility promise exists.

</StatusCallout>

<PhaseRail />

## P0 — Design proof <StatusBadge status="available" />

Proved that Agent input/output Schemas, Effectful instructions, native Effect AI Toolkits, Model
Bindings, and handler requirements can flow through one interpreter without erasing `E` or `R`.

The first Travel Planner slice also proved deterministic model testing and Scope finalization.

## P1 — Ephemeral core <StatusBadge status="available" />

Delivered `Agent.define`, `Agent.withModel`, `AgentRuntime.run`, `stream`, and `start`; bounded Tool
batches; semantic Run Events; finite Stop Policy; typed output; and upstream provider Model
Bindings.

**Claim:** bounded class `E` execution inside one process Scope.

## P2 — Operational local runtime <StatusBadge status="available" />

Added safe-seam steering and follow-up, process-local Conversations, approval, hierarchical budgets,
ordered context preparation and compaction, MCP connection policy, scheduling overrides, structural
redaction, and an explicitly unisolated local sandbox adapter.

**Claim:** interactive local behavior, not crash recovery or security isolation.

## P3 — Persistent Conversations <StatusBadge status="available" />

Added versioned canonical records, pure replay, digest-bound checkpoints, export, resumable
observation, and memory/SQLite Conversation Store Layers.

**Claim:** class `P` Conversation persistence. The `SubmissionStore` explicitly rejects a durable
accepted-work claim.

## P4 — Durable Node/SQLite <StatusBadge status="next" />

The next phase introduces the first durable host boundary:

- commit admission, Conversation readiness, and attachments before returning a Receipt;
- serialize one ordered active head per Conversation;
- fence Attempts with stable ownership identity and producer epochs;
- classify recovery from persisted state;
- reserve, append, and finalize exactly one Settlement;
- prove behavior with deterministic failpoints and real process-kill tests.

**Target claim:** `DN`, durable accepted work on the tested Node/SQLite assembly.

## P5 — Durable Tools and joined input <StatusBadge status="planned" />

Adds prepared/settled ordinary Tool records, honest unknown external outcomes, named durable Steps,
reconciliation, durable approval suspension, and recoverable `joining`/`joined` queued input.

**Target claim:** crash recovery does not fabricate or blindly replay consequential Tool outcomes.

## P6 — Cloudflare runtime <StatusBadge status="planned" />

Maps the same durable service contracts to one SQLite-backed Durable Object per Conversation,
alarm-driven recovery, eviction tests, and optional R2 artifacts. Platform bindings remain outside
the engine behind Effect services and Layers.

**Target claim:** `DC`, equivalent canonical outcomes under the shared conformance suite.

## P7 — Internal hardening <StatusBadge status="planned" />

Adds adapter certification, executable state-machine modeling, administrative diagnostics, threat
modeling, chaos and soak tests, recovery runbooks, and validation through real internal Agents.

This phase is where a working runtime earns durable release confidence.

## Proposed insertion: attached Subagents <StatusBadge status="proposed" />

ADR-0010 recommends an S1 ephemeral slice after P3 and an S2 durable slice after the P4/P5 recovery
foundations. The owner has not accepted that decision. Subagents are therefore not active phase
scope and every interface in the [Subagent guide](./subagents) remains proposed.

## Deliberately outside the first release

Hosted orchestration, channels, a turnkey chat UI, visual agent builder, embedded coding sandbox,
marketplace, PostgreSQL, generic multi-node scheduling, and broad Agent-team patterns are not hidden
inside these phases.
