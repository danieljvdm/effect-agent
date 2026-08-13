---
title: Implementation status
description: Current, next, planned, and proposed surfaces with their evidence and explicit non-claims.
---

# Implementation status

The documentation describes both the current workspace and its intended destination. Status labels
prevent future interfaces from being mistaken for available code.

## Label semantics

| Label                              | Meaning                                                         |
| ---------------------------------- | --------------------------------------------------------------- |
| <StatusBadge status="available" /> | source exists and the current phase has implementation evidence |
| <StatusBadge status="next" />      | specified and assigned to the next implementation phase         |
| <StatusBadge status="planned" />   | specified in a later active roadmap phase                       |
| <StatusBadge status="proposed" />  | design recommendation still awaiting owner acceptance           |

“Available” does not mean published or stable. All packages are private, Effect v4 is pinned to a
beta release, and the user-facing specification remains Draft.

## Capability matrix

| Surface                          |       Phase | Status                             | Evidence                          | Explicit non-claim                 |
| -------------------------------- | ----------: | ---------------------------------- | --------------------------------- | ---------------------------------- |
| Agent Definition and Binding     |       P0–P1 | <StatusBadge status="available" /> | type proofs, core tests           | no public compatibility window     |
| `run`, `stream`, `start`         |          P1 | <StatusBadge status="available" /> | shared-trace runtime tests        | no process-loss recovery           |
| bounded Tool batches             |          P1 | <StatusBadge status="available" /> | scheduling and interruption tests | no automatic external-effect retry |
| provider Model Bindings          |          P1 | <StatusBadge status="available" /> | OpenAI/Anthropic compile examples | examples make no live request      |
| steering and follow-up           |          P2 | <StatusBadge status="available" /> | safe-seam capability tests        | queues are process-local           |
| approval, budgets, context       |          P2 | <StatusBadge status="available" /> | adapter and policy tests          | approval is not durable suspension |
| MCP connector policy             |          P2 | <StatusBadge status="available" /> | bounded discovery tests           | remote Tools are not exactly once  |
| local sandbox adapter            |          P2 | <StatusBadge status="available" /> | process/limit tests               | explicitly unisolated              |
| canonical Conversation Log       |          P3 | <StatusBadge status="available" /> | reducer and round-trip tests      | persistence is not accepted work   |
| memory and SQLite stores         |          P3 | <StatusBadge status="available" /> | shared adapter contracts          | current-version data only          |
| Receipt and durable admission    |          P4 | <StatusBadge status="next" />      | specification only                | no API exists today                |
| Attempt ownership and Settlement |          P4 | <StatusBadge status="next" />      | specification only                | no coordinator exists today        |
| unknown Tool outcome and Steps   |          P5 | <StatusBadge status="planned" />   | specification only                | no durable Tool surface exists     |
| Cloudflare durable runtime       |          P6 | <StatusBadge status="planned" />   | specification only                | no Cloudflare packages exist       |
| attached ephemeral Subagents     |          S1 | <StatusBadge status="available" /> | [S1 evidence](../S1-EVIDENCE)     | `E` only; ADR-0010 stays Proposed  |
| durable attached Subagents       | proposed S2 | <StatusBadge status="proposed" />  | ADR/spec design                   | not accepted roadmap scope         |

S1 attached ephemeral Subagents are implemented as the roadmap-assigned proposed default: the
owner has not accepted [ADR-0010](../adr/0010-declared-attached-subagents), so the delegation
surface is governed by a Proposed decision even though its ephemeral implementation and tests
exist. Durable attached Subagents (S2) remain a proposal with no implementation, no durable child
records, and no accepted-child claim.

## Current milestone

<PhaseRail />

The source of truth for completion evidence is:

- [Phase 0 evidence](../PHASE-0-EVIDENCE)
- [Phase 1 evidence](../PHASE-1-EVIDENCE)
- [Phase 2 evidence](../PHASE-2-EVIDENCE)
- [Phase 3 evidence](../PHASE-3-EVIDENCE)
- [S1 evidence](../S1-EVIDENCE) — attached ephemeral Subagents

The [requirements index](../REQUIREMENTS) distinguishes specified, planned, implemented, verified,
and released. Editing a specification to say “must” does not make the behavior available.

## Durable wording policy

Use these labels precisely:

- `E`: ephemeral execution;
- `P`: persistent Conversation history;
- `DN`: durable Node/SQLite after its complete gate passes;
- `DC`: durable Cloudflare after its complete gate passes.

Today the highest implemented deployment class is `P`.
