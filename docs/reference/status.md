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

| Surface                              | Phase | Status                             | Evidence                           | Explicit non-claim                                                                             |
| ------------------------------------ | ----: | ---------------------------------- | ---------------------------------- | ---------------------------------------------------------------------------------------------- |
| Agent Definition and Binding         | P0–P1 | <StatusBadge status="available" /> | type proofs, core tests            | no public compatibility window                                                                 |
| `run`, `stream`, `start`             |    P1 | <StatusBadge status="available" /> | shared-trace runtime tests         | no process-loss recovery                                                                       |
| bounded Tool batches                 |    P1 | <StatusBadge status="available" /> | scheduling and interruption tests  | no automatic external-effect retry                                                             |
| provider Model Bindings              |    P1 | <StatusBadge status="available" /> | OpenAI/Anthropic compile examples  | examples make no live request                                                                  |
| steering and follow-up               |    P2 | <StatusBadge status="available" /> | safe-seam capability tests         | queues are process-local                                                                       |
| approval, budgets, context           |    P2 | <StatusBadge status="available" /> | adapter and policy tests           | ephemeral approvals do not suspend durably                                                     |
| MCP connector policy                 |    P2 | <StatusBadge status="available" /> | bounded discovery tests            | remote Tools are not exactly once                                                              |
| local sandbox adapter                |    P2 | <StatusBadge status="available" /> | process/limit tests                | explicitly unisolated                                                                          |
| canonical Conversation Log           |    P3 | <StatusBadge status="available" /> | reducer and round-trip tests       | persistence is not accepted work                                                               |
| memory and SQLite stores             |    P3 | <StatusBadge status="available" /> | shared adapter contracts           | current-version data only                                                                      |
| Receipt and durable admission        |    P4 | <StatusBadge status="available" /> | [P4 evidence](../PHASE-4-EVIDENCE) | no exactly-once external effects                                                               |
| Attempt ownership and Settlement     |    P4 | <StatusBadge status="available" /> | [P4 evidence](../PHASE-4-EVIDENCE) | model calls may repeat at Turn boundaries                                                      |
| unknown Tool outcome and Steps       |    P5 | <StatusBadge status="available" /> | [P5 evidence](../PHASE-5-EVIDENCE) | at-least-once execution; the P7 operator surface owns Unknown-lane aging                       |
| approval suspension and joined input |    P5 | <StatusBadge status="available" /> | [P5 evidence](../PHASE-5-EVIDENCE) | denial stays terminal; no suspension timeout                                                   |
| Cloudflare durable runtime           |    P6 | <StatusBadge status="available" /> | [P6 evidence](../PHASE-6-EVIDENCE) | workerd/Miniflare harness, not the hosted platform; no exactly-once external effects           |
| attached ephemeral Subagents         |    S1 | <StatusBadge status="available" /> | [S1 evidence](../S1-EVIDENCE)      | `E` only; ADR-0010 stays Proposed                                                              |
| durable attached Subagents           |    S2 | <StatusBadge status="available" /> | [S2 evidence](../S2-EVIDENCE)      | no exactly-once child effects; `DC` row via [P6 evidence](../PHASE-6-EVIDENCE)                 |
| admin operations and obligations     |    P7 | <StatusBadge status="available" /> | [P7 evidence](../PHASE-7-EVIDENCE) | possession-default authorization; hosts own alerting; DC entry points lack an authorizer lever |
| certification, formal model, chaos   |    P7 | <StatusBadge status="available" /> | [P7 evidence](../PHASE-7-EVIDENCE) | bounded-instance design check, not code proof; live suites opt-in and not executed here        |

Both Subagent slices are implemented as roadmap-assigned proposed defaults: the owner has not
accepted [ADR-0010](../adr/0010-declared-attached-subagents), so the delegation surface is
governed by a Proposed decision even though its ephemeral (S1) and durable (S2) implementations
and tests exist. The S2 shape-level defaults are recorded in
[ADR-0013](../adr/0013-durable-subagent-establishment) as Accepted by default. Durable attached
Subagents are claimed for `DN` — child accepted-work admission, `waitingForChild`
suspension/wakeup, verified Settlement joins, independent fencing, and abort propagation under
real process-kill tests — and, since Phase 6, for `DC`: the same delegation matrix re-run with
parent and child in different Durable Objects under eviction and alarm redelivery
([P6 evidence](../PHASE-6-EVIDENCE)). Child external effects stay honestly at-least-once on
both platforms.

## Current milestone

<PhaseRail />

Every numbered roadmap phase (P0–P7) is Complete and both Subagent slices are implemented as
roadmap-assigned proposed defaults; the roadmap table is fully discharged. This is an
engineering claim, not a governance one — the specifications stay Draft, the project stays
pre-1.0 and private, ADR-0010 remains Proposed, and ADRs 0011–0015 (D-029…D-033) are accepted by
default awaiting owner review ([P7 evidence](../PHASE-7-EVIDENCE)). The source of truth for
completion evidence is:

- [Phase 0 evidence](../PHASE-0-EVIDENCE)
- [Phase 1 evidence](../PHASE-1-EVIDENCE)
- [Phase 2 evidence](../PHASE-2-EVIDENCE)
- [Phase 3 evidence](../PHASE-3-EVIDENCE)
- [Phase 4 evidence](../PHASE-4-EVIDENCE)
- [Phase 5 evidence](../PHASE-5-EVIDENCE)
- [S1 evidence](../S1-EVIDENCE) — attached ephemeral Subagents
- [S2 evidence](../S2-EVIDENCE) — durable attached Subagents
- [Phase 6 evidence](../PHASE-6-EVIDENCE) — Cloudflare Durable Object runtime
- [Phase 7 evidence](../PHASE-7-EVIDENCE) — internal hardening (certification, formal model,
  admin operations, security review, chaos/soak, internal agents)

The [requirements index](../REQUIREMENTS) distinguishes specified, planned, implemented, verified,
and released. Editing a specification to say “must” does not make the behavior available.

## Durable wording policy

Use these labels precisely:

- `E`: ephemeral execution;
- `P`: persistent Conversation history;
- `DN`: durable Node/SQLite after its complete gate passes;
- `DC`: durable Cloudflare after its complete gate passes.

Today both durable deployment classes are implemented: `DN` on the tested Node/SQLite assembly
and `DC` on the tested Cloudflare Durable Object assembly (workerd/Miniflare harness). `DN`
carries durable Submissions with durable-boundary recovery, consequential external mutation
under the Phase 5 uncertainty protocol — prepared/settled ordinary Tool records, Unknown
Outcomes with an audited resolution path, Durable Steps, durable approval suspension, and
joined queued input ([Phase 4](../PHASE-4-EVIDENCE) and [Phase 5](../PHASE-5-EVIDENCE)
evidence) — and durable attached Subagents: separately admitted child Submissions with verified
Settlement joins, `waitingForChild` suspension/wakeup, and independent fencing
([S2 evidence](../S2-EVIDENCE)). `DC` re-earns the same claims — the same ports, the same
conformance suites, the same crash-row names, byte-equal cross-platform normalized canonical
Travel Planner evidence — with one SQLite-backed Durable Object per Conversation, eviction and
alarm redelivery as the exercised recovery path, and cross-Object Subagent delegation
([Phase 6 evidence](../PHASE-6-EVIDENCE)). The P3 `P` label described persistence whose
canonical appends were application-driven; the durable coordinator now commits engine Turns to
the log itself. Neither class claims exactly-once model inference or external Tool/Step
execution — recovery may re-invoke the model from the last committed boundary, external effects
are at-least-once, and an unresolved ordinary Tool stops at a visible Unknown Outcome instead
of replaying.
