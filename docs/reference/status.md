---
title: Implementation status
description: What the runtime claims today, the evidence behind each claim, and the explicit non-claims.
---

# Implementation status

Every surface documented on this site is implemented and tested. That is an engineering claim,
not a stability one: the project is pre-1.0, all packages are private, Effect v4 is pinned to a
beta release, and the specifications remain Draft. The source of truth for each claim is the test
tree — `bun run requirements:coverage` mechanically ties every specification requirement to a
test title or a documented exception.

## Capability matrix

Each row names the claimed surface, the evidence that proves it, and what the claim deliberately
does not include.

| Surface                              | Evidence                          | Explicit non-claim                                                                             |
| ------------------------------------ | --------------------------------- | ---------------------------------------------------------------------------------------------- |
| Agent Definition and Binding         | type proofs, core tests           | no public compatibility window                                                                 |
| `run`, `stream`, `start`             | shared-trace runtime tests        | no process-loss recovery                                                                       |
| bounded Tool batches                 | scheduling and interruption tests | no automatic external-effect retry                                                             |
| provider Model Bindings              | OpenAI/Anthropic compile examples | examples make no live request                                                                  |
| steering and follow-up               | safe-seam capability tests        | queues are process-local                                                                       |
| approval, budgets, context           | adapter and policy tests          | ephemeral approvals do not suspend durably                                                     |
| MCP connector policy                 | bounded discovery tests           | remote Tools are not exactly once                                                              |
| local sandbox adapter                | process/limit tests               | explicitly unisolated                                                                          |
| canonical Conversation Log           | reducer and round-trip tests      | persistence is not accepted work                                                               |
| memory and SQLite stores             | shared adapter contracts          | current-version data only                                                                      |
| Receipt and durable admission        | ledger conformance, crash matrix  | no exactly-once external effects                                                               |
| Attempt ownership and Settlement     | durable-runtime suite             | model calls may repeat at Turn boundaries                                                      |
| unknown Tool outcome and Steps       | durable-tool + crash suites       | at-least-once execution; the operator surface owns Unknown-lane aging                          |
| approval suspension and joined input | durable-approval suite            | denial stays terminal; no suspension timeout                                                   |
| Cloudflare durable runtime           | eviction + restart matrices       | workerd/Miniflare harness, not the hosted platform; no exactly-once external effects           |
| attached ephemeral Subagents         | capabilities subagent suite       | `E` only                                                                                       |
| durable attached Subagents           | durable-subagents + cross-DO rows | no exactly-once child effects                                                                  |
| admin operations and obligations     | admin operation suites            | possession-default authorization; hosts own alerting; DC entry points lack an authorizer lever |
| certification, formal model, chaos   | certification + formal + chaos    | bounded-instance design check, not code proof; live suites opt-in and not executed here        |

## Deployment classes

Durability wording is precise. Four labels describe what an assembly claims:

- **`E` — ephemeral execution.** A Run lives and dies with its process.
- **`P` — persistent Conversation history.** Canonical records survive restart; active work does
  not.
- **`DN` — durable Node/SQLite.** Durable Submissions with durable-boundary recovery,
  prepared/settled ordinary Tool records, Unknown Outcomes with an audited resolution path,
  Durable Steps, durable approval suspension, joined queued input, and durable attached
  Subagents with verified Settlement joins and independent fencing.
- **`DC` — durable Cloudflare.** The same claims re-earned on one SQLite-backed Durable Object
  per Conversation — the same ports, conformance suites, and crash rows, with eviction and alarm
  redelivery as the exercised recovery path and cross-Object Subagent delegation. The tested
  harness is workerd/Miniflare, not the hosted platform.

Neither durable class claims exactly-once model inference or external Tool/Step execution:
recovery may re-invoke the model from the last committed boundary, external effects are
at-least-once, and an unresolved ordinary Tool stops at a visible Unknown Outcome instead of
replaying.
