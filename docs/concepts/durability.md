---
title: Persistence and durability
description: Why replayable history and accepted-work recovery are separate product capabilities.
---

# Persistence and durability

Persistence answers “can I rebuild what was recorded?” Durability answers “after I acknowledge
work, who owes the terminal outcome?” The second promise requires substantially more machinery.

## Four deployment classes

| Class | Meaning                                                  |
| ----- | -------------------------------------------------------- |
| `E`   | ephemeral execution; no process-loss recovery            |
| `P`   | Conversation history survives restart; clients may retry |
| `DN`  | durable admission and recovery on Node/SQLite            |
| `DC`  | the equivalent contract on Cloudflare Durable Objects    |

All four classes are implemented. No package or example uses “durable” without naming the
deployment class and tested adapter.

## Canonical history

The Conversation Log is an append-only sequence of versioned facts. It is authoritative for
applied input and terminal outcomes. Projections, checkpoints, search indexes, and UI views are
rebuildable.

Replay reads records to rebuild state. It never executes a Tool or repeats an external effect.

## Operational obligation

The Submission Ledger separately owns admission, FIFO readiness, Attempt ownership, abort
intent, recovery classification, and the obligation to settle accepted work.

Keeping the two structures separate makes both contracts explicit:

```text
Conversation Log              Submission Ledger
what happened                 what is still owed
append-only                   operational, mutable + audited
replay authority              claim and scheduling authority
canonical Settlement          outstanding Settlement obligation
```

## Exactly-once recording

The contract promises one accepted Settlement record, not one physical execution of every
external operation. Model inference and external APIs can repeat across crash windows.

An ordinary Tool may finish externally just before its worker disappears. If storage cannot prove
what happened, recovery must expose `UnknownToolOutcome` rather than rerun the operation or invent
a failure.

Durable Steps narrow that ambiguity by exactly-once-recording a named result while allowing
at-least-once execution. Applications still need external idempotency, reconciliation, or
compensation.

## One authoring model

Durability never changes how an Agent is written, because the interfaces are constrained from the
start:

- Definitions are immutable and digestible;
- Tool Call IDs are stable across scheduling and records;
- Tool batches commit deterministically;
- safe-seam input has one semantic rule;
- stores implement fenced, idempotent canonical append;
- provider SDK objects never become recovery truth.

The [durability specification](../spec/durability) defines the admission, recovery, and Settlement
contracts.
