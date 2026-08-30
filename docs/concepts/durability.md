---
title: Persistence and durability
description: Why replayable history and accepted-work recovery are separate product capabilities.
---

# Persistence and durability

Persistence answers "can I rebuild what was recorded?" Durability answers "after I acknowledge
work, who owes the terminal outcome?" Durability requires a separate work ledger and recovery
protocol.

## Four deployment classes

| Class | Meaning                                                  |
| ----- | -------------------------------------------------------- |
| `E`   | ephemeral execution; no process-loss recovery            |
| `P`   | Conversation history survives restart; clients may retry |
| `DN`  | durable admission and recovery on Node/SQLite            |
| `DC`  | the equivalent contract on Cloudflare Durable Objects    |

All four classes are implemented. No package or example uses "durable" without naming the
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

## Admission and recovery

Return a Receipt only after ledger admission, Conversation materialization, and readiness are
durable. Reusing an admission key with the same input returns the same Receipt; a different input
conflicts. Queue order comes from admission sequence, not wall-clock arrival. Every producer write
checks its ownership token and epoch atomically, rejecting stale Attempts.

Recovery validates a strongly consistent canonical prefix before repairing ledger state. It
classifies the last committed boundary instead of replaying the Run:

| Last committed boundary                          | Recovery                                                                             |
| ------------------------------------------------ | ------------------------------------------------------------------------------------ |
| Admission without readiness                      | Finish materialization and readiness.                                                |
| Input appended without its ledger marker         | Repair the marker without applying input twice.                                      |
| `RunStarted`                                     | Preserve its original deadline; reject a replacement duration allowance.             |
| Incomplete model response                        | Retry inference if permitted; duplicate provider charges are possible.               |
| Complete Tool declaration without preparation    | Reauthorize and resume that batch without another model call.                        |
| Ordinary Tool prepared without a result          | Reconcile or expose `UnknownToolOutcome`; never infer that invocation did not start. |
| Canonical Tool result or Durable Step result     | Reuse the recorded result.                                                           |
| Reserved Settlement                              | Append that exact outcome, then finalize the ledger idempotently.                    |
| Canonical Settlement without ledger finalization | Finalize from history; do not choose another outcome.                                |

Joined input follows the same rule: a claimed input without a canonical append returns to ready;
an appended input reattaches to its host Run and settles with it. Approval decisions must be
canonical before execution resumes. Unknown work releases execution permits but remains an
accepted obligation requiring an authorized resolution path. Abort does not roll back external
effects or rewrite a winning Settlement. See [Operations](../guide/operations).

## Attached subagents

`Subagent.define` annotates its Tool with the core-owned `DelegationTool` marker. Preparation
persists `executionKind`, and recovery checks it against the resolved Tool before re-entry.
The `delegate_` name prefix is only an authoring convention. An ordinary `delegate_export`
without a recorded outcome follows ordinary reconciliation and Unknown Outcome handling.
A prepared delegation interrupted before `SubagentRequested` can resume idempotent establishment.
Missing classification grants no delegation replay authority; conflicting evidence fails closed.

Run limits survive replacement Attempts. Canonical responses account for Turns and declared
Tool Calls, including a pending batch; fully settled batches account for the trailing failure
streak in declaration order. Replaying a pending batch folds its outcomes once. Synthetic budget
rejections neither advance nor reset that streak. Provider-executed outcomes remain in canonical
assistant content and resume alongside application outcomes in their original declaration order.
Programmatic Tool Calls and the single grace
finalization reserve their allowance canonically before execution. A crash after reservation
can consume allowance without execution; it cannot restore allowance. Token, cost, and duration
accounting retain their existing contracts. `RunStarted.policyAccountingVersion` identifies this
accounting contract; incompatible private-development histories must be reset, not migrated.
Custom coordinators must supply `resumeUsage` when resuming a declared batch. The seed includes
the pending Turn and its declared calls, while its failure streak excludes that batch. Missing
seeds, a different Turn count, too few declared calls, or a failure streak exceeding the prior
declared calls fail with `ModelProtocolError` before execution.

A durable child owns a distinct Conversation and Attempt. Under the parent's fence, reserve its
budget, append `SubagentRequested` with the intended identity and exact binding/input digests,
then admit using a stable parent-Run/Tool-Call idempotency key. Commit child lineage before
readiness. Only after its Receipt exists may the parent append `SubagentStarted` and enter
`waitingForChild`, releasing its worker permit.

Resolve lost admission replies against the authoritative admission owner. `indeterminate` means
wait, never create a replacement child. Missing exact binding or projection digests fail closed;
current code cannot silently substitute for the recorded binding.

Join by verifying the child's canonical Settlement, lineage, and digests, then Schema-decode and
bound its projected result. Append `SubagentJoined` and the parent Tool result atomically before
idempotently releasing the reservation. A crash can leave budget unavailable until repair, never
available twice. Child transcripts stay private unless explicitly projected.

Parent abort uses `request-abort-and-join`: persist abort intent for each child and join its actual
terminal outcome before settling the parent. Unknown child effects remain resolution obligations.
After the parent's deadline expires, recovery may finish existing child abort/join/accounting
work, but cannot start a new child, invoke application projectors, run Tools, or continue the model.

The [recovery classifier](https://github.com/danieljvdm/effect-agent/blob/main/packages/session/src/recovery.ts),
coordinator failpoints, and [adapter certification](../guide/certify-adapters) exercise these boundaries.
