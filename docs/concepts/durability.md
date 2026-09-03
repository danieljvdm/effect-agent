---
title: Persistence & durability
description: Keep history and recover accepted work after a crash.
---

<a id="persistence-and-durability"></a>

# Persistence & durability

Effect Agent supports persistent history and durable execution as separate capabilities.
Persistence rebuilds recorded state. Durable execution accepts work, survives lost attempts, and
owes one terminal settlement for every acknowledged submission.

## Execution modes {#four-deployment-classes}

| Class | Meaning                                            |
| ----- | -------------------------------------------------- |
| `E`   | ephemeral execution with no process-loss recovery  |
| `P`   | thread history survives restart; clients may retry |
| `DN`  | durable admission and recovery on Node and SQLite  |
| `DC`  | the same contract on Cloudflare Durable Objects    |

Choose a deployment class and adapter with the recovery guarantees your application needs.
See the [Node.js](../platforms/node) and [Cloudflare](../platforms/cloudflare) guides for setup.

## Rebuild from the log {#canonical-history}

The thread log is an append-only sequence of versioned facts. It is authoritative for
applied input and terminal outcomes. Projections, checkpoints, indexes, and UI views can be rebuilt.

Replay rebuilds state from records. It never executes a tool or repeats an external effect.

## Track unfinished work {#operational-obligation}

The submission ledger owns admission, FIFO readiness, attempt ownership, abort intent, recovery,
and the obligation to settle accepted work.

```text
thread log              submission ledger
what happened                 what is still owed
append-only                   operational, mutable, audited
replay authority              claim and scheduling authority
canonical settlement          outstanding settlement obligation
```

## Exactly-once recording

The runtime records one accepted settlement. It does not promise one physical execution of every
external operation. Model calls and external APIs may repeat across crash windows.

If an ordinary tool may have finished before its worker disappeared, recovery records an
`UnknownToolOutcome`. It cannot safely infer failure or replay the call.

Durable Steps record one result for each deterministic Step name. Their external execution is at
least once and may repeat. Applications still need idempotency, reconciliation, or compensation.

## Reuse the same agent definition {#one-authoring-model}

Use the same agent definition for ephemeral runs and durable registration. Durable hosts also
need storage, versioned registrations, and a recovery driver. See the platform setup guides above.

The optional [`WorkflowAgentHost`](../platforms/node#workflow) drives this runtime through an
injected Effect `WorkflowEngine`. Replacing the engine Layer leaves the agent definitions and
durable driver unchanged. The certified SQL setup is a single Node process; it adds no fleet or
Cloudflare Workflow guarantee.

Each native Workflow advances a submission through journal recovery and bounded Attempts.
Pending work, approvals, unknown outcomes, and native suspension remain unfinished until the
canonical log contains a Settlement. Infrastructure failures suspend the Workflow for repair.
Ordinary tools remain ordinary tools; the driver does not wrap them in replayable Activities.

Inside an application Workflow, `AgentWorkflow.execute` assigns each named step a stable
submission identity and awaits an upstream `DurableDeferred`. The dispatch intent retains its
completion token until repair delivers a reference to the canonical Settlement. Notification
and cleanup are separate recoverable commits. A resumed handler rechecks admission identity and
authorization and decodes the canonical result; the deferred does not store a second copy of
the Agent output. Parent interruption detaches the caller without cancelling accepted work.

Admission, dispatch intent storage, and native Workflow storage commit independently. A required
host-owned repair trigger discovers accepted work and retries retained dispatch intents after
lost hints or process loss. An intent remains until native success identifies the matching
canonical Settlement. No cross-database transaction encloses an agent execution.

Interrupting an observer or settlement waiter detaches it. Abort and resolution commands use the
durable runtime's authorization and intent protocol. Native Workflow interruption is not the
agent cancellation API. Each Attempt releases its ownership and resources before suspension.

## Admission and recovery

The runtime returns a Receipt after durable ledger admission, thread materialization, and
readiness. Reusing an admission key with the same input returns the same Receipt. Different input
conflicts. Admission sequence sets queue order.

Each producer write checks its ownership token and epoch. A stale attempt cannot append after a
replacement takes ownership. Recovery validates a strongly consistent canonical prefix, then
classifies the last committed boundary:

| Last committed boundary                          | Recovery                                                        |
| ------------------------------------------------ | --------------------------------------------------------------- |
| admission without readiness                      | finish materialization and readiness                            |
| input appended without its ledger marker         | repair the marker without applying input twice                  |
| `RunStarted`                                     | preserve the original deadline                                  |
| incomplete model response                        | retry inference when policy allows; provider charges may repeat |
| complete tool declaration without preparation    | reauthorize and resume the batch without another model call     |
| ordinary tool prepared without a result          | reconcile or record `UnknownToolOutcome`                        |
| canonical tool or Durable Step result            | reuse the recorded result                                       |
| reserved settlement                              | append that outcome, then finalize the ledger idempotently      |
| canonical settlement without ledger finalization | finalize from history                                           |

Joined input follows the same rule. Claimed input without a canonical append returns to ready.
Appended input rejoins its host run and settles with it. Approval must be canonical before work
resumes. Unknown work releases execution permits while keeping its accepted obligation open.
Abort preserves evidence and cannot roll back external effects or replace a settlement that won.
See [Operations](../guide/operations).

## Attached subagents

Use [`Subagent.define`](../guide/subagents#define-a-delegation) to expose a child agent as a tool.
A durable child owns a separate thread and attempt. While waiting for it, the parent releases
its worker permit.

Recovery preserves child identity and checks the registered tool's delegation classification.
Missing or conflicting classification fails closed. If admission cannot confirm whether a child
was accepted, the parent keeps waiting; it never starts a replacement child.

Joining verifies the child's settlement, lineage, and definition digests, decodes and bounds the
projected result, and commits it atomically with the parent tool result. Child transcripts remain
private unless the projection exposes them.

Budget release happens once. A crash may hold a reservation until repair, but cannot make it
available twice. Run accounting survives replacement attempts, including pending turns,
programmatic calls, failure streaks, usage, cost, and the original deadline. A reservation made
before a crash may consume allowance even when the corresponding work never executes.

Parent abort records intent for each child and joins the child's terminal outcome before settling
the parent. Unknown child effects remain operator obligations. After the parent deadline, recovery
may finish child abort, join, and accounting work. It cannot start a child, call application
projectors, run tools, or continue the model.
