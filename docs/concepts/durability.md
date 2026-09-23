---
title: Persistence & durability
description: Keep history and recover accepted work after a crash.
---

<a id="persistence-and-durability"></a>

# Persistence & durability

Effect Agent supports persistent history and durable execution as separate capabilities.
Persistence rebuilds recorded state. Durable execution accepts work, survives lost attempts, and
owes one terminal settlement for every acknowledged submission.

`InMemory.layer` retains conversations across Runs for as long as its application Scope stays
open, within the store's capacity limits. In-memory describes where state lives. Ephemeral
execution means work cannot recover after process loss; it can use either in-memory or persistent
history. Neither term implies that an application or conversation must be short-lived.

## Execution modes {#four-deployment-classes}

| Class | Meaning                                                    |
| ----- | ---------------------------------------------------------- |
| `E`   | in-memory history; execution has no process-loss recovery  |
| `P`   | persistent history; execution has no process-loss recovery |
| `DN`  | durable admission and recovery on Node and SQLite          |
| `DC`  | the same contract on Cloudflare Durable Objects            |

Choose a deployment class and adapter with the recovery guarantees your application needs.
See the [Node.js](../platforms/node) and [Cloudflare](../platforms/cloudflare) guides for setup.

## Rebuild from the log {#canonical-history}

The thread log is an append-only sequence of versioned facts. It is authoritative for
applied input and terminal outcomes. Projections, checkpoints, indexes, and UI views can be rebuilt.

Replay rebuilds state from records. It never executes a tool or repeats an external effect.

An Attempt captures a fixed canonical tail and validates contiguous pages. For uncompacted history,
it gathers control and journal metadata together. Later appends enter through a separately captured
suffix; a gap or short page fails before that view can drive recovery. Compaction records and
checkpoint-seeded views use the full journal validation path. Attempt metadata does not replace
canonical prompt or unresolved-tool validation.

`ThreadProjection` version 2 scopes open tool calls and subagent invocations by Run and Tool Call
ID. Decode checkpoint state with its Schema before replaying a suffix. Earlier projection states,
including empty views, fail decoding and must be discarded and rebuilt from canonical records.
The canonical record and checkpoint envelope versions remain unchanged.

## Resume through a recovery checkpoint {#recovery-checkpoints}

After a durable compaction or context rollover commits its replacement, the runtime can save a
recovery checkpoint through `ThreadStore.recoveryCheckpoints`. The checkpoint preserves the
replacement context, protected instructions and input, cumulative usage and policy accounting,
the latest replayable tool batch, and required control and Durable Step evidence. Completed Step
results remain available for reuse after an ownership change.

This optional cache holds one latest snapshot per Thread. Saves require the current producer
epoch and bind the snapshot to a canonical batch tail. It is separate from the generic
`ThreadStore.checkpoints` slot used by application projections. Neither slot changes canonical
history or owns a submission.

Recovery checks the checkpoint's versions, state digest, agent/model/tool definitions, retained
submissions, and canonical binding before replaying the suffix. The suffix is limited to 4,096
records, read in pages of at most 1,024. Missing, corrupt, incompatible, or ineligible checkpoints
fall back to the captured canonical prefix. A longer or incompatible suffix also uses full replay;
cache capacity never justifies dropping control or Step evidence. Storage infrastructure failures
remain typed failures.

The canonical log and submission ledger remain authoritative. A history-search index supplies
retrieval candidates and cannot stand in for this recovery state. Ordinary unresolved tools keep
the same reconciliation and unknown-outcome rules with or without a checkpoint.

## Track unfinished work {#operational-obligation}

The submission ledger owns admission, FIFO readiness, attempt ownership, abort intent, recovery,
and the obligation to settle accepted work. An unknown Submission without abort intent is parked:
later input can run in the same Thread while the original settlement obligation stays open.
Suspended, joining, and joined work retain their ordering barriers. At most one live owner can
claim a Thread; a wake hint does not acquire ownership or advance its fencing epoch.

A host can opt into `SubmissionScheduling.yieldTo` to handle the next same-Agent input in its
own Run after a completed Turn. The prior Attempt closes before the next one acquires ownership;
principals, captured inputs, receipts and reply identities remain separate. Keep independent inputs
out of `claimJoining`. The policy cannot bypass an admission gap, pending approval or child wait,
and does not interrupt in-flight model requests or Tool batches. Deferred Runs remain owed and
resume with their original authority and deadlines. Their model context contains prior history and
their own Turns; later independent Runs cannot replace their input or compact their context.
Install matching runtime and storage packages.

`SubmissionLedger.scanNonterminal` discovers work through `SubmissionWorkItem`: identities,
receipt, deployment, queue order and state, without execution payloads. Read `lookup` or
`loadRecoverySnapshot` only for selected work. Recovery hydrates each Thread inside its fault
boundary, so an unreadable retained input or worker origin cannot poison global discovery.

`runRecovery()` isolates history, retained payload and child-recovery faults by Thread. It returns ordinary
Submission `reports` and one `blocked` fault per failed Thread. Blocked Threads cannot be
claimed until recovery succeeds. Pass `{ threadId }` to recover a selected Thread independently.
The host owns durable fault visibility and retry scheduling outside the execution log. The
default cooperative recovery bound is 30 seconds per Thread (`recoveryTimeout`). Interruption
and global SQL/control-identity scan failures still fail the sweep. A recovery fault never settles accepted work,
proves an external effect failed, or authorizes replay.

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

Step identity includes the Run ID, Tool Call ID, and Step name. New Step record and batch IDs use
a versioned JSON tuple so separator characters cannot merge distinct Steps. Recovery derives
the same identity from each recorded payload, preserving completed Steps stored with older IDs
without executing their bodies again.

## Reuse the same agent definition {#one-authoring-model}

Use the same agent definition for ephemeral runs and durable registration. Durable hosts also
need storage, versioned registrations, and a recovery driver. See the platform setup guides above.

The optional [`WorkflowAgentHost`](../guide/workflows) drives this runtime through an
injected Effect `WorkflowEngine`. Replacing the engine Layer leaves the agent definitions and
durable driver unchanged. See the guide for host composition and platform setup.

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

## Retain resources while awaiting approval

`AgentRegistration.attemptLayer` owns services for one fenced Attempt. Its resources finalize
when the Attempt completes, suspends, fails, or is interrupted. A replacement Attempt builds
fresh services around any externally retained resource.

When an approval wait must retain a resource, provide `DurableApprovalSuspension` from that same
Layer. This optional `Effect<void, ApprovalSuspensionError>` captures the live services and finishes
the host's checkpoint and handoff before returning. Import both names from
`effect-agent/durable-agent-runtime`. Wrap typed failures in `ApprovalSuspensionError.make({ cause })`
using `Effect.catchCause` and `Cause.map` to preserve accompanying defects and interruptions.

The runtime calls it after recording the approval request, while claim renewal and abort observation
remain active. Failure preserves its cause and leaves accepted work owed; interruption runs normal
cleanup. If approval arrives during retention, the old services and claim finalize before a fresh
Attempt resumes the same Run's pending tool batch. Completed tools are not repeated, and unresolved
ordinary effects still require reconciliation.

## Admission and recovery

The runtime returns a Receipt after durable ledger admission, thread materialization, and
readiness. Reusing an admission key with the same input returns the same Receipt. Different input
conflicts. Admission sequence sets queue order.

Each producer write checks its ownership token and epoch. A stale attempt cannot append after a
replacement takes ownership. Recovery uses one current binding per stable Agent ID and validates
a strongly consistent canonical prefix before classifying the last committed boundary:

| Last committed boundary                          | Recovery                                                                     |
| ------------------------------------------------ | ---------------------------------------------------------------------------- |
| admission without readiness                      | finish materialization and readiness                                         |
| ready input with no attempted execution          | leave input application to the normal worker claim                           |
| input appended without its ledger marker         | repair the marker without applying input twice                               |
| `RunStarted`                                     | preserve the original deadline                                               |
| incomplete model response                        | retry inference when policy allows; provider charges may repeat              |
| complete tool declaration without preparation    | check the original operation contract, then resume or record unavailability  |
| ordinary tool prepared without a result          | reconcile or record `UnknownToolOutcome`                                     |
| canonical tool or Durable Step result            | reuse the recorded result                                                    |
| `RunCompleted`                                   | preserve stored output and disposition; validate `resultDigest` when present |
| reserved settlement                              | append that outcome, then finalize the ledger idempotently                   |
| canonical settlement without ledger finalization | finalize from history                                                        |

Joined input follows the same rule. Claimed input without a canonical append returns to ready.
Appended input rejoins its host run and settles with it. Approval must be canonical before work
resumes. Unknown work releases execution permits while keeping its accepted obligation open.
Abort preserves evidence and cannot roll back external effects or replace a settlement that won.
See [Operations](../guide/operations).

`ModelResponseRecorded.toolOperations` retains compact per-call identity, execution class, kind,
and replay hash. `ToolCallPrepared` retains the original parameters and may also carry the class,
kind, and hash. These facts govern pending operation replay independently of later Agent or
toolbox changes; old records without proof do not authorize a changed handler.

An incompatible mutating call with proof that dispatch never started receives `ToolUnavailable`
with `execution: "not-executed"`. Readonly calls can run without a prepared record, so that
absence alone yields no never-started proof; an unavailable readonly result uses
`execution: "unavailable"`. Prepared calls require their original execution semantics before
retry. Reconciliation's `CompletedWithResult` injects a confirmed result; `NeverStarted` can retire an unavailable
call without executing it. `SafeToRetry` does not authorize changed code or erase uncertainty
about an unsupported operation. Unproven effects stay unknown.

Later model requests preserve earlier user intent, assistant text, and settled sibling results.
For an incomplete earlier batch, the model-facing history uses `ToolUnavailable` with
`execution: "not-executed"` when the recorded operation required preparation and no preparation
or unknown-outcome evidence exists. Other missing results remain explicitly unknown, including
ordinary readonly calls that can run without preparation. This explanatory view creates no
canonical tool settlement or compaction coverage and does not resolve a dispatched operation.
A committed `RunCompleted` output and
disposition remain authoritative across later codec or completion-projector changes.

## Attached subagents

Use [`Subagent.make`](../guide/subagents/durable-attached#define-the-delegation) to expose a child agent as a tool.
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
