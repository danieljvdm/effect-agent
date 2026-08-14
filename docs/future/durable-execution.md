---
title: Durable execution
description: The implemented Receipt, Attempt, recovery, and Settlement contracts, plus the durable Tool uncertainty and Step contracts.
---

# Durable execution

<StatusCallout status="available" phase="P4–P5 implemented" title="The durable runtime exists on Node/SQLite.">

The `DN` submit/await/observe/abort surface below is implemented and tested
([Phase 4 evidence](../PHASE-4-EVIDENCE)), and so are ordinary-Tool uncertainty records, unknown
outcomes, durable Steps, durable approval suspension, and joined queued input
([Phase 5 evidence](../PHASE-5-EVIDENCE)).

</StatusCallout>

The durable runtime makes one bounded promise:

> After a Submission is durably accepted, the runtime owes exactly one durable terminal Settlement:
> `completed`, `failed`, or `aborted`.

That is exactly-once **recording** over at-least-once Attempt execution. Recovery resumes from
the last committed durable boundary and may re-invoke the model, so duplicate provider cost stays
possible and observable. Since Phase 5, consequential external mutations are covered by an
explicit uncertainty protocol instead of being excluded from the claim.

## Submit accepted work

`DurableAgentRuntime` is an Effect Context service; submission is an Effect, not a background
Promise:

```ts
const runtime = yield * DurableAgentRuntime;

const receipt =
  yield *
  runtime.submit(agent, input, {
    conversationId,
    principal,
    idempotencyKey,
    definitions,
  });
```

`submit` returns only after all of these are durable:

1. the Submission Ledger obligation;
2. Conversation materialization and its first canonical record;
3. the readiness marker.

(Durable attachments are not part of Phase 4 admission; an `AttachmentStore` port is deferred.)

The returned `Receipt` carries `receiptId`, `submissionId`, `conversationId`, and
`queueSequence`. It is stable identity for observation and reattachment, not an authorization
capability. Retrying the same `(conversationId, principal, idempotencyKey)` with the same input
returns the original Receipt; a conflicting payload under the same key fails with
`AdmissionConflict`.

## Await or observe without owning liveness

```ts
const settlement = yield * runtime.awaitSettlement(receipt);
const records = runtime.observe(receipt, { after: offset });
```

Interrupting a local await or observation fiber detaches that caller. It does not cancel accepted
work. Durable abort is a separate command:

```ts
const intent = yield * runtime.abort(AbortCommand.make({ submissionId, author, reason }));
```

Abort is idempotent; ready work settles `aborted` without an Attempt, an active worker appends
the canonical `AbortRequested` record before its Run fiber is interrupted, and settled work
answers with `SettlementConflict`.

## One Conversation lane

The default scheduler allows one active head per Conversation. FIFO order comes from the queue
sequence allocated at admission, not wall-clock arrival. Different Conversations may execute
concurrently.

When claimed, the Submission's exact input is appended canonically before the Model can consume
it. Later queued input can **join** an active Run at safe Turn seams: the contiguous ready prefix
behind the head transitions `joining`, its content is appended canonically as the deterministic
`input:{submissionId}` record, and the now-`joined` Submission settles with its host Run — each
accepted Submission still receives its own Settlement record. Recovery returns pre-append
`joining` input to ready and reattaches post-append `joined` input without duplicate delivery
(DUR-016). Aborting a `joined` Submission fails with a typed host-linkage conflict; the abort
target is the host.

## Attempts and fencing

A durable Run may span several process Attempts. Each ledger claim returns an Attempt ID, an
ownership token, a renewable lease, and an atomically bumped producer epoch. Every canonical
append carries that fencing evidence.

A replaced worker may resume in memory, but its stale epoch cannot append a late model token,
Tool result, or Settlement. Leases are liveness only — expiry makes the lane claimable again;
epochs are the correctness authority (ADR-0011).

## Terminalization

Settlement crosses the operational ledger and canonical Conversation Log through a recoverable
sequence:

```text
reserve one exact Settlement in the ledger
  → append that exact record to canonical history
  → finalize the ledger and release the lane
```

Every step is idempotent. If a worker dies after canonical append, recovery repairs the ledger
from history. Canonical history never gets rewritten from a stale cached status.

## Recovery

Recovery classification is a pure function over a persisted snapshot plus canonical evidence.
The host runs a full recovery pass before opening admission, and every executed repair appends a
deterministic audit record. Deterministic failpoints surround every durable mutation in both the
coordinator and the SQLite ledger.

## Ordinary Tool uncertainty

<StatusBadge status="available" />

Tools declare an execution class with `Tool.annotate(ToolExecutionClass, ...)`:
`"readonly"` (no external mutation — a crash is a free re-run), `"idempotent"` (a declared
external idempotency contract — recovery may re-execute without proof), or `"uncertain"` — the
fail-closed default for unannotated Tools.

Before a non-readonly Tool Call's handler starts, the coordinator commits `ToolCallPrepared`;
after validated completion it commits `ToolCallSettled`. If recovery finds only the prepared
record, it consults the registered `ToolReconciler` and proceeds only when the policy can prove
that:

- invocation never started;
- the external result can be recovered (it then settles canonically without executing); or
- retry is safe under a stable external idempotency contract.

Otherwise the Submission enters the operational `unknown` state, the lane blocks without
consuming a worker permit, and the canonical `ToolCallUnknown` records the uncertainty. The
framework will not lie to the Model about whether the effect happened. Resolution is a durable,
audited operation — `resolveUnknown` with supplier truth (`CompletedWithResult`,
`NeverHappened`, `SafeToRetry`, or `AbortSubmission`) converges the lane to one Settlement
([Phase 5 evidence](../PHASE-5-EVIDENCE)).

## Durable Steps

<StatusBadge status="available" />

```ts
const result = yield * step.do("charge-customer", ChargeResult, chargeEffect);
```

Declaring `DurableStep` in a Tool's `dependencies` is what makes it a Durable Tool. A named Step
may execute at least once while exactly one accepted result is durably recorded
(`ToolStepSettled`, identity `step:{runId}:{toolCallId}:{stepName}`); a previously committed
result decodes through the declared output Schema and replays without executing the body. Only
success is ever recorded. Without a durable runtime the engine provides a pass-through that
executes once and records nothing — the durable claim attaches to the runtime, not the Tool.

This does not turn a non-idempotent external API into an exactly-once system. Step authors still
provide idempotency (for example, keys derived from `(toolCallId, stepName)`), reconciliation, or
compensation.

## Durable approval suspension

<StatusBadge status="available" />

An approval-gated Tool Call whose decision is not yet known suspends the Submission durably: the
canonical `ToolApprovalRequested` is the safe boundary, ownership ends, and the lane consumes no
worker permit. `resolveApproval` records the durable decision idempotently; the resuming Attempt
appends the canonical `ToolApprovalDecided` and replays the declared batch without re-invoking
the model. Denial remains terminal (the P2 policy default).

## Proof behind the claim

The `DN` label rests on executable evidence ([Phase 4](../PHASE-4-EVIDENCE) and
[Phase 5](../PHASE-5-EVIDENCE) evidence):

- before/after failpoints exist for every durable mutation, including the Phase 5 prepared,
  step, approval, join, and resolution seams;
- deterministic failpoint-kill tests and a real process-kill harness cover admission through
  Settlement, including the full recovery-classifier crash matrix;
- stale owners are fenced out of canonical history;
- later FIFO work cannot pass an unsettled head;
- both ledger adapters pass one shared conformance suite;
- crash tests never fabricate a booking result: they recover confirmed supplier truth, repeat
  safely under a declared idempotency contract, or stop at an Unknown Outcome.

What `DN` still does not claim: exactly-once model inference or external Tool or Step execution,
and automatic replay of unresolved ordinary Tools remains forbidden by construction (DUR-009).
The operator surface for Unknown Outcomes was delivered by Phase 7: `scanObligations` reports
aging and blocked lanes, the admin operations explain and resolve them, and hosts own the alert
loop ([operations guide](../guides/operations.md),
[Phase 7 evidence](../PHASE-7-EVIDENCE)).
