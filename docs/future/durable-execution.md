---
title: Durable execution
description: The implemented Receipt, Attempt, recovery, and Settlement contracts, plus the planned durable Tool and Step contracts.
---

# Durable execution

<StatusCallout status="available" phase="P4 implemented; P5 planned" title="The base durable runtime exists on Node/SQLite.">

The `DN` submit/await/observe/abort surface below is implemented and tested
([Phase 4 evidence](../PHASE-4-EVIDENCE)). Ordinary-Tool uncertainty records, unknown outcomes,
and durable Steps remain Phase 5 targets and are marked planned.

</StatusCallout>

The durable runtime makes one bounded promise:

> After a Submission is durably accepted, the runtime owes exactly one durable terminal Settlement:
> `completed`, `failed`, or `aborted`.

That is exactly-once **recording** over at-least-once Attempt execution. In Phase 4 recovery
resumes from the last committed Turn boundary and may re-invoke the model, so the `DN` claim is
valid for safe-to-repeat toolkits; consequential external mutations wait for Phase 5.

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
it. Later input joining an active Run at safe Turn seams (`joining`/`joined`) remains Phase 5
scope.

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

<StatusBadge status="planned" />

Before an ordinary external Tool runs, the future engine records `ToolCallPrepared`. After
validated completion, it records `ToolCallSettled`.

If recovery finds only the prepared record, it may proceed only when policy can prove that:

- invocation never started;
- the external result can be reconciled; or
- retry is safe under a stable external idempotency contract.

Otherwise the Submission enters an operational `Unknown` state and automatic continuation stops.
The framework will not lie to the Model about whether the effect happened. The `MarkUnknown`
recovery branch already exists in the classifier, but no Phase 4 flow can trigger it: Phase 4
commits ordinary Tool results atomically inside the Turn batch.

## Durable Steps

<StatusBadge status="planned" />

```ts
const result = yield * step.do("charge-customer", ChargeResult, chargeEffect);
```

The exact API remains subject to implementation, but its contract is settled: a named Step may
execute at least once while exactly one accepted result is durably recorded. A previously committed
result replays without executing the body.

This does not turn a non-idempotent external API into an exactly-once system. Step authors still
provide idempotency, reconciliation, or compensation.

## Proof behind the claim

The `DN` label rests on executable evidence ([Phase 4 evidence](../PHASE-4-EVIDENCE)):

- before/after failpoints exist for every durable mutation;
- deterministic failpoint-kill tests cover admission through Settlement, including the full
  recovery-classifier crash matrix;
- stale owners are fenced out of canonical history;
- later FIFO work cannot pass an unsettled head;
- both ledger adapters pass one shared conformance suite.

What `DN` still does not claim: exactly-once model inference or external Tool execution,
automatic replay of unresolved ordinary Tools, or safe recovery through consequential external
mutations — those are the Phase 5 gates above.
