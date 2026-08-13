---
title: Durable execution
description: The planned Receipt, Attempt, recovery, Settlement, and durable Tool contracts.
---

# Durable execution

<StatusCallout status="next" phase="P4–P5" title="Target interface—specified, not implemented.">

This page is intentionally written as future user documentation so the architecture can be judged
from its external contract. There is no current `submit`, Receipt, Attempt coordinator, Settlement,
or durable Step implementation.

</StatusCallout>

The durable runtime will make one bounded promise:

> After a Submission is durably accepted, the runtime owes exactly one durable terminal Settlement:
> `completed`, `failed`, or `aborted`.

That is exactly-once **recording** over at-least-once Attempt execution.

## Submit accepted work

The intended boundary is an Effect, not a background Promise:

```ts
const receipt =
  yield *
  DurableAgentRuntime.submit(agent, {
    conversationId,
    input,
    idempotencyKey: "support-ticket:8472:v3",
  });
```

`submit` may return only after all of these are durable:

1. the Submission Ledger obligation;
2. Conversation materialization;
3. required durable attachments;
4. the readiness marker.

The returned Receipt is stable identity for observation and reattachment. It is not an
authorization capability.

## Await or observe without owning liveness

```ts
const settlement = yield * DurableAgentRuntime.await(receipt);
const records = DurableAgentRuntime.observe(receipt, { after: offset });
```

Interrupting a local await or observation fiber detaches that caller. It does not cancel accepted
work. Durable abort is a separate authorized command.

## One Conversation lane

The default scheduler allows one active head per Conversation. FIFO order comes from the sequence
allocated at admission, not wall-clock arrival. Different Conversations may execute concurrently.

When claimed, the Submission's exact input is appended canonically before the Model can consume
it. Later input may join an active Run only at the same safe Turn seams used by ephemeral steering.

## Attempts and fencing

A durable Run may span several process Attempts. Each claim returns an Attempt ID, ownership token,
and producer epoch. Every append and state transition carries that fencing evidence.

A replaced worker may resume in memory, but its stale epoch cannot append a late model token, Tool
result, or Settlement.

## Terminalization

Settlement crosses the operational ledger and canonical Conversation Log through a recoverable
sequence:

```text
reserve one exact Settlement in the ledger
  → append that exact record to canonical history
  → finalize the ledger and release the lane
```

Every step is idempotent. If a worker dies after canonical append, recovery repairs the ledger from
history. Canonical history never gets rewritten from a stale cached status.

## Ordinary Tool uncertainty

Before an ordinary external Tool runs, the future engine records `ToolCallPrepared`. After validated
completion, it records `ToolCallSettled`.

If recovery finds only the prepared record, it may proceed only when policy can prove that:

- invocation never started;
- the external result can be reconciled; or
- retry is safe under a stable external idempotency contract.

Otherwise the Submission enters an operational `Unknown` state and automatic continuation stops.
The framework will not lie to the Model about whether the effect happened.

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

## Required proof before the claim

The runtime cannot call itself `DN` until:

- every durable mutation has before/after failpoints;
- real process-kill tests cover admission through Settlement;
- stale owners are rejected;
- later FIFO work cannot pass an unsettled head;
- uncertain ordinary Tools do not replay automatically;
- adapter conformance and the executable state model agree.
