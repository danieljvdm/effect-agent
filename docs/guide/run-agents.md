---
title: Run and stream
description: Interpret one Agent Binding as a result, semantic event stream, or scoped detached Run.
---

# Run and stream

The runtime exposes one semantic loop through three views. Choose based on how the caller needs to
observe work—not to change execution behavior.

## Await one result

```ts
const result = yield * AgentRuntime.run(agent, input);
```

`run` reduces the semantic event stream and decodes the terminal output Schema.

```ts
interface AgentResult<Output> {
  readonly output: Output;
  readonly runDisposition?: Json;
  readonly conversationId: ConversationId;
  readonly runId: RunId;
  readonly turns: number;
  readonly finishReason: "completed" | "model-stop" | "budget-exhausted";
}
```

`runDisposition` is present only when the Definition declares a disposition Schema, its selector
returns a value, and the Run completes ordinarily. It is the Schema-encoded JSON value; durable
callers read the same value from the canonical `SubmissionSettled` record and decode it with the
application Schema.

Under the default `onExhaustion: "final-answer"` policy, a Run that exhausts its Turn or Tool
Call budget settles with one constrained final-answer Turn and reports it honestly as
`finishReason: "budget-exhausted"` (its `turns` count may exceed `maxTurns` by that one grace
Turn). Duration, token, and cost exhaustion, pending approval, interruption, and failed output
decoding are never successful finish reasons; with `onExhaustion: "fail"`, Turn and Tool Call
exhaustion fail typed as well.

## Observe semantic events

```ts
const events = AgentRuntime.stream(agent, input);

yield *
  events.pipe(
    Stream.tap((event) => Effect.log(event._tag)),
    Stream.runDrain,
  );
```

Events include Run and Turn lifecycle, text and reasoning deltas, Tool declaration/progress/result,
approval requests, and exactly one complete terminal classification. Provider SDK chunks do not
enter the stable event union.

Local streams use bounded backpressure. Interrupting the sole ephemeral consumer interrupts the
Run and closes its Scope.

## Start and re-observe locally

```ts
const detached = yield * AgentRuntime.start(agent, input);

const result = yield * detached.await;
const completeTrace = yield * detached.events;
const live = detached.observe;
```

`start` is still scoped. `observe` is a live multicast subscription: each subscription replays the
events already emitted, follows the Run as it progresses, and ends once the Run settles. `events`
is the complete replay, available after settlement. “Detached” means observers cannot backpressure
completion; it does not create a daemon fiber or survive process loss.

## Turn boundaries

Each Turn follows one visible sequence:

```text
prepare context
  → stream and reduce one model response
  → decode the complete Tool batch
  → execute bounded Tool handlers
  → commit results in declaration order
  → drain steering
  → evaluate stop policy
  → drain follow-up only if otherwise complete
```

`run` and `stream` share this implementation. Golden tests compare the materialized result with a
reduction of the Stream trace.

## Operational hooks

`RunOptions` is a dependency-neutral seam for the capability adapters:

```ts
const options: RunOptions<AppError, AppRequirements> = {
  conversationId,
  history,
  input: toRunInputHook(commands),
  approval: toRunApprovalHook(approvalPolicy),
  budget: toRunBudgetHook(budget),
  context: toRunContextHook(contextTransform),
  scheduling: toRunSchedulingHook({ mode: "bounded", concurrency: 2 }),
  onHistory,
};
```

Hook failures join the Run's error channel. Hook requirements join `R`. Capability packages adapt
richer domain contracts to this narrow engine boundary rather than creating a second runtime.

## Interruption is ownership

A Run Scope owns its Model stream, Tool fibers, input queues, MCP clients, sandbox processes, and
other acquired resources. Closing it interrupts children and runs finalizers.

This is distinct from durable abort. Interrupting a local waiter for accepted durable work
detaches that waiter; aborting the Submission requires an explicit persisted command
([Persistence & durability](../concepts/durability)).
