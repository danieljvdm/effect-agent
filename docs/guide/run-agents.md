---
title: Run and stream
description: Interpret one Agent Binding as a result, semantic event stream, or scoped detached Run.
---

# Run and stream

The runtime exposes one semantic loop through three views. Choose based on how the caller needs to
observe work. The choice does not change execution behavior.

All three entry points require a provided `ConversationHistory` service. Choose
`ConversationHistory.layerTransient` for execution without retained history, or provide
`PersistentHistory.layer` with a store as shown in [Conversations](./conversations).
History commits finish before successful results or `RunCompleted` events become visible.
The examples below assume the application provides that Layer and the Agent's other services.

## Await one result

```ts
const result = yield * AgentRuntime.run(agent, input);
```

`run` completes execution and closes run-owned resources before returning the decoded terminal
result. A self-contained Run needs no caller `Effect.scoped`. Requirements contributed by your
instructions, input projection, hooks, or output decoder remain visible, including `Scope` when
those operations require it.

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
callers receive the same value from `DurableAgentRuntime.awaitSettlement` and canonical
`SubmissionSettled` record readers, then decode it with the application Schema. The exported
`AgentResultSchema` enforces the same boundary and rejects a `runDisposition` paired with
`finishReason: "budget-exhausted"`.

Under the default `onExhaustion: "final-answer"` policy, a Run that exhausts its Turn, Tool Call,
or token budget settles with one constrained final-answer Turn and reports it honestly as
`finishReason: "budget-exhausted"` (its `turns` count may exceed `maxTurns` by that one grace
Turn). Duration and cost exhaustion, pending approval, interruption, and failed output decoding
are never successful finish reasons. With `onExhaustion: "fail"`, Turn, Tool Call, and token
exhaustion fail typed before any declared application Handler starts.

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

Local streams use bounded backpressure. Consumption owns the stream's resources; completion,
failure, and interruption close them. Interrupting the sole ephemeral consumer interrupts the Run.

## Start and re-observe locally

```ts
const detached = yield * AgentRuntime.start(agent, input);

const result = yield * detached.await;
const completeTrace = yield * detached.events;
const live = detached.observe;
```

`start` requires a caller Scope that owns ongoing execution and the event source. `observe` is a
live multicast subscription. Each subscription replays the events already emitted, follows the
Run as it progresses, and ends once the Run settles. `events`
is the complete replay, available after settlement. Execution resources close before `await`
returns, while replay remains available within the owner's lifetime. Closing the owner interrupts
ongoing execution and shuts down the event source; waiting observers terminate and new
subscriptions are interrupted. Each observer consumption owns its subscription and any downstream
work after delivery. "Detached" means observers cannot backpressure completion; it does not create
a daemon fiber or survive process loss.

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

`RunOptions` carries per-Run values and advanced capability hooks. For retained history, use
`ConversationHistory` as shown in [Conversations](./conversations). The following interactive
integration uses `ConversationHistory.layerTransient`; `history` supplies an explicit initial
Prompt and `onHistory` receives incremental updates, including from Runs that later fail:

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
`onHistory` runs inline before successful completion and resource cleanup. Its earlier writes are
not rolled back on failure or interruption. A retaining history Layer rejects these competing
history and input-queue hooks before model or Tool execution.

Ephemeral callers can reuse independent host preparation and authorization services through these
hooks. Yielding them keeps both requirements visible in the caller's `R`:

```ts
const run = Effect.gen(function* () {
  const preparation = yield* RunContextPreparation;
  const authorization = yield* RunToolAuthorization;
  return yield* AgentRuntime.run(agent, input, {
    context: preparation.hook,
    toolAuthorization: authorization,
  });
}).pipe(Effect.provide(Layer.mergeAll(preparationLayer, authorizationLayer)));
```

Ephemeral compactor selection still uses `ContextCompactor` directly. Per-run hooks can retain
their own typed errors and requirements. Supplying neither hook preserves the default prompt and
requires no additional Tool authorization. Durable hosts capture the two services at runtime
construction, as described in [Context management](/guide/context-management#composing-preparation-and-tool-authorization).

## Observe recovered Tool failures

A Tool may fail while the model recovers and the Run completes. Install
`toolFailureObserverLayer` from `@effect-agent/engine` to observe those failures locally. There is
no `RunOptions` member. Providing the Layer does not change inferred errors or requirements.

```ts
import { toolFailureObserverLayer } from "@effect-agent/engine";
import { Effect, ErrorReporter } from "effect";

const failureReporting = toolFailureObserverLayer({
  observe: (observation) =>
    observation.cause === undefined ? Effect.void : ErrorReporter.report(observation.cause),
});

yield * AgentRuntime.run(agent, input).pipe(Effect.provide(failureReporting));
```

This example explicitly forwards Cause-bearing observations to the application's configured
`ErrorReporter`. The engine does not do that automatically. Applications choose what to capture
and how to redact it. Capture any reporting dependencies before installing the closed observer.

`ModelToolFailure` describes a direct declared failure. `ProgrammaticToolFailure` includes a raw
parent ID and sequence index for a started Handler. `ProgrammaticPreflightFailure` has no inner
ID or index because no Handler started. Declared failures expose their tag only. Handler errors
retain the live Cause but no message; infrastructure and protocol messages have a 4096-byte UTF-8
bound. Propagating direct errors are left to the ordinary Run failure boundary.

Delivery is inline and at most once per in-memory attempt. Observer/reporter defects cannot
change the Tool result, but a slow observer occupies a Tool permit and external interruption can
stop delivery. Do not call the broker, emit Run events, run another Agent, or self-interrupt from
the observer. These values are never serialized or persisted. Replacement Attempts may repeat
IDs and observations, and replay-injected settled calls are not observed.

Durable hosts pass the same closed value as `NodeDurableRuntimeOptions.toolFailureObserver` or
`CloudflareDurableRuntimeOptions.toolFailureObserver`. The coordinator captures it when its Layer
is built and uses that choice for each Attempt, independently of the worker caller's context.
Omitting the platform option disables observation even if an observer surrounds Layer construction.
Direct errors that propagate to the Run, interruption, waiting, provider-executed results, and
synthetic budget rejections are excluded. Programmatic defects propagate through the outer call;
opening a broker pass without invoking a Tool creates no inner-call observation.

## Interruption is ownership

A Run Scope owns its Model stream, Tool fibers, and resources acquired for that Run, such as
run-local input queues, MCP clients, or sandbox processes. Closing it interrupts children and
runs their finalizers. Shared model, provider, and client services supplied by an enclosing
application Layer remain owned by the application Scope and can serve multiple Runs.

`Effect.provide(AppLive)` owns the supplied Layer's lifetime around the Effect it wraps. Provide
shared services around a program containing several Runs to reuse them. Keep caller scoping for
`start`, explicit resource acquisition, and operations that contribute their own `Scope` requirement.

Successful history retention waits for run-local cleanup, then result validation where applicable,
then history commit, before publishing `RunCompleted`. It does not close application-owned
services at each commit.

This is distinct from durable abort. Interrupting a local waiter for accepted durable work
detaches that waiter; aborting the Submission requires an explicit persisted command
([Persistence & durability](../concepts/durability)).
