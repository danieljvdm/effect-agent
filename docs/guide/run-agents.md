---
title: Run & stream
description: Run an agent, stream its events, or observe it through a scoped handle.
---

# Run & stream

The runtime exposes one agent loop through `run`, `stream`, and `start`. All three decode input
before instructions execute and require native model services. Use `runUnknown`, `streamUnknown`,
or `startUnknown` for external values typed as `unknown`. See
[Agent definitions](./agents#typed-and-external-inputs).

Every entry point also requires `ThreadHistory`. Use
`ThreadHistory.layerTransient` when you do not need retained history. Use
`PersistentHistory.layer` with a store to [retain completed runs](./threads#retain-completed-runs).
History commits before a successful result or `RunCompleted` event becomes visible.

Context preparation is optional. Provide `RunContextPreparation` to load extra context;
without it, Runs use their normal prompt and compaction behavior. See
[context management](./context-management#recall-memory) for service-based recall and tagged errors.

## Await one result

```ts
const program = Effect.gen(function* () {
  const result = yield* AgentRuntime.run(agent, input);
  return result;
});
```

`run` closes run-owned resources before returning decoded output. A self-contained run needs no
caller `Effect.scoped`.

The result contains `output`, `threadId`, `runId`, `turns`, and `finishReason`.
Budget-limited results also include `exhausted`, naming `"turns"`, `"tool-calls"`, or `"tokens"`.

`runDisposition` appears only after ordinary completion when the definition declares one and its
selector returns a value. It contains schema-encoded JSON. Decode durable settlement values with
the same application schema.

With the default `onExhaustion: "final-answer"`, turn, tool call, or token exhaustion allows one
constrained final turn. The result reports `finishReason: "budget-exhausted"`, and `turns` may
exceed `maxTurns` by one. Duration or cost exhaustion, pending approval, interruption, and output
decoding failure remain failures. Set `onExhaustion: "fail"` to fail before the final turn.

## Observe semantic events

```ts
const events = AgentRuntime.stream(agent, input);

const program = events.pipe(
  Stream.tap((event) => Effect.log(event._tag)),
  Stream.runDrain,
);
```

Events cover run and turn lifecycle, text and reasoning deltas, tool activity, approval requests,
and one terminal classification. Provider SDK chunks do not enter this stable union.

The stream uses bounded backpressure. Completion, failure, and interruption close its resources.
Interrupting the only ephemeral consumer interrupts the run.

## Start and re-observe locally

```ts
const program = Effect.gen(function* () {
  const detached = yield* AgentRuntime.start(agent, input);
  const result = yield* detached.await;
  const completeTrace = yield* detached.events;
  return { result, completeTrace };
}).pipe(Effect.scoped);
```

`start` requires a caller Scope. `observe` replays prior events, follows new events, and ends when
the run settles. `events` returns the complete replay after settlement. Execution resources close
before `await` returns, while replay remains available until the owner closes.

Observers cannot backpressure execution. Closing the owner interrupts active work and observers.
The handle remains process-local and never creates a daemon fiber.

## Run durably on Cloudflare

Use a [Cloudflare thread object](../platforms/cloudflare#create-the-thread-object)
to accept work that survives eviction. For a process with SQLite, use the
[Node host](../platforms/node).

## Assemble a custom durable runtime

Platform hosts assemble storage and runtime services for you. When building your own host,
`DurableAgentRuntime.layer` supplies default prompt preparation and tool authorization.
Use `layerWithServices` to supply your own service layers. It requires
`RunToolAuthorization` and captures `RunContextPreparation` when provided.

Here is the default authorization policy; replace it with your application's implementation:

```ts twoslash
import { RunToolAuthorization } from "@effect-agent/engine";
import { DurableAgentRuntime } from "@effect-agent/thread";
import { Layer } from "effect";

export const RuntimeLive = DurableAgentRuntime.layerWithServices.pipe(
  Layer.provide(RunToolAuthorization.allowAll),
);
```

This layer still requires `SubmissionLedger`, `ThreadStore`, `WakeScheduler`,
`DurableRuntimeFailpoint`, `DurableRuntimeConfig`, `ToolReconciler`, and `Crypto.Crypto`.
Provide those before acquiring the runtime.

The runtime captures its services at acquisition. Supplying a different layer around a later
worker call does not replace them. Acquire service dependencies in their layers and keep them
alive for the runtime's Scope. Durable service hooks must have no unresolved dependencies.
Preparation failures retain their `AgentInputError`, `MemoryRecallError`, or `CompactionError`
tags; `RunContextPreparationError` is their type union, not a wrapper. Durable execution records
failed Runs in Settlements with bounded diagnostics; it does not reconstitute the original error
object from storage. Authorization returns an allowed or denied
decision. Configure [prompt preparation](./context-management#prompt-preparation-order)
and [tool authorization](./tools#authorize-tool-calls) in their respective services.

## Understand turn boundaries {#turn-boundaries}

Each turn follows this sequence:

```text
prepare context
  → stream and reduce one model response
  → decode the complete tool batch
  → execute bounded tool handlers
  → commit results in declaration order
  → drain steering
  → evaluate stop policy
  → drain follow-up only if otherwise complete
```

`run` and `stream` use the same loop.

## Add per-run hooks {#operational-hooks}

`RunOptions` accepts per-run capability hooks. This process-local example uses transient history.
`history` provides an initial Prompt, and `onHistory` receives incremental updates.

```ts
const options: RunOptions<AppError, AppRequirements> = {
  threadId,
  history,
  input: toRunInputHook(commands),
  approval: toRunApprovalHook(approvalPolicy),
  budget: toRunBudgetHook(budget),
  context: toRunContextHook(contextTransform),
  scheduling: toRunSchedulingHook({ mode: "bounded", concurrency: 2 }),
  onHistory,
};
```

Hook errors join the run error channel, and their services join `R`. `onHistory` runs inline.
Writes completed before a later failure or interruption remain caller-owned. Persistent history
rejects competing history and input queue hooks before model or tool execution.

Pass [prompt preparation](./context-management#prompt-preparation-order) as `context` and
[tool authorization](./tools#authorize-tool-calls) as `toolAuthorization` when needed.
Ephemeral runs read these options; providing the durable service layers alone does not install
per-run hooks.

## Observe recovered tool failures

A tool may fail and the model may still complete the run. Install `toolFailureObserverLayer` from
`@effect-agent/engine` to report such failures.

```ts
import { toolFailureObserverLayer } from "@effect-agent/engine";
import { Effect, ErrorReporter } from "effect";

const failureReporting = toolFailureObserverLayer({
  observe: (observation) =>
    observation.cause === undefined ? Effect.void : ErrorReporter.report(observation.cause),
});

const program = AgentRuntime.run(agent, input).pipe(Effect.provide(failureReporting));
```

The engine does not forward observations to `ErrorReporter` by itself. Choose what to record and
redact. The observer runs inline at most once per in-memory Attempt. Replacement Attempts may
repeat an observation. Nothing here is serialized into thread history.

Observer defects cannot change the tool result, though a slow observer holds a tool permit. Avoid
calling the broker, running another agent, or interrupting the observer itself. Durable hosts
accept the same observer through their platform options.

## Scope run resources {#interruption-is-ownership}

A run Scope owns its model stream, tool fibers, and run-local resources. Closing it interrupts
children and runs finalizers. Services from an enclosing application layer remain available to
other runs until the application Scope closes.

Wrap several runs with one `Effect.provide(AppLive)` to reuse shared services. Keep caller scoping
for `start`, explicit resource acquisition, and any operation that requires `Scope`.

History retention waits for run-local cleanup, result validation, and commit before publishing
`RunCompleted`. Interrupting a waiter for durable accepted work only detaches that waiter. Abort a
durable Submission with an explicit persisted command. See
[Persistence & durability](../concepts/durability).
