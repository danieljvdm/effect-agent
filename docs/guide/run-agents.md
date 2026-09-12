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

A valid no-tool answer needs one model call. A designated completion Tool can complete without a
follow-up model call. Independent application Tools default to four concurrent handlers, behind
the batch's authorization and approval barrier. Use one when execution must be serial or a fixture
deliberately measures a sequential workflow. Continuity fixtures serialize changes to shared notes;
Code Mode examples bound generated programs separately. Node host worker concurrency is a separate
setting.

The immutable output contract retains its message identity across turns, allowing opt-in native
`ResponseIdTracker` reuse when the rest of the prompt prefix is unchanged. Prepared context,
compaction, and a resumed Run can still invalidate that prefix. Provider caching and billing depend
on the selected provider and configuration.

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

For structured output, treat text deltas as provisional wire data. Show activity or received-character
progress until the terminal output passes its Schema; do not display partial JSON as an answer.
The demo follows this pattern. Plain-text output can render provisional text directly.
Primitive text and reasoning deltas are copied into owned, Schema-validated values; their transport
fragmentation does not create one ownership tracing span per delta. Complex metadata retains the
general bounded ownership path.

The stream uses bounded backpressure. Completion, failure, and interruption close its resources.
Interrupting the only ephemeral consumer interrupts the run.

Published `ToolProgress` results are owned JSON snapshots. Their cumulative UTF-8 JSON size is
limited to 8 MiB per run, shared by application and provider progress. This also bounds progress
payloads retained for detached replay. Oversized progress fails with `ModelProtocolError` without
truncation. Application progress must contain plain JSON data; accessors, custom serialization,
and non-finite numbers fail with the same error. Terminal tool results use `toolResultBounds`
separately.

Lower the progress allowance with `bufferLimits` on `run`, `stream`, or `start`. Larger values
cannot raise the engine's ceiling:

```ts twoslash
import { type RunBufferLimits } from "@effect-agent/engine/RunOptions";

export const progressBufferLimits: RunBufferLimits = {
  maxToolProgressBytes: 1024 * 1024,
};
```

### Connect a voice conversation

A voice adapter can delegate to the same agent and Thread as a text interface. Keep its media
Scope separate from accepted durable work: closing a call or stopping playback closes media and
observation, while the durable runtime retains its accepted-work obligation. Use the original
idempotency key and frozen input to reconcile uncertain admission. A transcript delta is context,
not an instruction to admit another Run. Corrections use ordinary queued input and steering.

The [travel planner](https://github.com/danieljvdm/effect-agent/tree/main/examples/travel-planner) demonstrates GPT-Live client
delegation. Its adapter constructs schema-validated planner requests from attributed transcripts,
uses the existing planner admission path, and reconciles receipts against canonical settlement.
It tracks corrections by work identity rather than treating every caption as a new task. Spoken
and typed input share the demo conversation; attributed speech context is separate from the
user's work request and visible message. Typed results and later research answers return to the
active voice exchange. Reconnect restores relevant conversation history without restarting
accepted work. The demo retains undelegated speech in the current tab; it does not add a durable
partial-transcript journal.

For application-selected previews, decorate the native Effect AI `LanguageModel` service in the
model Layer. Its `streamText` exposes ordered `text-delta`, `tool-params-start`, and
`tool-params-delta` parts. The demo selects native text and only the designated
`deliver_response.message` field; it retains a bounded provisional preview and fences writes by
Submission and Attempt. Parsing and presentation belong to the adapter. Never forward reasoning,
arbitrary tool arguments, or diagnostics to a voice provider. Provider protocol interception is
unnecessary for this public-output path, and no additional SDK output hook is required.

Keep generation, schema validation, durable settlement, provider acknowledgment, and actual audio
playback distinct. A partial completion-tool argument is provisional even when it resembles a
complete sentence. A provider acknowledgment does not prove that the user heard the result.
Use canonical, schema-decoded output for final answers, including after reconnect. The
[official Live delegation guide](https://developers.openai.com/api/docs/guides/live-delegation)
describes the provider-specific half of this integration.

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
import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
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

This observer covers failures contained as results, including programmatic broker outcomes. It does
not duplicate model-declared failures that propagate through the run's Effect error channel, or
defects and interruptions. Use [`ToolCallFailed.failureHandling` and tool telemetry](./tools#failure-remains-failure)
to distinguish returned failures from propagated ones, and handle the run's Effect exit separately.

```ts
import { toolFailureObserverLayer } from "@effect-agent/engine/RunOptions";
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

## Provider usage and cost evidence

`AgentRuntime` results and `RunCompleted` expose optional `usage` (own calls, including compaction) and
`delegatedUsage` (all attached descendants). Combine them once with `Usage.sumRunTotals`;
background workers are excluded. Child events repeat cumulative totals, so deduplicate by Run ID.

For failed or interrupted Runs, read `handle.usageReport` after `handle.await` settles or its
owning Scope closes. Earlier reads are live snapshots and may omit in-flight usage. Durable
settlements retain their richer per-model `usageSummary` for the Run's own calls.

A cost estimator receives the configured binding name in `request.model` and the actual
provider-reported identity in `request.response`. Use the latter for response-sensitive pricing.
`request.finishMetadata` carries native Effect AI finish metadata only during estimation; the
engine never persists provider HTTP details or raw metadata in accounting records. A summarizer
uses the same estimator with `purpose: "summary"`.

Calls and Run totals retain `usageStatus` and `pricingStatus`. Missing legacy status is unknown, and a numeric
zero without an estimate is not evidence of free execution. Run summaries distinguish complete,
partial, and unknown coverage; `unobservedModelCalls` counts observed calls without retained accounting,
which are excluded from numeric token and call totals. A canonical `ModelResponseInterrupted`
can indicate additional unquantified provider work beyond this count.

An explicitly configured `costBudgetMicrousd` fails with a typed cost-policy error when the
estimator reports unknown pricing, after retaining the call's usage. Uncapped Runs may continue;
legacy numeric estimates and estimates without a status remain trusted host estimates.

Response records also retain each Turn's missing-call count, so approval and child suspension
preserve incomplete accounting when a fresh runtime resumes the Run.

Runtime hosts use `AgentRuntime.streamWithUsageAccountingUnknown` with the inward
`ModelUsageAccounting` and `AgentUpdateAcceptance` services from `RunOptions`. These dependencies
remain visible in `R`. The native durable runtime supplies its canonical Turn accumulator and
Attempt-bound update acceptance at composition, retaining updates before acknowledging them.
Ordinary `stream`, `run`, and `start` calls provide ephemeral accounting and update acceptance.

Canonical response records own committed per-call usage. Terminal settlement
`uncommittedModelUsage` retains only staged calls not already present in a response record;
its charges are already included in `usageSummary`, so do not add them a second time. An isolate
loss before either response or settlement commit cannot prove the lost call's usage or cost.
