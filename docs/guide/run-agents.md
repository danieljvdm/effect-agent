---
title: Run and stream
description: Interpret an Agent Definition as a result, semantic event stream, or scoped detached Run.
---

# Run and stream

The runtime exposes one semantic loop through three views. Choose based on how the caller needs to
observe work. The choice does not change execution behavior.

Each view accepts a Definition and requires native model services supplied with `Effect.provide`
or `Stream.provide`. Explicit Bindings remain supported for durable and Subagent composition.
Inputs use the Schema's encoded representation. External data typed as `unknown` enters through
`runUnknown`, `streamUnknown`, or `startUnknown`; all views decode before instructions execute.
See [Agent definitions](./agents#typed-and-external-inputs) for transforming Schemas.

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

## Run durably on Cloudflare

`ConversationObject.make` uses `effect-cf`'s `DurableObject.make`. Export the returned class
from your Worker and supply an Effect that constructs the Agents it can execute. This example
assumes your generated `Cloudflare.Env` includes the `OPENAI_API_KEY` secret and the
`CONVERSATIONS` Durable Object namespace:

```ts
import { Agent, AgentPolicy } from "@effect-agent/core";
import { ConversationObject } from "@effect-agent/platform-cloudflare";
import {
  DefinitionDigestInput,
  DurableWorkerBinding,
  digestDefinitions,
} from "@effect-agent/session";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Effect, Layer, Redacted, Schema } from "effect";
import { Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import { WorkerEnvironment } from "effect-cf";

const TravelPlanner = Agent.make("travel-planner", {
  input: Schema.Struct({ destination: Schema.String, days: Schema.Number }),
  output: Schema.Struct({ itinerary: Schema.Array(Schema.String) }),
  instructions: "Create a practical travel itinerary.",
  toolkit: Toolkit.make(),
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const modelName = "gpt-4.1-mini";

// Share these version declarations with the submitter.
export const travelDefinitions = DefinitionDigestInput.make({
  agent: { id: TravelPlanner.id, revision: 1 },
  model: { provider: "openai", name: modelName },
  tools: [],
});

const travelAgents = Effect.gen(function* () {
  const env = yield* WorkerEnvironment;
  const model = OpenAiLanguageModel.model(modelName).pipe(
    Layer.provide(
      OpenAiClient.layer({
        apiKey: Redacted.make(env.OPENAI_API_KEY),
      }).pipe(Layer.provide(FetchHttpClient.layer)),
    ),
  );
  const digests = yield* digestDefinitions(travelDefinitions);
  const registered = yield* DurableWorkerBinding.make(
    Agent.withModel(TravelPlanner, model),
    digests,
  );
  return [registered];
});

const options = {
  namespaceBinding: "CONVERSATIONS",
  deploymentId: "travel-planner",
  producerPrefix: "travel-worker",
} satisfies ConversationObject.Options;

export class TravelConversation extends ConversationObject.make(travelAgents, options) {}
```

`travelAgents` returns executable registrations without running the Agents. The platform builds
the resolver internally and matches queued Submissions against their Agent ID and exact definition
digests. These digests hash the explicit version declarations, not JavaScript implementation code.
Bump the Agent revision when its instructions, input/output Schemas, or policy change; version
Tool implementations and model configuration when those change too. Submit the same result of
`digestDefinitions(travelDefinitions)` through `DurableSubmitOptions.definitions`.

`namespaceBinding` names the Wrangler binding, `deploymentId` identifies the deployment in durable
records, and `producerPrefix` prefixes each Object's derived writer identity. The registration
Effect can yield `effect-cf`'s `WorkerEnvironment` for typed Worker bindings,
`DurableObjectState.DurableObjectState` for Object state, and `ConversationObjectIdentity` for the
derived Conversation and producer IDs. The platform also provides Crypto and the Layer's Scope.
Supply other application services with `Effect.provide`; missing dependencies are type errors at
the class factory. Wrap already constructed registrations in `Effect.succeed(registrations)`.

When a registration captures services from a resource-owning application Layer, build it in the
supplied Scope and provide the resulting Context:

```ts
const registrations = Effect.gen(function* () {
  const services = yield* Layer.build(AppLive);
  return yield* travelAgents.pipe(Effect.provide(services));
});
```

Here `AppLive` is the application's service Layer, such as Tool handlers backed by a database.
`Effect.provide(AppLive)` directly around registration would close that Layer as soon as
registration returns, before the captured services execute a Run. `Layer.build` keeps those
resources in the enclosing runtime Scope. With `ConversationObject.layer`, ordinary
`Layer.provide(AppLive)` around the returned runtime Layer also preserves that lifetime.

The registration Effect is acquired once per Object incarnation and rebuilt after eviction.
Construction errors fail initialization; they never become an empty registration. The platform
owns SQLite, the constructor gate, alarms, and recovery. Keep acquisition local and bounded.
Resources acquired directly into the supplied Scope live with the runtime, but eviction does not
guarantee finalizers; acquire resources needing cleanup within scoped operations.
Register `TravelConversation` as a SQLite Durable Object under `CONVERSATIONS` in your Worker
configuration. `ConversationObject.Options` names the factory's configuration type;
`ConversationObject.Class` and `ConversationObject.Instance` describe the returned constructor
and its instances.

For a custom Effect runtime, use `ConversationObject.layer(travelAgents, options)` with
`ConversationObject.RuntimeOptions`. It preserves application dependencies and initialization
errors in the returned Layer; the native class boundary is supplied by `ConversationObject.make`.

BEHAVIOR CHANGE: replace `makeConversationObjectClass` with `ConversationObject.make` and
`CloudflareDurableRuntime.layer` with `ConversationObject.layer`. Pass the registration Effect
as the first argument instead of the `bindings` callback option. Construction types and
administrative schemas/decoders now live under `ConversationObject` too. The former
`CloudflareBindingSource` and `CloudflareBindingSourceContext` types have been removed. Use
`Effect.succeed([])` when intentionally registering no Agents.

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
`ConversationObject.Options.toolFailureObserver`. The coordinator captures it when its Layer
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
