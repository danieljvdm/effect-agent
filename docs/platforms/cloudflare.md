---
title: Cloudflare
description: Run durable agents on Cloudflare Workers and Durable Objects.
---

# Cloudflare

`@effect-agent/platform-cloudflare` stores each thread and its pending work in a
SQLite-backed Durable Object. RPC calls and alarms drive execution and recovery.
See [Cloudflare storage](../storage/cloudflare) for database ownership and adapter composition.

## Install

```sh
bun add @effect-agent/platform-cloudflare@beta
```

Keep framework packages at one release and add your [model provider](../guide/getting-started#installation-and-compatibility).

## AI Gateway {#ai-gateway}

The Node-safe `@effect-agent/platform-cloudflare/cloudflare-ai-gateway` subpath configures
upstream Effect clients in Workers, Durable Objects, Node, or Bun. `Gateway.provide` supplies
the client directly in a Layer pipeline; model selection, tools, response decoding, streaming,
and typed provider errors stay with upstream Effect AI. Use the configured client for primary agents, subagents,
compaction models, [WebSearch](../guide/tools#web-search), or embeddings supported by its provider.

Two endpoint families have different credentials and model names:

| Helper                                                           | Authentication                                             | Model names                                     |
| ---------------------------------------------------------------- | ---------------------------------------------------------- | ----------------------------------------------- |
| `Gateway.rest({ accountId, gatewayId, apiToken, protocol })`     | Cloudflare API token with Workers AI Read permission       | Provider-qualified, such as `openai/gpt-6-luna` |
| `Gateway.provider({ accountId, gatewayId, provider, apiToken })` | `cf-aig-authorization`; optionally a separate provider key | Native provider name, such as `gpt-6-luna`      |

For provider-native routing with stored keys or Unified Billing, pass the upstream client's
`layer` factory and your resolved gateway configuration:

```ts twoslash
import * as Gateway from "@effect-agent/platform-cloudflare/cloudflare-ai-gateway";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Layer, Redacted } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

const gateway = {
  accountId: "your-account",
  gatewayId: "your-gateway",
  apiToken: Redacted.make("your-cloudflare-token"),
};

const ModelLive = OpenAiLanguageModel.model("gpt-6-luna").pipe(
  Gateway.provide(OpenAiClient.layer, {
    ...gateway,
    provider: "openai",
  }),
  Layer.provide(FetchHttpClient.layer),
);
```

Supply real credentials from your host configuration or secret store. For account REST routing,
replace `provider` with `protocol: "responses"` and use a provider-qualified model name.
`Gateway.provide` preserves client initialization errors and remaining dependencies, including
`HttpClient`. The upstream Layers retain their normal resource lifetimes.

For custom client options, pass a factory such as
`Gateway.provide((options) => OpenAiClient.layer({ ...options, apiKey }), route)`.
The lower-level `Gateway.provider` and `Gateway.rest` helpers return `apiUrl` and
`transformClient` for direct client construction or raw HTTP requests.

Omit the provider `apiKey` when the gateway supplies a stored key. Use `layer` here: provider `layerConfig`
can load a provider API key from the environment when its `apiKey` option is omitted.
An unauthenticated provider gateway can omit `apiToken` when sending its own provider key.

`rest` selects `protocol: "responses"` for `OpenAiClient`, `"messages"` for `AnthropicClient`,
or `"chat-completions"` for a compatible client. It sends `cf-aig-gateway-id` and sets the
correct base path, including Anthropic's separately appended `/v1`. This uses Cloudflare's
[account REST API](https://developers.cloudflare.com/ai-gateway/usage/rest-api/).
The native `provider` helper also accepts other provider path names, including `google-ai-studio`,
`google-vertex-ai`, `perplexity-ai`, and `parallel`. Supply the provider's matching upstream
client or Effect HttpClient request format; additional provider path components belong after
`apiUrl`. Routing does not translate request bodies or make unsupported models compatible.

Cloudflare's [web search support](https://developers.cloudflare.com/ai-gateway/usage/web-search/)
varies by provider. This repository exercises OpenAI and Anthropic hosted search through their
pinned Effect clients. xAI uses Responses search; Alibaba requires its own chat request flag;
Gemini requires native grounding; Perplexity and Parallel use provider-native APIs. Those can
use the same Gateway transport but are not interchangeable native WebSearch backends here.

Client configuration validates account, gateway, and provider path segments. Requests must stay
inside that endpoint; Fetch redirects are disabled to prevent credential forwarding. Custom
HTTP transports must also avoid following redirects internally. Gateway authorization is
redacted in HTTP telemetry and returned request/error headers, and provider authentication is
preserved. Gateway logging and caching follow gateway settings or headers supplied by the host;
no automatic retries, fallback models, or cache overrides are added.

## Create the thread object

Compose agent registrations and application services as a layer, then pass it to
`ThreadObject.make`. This example expects `OPENAI_API_KEY` and a `THREADS` Durable
Object namespace in the generated `Cloudflare.Env`.

```ts twoslash
// @types: @cloudflare/workers-types
import { Agent } from "effect-agent";
import { ThreadObject } from "@effect-agent/platform-cloudflare";
import { DefinitionDigestInput } from "effect-agent/records";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Layer, Schema } from "effect";
import { Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

const TravelPlanner = Agent.make("travel-planner", {
  input: Schema.Struct({ destination: Schema.String, days: Schema.Number }),
  output: Schema.Struct({ itinerary: Schema.Array(Schema.String) }),
  instructions: "Create a practical travel itinerary.",
  toolkit: Toolkit.make(),
  policy: {
    maxTurns: 3,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
  },
});

const modelName = "gpt-6-luna";

export const travelDefinitions = DefinitionDigestInput.make({
  agent: { id: TravelPlanner.id, revision: 1 },
  model: { provider: "openai", name: modelName },
  tools: [],
});

const OpenAiLive = OpenAiClient.layerConfig({
  apiKey: Config.Redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

const RuntimeLive = ThreadObject.layer([
  {
    agent: TravelPlanner,
    model: OpenAiLanguageModel.model(modelName),
    definitions: travelDefinitions,
  },
]).pipe(Layer.provide(OpenAiLive));

export class TravelThread extends ThreadObject.make(RuntimeLive, {
  namespaceBinding: "THREADS",
  deploymentId: "travel-planner",
  producerPrefix: "travel-worker",
}) {}
```

Each registration supplies an agent definition, its model Layer, and explicit agent, model, and
tool versions. The submitter passes `digestDefinitions(travelDefinitions)` through
`DurableSubmitOptions.definitions`. Bump the agent revision when instructions, schemas, or policy
change. Version tool implementations and model configuration when they change. Register one
current binding per stable `agentId` by default. Hosts with intentionally shared identities can
provide `CurrentBindingSelection` from `effect-agent/agent-registration` when constructing the
runtime. Its `select(submission)` returns an exact registered Definition using canonical input
and authoritative host state; `undefined` retains unique-identity resolution. Both execution
and recovery use this selection. Set a stable `key` and change it when routing changes. Selection
does not bypass input decoding, operation replay contracts, or authorization. Queued work keeps
its original identity, digests and payload without requiring historical executable versions.
Worker declarations and peer-messaging endpoints still require unique Agent identities.

Application layers can use `WorkerEnvironment`, `DurableObjectState`,
`ThreadObjectIdentity`, and Crypto. Scalar Worker vars and secrets are available through Effect
`Config`: `ThreadObject.make` installs `effect-cf`'s environment config provider. Read secrets
with `Config.Redacted`, and use `WorkerEnvironment` for resource bindings such as R2 or Durable
Object namespaces. Use `Layer.unwrap` when configuration selects registrations or services.
The application is acquired once per Object instance and rebuilt after eviction. Keep initialization
local and bounded. Eviction does not guarantee finalizers; acquire resources needing timely cleanup
inside scoped operations or `options.eventLayer`. Each event runs a bounded recovery pass;
no worker loop is needed.

Register the exported class as a SQLite Durable Object under `THREADS`.
`ThreadObject.layer([])` registers no agents and refuses every agent identity.

## Configure the binding

```jsonc
{
  "name": "travel-planner",
  "main": "src/worker.ts",
  "compatibility_date": "2026-08-31",
  "compatibility_flags": ["nodejs_compat"],
  "durable_objects": {
    "bindings": [{ "name": "THREADS", "class_name": "TravelThread" }],
  },
  "exports": {
    "TravelThread": { "type": "durable-object", "storage": "sqlite" },
  },
}
```

Match `THREADS` to `namespaceBinding` and `TravelThread` to the exported class.
Enable `nodejs_compat` for the async context support used by `effect-cf`.
See Cloudflare's [class configuration guide](https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/)
for Workers using the older `migrations` array.

## Connect from your Worker

```ts twoslash
// @types: @cloudflare/workers-types
import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/cloudflare-thread-client";
import { type ThreadObjectRpc } from "@effect-agent/platform-cloudflare/cloudflare-bindings";

export const threadClientLayer = (env: { THREADS: DurableObjectNamespace<ThreadObjectRpc> }) =>
  CloudflareThreadClient.layerFromBinding({ namespace: env.THREADS });
```

This constructor supplies the namespace and platform Crypto. Pass `rpcTracing: "THREADS"` only
when the receiver also enables native RPC tracing. Keep `CloudflareThreadClient.layer` for
custom Crypto or namespace composition, and `threadNamespaceLayer` for untyped environment lookup.

In an authenticated handler, call `client.submit(agent, input, options)` with the thread ID,
principal, idempotency key, and definition digests. Return its receipt after admission.

Use `client.awaitSettlement(receipt)` for completion.
For updates, call `readPage`, then `awaitProgress`, then read after the last received sequence.
Scope progress waits so interruption cancels them remotely.
Cancellation is best effort and waits at most one second for the remote reply, so a lost reply
does not prevent local shutdown. The Object retains bounded cancellation hints for late retries.
Expose these Effects through your application's HTTP or RPC API.

## Configure runtime services

Provide custom services to `ThreadObject.layer(registrations)` before passing the resulting
layer to `ThreadObject.make`. For example, add `Layer.provide(RunContextLive)` to
`RuntimeLive` above to install [prompt preparation or compaction](../guide/context-management).
Provide a [tool authorization layer](../guide/tools#authorize-tool-calls) in the same place when needed.

The host supplies passthrough preparation and `RunToolAuthorization.allowAll` by default.
Your application layers override those defaults. Preparation can supply a prompt `hook`, a
`compactor`, or both; otherwise the runtime uses an available `ContextCompactor` or its default.
Close custom layers' dependencies with application services or the host services listed above.
They are captured when the Object acquires the runtime, not on each worker call.

Use `options.eventLayer` for per-event observability and resources. Use
`options.toolFailureObserver` for [recovered tool failures](../guide/run-agents#observe-recovered-tool-failures).

### Agent tracing

Provide `CloudflareTracer.layer` from `effect-cf` as `ThreadObject.make`'s
`eventLayer`, and enable `observability.traces.enabled` in the deployed Worker settings.
The tracer must be acquired per invocation so alarms and RPC calls use their own
Cloudflare tracing context.

The [agent and model span attributes](../guide/run-agents#trace-agent-and-model-calls)
follow Cloudflare's [custom harness conventions](https://developers.cloudflare.com/agents/runtime/operations/observability/tracing/).
Use the Cloudflare Agents tab to group activity by agent and conversation, or filter
Workers Observability with `gen_ai.operation.name = "invoke_agent"`.
The outer platform trace can still be named `alarm`: durable execution wakes independently
of the submitting HTTP request. Named agent, model, and tool spans appear inside it.
Separate alarm invocations do not become one trace solely because they share a Run ID.

Storage append, materialization, and ownership-release spans record expected contention or
cleanup refusals in `storage.outcome` and end successfully. Their typed port errors still reach
callers for recovery. Real failures retain `error.type` and, when tagged, `error.cause.type`;
these attributes contain error tags only, without messages or payloads.

### Share an application Object

Use `ThreadObject.layerInHost(application)` when an existing SQLite Durable Object owns related
logical Threads. The application Layer receives the existing `SqlClient`, native stores,
`ThreadMutationGate`, wake scheduler, and `PreparedInputAdmission`. Build its Bindings from those
services and return `DurableAgentRuntime` plus the application's services. The platform then
constructs one maintenance coordinator from that runtime. Do not construct a second runtime or
require `ThreadMaintenance` while building the application.

Supply `ThreadObject.layerHostConfig(options, ownsThread)`, `DurableObjectContext`, and
`ThreadObjectNamespace`. The namespace's `get(threadId)` returns a bound logical endpoint;
its methods call the application's RPC with the selected Thread ID and native encoded payload.
The receiver dispatches with `ThreadObject.handleRpc(threadId, operation, encoded)`. Direct local
admission uses `ThreadObject.submit(threadId, decodedRequest)` and the same validation and prearm.
The [shared-owner example](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/examples/shared-owner.ts) composes
an existing SQL client, application services and an optional projection without external requirements.

Placement must be deterministic and stable across reconstruction. It grants no access: authenticate
callers and validate membership at the application's boundary. Encoded controls additionally check
the addressed Thread against the local receipt or submission before routed runtime access. No
logical `ThreadObjectIdentity` is installed globally; addressed dispatch binds it per invocation.
The producer identity belongs to the physical Object. Native Thread and receipt identities remain
unchanged. Moving existing Threads between physical Objects requires a host-owned fenced transfer;
changing the resolver alone does not move their durable records.

Own one `SqlClient`, `ThreadMutationGate` and alarm slot per physical Object. Native migrations use
their own migration history, leaving the application's migration rows intact. Call
`ThreadMaintenance.ensureAlarm` in the local constructor gate and one bounded
`ThreadMaintenance.pass` from `alarm()`. The event runs at most two independent Thread Attempts
concurrently, with one active FIFO head per Thread and a durable cursor rotating between Threads.
A free slot admits newly ready Threads while another Attempt is busy. Remaining work retains the
alarm; generation acknowledgement waits for admitted Attempts to finish.

An unresolved tool effect stays parked as an Unknown Outcome while later input in the same Thread
can run. The unknown record and settlement obligation remain intact across eviction, and the effect
is not replayed. Approval waits and joined input remain ordering barriers; live ownership still
prevents another claim. `explainThread` exposes parked operations for authorized resolution or abort.

Failed and no-progress passes preserve the dirty generation and use jittered exponential
backoff up to `alarmBackoffCap` (5 seconds by default).
Missing or duplicate agent bindings park the original submission and report the refusal once.
Interruption before the wait commits can repeat the report; reporting is not exactly once.
The wait survives eviction and does not schedule an alarm. Unrelated host work and aborts
remain serviceable. On the next invocation with changed registered identities, definition
digests, or selection key, constructor maintenance clears binding waits and schedules one native pass. A dormant
Object still needs an invocation after deployment; deployment alone does not invoke it.
The original receipt, admission evidence and unresolved tool or child obligations remain intact.
An obsolete pending tool operation does not wait for
historical code: it receives an unavailable result when no mutation was dispatched, or stays
unknown when an external effect may have occurred.

The scheduler and admission limits discover work from the ledger's control-only worklist.
The scheduler then hydrates and recovers the selected Thread before its claim. Accepted abort
intents identify cleanup even for input that was never claimed; their reads share the recovery
timeout and fault boundary, and skip Threads pending recovery or waiting for retry. Old recovery
runs sequentially in the event Scope with a 30-second bound per Thread; the same alarm and
wake loop can dispatch fresh Threads and publish their replies while old history is stalled.
Unfinished cleanup retains its fences and settlement obligations across eviction.

Unreadable retained payloads, history or a failed child recovery block only their Thread.
Maintenance retains the fault outside canonical history and retries after 5, 10, 20, 40, then
60 seconds. New admissions retain their receipts and do not bypass that deadline; other Threads
remain eligible. Successful recovery clears the fault without resolving uncertain external effects.

Hosts consume per-Submission fault transitions through `ThreadRecoveryEvents`:

```ts
import { ThreadRecoveryEvents } from "@effect-agent/platform-cloudflare/alarm";
import { Layer } from "effect";

const recoveryEvents = Layer.succeed(ThreadRecoveryEvents, {
  publish: (event) =>
    failures.apply({
      threadId: event.threadId,
      submissionId: event.submissionId,
      sequence: event.sequence,
      failed: event.transition !== "cleared",
      failure: event.failure,
    }),
});
// Supply { recoveryEvents } to ThreadObject.layer or ThreadObject.layerInHost.
```

`created` marks each affected Submission, including new admissions during backoff. `changed`
means the failure classification or content-free diagnostic changed. `cleared` names every
previously affected Submission, even if recovery has since settled it. Events carry Thread and
Submission IDs, first-failure and transition times, and the bounded `RecoveryFailure` details.
Retry counters and deadlines are private: bookkeeping emits no event or wake. Healthy execution
performs no host recovery-status checks.

Transitions commit atomically with fault state, then an independent maintenance lane delivers
them in order. The lane has a 30-second allowance per wave; a failed or interrupted delivery
remains pending across eviction and does not gate native execution. Return success only after
durably applying the event or retaining it in an application outbox. Delivery is at least once:
deduplicate by physical Object and `sequence`, including when the host commit succeeds but its
acknowledgement is lost. Capture application services when constructing the handler Layer.

These are private host events; authorize recipients before exposing a user-facing failure flag.
A fault is not a Settlement, and a clear does not prove completion. Source-owned accepted-message
notices must not wait for native settlement: a pre-claim fault can precede any reply obligation.
Install the handler from the first maintenance pass; an omitted handler discards transitions.
Existing retained faults announce their pending Submissions on the next native scan without
resetting history or retry deadlines. This replaces `ThreadMaintenance.recoveryStatus`; hosts
must remove their status polling and consume these transitions instead.

Application outboxes enroll independent lanes in one durable due queue:

```ts
import { ThreadHostMaintenance, ThreadMutationGate } from "@effect-agent/platform-cloudflare/alarm";
import { Context, Effect } from "effect";

const maintenance = Context.make(ThreadHostMaintenance, {
  lanes: [{ id: "replies", dispatchTimeoutMillis: 30_000, run: replies.deliverWave }],
});

const retainReply = Effect.gen(function* () {
  const gate = yield* ThreadMutationGate;
  yield* gate.withMutation(replies.retain, {
    invalidatesRecovery: false,
    lanes: ["replies"],
  });
});
```

Each lane has a stable ID, unique within the physical Object. Reserve `effect-agent:` IDs for
framework lanes. `run` returns `Effect<Option<number>, DurableAlarmError, Scope>`: the next deadline
in epoch milliseconds, or `None` when idle. Calculate it as part of the wave that commits the
receipts and retries. The scheduler never calls a separate host deadline reader. Compose hosts
by concatenating their lanes.

When native input or settlement creates application work, select its lanes at the native owner:

```ts
const RuntimeLive = ThreadObject.layer(registrations, {
  hostLanesForMutation: (mutation) => {
    switch (mutation._tag) {
      case "Admission":
        return ["guidance"];
      case "Settlement":
        return ["replies", "memory"];
      case "WorkerStop":
        return ["replies"];
    }
  },
});
```

This pure, bounded selector receives the native request. The owner prearms the selected IDs and
guards them through the source mutation, including replay and recovery finalization without a new
canonical append. Delivery does not depend on lifecycle publication reaching an external database.

Lanes run concurrently by default. Set `phase: "after-native"` on a lane such as memory delivery
to give it one due wave after native Attempts and their scoped cleanup finish, including on native
failure. Selection uses the final queue revisions; idle lanes do not run. The phase stays inside
the same pass permit and fourteen-minute event deadline. Its failures retain independent retries
and are reported together with any native failure.

Newly enrolled concurrent work yields an active post-native phase. Its wave scopes close before
the alarm retires, allowing the next alarm to admit input promptly. Unfinished waves retain their
due revision and exact delivery receipts; completed waves stay acknowledged.

A registered host lane starts idle. Producers name only the lanes receiving work, and the gate
prearms those entries before the mutation body. A failed mutation can leave a discovery wave;
validation and authorization should precede enrollment when they establish that no work is needed.
`gate.schedule(id, dueAt)` explicitly enrolls an existing obligation or an earlier deadline. Use it
inside a local source transaction. For a remote source, retain a scheduling notice atomically with
the work and retry its delivery to `schedule` until acknowledged, using the source's existing retry
identity. Prearming alone cannot fence a remote commit that finishes after Object eviction. Keep the
same gate instance when rebuilding runtime services. Receipt bookkeeping that creates no new work
uses `invalidatesRecovery: false` with no lanes. Native admissions and controls keep the default
`invalidatesRecovery: true`.

The queue retains each lane's own revision and deadline. A finishing wave cannot erase a newer
producer enrollment, and a lane waits for an in-flight source mutation body to finish.
Completions and producer notifications drive the active event; there is no wake-scan timer.
The one alarm retains the earliest queued deadline after all admitted resources close. Constructor
repair reads only local scheduling state, never application deadline tables or execution history.
Future deadlines remain armed when an otherwise quiet event retires.

Declare `dispatchTimeoutMillis` as an integer from 1 to 300000 milliseconds, covering selection,
delivery, retry/receipt commits and scoped cleanup. A wave starts only if its allowance fits the
event. Failed lanes retain independent backoff and do not retry in the same event; healthy lanes
and native work keep their own opportunities. A malformed deadline, missing handler or duplicate
ID fails without discarding the obligation. Persist exact envelopes and deduplicate delivery by
domain identity: interruption cannot roll back remote effects, and delivery remains at least once.
Hooks must not write the raw alarm slot.

Native message delivery keeps the driver's actual Claim deadline, including timeout/retry commits.
`ThreadMessageDelivery.prepare` returns `{ timeoutMillis, run }`, with `run` returning the next
`Option<number>` deadline. Disposable projection backfill keeps one wave per event, bounded by
`projectionDispatchTimeoutMillis` (default 30000ms). All native Attempts share the original ten-minute
yield deadline, and the entire event shares one fourteen-minute ceiling. New arrivals renew neither
budget. Cooperative cancellation cannot preempt synchronous code or stuck finalizers.

**BEHAVIOR CHANGE:** add IDs to host lanes, return their deadlines from `run`, and remove
`pendingDeadline` callbacks and `wakeScanInterval`. Enroll each affected ID explicitly; a generic
`WakeScheduler.notify` is only a promptness hint and does not create host work. On adoption, seed
existing host obligations into the queue before serving traffic; registering a lane alone does not
discover them. Built-in lanes perform one initial discovery when their queue entries are first
created. The queue is new scheduling metadata; canonical history, retry identities, outboxes and
receipts require no reset. Upgrade consumers after the matching framework release is published.

### Publish native lifecycle facts

Use `lifecyclePublication` to publish native admissions, progress, controls, waiting, and
settlement to an eventually consistent application view:

```ts
import { LifecyclePublicationHandler } from "effect-agent/lifecycle-publication";

const RuntimeLive = ThreadObject.layer(registrations, {
  lifecyclePublication: Layer.effect(LifecyclePublicationHandler)(makeLifecycleHandler),
});
```

Source transactions durably retain publication intent. A Run's start, or a Subagent's actual
start, retains the ordered prefix through that record before execution continues. An independent
maintenance wave publishes that prefix while native work runs; later canonical facts stay in the
journal until the settlement wave materializes them together. Ledger and delivery facts retain
their own source receipts. Attempts, model calls, input joins, and handoffs continue while
publication is pending or failing. Eviction reconstructs unmaterialized intent from the journal.

Implement `publish(batch)` for a nonempty, ordinal-ordered array of at most eight facts from one
`ownerThreadId`. Larger backlogs continue in later batches.
Commit the whole batch's authorization decisions, records, receipts, and delivery intents in one
idempotent host transaction before returning. Each call takes a bounded prefix of its owner's
pending facts; facts committed during delivery belong to a later batch. Retries can include
already committed identities, so deduplicate each fact's `id`. Acknowledgement is atomic for the
selected batch and preserves its identity/fingerprint receipts. Publication never replays a model
or Tool operation. Destination deletion or revoked authority is an acknowledged domain decision.

`source` contains immutable private admission evidence, resolved by exact native identities.
Delivery facts carry their retained envelope and accepted receipt. Select declared public fields;
input, private results, and report payloads are not automatically safe to display. `ordinal` orders
facts within `ownerThreadId`; `source.queueSequence` orders accepted inputs within the worker's
Thread. These are separate orders. The handler must reject superseded inputs.
`AbortIntentRecorded` reports the ledger's accepted abort intent. `WorkerInboxSealed.terminal`
retains the first native seal decision: an assignment outcome, or `null` for an explicit stop.
A Run settlement alone does not establish an inbox seal.

Each batch has a 10-second delivery deadline. Retry state persists before dispatch, with eight
automatic attempts and exponential backoff from 1 second to a 60-second cap after the dispatch
deadline. An exhausted owner parks with its payload retained; later facts for that owner wait
behind it, while execution and other owners continue. After repairing the destination, an operator
can call `ThreadStore.lifecyclePublications.retryParked(ownerThreadId, nowMillis)` through the
assembled Cloudflare store; it enrolls the lifecycle lane in the due queue. Serialize publication
drains and operator retries per owner.

Pending and parked obligations retain private payloads until acknowledgement. Keep native source
admissions and Run-input records, and do not delete their Object, until publication debt is
acknowledged. First enabling the option starts with new commits without backfilling history.
Existing source cursors resume retained journal intent; keep the handler enabled while writing
new commits and until all debt drains. Existing SQL publication payloads and receipts are preserved
when upgrading. In-memory/custom adapters do not retain these obligations. SQL assemblies outside
Cloudflare can provide `lifecyclePublicationLayer` and call `drainLifecyclePublications` from their
existing durable maintenance coordinator; its limit counts owners, not individual facts.

### Publish durable host activity

Use the optional publication Layer when canonical records or durable approval, abort, and
unknown-resolution intents must be published before dependent native execution. Independent
UI relays and outboxes belong in `ThreadHostMaintenance`, since publication is an execution gate:

```ts
import { ThreadPublication } from "@effect-agent/platform-cloudflare/alarm";
import {
  DurableObjectContext,
  ThreadObjectIdentity,
} from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import { ThreadStore } from "effect-agent/thread-store";
import { SubmissionLedger } from "effect-agent/submission-ledger";

// `makePublication` is an application Effect yielding ThreadPublicationService.
// It yields the raw LOCAL ThreadStore and SubmissionLedger, native DurableObjectContext,
// ThreadObjectIdentity, and any application services its implementation needs.
const RuntimeLive = ThreadObject.layer(registrations, {
  publication: Layer.effect(ThreadPublication)(makePublication),
});
```

Setup errors and service requirements remain in the resulting Layer; its Scope owns acquired
resources. Initialization must remain local and bounded. The raw source ports are for reading;
publication must not mutate them or write the native alarm slot. Other consumers need no setup.

The host owns schema-versioned cursors, destination idempotency, acknowledgements and retry
policy. Implement three hooks, with failures typed as `DurableAlarmError`:

- `invalidate` durably marks source-derived work pending after a source commit.
- `prepareGeneration(generation)` invalidates a scan when the native generation changes. Repeated
  calls for the same generation must preserve bounded scan progress.
- `drain` performs bounded delivery, persists acknowledgements or retries, and returns the next
  epoch-millisecond deadline as `Option<number>` (`None` when caught up). Delivery is at least once;
  use destination idempotency and scope per-delivery resources explicitly.

All hooks except `drain` must be bounded local operations, without waiting behind network I/O.
Hooks can overlap: the host must prevent an older drain from overwriting newer cursor or retry
state. Do not reenter source mutations from a publication hook. A parked obligation is host-owned
and needs a host repair operation to restore its deadline.

The platform prearms a native generation before ingress mutations and publication-producing
runtime writes. It prepares a generation only after its producers have returned, drains publication
before recovery or potentially slow Agent work, and keeps the earliest publication/runtime alarm.
Pending publication defers runtime work, including when its retry deadline is in the future.
Required custom publication also drains after each source commit. Native lifecycle publication
uses its independent maintenance lane. A post-commit publication failure is logged without
changing the committed source result; a new generation repairs missed invalidation after a crash.
Alarm failures propagate for Workerd
retry, and interruption remains interruption. Custom host facts must be committed through
`ThreadMaintenance.withMutation` to get the same prearm and post-commit hooks.

### Maintain a disposable Thread index

Supply `projection` to `ThreadObject.layer` with a Layer providing
`ThreadProjectionMaintenance` from `effect-agent/thread-projection-maintenance`.
The Layer receives the raw local `ThreadStore` and the same owner `SqlClient`; additional
services it provides are exposed by the resulting runtime Layer so Tools can share that index.

Implement `applyCommitted(request, result)` to keep an already-caught-up index current through
the complete committed batch before Tools execute. One batch contains at most 256 records;
chunk within local byte limits and stop at `result.lastSequence`. An earlier gap belongs to
bounded `drain` backfill. Rows and their contiguous watermark must commit atomically, including
records with no indexable content. Replays and concurrent backfill must be idempotent.

The owner serializes canonical append and live projection; it releases that local gate before
publication. Live failures are logged while the source commit remains authoritative. The native
alarm runs at most one due backfill batch, and `pendingDeadline` keeps unfinished work scheduled
across reconstruction. A projection deadline never gates approval publication or runtime work.
Backfill failures are reported after eligible canonical work, retaining the prearmed generation;
interruption stops the event. Hooks return typed `ThreadProjectionError` failures, own scoped
resources, and never write the alarm slot or call source mutation ports.

## Shared memory {#shared-memory}

Memory is optional and belongs in a separate SQLite Durable Object per host-selected
`MemoryNamespace`, not in a Thread Object. Multiple Threads and application ingestion jobs can
use the same owner. Canonical Thread history, extraction, and scheduling remain separate.

The [compiling setup](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/examples/memory.ts) defines a namespace,
owner authorization Layer, `ProjectMemory` class, and conditional update caller. Register the class:

```jsonc
{
  "durable_objects": {
    "bindings": [{ "name": "MEMORIES", "class_name": "ProjectMemory" }],
  },
  "exports": {
    "ProjectMemory": { "type": "durable-object", "storage": "sqlite" },
  },
}
```

Add `@effect-agent/storage-cloudflare` alongside the packages above.
The owner assembles `doMemoryStoreLayerWithFailpoints` with `SqliteClient.layer({ storage: ctx.storage })`.
Its storage-backed transaction commits the revision and operation receipt together. Local users can
instead provide `doMemoryStoreLayer(ctx.storage)` directly. Neither path imports Node storage.

Bind the namespace and principal in authenticated host code. Never accept them from model output:

```ts twoslash
// @types: @cloudflare/workers-types
import { MemoryNamespace } from "effect-agent";
import { MemoryAccess } from "effect-agent/memory-revalidation";
import { MemoryLookup, MemoryRecallLimits } from "effect-agent/memory-reference";
import { MemoryScope } from "effect-agent/memory-store";
import {
  CloudflareMemoryClient,
  type MemoryObjectRpc,
} from "@effect-agent/platform-cloudflare/cloudflare-memory";
import { Principal } from "effect-agent/submission-ledger";
import { Effect, Schema } from "effect";

const Projects = MemoryNamespace.define({
  name: "app/projects",
  version: 1,
  identity: Schema.String,
});
const access = MemoryAccess.make({
  namespace: Projects.make("authorized-project"),
  scope: MemoryScope.make("project"),
});
const limits = MemoryRecallLimits.make({
  maxSources: 16,
  maxItems: 32,
  maxBytes: 32000,
  maxTokens: 32000,
  maxInputBytes: 1000000,
  timeoutMillis: 5000,
});

export const recall = (
  binding: DurableObjectNamespace<MemoryObjectRpc>,
  candidates: MemoryLookup,
) =>
  Effect.gen(function* () {
    const client = yield* CloudflareMemoryClient.fromBinding(binding, {
      access,
      principal: Principal.make("authenticated-principal"),
    });
    return yield* client.recall(candidates, limits);
  });
```

`CloudflareMemoryClient.fromBinding` accepts a resolved binding from either a Worker or another
Durable Object. It provisions `MemoryObjectNamespace` internally; constructing the client does not
make an RPC. Applications that provide that service once through their Effect Layers can use
`CloudflareMemoryClient.make(access, principal)` instead. Both return the same Effect-native client
with the same validation and budgets.

When the host already knows the document key, use `client.get(key)` with a `MemoryKey` in the
bound namespace. The [compiling example](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/examples/memory.ts)
reads `project-profile` directly. It sends one `Get` owner request and returns a schema-validated
`MemoryDocument` with its current `source.revision`, or `null` only when the key is absent.
A withdrawn key returns `WithdrawnMemoryDocument`, containing its terminal revision and no content.
There is no extraction, job draining, embedding, candidate search, index refresh, rendering, or
background readiness wait on this path.

The owner invokes `MemoryOwnerAuthorizer` for the exact key, namespace, authenticated principal,
and scope before reading, including for absent and withdrawn documents. It also denies active
documents whose `scopes` omit the bound scope. Key or scope possession never grants access.
For source-dependent memories, the application's owner authorizer must preserve its source authority
and provenance checks; a current document revision does not prove that its original evidence remains
authorized or current. These checks remain application-owned and are not replaced by `get`.

Reads begun after an acknowledged write see that revision or a later one through the same SQLite
owner. Reads after withdrawal return the tombstone; an already captured read may finish. The adapter
must fail with `MemoryStorageError` if it cannot provide a current view. Denied access, expired
deadlines, unavailable owners, invalid wire data, and exceeded budgets remain typed failures,
never `null`. Both client and owner enforce `MemoryRpcLimits`: encoded request and response bytes,
encoded document `maxSourceBytes`, and `timeoutMillis`. Storage row limits bound local reads before
wire encoding. An interrupted caller stops waiting; the owner's own deadline finalizes its work.
The `CloudflareMemoryClient.get` span measures the client operation without adding document text,
keys, principals, or scopes as span attributes. Measure application authentication and rendering
separately; this adapter operation alone does not establish a 100–200 ms complete lookup target.

Access and document scopes share the `MemoryScope` brand from core. Clients and owner authorizers
use the existing `Principal` brand from thread. Decode external values with their Effect Schemas
after authentication; `.make` is suitable for trusted constants. Scopes are nonempty strings of at
most 1,024 characters, and principals are nonempty strings of at most 256 characters. Both encode
as ordinary strings on the wire. Brands prevent category mix-ups; they do not grant authorization.

One `recall` sends all admitted candidates in one RPC to `namespace.address`. The owner verifies
its name, request namespace, principal, and scope; it then reads each distinct source locally once.
Passage ordering and authoritative attribution survive the round trip. The client applies the
final rendered item, byte, and token budgets locally and returns `RecalledMemory`. Oversized batches fail typed and are never
split into per-document calls. There is deliberately no remote per-document `MemoryReader` Layer.

The bound source is essential: unavailable or insufficiently fresh results fail, as do matches
that cannot fit the output budget. No-match succeeds with empty text. The result has one source
outcome with `sourceId: "memory"`. An optional third argument supplies the selected model's token
estimator; without it, recall conservatively estimates one token per UTF-8 byte. The recall deadline
covers revalidation and local composition; the engine still enforces its full per-call context budget.

Use `client.revalidate(candidates, limits)` when you need validated passages without rendering.
To combine multiple readers under one shared budget, use `Memory.recall` from `effect-agent` with their revalidation effects as sources. It retains explicit source
IDs and essential/optional policy for that multi-reader case.

For an external semantic index, call `client.revalidateSemantic(search, profile, limits)` with its
`MemoryIndexSearch` result. Embedding and search stay application-owned. This one RPC checks current
generation, revision, locator, exact UTF-8 ranges, scope, and withdrawal before returning `result.lookup`.
Stale scored candidates are omitted. Ordinary cached lookup revalidation instead replaces stale text
with the current document, matching local recall. Neither path trusts cached attribution.
Semantic validation counts the complete UTF-8 JSON of every accepted passage before retaining it,
including repeated metadata and attribution. Its `maxOutputBytes` defaults to 16 MiB and is capped
at the owner's `maxResponseBytes`; the final envelope is checked separately. Duplicate-heavy output
fails with `SemanticMemoryError` reason `budget` before an oversized result is assembled.

Default owner limits are 16 distinct sources, 1 MiB encoded request, 4 MiB encoded response,
16 MiB revalidation input, and a 10-second deadline. `MemoryObject.make` accepts `rpcLimits` and
`storageLimits`. Storage defaults cap encoded rows at 1,900,000 bytes, 10,000 documents, 100,000
operation receipts, and 512 MiB of conservatively accounted row data. SQLite page/index overhead is
not included. Tombstones and receipts count toward capacity; there is no automatic pruning.
Replacements charge the difference between the old and new encoded document, plus the new receipt.
Persistent counters make admission independent of retained history size. Opening an existing
version-2 store initializes counters once, atomically, without rewriting documents or receipts.

Optional `reservedWithdrawalReceipts` and `reservedWithdrawalBytes` storage limits default to zero.
They withhold capacity from ordinary `Put` within the existing hard receipt and byte limits;
`Withdraw` can use the remaining hard budget. Row and document limits still apply. A withdrawal
of an existing source adds one receipt and no document identity. A missing source cannot be
withdrawn: the host must retain suppression for work that arrives before a document exists.

Reserves are finite. Budget enough receipts and encoded bytes for the cleanup commands the host
must complete; they do not guarantee unlimited cleanup. An existing store above the ordinary
threshold remains readable and replayable, while new ordinary writes fail typed. A full store
needs an explicit capacity increase within supported bounds before it has cleanup headroom.

Deploy exclusively upgraded writers before relying on reserves. Accounting triggers include
already-open older writers, but those writers can consume the reserved region because they do not
know the new admission policy. Keep the database, accounting table, triggers and metadata together
in backups; missing established accounting fails rather than silently resetting usage.

Cleanup that edits a shared profile is a `Put`. A trusted host can build a second
`memoryStoreLayer` with `SqlMemoryLimits` over the **same owner SQL client**, omitting the reserves
while retaining the same hard totals. Authorize that capability only for cleanup obligations.
Limits are captured when the Layer is built; providing different limits around an existing
writer call does not change them. Do not create a second independently locked DO SQL client.

`ThreadObject.layer` exposes its existing generic Effect `SqlClient` through `ThreadObject.Services`.
Build optional owner-local repositories after that Layer and reuse this client. For local Memory,
provide `memoryStoreLayer` with explicit `SqlMemoryLimits`, using `defaultDoMemoryStorageLimits`
from `@effect-agent/storage-cloudflare/do-memory-store` or stricter validated limits. The generic SQL
Memory defaults are not Durable Object limits. Thread Objects install no Memory tables unless
the host composes the Memory store.

Owner-local Memory reads reuse validated, write-through documents, operation receipts, and
usage counters under the Thread Object's transaction gate. Rebuilt Layers share those views;
eviction and failed transactions discard them. Cache misses read SQLite. Direct maintenance
writes must follow the [storage invalidation contract](../storage/cloudflare#use-an-existing-object).

The SQL Memory Layer also supplies `SqlMemoryBatchWriter` from `effect-agent/sql-memory-store`.
Use `changeMany(commands)` to commit up to 128 commands atomically, with results in input order.
Commands see earlier revisions in the batch; identical operation IDs recover their original
results, and any conflict or exceeded limit rolls back the whole batch. Storage limits apply
to every intermediate revision. Single-command `MemoryWriter.change` uses the same writer.
The batch service is owner-local; routed `CloudflareMemoryClient.change` remains one command.

Expected failures cross RPC in Schema-defined envelopes. `MemoryRpcError` distinguishes denied,
protocol, budget, timeout, and unavailable failures; source and write errors retain their domain tags.
`cloudflareMemoryWriterLayer(access, principal)` adapts the client for an application's committed
activity destination, preserving domain errors and mapping transport failures to `MemoryStorageError`.
The application still owns invoking `processCommittedActivity` and persisting its progress.

Successful writes are visible to checks begun afterward. Already captured views may finish.
Caller interruption stops waiting but does not promise remote cancellation; the owner enforces its
own deadline and finalizes request-scoped work. A failed or timed-out write may have committed.
Retry only its identical operation ID and command to recover the original receipt. Changed commands
with the same ID fail; withdrawal is terminal. Owner eviction preserves SQLite records and receipts.

Named Effect spans cover calls and local validation without adding source text, private namespace
values, or metadata to span attributes. Keep RPC bindings private and audit host authorization.

The [opt-in deployed benchmark](https://github.com/danieljvdm/effect-agent/tree/main/tooling/cloudflare-memory) measures 1, 4, 8, and
16 sources plus duplicate-heavy candidates, with separate validation-RPC and full-recall durations.
Local SQLite and workerd runs do not establish deployed latency.

### Background remembering

Bind the [remembering checkpoint contract](../guide/context-management#background-remembering)
to the application's existing owner-local jobs. Source commit and outbox admission must be durable;
the owner then runs finite remembering passes in a separate Scope. Keep its model permits separate
from foreground runs. A blocked extraction or profile write must not hold a producer lock or a
database transaction.

The application's job discovery, retry schedule, quotas, authorization, and alarm composition
remain host responsibilities.

Retain source-to-target checkpoints after pruning active jobs. Invalidation reactivates them for
conditional cleanup and preserves uncertain prepared commands. Cleanup of an aggregate profile's
last contribution should leave an empty writable profile; a `Withdraw` memory command permanently
withdraws the entire target. Do not discard receipts or suppression to admit more work.

## Recovery and limits

Alarms recover pending work after eviction without another user request.
The host owns the Object's [single alarm](https://developers.cloudflare.com/durable-objects/api/alarms/);
do not replace its handler or schedule unrelated alarms on that Object.

Each Thread alarm grants an initial head Attempt and can advance further heads while auxiliary
delivery remains in flight. Recovery precedes each claim, and all Attempts share the event's
original ten-minute yield deadline. Accepted input can still join the active Run at normal turn
boundaries. At the yield deadline, the Attempt commits its completed turn before yielding; a later
alarm resumes the same Run with its original duration deadline and cumulative usage.

The whole Thread alarm has a fourteen-minute watchdog, including time waiting for another pass.
The Schedule Owner uses the same watchdog while scanning due schedules. It continues past failed
pages so a page of broken schedules cannot block healthy followers. Interrupted work keeps its
durable retry obligation. These timers leave room below Cloudflare's
[fifteen-minute alarm lifetime](https://developers.cloudflare.com/durable-objects/platform/limits/),
but cannot preempt synchronous CPU work or an uninterruptible finalizer. Cloudflare's CPU limit
is separate from elapsed time.

`maxQueueDepthPerLane`, `maxInputBytes`, and `maxDatabaseBytes` refuse excess work with
`AdmissionLimitExceeded` before admission. Keep Object RPC private and supply
`operationAuthorizer` for application access rules. The default policy trusts service possession.

An ordinary tool interrupted before its outcome is confirmed can become Unknown during recovery;
it is never automatically replayed. Unconfirmed outcomes need authorized resolution. See
[operations](../guide/operations).

## Runtime memory

Cloudflare's 128 MB memory limit applies to an isolate, which can contain multiple Durable Objects
and their Worker. It is not a separate allowance for every Object. See
[memory usage metrics](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/#memory-usage).

Use the package subpaths shown above to keep dependencies explicit. The package also declares
unused modules removable, so Wrangler can remove unused adapters from root imports.

Canonical reads and observations fetch at most 4 MiB of record JSON per internal SQL page, after
capturing up to 1,024 sequence and size entries. Decoded objects and strings require additional
heap. Recovery retains evidence for the addressed runs; prompt projection scans a fixed canonical
tail and avoids retaining summarized response payloads. Metadata and scanning work still grow
with history, and an uncompacted prompt still grows with the conversation. Configure
[`contextTokenLimit`, compaction, tool result bounds, and concurrency](../guide/context-management)
for the workload from the start. Admission and record-size limits do not reserve isolate memory.
Whole-thread export still returns a complete collection; use paged reads for large histories.

The [local heap benchmark](https://github.com/danieljvdm/effect-agent/tree/main/tooling/cloudflare-memory#local-heap-measurements)
measures exact Worker bundles and several concurrent Thread Objects using a synthetic model and
tools. It requires no model key or deployment. Its local JavaScript heap snapshots help compare
changes; profile production-like histories and tool payloads before choosing deployment capacity.

## Code execution and browsers

Use the [Code Mode guide](../guide/code-mode) to run generated JavaScript in a Dynamic Worker with
allowlisted host tools. The warehouse example queries a SQLite Durable Object through that broker;
its agent runs ephemerally and uses the Object for data only.

The [browser guide](../guide/browser) covers Quick Actions, screenshots, REST capture and crawl,
and interactive passes with Live View and handoff. Browser adapters use separate package imports
and can be used without a durable thread host. REST capture and crawl also work on Node.
