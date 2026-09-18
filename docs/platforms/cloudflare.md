---
title: Cloudflare
description: Run durable agents on Cloudflare Workers and Durable Objects.
---

# Cloudflare

`@effect-agent/platform-cloudflare` stores each thread and its pending work in a
SQLite-backed Durable Object. RPC calls and alarms drive execution and recovery.

## Install

```sh
bun add @effect-agent/platform-cloudflare@beta
```

Also install `effect@4.0.0-rc.115`, `effect-cf@^0.44.1`, `effect-agent@beta`,
`@effect/ai-openai@4.0.0-rc.115` for the examples below.
Keep framework packages at one release and add your [model provider](../guide/getting-started#installation-and-compatibility).

## AI Gateway {#ai-gateway}

The Node-safe `@effect-agent/platform-cloudflare/cloudflare-ai-gateway` subpath configures
upstream Effect clients in Workers, Durable Objects, Node, or Bun. `Gateway.provide` supplies
the client directly in a Layer pipeline; model selection, tools, response decoding, streaming,
and typed provider errors stay with upstream Effect AI. Use the configured client for primary agents, subagents,
compaction models, [WebSearch](../guide/tools#web-search), or embeddings supported by its provider.

Two endpoint families have different credentials and model names:

| Helper                                                           | Authentication                                             | Model names                                       |
| ---------------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------- |
| `Gateway.rest({ accountId, gatewayId, apiToken, protocol })`     | Cloudflare API token with Workers AI Read permission       | Provider-qualified, such as `openai/gpt-4.1-mini` |
| `Gateway.provider({ accountId, gatewayId, provider, apiToken })` | `cf-aig-authorization`; optionally a separate provider key | Native provider name, such as `gpt-4.1-mini`      |

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

const ModelLive = OpenAiLanguageModel.model("gpt-4.1-mini").pipe(
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

const modelName = "gpt-4.1-mini";

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
current binding per stable `agentId`: queued and resumed work uses the current binding without
requiring historical agent or toolbox versions. Accepted inputs and prepared deliveries keep
their original identities and payloads.

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
`ThreadMaintenance.pass` from `alarm()`. Each native opportunity recovers local lanes and serves
one eligible FIFO head, with a durable cursor rotating between Threads. Remaining work retains the alarm.

An unresolved tool effect stays parked as an Unknown Outcome while later input in the same Thread
can run. The unknown record and settlement obligation remain intact across eviction, and the effect
is not replayed. Approval waits and joined input remain ordering barriers; live ownership still
prevents another claim. `explainThread` exposes parked operations for authorized resolution or abort.

Failed and no-progress passes preserve the dirty generation and use jittered exponential
backoff up to `alarmBackoffCap` (5 seconds by default), independently of `wakeScanInterval`.
Missing agent bindings wait 5, 10, 20, 40, then 60 seconds between attempts;
further attempts remain one minute apart. The retry deadline survives eviction:
`ensureAlarm` and early alarm deliveries cannot accelerate native recovery for the same
generation. A newer durable mutation can wake it immediately, and host deadlines remain
independently serviceable. Each blocked Thread retains its own waiting period, so it cannot
monopolize other Threads. Binding refusals still fail closed and retain the original submission.
There is no terminal retry-count limit: dropping the alarm would strand accepted work after a
later deployment that registers the agent. An obsolete pending tool operation does not wait for
historical code: it receives an unavailable result when no mutation was dispatched, or stays
unknown when an external effect may have occurred.

Unreadable history or a failed child recovery blocks only its Thread. Maintenance stores a
bounded `ThreadRecoveryFault` outside canonical history and retries after 5, 10, 20, 40, then
60 seconds. New admissions retain their receipts and do not bypass that Thread's deadline;
other Threads remain eligible. A successful recovery clears the fault without changing history
or resolving uncertain external effects.

After a native pass, an authenticated host can inspect the local fault without decoding history:

```ts
import { ThreadMaintenance } from "@effect-agent/platform-cloudflare/alarm";
import { ThreadId } from "effect-agent/identifiers";

const status = ThreadMaintenance.use((maintenance) =>
  maintenance.recoveryStatus(ThreadId.make("thread-1")),
);
// Effect<Option<ThreadRecoveryFault>, DurableAlarmError | OperationDenied, ThreadMaintenance>
```

`recoveryStatus` authorizes `explain` before reading storage, using the `OperationAuthorizer`
provided when constructing `ThreadMaintenance.layer`. The host verifies local Thread membership
before exposing it over RPC. `Some` carries failure phase, content-free diagnostics,
first/last failure time, retry time and attempt count; `None` means no recorded fault. Neither
proves settlement or health. Source-owned accepted-message notices must not wait for native
settlement: a pre-claim fault can occur before any reply obligation or binding attempt exists.

Application outboxes can supply `ThreadHostMaintenance` from the application Layer:

```ts
import { ThreadHostMaintenance } from "@effect-agent/platform-cloudflare/alarm";
import { Context, Effect } from "effect";

// Capture the application's services when constructing the hooks.
const maintenance = Context.make(ThreadHostMaintenance, {
  // For example, a wave of four sequential commits allowing 15 seconds each.
  dispatchTimeoutMillis: 60_000,
  pendingDeadline: outbox.pendingDeadline,
  drainUntil: (dispatchClosed, dispatchUntil) =>
    Effect.gen(function* () {
      // The supplied Scope belongs to the physical alarm, including retirement.
      yield* admission.listen.pipe(Effect.forkScoped);
      yield* outbox.drainUntil(dispatchClosed, dispatchUntil);
    }),
});
```

The application pump admits an initial bounded wave, even on a caught-up alarm, and may dispatch
new wake-driven waves until `dispatchClosed`. This signal stops new external waves; native
execution can continue while the admitted waves finish. Keep local admission and hub subscriptions
in the supplied event Scope until maintenance tears it down. The pump returns after its active
wave finishes. Delivery retries belong to later alarms, not deadline-sleep loops in the hook.

Declare the whole-wave `dispatchTimeoutMillis` as an integer from 1 to 300000 milliseconds.
Use the sum for sequential operations and the maximum for parallel lanes. Admit a new wave only
if its full allowance fits before `dispatchUntil`; otherwise leave the work durably due without
claiming an attempt. Setup and `pendingDeadline` are bounded local operations.

The native scheduler owns recovery, FIFO selection and retry timing. After its initial opportunity,
it closes admission of external waves and keeps responding to native wakes while the admitted
waves finish. Already-ready work and new admissions can execute without cancelling or restarting
an unrelated delivery. Periodic generation checks recover dropped wake hints; receipt-only
bookkeeping does not create native recovery debt.

Native message delivery keeps the driver's actual Claim deadline, including its timeout/retry
commit. The driver has four parallel permits and a retained attempt allowance of at most five
minutes. Host delivery uses its declared whole-wave allowance; disposable backfill has one wave
bounded by `projectionDispatchTimeoutMillis` (default 30000ms, maximum 300000ms). These limits
are independent of native arrivals. All native Attempts share the original ten-minute yield
deadline, and the entire event shares one fourteen-minute ceiling. Neither input nor delivery
renews these budgets. Cooperative cancellation cannot preempt synchronous code or stuck finalizers.

Each native step checkpoints its observed generation without changing the physical alarm. After
all event resources close, the owner reads durable deadlines under the mutation gate and rearms
or clears the alarm once. A producer racing either checkpoint or retirement retains its newer
generation and prearmed wake. Persist exact envelopes and claims before network dispatch; local
cancellation cannot roll back remote effects. Interrupted work remains recoverable after
reconstruction. Typed failures, defects and a hook's own interruption retain recovery; only the
owner's finite delivery cutoff is deferred work. The maintenance span records host cutoff use.

Every accepted host mutation uses the shared `ThreadMutationGate`; hooks must not write the raw
alarm slot. Native admission, approval, abort and unknown resolution retain the default
`invalidatesRecovery: true`. Projection, relay and reply receipt-only bookkeeping uses
`invalidatesRecovery: false`, with its local `pendingDeadline` owning scheduling. Reuse the same
gate instance when rebuilding maintenance or runtime services. Migrate consumer hooks only with
an actual published release containing this API, keeping the framework packages on one matching
release; do not pin an unpublished branch or patch installed dependencies.

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
policy. Implement four hooks, with failures typed as `DurableAlarmError`:

- `invalidate` durably marks source-derived work pending after a source commit.
- `prepareGeneration(generation)` invalidates a scan when the native generation changes. Repeated
  calls for the same generation must preserve bounded scan progress.
- `drain` performs bounded delivery and persists acknowledgements or a retry deadline. External
  delivery is at least once; use destination idempotency. Scope per-delivery resources explicitly.
- `pendingDeadline` returns `Option<number>` in epoch milliseconds, or `None` when caught up.

All hooks except `drain` must be bounded local operations, without waiting behind network I/O.
Hooks can overlap: the host must prevent an older drain from overwriting newer cursor or retry
state. Do not reenter source mutations from a publication hook. A parked obligation is host-owned
and needs a host repair operation to restore its deadline.

The platform prearms a native generation before ingress mutations and publication-producing
runtime writes. It prepares a generation only after its producers have returned, drains publication
before recovery or potentially slow Agent work, and keeps the earliest publication/runtime alarm.
Pending publication defers runtime work, including when its retry deadline is in the future.
A post-commit publication failure is logged without changing the committed source result; the
new generation repairs missed invalidation after a crash. Alarm failures propagate for Workerd
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

The [persistent host fixture](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/test/restart/remembering-worker.ts)
uses the existing memory reader/writer and SQLite-backed source, outbox, job, and retained checkpoint
tables. It demonstrates host-owned wake repair and current-source recall. It is an application
binding example, not an additional scheduler or a Cloudflare `ActivityProcessorStore` requirement.
The actual application's job discovery, retry schedule, quotas, authorization, and alarm composition
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
