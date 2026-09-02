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

Also install `effect@4.0.0-rc.111`, `effect-cf@^0.37.0`, `@effect-agent/core@beta`,
`@effect-agent/thread@beta`, `@effect/ai-openai@4.0.0-rc.111`, and
`@effect/platform-browser@4.0.0-rc.111` for the examples below.
Keep framework packages at one release and add your [model provider](../guide/getting-started#installation-and-compatibility).

## Create the thread object

Compose agent registrations and application services as a layer, then pass it to
`ThreadObject.make`. This example expects `OPENAI_API_KEY` and a `THREADS` Durable
Object namespace in the generated `Cloudflare.Env`.

```ts
import { Agent, AgentPolicy } from "@effect-agent/core";
import { ThreadObject } from "@effect-agent/platform-cloudflare";
import { DefinitionDigestInput } from "@effect-agent/thread";
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

export const travelDefinitions = DefinitionDigestInput.make({
  agent: { id: TravelPlanner.id, revision: 1 },
  model: { provider: "openai", name: modelName },
  tools: [],
});

const OpenAiLive = Layer.unwrap(
  Effect.map(WorkerEnvironment, (env) =>
    OpenAiClient.layer({ apiKey: Redacted.make(env.OPENAI_API_KEY) }).pipe(
      Layer.provide(FetchHttpClient.layer),
    ),
  ),
);

const RuntimeLive = ThreadObject.layer([
  {
    agent: Agent.withModel(TravelPlanner, OpenAiLanguageModel.model(modelName)),
    definitions: travelDefinitions,
  },
]).pipe(Layer.provide(OpenAiLive));

export class TravelThread extends ThreadObject.make(RuntimeLive, {
  namespaceBinding: "THREADS",
  deploymentId: "travel-planner",
  producerPrefix: "travel-worker",
}) {}
```

Each registration pairs a model-bound agent with explicit agent, model, and tool versions. The
submitter passes `digestDefinitions(travelDefinitions)` through
`DurableSubmitOptions.definitions`. Bump the agent revision when instructions, schemas, or policy
change. Version tool implementations and model configuration when they change.

Application layers can use `WorkerEnvironment`, `DurableObjectState`,
`ThreadObjectIdentity`, and Crypto. Use `Layer.unwrap` for configuration-dependent registrations.
The application is acquired once per Object instance and rebuilt after eviction. Keep initialization
local and bounded. Eviction does not guarantee finalizers; acquire resources needing timely cleanup
inside scoped operations or `options.eventLayer`. Each event runs a bounded recovery pass;
no worker loop is needed.

Register the exported class as a SQLite Durable Object under `THREADS`.
`ThreadObject.layer([])` registers no agents and refuses every agent identity.

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

## Configure the binding

```jsonc
{
  "name": "travel-planner",
  "main": "src/worker.ts",
  "compatibility_date": "2026-08-31",
  "durable_objects": {
    "bindings": [{ "name": "THREADS", "class_name": "TravelThread" }],
  },
  "exports": {
    "TravelThread": { "type": "durable-object", "storage": "sqlite" },
  },
}
```

Match `THREADS` to `namespaceBinding` and `TravelThread` to the exported class.
See Cloudflare's [class configuration guide](https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/)
for Workers using the older `migrations` array.

## Connect from your Worker

```ts twoslash
// @types: @cloudflare/workers-types
import { CloudflareThreadClient, type ThreadObjectRpc } from "@effect-agent/platform-cloudflare";

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
Expose these Effects through your application's HTTP or RPC API.

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

Add `@effect-agent/storage-cloudflare` and `@effect-agent/capabilities` alongside the packages above.
The owner assembles `doMemoryStoreLayerWithFailpoints` with `SqliteClient.layer({ storage: ctx.storage })`.
Its storage-backed transaction commits the revision and operation receipt together. Local users can
instead provide `doMemoryStoreLayer(ctx.storage)` directly. Neither path imports Node storage.

Bind the namespace and principal in authenticated host code. Never accept them from model output:

```ts twoslash
// @types: @cloudflare/workers-types
import {
  MemoryAccess,
  MemoryLookup,
  MemoryNamespace,
  MemoryRecallLimits,
} from "@effect-agent/core";
import { recallMemory } from "@effect-agent/capabilities";
import { CloudflareMemoryClient, type MemoryObjectRpc } from "@effect-agent/platform-cloudflare";
import { Effect, Schema } from "effect";

const Projects = MemoryNamespace.define({
  name: "app/projects",
  version: 1,
  identity: Schema.String,
});
const access = MemoryAccess.make({
  namespace: Projects.make("authorized-project"),
  scope: "project",
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
      principal: "authenticated-principal",
    });
    return yield* recallMemory(
      [{ id: "project", essential: true, read: client.revalidate(candidates, limits) }],
      limits,
    );
  });
```

`CloudflareMemoryClient.fromBinding` accepts a resolved binding from either a Worker or another
Durable Object. It provisions `MemoryObjectNamespace` internally; constructing the client does not
make an RPC. Applications that provide that service once through their Effect Layers can use
`CloudflareMemoryClient.make(access, principal)` instead. Both return the same Effect-native client
with the same validation and budgets.

One `revalidate` sends all admitted candidates in one RPC to `namespace.address`. The owner verifies
its name, request namespace, principal, and scope; it then reads each distinct source locally once.
Passage ordering and authoritative attribution survive the round trip. `recallMemory` applies the
final rendered item, byte, and token budgets on the caller. Oversized batches fail typed and are never
split into per-document calls. There is deliberately no remote per-document `MemoryReader` Layer.

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
not included. Tombstones and receipts count toward capacity; there is no automatic pruning. A
replacement conservatively charges both the old and new document while admitting the write.

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

The [opt-in deployed benchmark](https://github.com/danieljvdm/effect-agent/tree/main/examples/cloudflare-memory) measures 1, 4, 8, and
16 sources plus duplicate-heavy candidates, with separate validation-RPC and full-recall durations.
Local SQLite and workerd runs do not establish deployed latency.

## Recovery and limits

Alarms recover pending work after eviction without another user request.
The host owns the Object's [single alarm](https://developers.cloudflare.com/durable-objects/api/alarms/);
do not replace its handler or schedule unrelated alarms on that Object.

`maxQueueDepthPerLane`, `maxInputBytes`, and `maxDatabaseBytes` refuse excess work with
`AdmissionLimitExceeded` before admission. Keep Object RPC private and supply
`operationAuthorizer` for application access rules. The default policy trusts service possession.

Unconfirmed tool outcomes need authorized resolution. See [operations](../guide/operations).

## Code execution and browsers

Use the [Code Mode guide](../guide/code-mode) to run generated JavaScript in a Dynamic Worker with
allowlisted host tools. The warehouse example queries a SQLite Durable Object through that broker;
its agent runs ephemerally and uses the Object for data only.

The [browser guide](../guide/browser) covers Quick Actions, screenshots, REST capture and crawl,
and interactive passes with Live View and handoff. Browser adapters use separate package imports
and can be used without a durable thread host. REST capture and crawl also work on Node.
