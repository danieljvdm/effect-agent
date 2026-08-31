---
title: Cloudflare
description: Run durable agents on Cloudflare Workers with SQLite-backed Durable Objects.
---

# Cloudflare

`@effect-agent/platform-cloudflare` runs each Conversation in a SQLite-backed Durable Object.
The Object owns its history and pending work. RPC calls and alarms drive execution and recovery.

## Install

```sh
npm install --save-exact @effect-agent/platform-cloudflare@beta @effect-agent/session@beta @effect/platform-browser@4.0.0-rc.111 effect@4.0.0-rc.111 effect-cf@0.37.0
```

Add your model provider package and credentials separately. Keep framework packages on the same
release; see [installation and compatibility](../guide/getting-started#installation-and-compatibility).
The durable host does not require Puppeteer.

## Create the Conversation Object

`ConversationObject.make` creates the class you export from your Worker. This helper accepts
your application's composed runtime Layer:

```ts twoslash
// @types: @cloudflare/workers-types
import { ConversationObject } from "@effect-agent/platform-cloudflare";

export const createConversationObject = (
  application: Parameters<typeof ConversationObject.make>[0],
) =>
  ConversationObject.make(application, {
    namespaceBinding: "CONVERSATIONS",
    deploymentId: "travel-planner",
    producerPrefix: "travel-worker",
    maxQueueDepthPerLane: 64,
  });
```

Build the application with `ConversationObject.layer(registrations)`, then provide its model
clients and Tool handlers through `Layer.provide`. Each registration pairs an Agent Binding
with Agent, Model, and Tool version declarations. The runtime hashes those declarations and
captures the required services during Layer construction. Initialization failures remain typed;
missing application services must be supplied before passing the Layer to the class factory.
See [Run durably on Cloudflare](../guide/run-agents#run-durably-on-cloudflare) for a complete
Agent registration and provider Layer.

Call this helper with that Layer and export the returned class as `TravelConversation` from
the Worker entry point. Application Layers can yield `effect-cf`'s `WorkerEnvironment` and
`DurableObjectState.DurableObjectState`, along with `ConversationObjectIdentity` and Crypto.
Use `Layer.unwrap` when the registrations depend on configuration or other services.

The application is acquired once per Object incarnation. Keep initialization local and bounded.
Cloudflare eviction does not guarantee finalizers, so acquire resources needing timely release
in scoped operations or `options.eventLayer`. Use the same definition digests for registration
and submission, and retain matching registrations for outstanding work across deployments.

The class factory handles initialization, RPC, and alarms. Each event runs a bounded recovery
and processing pass. There is no process-wide worker loop to start.

## Configure the binding

Declare the exported class and its SQLite storage in your Wrangler configuration:

```jsonc
{
  "name": "travel-planner",
  "main": "src/worker.ts",
  "compatibility_date": "2026-08-31",
  "durable_objects": {
    "bindings": [{ "name": "CONVERSATIONS", "class_name": "TravelConversation" }],
  },
  "exports": {
    "TravelConversation": { "type": "durable-object", "storage": "sqlite" },
  },
}
```

`CONVERSATIONS` must match `namespaceBinding`, and `TravelConversation` must match the exported
class. Cloudflare's [class configuration guide](https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/)
also covers existing Workers that use the older `migrations` array.

## Connect from your Worker

`CloudflareConversationClient` wraps the Object RPC calls in Effects. Build its Layer from
the Worker's environment and a Crypto implementation:

```ts twoslash
// @types: @cloudflare/workers-types
import {
  CloudflareConversationClient,
  conversationNamespaceLayer,
} from "@effect-agent/platform-cloudflare";
import { BrowserCrypto } from "@effect/platform-browser";
import { Layer } from "effect";

export const conversationClientLayer = (env: unknown) =>
  CloudflareConversationClient.layer.pipe(
    Layer.provide(conversationNamespaceLayer(env, "CONVERSATIONS")),
    Layer.provide(BrowserCrypto.layer),
  );
```

Inside your authenticated request handler, use `client.submit(agent, input, options)` with a
Conversation ID, principal, idempotency key, and definition digests. Return the Receipt after
admission. The client routes that Conversation to its named Object.

Use `client.awaitSettlement(receipt)` for the terminal outcome. For incremental updates, read
a page with `client.readPage`, wait with `client.awaitProgress`, then read after the last
sequence you received. Keep a progress wait in a Scope so interruption cancels its remote wait.
Mount these Effects behind your application's HTTP or RPC API; the package does not create routes.

## Recovery and limits

Pending work is stored before acknowledgement. Alarms allow maintenance to continue after
an Object is evicted, without another user request. The host owns the Object's
[single alarm](https://developers.cloudflare.com/durable-objects/api/alarms/); do not replace
its alarm handler or independently schedule alarms on the same Object.

`maxQueueDepthPerLane`, `maxInputBytes`, and `maxDatabaseBytes` bound admission. A refusal returns
`AdmissionLimitExceeded` before accepting new work. These limits do not replace authentication
or authorization. The default operation policy relies on possession of the service; keep Object
RPC access private and supply `operationAuthorizer` for application-specific access rules.

After an interrupted Tool call, recovery may need an authorized Unknown Outcome resolution.
See [durability](../concepts/durability) and [operations](../guide/operations) for that workflow.

## Browser and Code Mode adapters

The package also supplies Code Mode execution and optional Browser Run adapters:

| Import                                                   | Use                                                        |
| -------------------------------------------------------- | ---------------------------------------------------------- |
| `@effect-agent/platform-cloudflare`                      | Durable host and Code Mode executor                        |
| `@effect-agent/platform-cloudflare/browser-quick-action` | Page capture through a browser binding                     |
| `@effect-agent/platform-cloudflare/browser-rest-capture` | Page capture through Cloudflare's REST API                 |
| `@effect-agent/platform-cloudflare/browser-rest-crawl`   | Bounded same-host Markdown crawl                           |
| `@effect-agent/platform-cloudflare/interactive-browser`  | Scoped navigation, reading, clicks, fills, and screenshots |

The interactive adapter requires `@cloudflare/puppeteer` and a browser binding. Browser handles
last for one scoped pass and do not survive Object eviction. See the
[package reference](../reference/packages#effect-agent-platform-cloudflare) for adapter requirements.
