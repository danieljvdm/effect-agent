---
title: Cloudflare
description: Store thread history and accepted work in Durable Object SQLite.
---

# Cloudflare

Cloudflare stores thread history and accepted work in Durable Object SQLite.
Each Object owns its database, separate from other Objects. Records survive Object
eviction and reconstruction.

## Run durable agents

Use [`@effect-agent/platform-cloudflare`](../platforms/cloudflare) for the complete
host. `ThreadObject.layer` and `ThreadObject.make` assemble storage, registrations,
admission, RPC, and alarm-driven recovery. The
[Cloudflare setup](../platforms/cloudflare#create-the-thread-object) includes the
agent, provider, Object class, and binding configuration. No separate storage Layer
is needed.

## Use an existing Object

If your application already owns a SQLite Durable Object, follow
[custom host integration](../platforms/cloudflare#share-an-application-object) to share
its database and alarm slot with the runtime. Reuse the host's Effect SQL client
for application queries.

`@effect-agent/storage-cloudflare` also exposes `DoThreadStore` and
`DoSubmissionLedger` for custom assemblies. Their convenience Layers accept
`ctx.storage` and supply the SQL client and Crypto. Both stores must use the same
database so ownership claims fence the same records. These adapters provide storage
ports; acquiring them alone does not drive agent execution or recovery.

## Optional shared memory

Thread history and memory shared across threads have different owners. For shared
documents, recall, and routed memory operations, see
[Cloudflare shared memory](../platforms/cloudflare#shared-memory).
