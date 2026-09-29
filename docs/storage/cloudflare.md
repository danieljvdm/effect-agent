---
title: Cloudflare
description: Store thread history and accepted work in Durable Object SQLite.
---

# Cloudflare

Cloudflare stores thread history and accepted work in Durable Object SQLite.
Each Object owns its database, separate from other Objects. Records survive Object
eviction and reconstruction. While the Object is live, the adapters share bounded,
write-through memory for thread headers, submissions, and recovery state. Warm
`loadRecoverySnapshot` calls see committed local writes without rereading SQLite;
cache misses reload validated rows. Failed transactions discard cached state.

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

Use the adapters for ordinary writes. If maintenance writes thread or submission tables directly,
call `DoThreadStore.invalidate(ctx.storage)` afterward, before serving port reads. Keep
port operations quiesced during both steps and enroll the write with the host's mutation gate.
This invalidates both adapters' shared view;
it does not change durable data. Canonical records remain append-only.

Background worker starts validate authority and reserve capacity at the source, then admit and
initialize the child through one destination RPC. Custom routed assemblies must install
`PortRouting.routedWorkerAdmissionLayer` alongside the routed ledger and thread store. The
owner-side port handler requires `WakeScheduler` and `DurableRuntimeFailpoint` and must run
mutations through the host's maintenance gate. `ThreadObject.make` and `ThreadObject.layerInHost`
provide this wiring, including enrollment of affected maintenance lanes. Upgrade the framework,
storage and host packages together; stored records and retry receipts need no reset.

## Optional shared memory

Thread history and memory shared across threads have different owners. For shared
documents, recall, and routed memory operations, see
[Cloudflare shared memory](../platforms/cloudflare#shared-memory).
