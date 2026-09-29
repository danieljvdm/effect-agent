---
"@effect-agent/platform-cloudflare": patch
---

Schedule Cloudflare maintenance through one durable due queue and alarm, running only explicitly enrolled host lanes without wake-scan polling.

BEHAVIOR CHANGE: Give each `ThreadHostMaintenance` lane a stable, unique `id` and return `Option<number>` (next epoch-millisecond deadline, or `None` when idle) from `run`; remove its `pendingDeadline` callback. Enroll only affected IDs through `ThreadMutationGate.withMutation(body, { invalidatesRecovery: false, lanes: [id] })`, or call `schedule(id, dueAt)` within the local source transaction. For remote sources, retain a scheduling notice atomically with the work and retry its delivery to `schedule` until acknowledged, preserving the source retry identity; prearming or a wake hint alone cannot recover a remote commit after Object eviction. Seed existing host obligations before serving traffic: registered host lanes no longer get an initial wave. Keep native admission/control mutations on the default recovery invalidation. Return the same deadline result from `ThreadPublication.drain` and `ThreadMessageDelivery.prepare().run`, removing their `pendingDeadline` callbacks, and remove `wakeScanInterval`. Retain existing authorization, delivery identities, outboxes and receipts; no data reset is required.
