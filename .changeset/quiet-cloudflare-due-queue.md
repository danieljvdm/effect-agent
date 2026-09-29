---
"@effect-agent/platform-cloudflare": patch
---

Schedule Cloudflare maintenance through one durable due queue and alarm, running only explicitly enrolled host lanes without wake-scan polling.

BEHAVIOR CHANGE: Give each `ThreadHostMaintenance` lane a stable, unique `id` and return `Option<number>` (next epoch-millisecond deadline, or `None` when idle) from `run`; remove its `pendingDeadline` callback. Enroll only affected IDs through `ThreadMutationGate.withMutation(body, { invalidatesRecovery: false, lanes: [id] })`, or call `schedule(id, dueAt)` within the local source transaction. Wrap remote commits with `withMutation` so enrollment precedes acknowledgement; a wake hint alone does not enroll work. Seed existing host obligations before serving traffic: registered host lanes no longer get an initial wave. Keep native admission/control mutations on the default recovery invalidation. Return the same deadline result from `ThreadPublication.drain` and `ThreadMessageDelivery.prepare().run`, removing their `pendingDeadline` callbacks, and remove `wakeScanInterval`. Retain existing authorization, delivery identities, outboxes and receipts; no data reset is required.
