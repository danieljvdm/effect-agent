---
"@effect-agent/platform-cloudflare": patch
---

Keep native dispatch and host abort/reply waves open together while unrelated alarm work retires. BEHAVIOR CHANGE: yield the event-scoped `ThreadMaintenanceActivity` service in `drainUntil`, acquire `subscribeChanges` before initial setup, register finite waves with `run`, and acknowledge readiness with `ready`; use `ThreadMaintenanceActivity.all` to compose independent pumps.
