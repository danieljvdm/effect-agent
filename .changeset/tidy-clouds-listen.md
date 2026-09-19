---
"@effect-agent/platform-cloudflare": patch
---

Keep native dispatch and host abort/reply waves open together while unrelated alarm work retires. BEHAVIOR CHANGE: accept the third `ThreadMaintenanceActivity` argument in `drainUntil`, register finite waves with `run`, subscribe to `changes`, and acknowledge initial setup with `ready`; use `ThreadMaintenanceActivity.all` to compose independent pumps.
