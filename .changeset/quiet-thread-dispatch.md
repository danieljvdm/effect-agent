---
"effect-agent": patch
"@effect-agent/platform-node": patch
---

Reduce redundant recovery reads and ledger scans during durable execution. Dispatch managed Node host work through one bounded queue that coalesces repeated thread notifications.
