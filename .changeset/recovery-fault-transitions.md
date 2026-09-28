---
"@effect-agent/platform-cloudflare": minor
---

Deliver durable per-submission recovery fault create, change, and clear events without notifying hosts for retry bookkeeping.

BEHAVIOR CHANGE: Replace `ThreadMaintenance.recoveryStatus` polling with a `ThreadRecoveryEvents` handler supplied through `ThreadObject.layer` or `ThreadObject.layerInHost`; durably apply or enqueue events before acknowledging them, and deduplicate by physical Object and event sequence.
