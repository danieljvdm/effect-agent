---
"effect-agent": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
---

Avoid rereading unfinished submissions on canonical progress by separating progress and settlement wake hints. Preserve broad wake behavior for existing schedulers and external notifications.
