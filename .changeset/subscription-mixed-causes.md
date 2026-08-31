---
"@effect-agent/thread": patch
"@effect-agent/platform-node": patch
---

Keep subscription recovery and Node polling active after failures combined with sibling interruption, while still stopping when the host Scope closes.
