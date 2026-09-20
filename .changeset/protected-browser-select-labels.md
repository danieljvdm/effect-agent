---
"effect-agent": patch
"@effect-agent/platform-cloudflare": patch
---

Expose visible choices and selection state for ordinary protected selects, and preserve prior dispatch evidence after acknowledged no-write refusals.

BEHAVIOR CHANGE: Fill ordinary selects with the exact observed option label; raw option values are no longer a fallback, while credential selects continue to use private values.
