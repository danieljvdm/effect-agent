---
"@effect-agent/core": patch
"@effect-agent/engine": patch
---

Keep explicit tool pins selected only while authorized, so disabling a common tool does not prevent other work. Omit unavailable optional completion tools from final-answer requests while preserving mandatory tool checks.
