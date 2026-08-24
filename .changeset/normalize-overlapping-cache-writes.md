---
"@effect-agent/engine": patch
---

Normalize provider cache-write usage when the provider also includes those tokens in uncached
input. Preserve additive canonical usage totals without rejecting valid model responses.
