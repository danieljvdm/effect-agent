---
"effect-agent": patch
---

Reuse compacted Thread context for Cloudflare's routed Submission IDs while preserving canonical replay for ambiguous or incompatible histories. Keep eligible Thread context cached when retaining the completed Run's recovery data would exceed cache bounds.
