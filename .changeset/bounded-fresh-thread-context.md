---
"@yielded/agent": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-cloudflare": patch
---

Reuse eligible compacted Thread context across fresh durable Runs, refreshing it from new canonical records while preserving full replay for incompatible histories. Validate stored checkpoints through indexed canonical batch lookups.
