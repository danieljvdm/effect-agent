---
"effect-agent": patch
"@effect-agent/storage-sql": patch
"@effect-agent/storage-cloudflare": patch
---

Reuse eligible compacted Thread context across fresh durable Runs, refreshing it from new canonical records while preserving full replay for incompatible histories. Validate stored checkpoints through indexed canonical batch lookups.
