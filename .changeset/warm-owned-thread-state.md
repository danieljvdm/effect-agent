---
"effect-agent": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": patch
---

Keep Cloudflare thread and submission state in bounded write-through memory, reuse decoded journal projections, and serve warm recovery snapshots without SQLite reads. Quiesce port operations during direct SQL maintenance and call `DoThreadStore.invalidate(ctx.storage)` before resuming them; adapter writes maintain the cache automatically, and stored data needs no reset.
