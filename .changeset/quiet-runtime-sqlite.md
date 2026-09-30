---
"effect-agent": patch
"@effect-agent/storage-sql": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": patch
---

Reduce Durable Object SQLite work with shared write-through reads and asynchronous lifecycle batches after native execution, preserving durable receipts across eviction. Use the SQL Memory Layer's `SqlMemoryBatchWriter.changeMany` to commit up to 128 ordered commands atomically and combine their writes.
