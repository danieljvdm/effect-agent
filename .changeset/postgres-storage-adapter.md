---
"@effect-agent/storage-postgres": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-sqlite": patch
"effect-agent": patch
---

Add a Postgres storage adapter, and a `SqlDialect` seam so the shared SQL stores can serve more than one dialect.
