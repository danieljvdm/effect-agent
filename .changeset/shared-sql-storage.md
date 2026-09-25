---
"@effect-agent/storage-sql": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"effect-agent": patch
---

Share SQL persistence implementations through `@effect-agent/storage-sql` while preserving SQLite storage formats and adapter APIs. BEHAVIOR CHANGE: import SQL subscription, message-delivery, native-read, and upgrade helpers from `@effect-agent/storage-sql` instead of `effect-agent`, and pass custom transactions through the factory options.
