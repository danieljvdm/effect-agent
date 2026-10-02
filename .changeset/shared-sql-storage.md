---
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent": patch
---

Share SQL persistence implementations through `@yielded/agent-storage-sql` while preserving SQLite storage formats and adapter APIs. BEHAVIOR CHANGE: import SQL subscription, message-delivery, native-read, and upgrade helpers from `@yielded/agent-storage-sql` instead of `@yielded/agent`, and pass custom transactions through the factory options.
