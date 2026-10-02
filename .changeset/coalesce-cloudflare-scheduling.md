---
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent": patch
---

Coalesce Cloudflare maintenance scheduling writes within each transaction and reuse its queue view without changing retry, publication, or recovery behavior.
