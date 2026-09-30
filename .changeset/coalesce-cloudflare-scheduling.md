---
"@effect-agent/platform-cloudflare": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-sql": patch
"effect-agent": patch
---

Coalesce Cloudflare maintenance scheduling writes within each transaction and reuse its queue view without changing retry, publication, or recovery behavior.
