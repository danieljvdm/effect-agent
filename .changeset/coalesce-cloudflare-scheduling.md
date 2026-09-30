---
"@effect-agent/platform-cloudflare": patch
---

Coalesce Cloudflare maintenance scheduling writes within each transaction and reuse its queue view without changing retry, publication, or recovery behavior.
