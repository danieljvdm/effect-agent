---
"effect-agent": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-sql": patch
---

Publish committed Run and Subagent start progress while native execution continues. Reduce SQLite statements for warm Durable Object turns while preserving recovery and ownership fencing.
