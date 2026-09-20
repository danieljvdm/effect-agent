---
"effect-agent": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/testing": patch
---

Add durable worker-wide stop and indexed summaries with exact accepted and applied input identities. Replay retained starts before public input preparation and drain up to 32 accepted worker inputs at each safe steering boundary.
