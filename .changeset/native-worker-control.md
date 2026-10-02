---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-testing": patch
---

Add durable worker-wide stop and indexed summaries with exact accepted and applied input identities. Replay retained starts before public input preparation and drain up to 32 accepted worker inputs at each safe steering boundary.
