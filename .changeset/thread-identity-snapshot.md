---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-cloudflare": patch
---

Read canonical worker identity and its producer fence in one bounded owner snapshot instead of four serial remote reads. Custom ThreadStore adapters must implement `readIdentity`; deploy matching Cloudflare client and owner packages for the new read-only operation.
