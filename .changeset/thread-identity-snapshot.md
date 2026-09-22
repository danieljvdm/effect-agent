---
"effect-agent": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": patch
---

Read canonical worker identity and its producer fence in one bounded owner snapshot instead of four serial remote reads. Custom ThreadStore adapters must implement `readIdentity`; deploy matching Cloudflare client and owner packages for the new read-only operation.
