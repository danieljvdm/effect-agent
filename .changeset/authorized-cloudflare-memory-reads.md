---
"@effect-agent/platform-cloudflare": patch
"@effect-agent/storage-cloudflare": patch
---

Add authorized, bounded exact-key current document reads through `CloudflareMemoryClient.get`, returning explicit absence or withdrawal tombstones while preserving typed access, storage, and transport failures.
