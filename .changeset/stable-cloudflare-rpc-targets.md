---
"@effect-agent/platform-cloudflare": patch
---

Reuse native RPC targets within each Cloudflare invocation so delegated work, progress observation, memory and scheduling callbacks do not exhaust subrequest depth. Replace failed channels and start fresh target scopes for incoming requests and durable retries.
