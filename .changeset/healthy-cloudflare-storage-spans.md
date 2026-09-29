---
"@effect-agent/storage-cloudflare": patch
---

Record expected ownership cleanup, stale fences, and append contention as successful storage span outcomes and skip unnecessary write transactions. Preserve concrete error and cause tags on real storage failures without adding payloads to trace attributes.
