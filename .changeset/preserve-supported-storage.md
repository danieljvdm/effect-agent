---
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-memory": patch
"@effect-agent/thread": patch
---

Upgrade supported beta49/beta50 persistent stores in place while preserving pending work, canonical history, receipts, and alarm state. Preserve unknown historical occurrence times when replaying retained events.
