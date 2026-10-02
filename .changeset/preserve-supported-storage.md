---
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent": patch
---

Upgrade supported beta49/beta50 persistent stores in place while preserving pending work, canonical history, receipts, and alarm state. Preserve unknown historical occurrence times when replaying retained events.
