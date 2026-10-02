---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
---

Add opt-in terminal worker assignments that remain steerable while waiting and permanently reject new work after completion, failure, or cancellation. Preserve existing reusable workers and upgrade native storage seals without resetting retained data.
