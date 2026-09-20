---
"effect-agent": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
---

Add opt-in terminal worker assignments that remain steerable while waiting and permanently reject new work after completion, failure, or cancellation. Preserve existing reusable workers and upgrade native storage seals without resetting retained data.
