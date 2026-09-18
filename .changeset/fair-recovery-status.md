---
"effect-agent": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/platform-node": patch
---

Isolate Thread recovery faults with history-independent status and bounded retries, let Cloudflare dispatch fresh Threads while old cleanup is pending, and keep Node startup closed on blocked recovery. Preserve content-free storage diagnostics.

BEHAVIOR CHANGE: Call `runtime.runRecovery()` instead of yielding `runtime.runRecovery`; its result contains ordinary Submission `reports` and one `blocked` fault per failed Thread. Blocked Threads remain ineligible for claims; pass `{ threadId }` to recover only a selected Thread.
