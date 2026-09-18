---
"effect-agent": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": patch
---

Isolate per-thread recovery faults, retain authorized history-independent status and bounded retries on Cloudflare, and preserve content-free storage diagnostics.

BEHAVIOR CHANGE: Call `runtime.runRecovery()` instead of yielding `runtime.runRecovery`, and treat `RecoveryBlocked` reports as pending work that cannot be claimed until recovery succeeds; pass `{ excludeThreads }` to retain host-owned retry deadlines.
