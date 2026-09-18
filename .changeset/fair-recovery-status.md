---
"effect-agent": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": patch
---

Isolate per-thread recovery faults, retain authorized history-independent status and bounded retries on Cloudflare, and preserve content-free storage diagnostics. Treat `RecoveryBlocked` reports from a successful recovery sweep as pending work that cannot be claimed until recovery succeeds.
