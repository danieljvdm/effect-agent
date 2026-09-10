---
"@effect-agent/core": patch
"@effect-agent/thread": patch
---

Distinguish durably retained worker input from storage failures with `WorkerError` reason `delivery-pending`. Treat this outcome as pending delivery, preserve the original idempotency key, and wait for a Receipt before reporting destination acceptance.
