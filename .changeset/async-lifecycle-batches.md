---
"effect-agent": minor
"@effect-agent/storage-sql": minor
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": minor
---

Publish retained lifecycle facts asynchronously in ordered owner batches without delaying model attempts, with atomic receipts and bounded retries that park exhausted work.

BEHAVIOR CHANGE: Implement `LifecyclePublicationHandler.publish(batch)` for a nonempty array of at most eight facts and commit the entire batch idempotently in one host transaction; custom lifecycle storage implementations must return bounded owner batches, replace `defer` with `claim`, and implement `retryParked` instead of `pendingDeadlineFor`.
