---
"effect-agent": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
---

Allow hosts to hand off at completed Turn boundaries to the next independent input while retaining each Run's authority, receipts and obligations. Install matching runtime and storage packages before enabling `SubmissionScheduling.yieldTo`.
