---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
---

Allow hosts to hand off at completed Turn boundaries to the next independent input while retaining each Run's authority, receipts and obligations. Install matching runtime and storage packages before enabling `SubmissionScheduling.yieldTo`.
