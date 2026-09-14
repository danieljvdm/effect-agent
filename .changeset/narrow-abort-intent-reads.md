---
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-node": patch
"effect-agent": patch
---

Poll durable abort intent without loading unrelated recovery state. Implement `SubmissionLedger.readAbortIntent` in custom ledger adapters.
