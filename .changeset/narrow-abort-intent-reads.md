---
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent": patch
---

Poll durable abort intent without loading unrelated recovery state. Implement `SubmissionLedger.readAbortIntent` in custom ledger adapters.
