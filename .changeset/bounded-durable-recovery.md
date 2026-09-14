---
"@effect-agent/testing": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"effect-agent": patch
---

Resume durable runs from disposable recovery checkpoints while preserving canonical side-effect obligations and cumulative accounting. Support up to 131,072 canonical records in storage, exports, and verification, with preserving upgrades for supported SQLite and Durable Object stores.
