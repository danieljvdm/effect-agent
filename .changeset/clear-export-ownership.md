---
"@effect-agent/capabilities": minor
"@effect-agent/engine": minor
"effect-agent": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": patch
"@effect-agent/testing": patch
"@effect-agent/thread": minor
---

Import memory and delegation contracts from core and compaction, command-drain, and scheduling contracts from engine instead of capabilities; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.
Use the documented compactor and run-status APIs in place of the removed engine implementation helpers, and import history and registration types through their matching thread subpaths.
