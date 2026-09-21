---
"effect-agent": patch
"@effect-agent/storage-memory": patch
---

Allow an authorized `Subagent.start` with `continuationOf` to create a new worker from a successfully completed assignment while preserving its seal, lineage and policy bounds. Retain explicit worker stops in native worker-state reads.
