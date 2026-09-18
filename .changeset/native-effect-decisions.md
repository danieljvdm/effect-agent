---
"@effect-agent/ai-decision": minor
"@effect-agent/platform-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/pr-review": patch
"@effect-agent/sandbox-local": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/testing": patch
"@effect-agent/workflow": patch
"effect-agent": patch
---

Require Effect rc.116 and replace the local decision and TypeSafe APIs with native `Decision`, `DecisionModel`, and `@effect/ai-typesafe`, retaining `AutoModel` for thread selection.

BEHAVIOR CHANGE: Import decisions from `effect/unstable/ai` and configure TypeSafe with `TypeSafeClient.layerConfig()`; AutoModel requires at least two profiles, writes version 2 selection records, and rejects version 1 records without reselection or mutation. Retain the previous runtime for active version 1 threads or explicitly upgrade their records in your storage adapter; native probability sums must be within `1e-6` of 1.
