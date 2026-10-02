---
"@yielded/agent-ai-decision": minor
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-pr-review": patch
"@yielded/agent-sandbox-local": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-testing": patch
"@yielded/agent-workflow": patch
"@yielded/agent": patch
---

Require Effect rc.116 and replace the local decision and TypeSafe APIs with native `Decision`, `DecisionModel`, and `@effect/ai-typesafe`, retaining `AutoModel` for thread selection.

BEHAVIOR CHANGE: Import decisions from `effect/ai` and configure TypeSafe with `TypeSafeClient.layerConfig()`; AutoModel requires at least two profiles, writes version 2 selection records, and rejects version 1 records without reselection or mutation. Retain the previous runtime for active version 1 threads or explicitly upgrade their records in your storage adapter; native probability sums must be within `1e-6` of 1.
