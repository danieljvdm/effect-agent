---
"@yielded/agent-sandbox-local": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-testing": patch
"@yielded/agent": patch
---

Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
artifacts depended on internal `@effect-agent/*` versions that were never
published (`workspace:*` ranges were resolved from a stale lockfile at
publish time); the release script now pins internal ranges to the exact
workspace versions itself.
