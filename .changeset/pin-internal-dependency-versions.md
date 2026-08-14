---
"@effect-agent/core": patch
"@effect-agent/engine": patch
"@effect-agent/capabilities": patch
"@effect-agent/sandbox": patch
"@effect-agent/sandbox-local": patch
"@effect-agent/session": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/platform-node": patch
"@effect-agent/testing": patch
---

Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
artifacts depended on internal `@effect-agent/*` versions that were never
published (`workspace:*` ranges were resolved from a stale lockfile at
publish time); the release script now pins internal ranges to the exact
workspace versions itself.
