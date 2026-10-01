---
"@effect-agent/ai-decision": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/pr-review": patch
"@effect-agent/sandbox-local": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-postgres": patch
"@effect-agent/storage-sql": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/testing": patch
"@effect-agent/workflow": patch
"effect-agent": patch
---

Require Effect 4.0.0 and use its current module paths and encoding APIs.

BEHAVIOR CHANGE: upgrade Effect and matching provider, platform, SQL, and Atom packages to 4.0.0; replace `effect/unstable/*` imports with `effect/*` and use `effect/http-api` for HTTP APIs.
