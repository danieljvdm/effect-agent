---
"@yielded/agent-ai-decision": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-pr-review": patch
"@yielded/agent-sandbox-local": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-postgres": patch
"@yielded/agent-storage-sql": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-testing": patch
"@yielded/agent-workflow": patch
"@yielded/agent": patch
---

Require Effect 4.0.0 and use its current module paths and encoding APIs. Require `effect-cf@^0.53.0` for the Cloudflare adapter.

BEHAVIOR CHANGE: upgrade Effect and matching provider, platform, SQL, and Atom packages to 4.0.0; replace `effect/unstable/*` imports with `effect/*` and use `effect/http-api` for HTTP APIs. Cloudflare logical alarms now back off from one second and park for hourly recovery after eight attempts without reported source progress.
