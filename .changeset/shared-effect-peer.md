---
"@effect-agent/capabilities": patch
"@effect-agent/core": patch
"@effect-agent/engine": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/pr-review": patch
"@effect-agent/sandbox": patch
"@effect-agent/sandbox-local": patch
"@effect-agent/session": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/testing": patch
"effect-agent": patch
---

Declare `effect` as a required peer across all public packages so they share the application's runtime. Keep `effect` in application dependencies at the release's exact supported version.
