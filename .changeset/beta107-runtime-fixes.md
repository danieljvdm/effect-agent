---
"effect-agent": patch
"@effect-agent/core": patch
"@effect-agent/engine": patch
"@effect-agent/capabilities": patch
"@effect-agent/sandbox": patch
"@effect-agent/sandbox-local": patch
"@effect-agent/thread": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/testing": patch
---

Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
Binding capture with live Durable Object context and derived identities, and prevent incomplete
application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
