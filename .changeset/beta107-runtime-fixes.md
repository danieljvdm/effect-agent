---
"@yielded/agent": patch
"@yielded/agent-sandbox-local": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-testing": patch
---

Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
Binding capture with live Durable Object context and derived identities, and prevent incomplete
application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
