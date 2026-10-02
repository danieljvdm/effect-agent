---
"@yielded/agent": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-platform-cloudflare": patch
---

Continue accepted requests with current Agent bindings, retaining original operations and outcomes while allowing later input around unknown work under one Thread lease. BEHAVIOR CHANGE: replace historical binding manifests with per-operation replay versions and deploy matching runtime and storage packages together.
