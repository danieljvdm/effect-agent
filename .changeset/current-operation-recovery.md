---
"effect-agent": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
---

Continue accepted requests with current Agent bindings, retaining original operations and outcomes while allowing later input around unknown work under one Thread lease. BEHAVIOR CHANGE: replace historical binding manifests with per-operation replay versions and deploy matching runtime and storage packages together.
