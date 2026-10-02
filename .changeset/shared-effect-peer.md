---
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-pr-review": patch
"@yielded/agent-sandbox-local": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-testing": patch
"@yielded/agent": patch
---

Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.
