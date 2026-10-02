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

Introduce the `@yielded/agent` umbrella package: the framework's complete pure
surface — schema-first authoring (core), the bounded interpreter (engine),
and operational capabilities — as one dependency-clean root package,
mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
remain scoped. The umbrella is version-fixed to its three constituents.
