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

Introduce the `effect-agent` umbrella package: the framework's complete pure
surface — schema-first authoring (core), the bounded interpreter (engine),
and operational capabilities — as one dependency-clean root package,
mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
remain scoped. The umbrella is version-fixed to its three constituents.
