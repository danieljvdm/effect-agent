---
"@effect-agent/core": patch
"@effect-agent/engine": patch
"@effect-agent/capabilities": patch
"@effect-agent/sandbox": patch
"@effect-agent/sandbox-local": patch
"@effect-agent/session": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/platform-node": patch
"@effect-agent/testing": patch
---

Initial beta-channel release of the Effect Agent framework packages for live
integration testing: the schema-first authoring core, the ephemeral
interpreter, operational capabilities, sandbox contracts and the local
adapter, canonical session records with the durable coordinator, the memory
and SQLite storage adapters, the Node platform assembly, and the
deterministic testing kit. The Cloudflare packages stay private until their
declaration-emit blocker (TS4094) is resolved.
