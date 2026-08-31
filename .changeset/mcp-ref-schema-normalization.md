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

Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.
