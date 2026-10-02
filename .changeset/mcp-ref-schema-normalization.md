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

Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.
