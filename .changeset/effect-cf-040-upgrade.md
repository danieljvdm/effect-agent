---
"@yielded/agent": patch
"@yielded/agent-sandbox-local": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-pr-review": patch
"@yielded/agent-testing": patch
---

Upgrade to Effect rc.112 and `effect-cf` 0.40.0 while preserving MCP transports and Cloudflare host behavior.

BEHAVIOR CHANGE: Upgrade Effect and its provider/platform/SQL packages to rc.112 or a compatible version. In Cloudflare hosts, provide `effect-cf@^0.40.0` and enable `nodejs_compat` for its async context support.
