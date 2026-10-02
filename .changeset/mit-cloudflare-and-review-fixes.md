---
"@yielded/agent-sandbox-local": patch
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
"@yielded/agent-platform-node": patch
"@yielded/agent-platform-cloudflare": patch
"@yielded/agent-testing": patch
"@yielded/agent": patch
---

Adopt the MIT license across every published package, and ship the Cloudflare
packages with type declarations for the first time: their Durable Object
class factory now carries an explicit `ThreadObjectClass` return type,
which unblocks TypeScript declaration emit (TS4094). Supersedes the
0.0.1-beta.2 round (and the Cloudflare pair's 0.0.1-beta.0), which was
published out of band from an uncommitted tree, still UNLICENSED, and without
`.d.mts` for the Cloudflare packages.
