---
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

Adopt the MIT license across every published package, and ship the Cloudflare
packages with type declarations for the first time: their Durable Object
class factory now carries an explicit `ThreadObjectClass` return type,
which unblocks TypeScript declaration emit (TS4094). Supersedes the
0.0.1-beta.2 round (and the Cloudflare pair's 0.0.1-beta.0), which was
published out of band from an uncommitted tree, still UNLICENSED, and without
`.d.mts` for the Cloudflare packages.
