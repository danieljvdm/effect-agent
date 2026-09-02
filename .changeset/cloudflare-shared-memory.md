---
"@effect-agent/core": minor
"@effect-agent/thread": minor
"@effect-agent/capabilities": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": minor
"@effect-agent/platform-cloudflare": minor
---

Add optional namespace-owned Cloudflare memory with bounded batch recall, authoritative semantic-candidate validation, and durable conditional writes shared across Threads. Limit semantic recall output with `maxOutputBytes`, counting repeated attribution and metadata.

BEHAVIOR CHANGE: Construct access and document scopes with `MemoryScope.make` or decode them with its Schema; Cloudflare memory clients require the existing branded `Principal`, capped at 256 characters.

BEHAVIOR CHANGE: Replace `recallMemory` with `Memory.recall` for multi-source composition, or use `client.recall(candidates, limits)` for a bound Cloudflare memory client. The old function is removed without an alias.
