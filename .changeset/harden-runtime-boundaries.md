---
"effect-agent": minor
"@effect-agent/core": minor
"@effect-agent/engine": minor
"@effect-agent/capabilities": minor
"@effect-agent/session": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/platform-node": minor
"@effect-agent/platform-cloudflare": minor
"@effect-agent/pr-review": minor
"@effect-agent/testing": minor
---

Make runtime authority, cryptography, definition digests, and operation callers explicit; validate
durable recovery for the `DN` and `DC` deployment classes against canonical evidence; repair
settlement projections atomically; bound event, diagnostic, context, and Cloudflare read surfaces;
and expose only curated Schema-decoded platform operations. These changes remove ambient
dependencies and fail closed when persisted, transported, or authorization evidence is absent or
contradictory.
