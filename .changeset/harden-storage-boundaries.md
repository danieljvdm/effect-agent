---
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
---

Validate storage configuration before acquiring SQLite resources, and compare replayed persisted JSON by Schema semantics instead of serialized key order. Keep Cloudflare transport failures typed under hostile foreign values and narrow routed responses with operation schemas.
