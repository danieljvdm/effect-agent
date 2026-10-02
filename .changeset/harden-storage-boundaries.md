---
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
---

Validate storage configuration before acquiring SQLite resources, and compare replayed persisted JSON by Schema semantics instead of serialized key order. Keep Cloudflare transport failures typed under hostile foreign values and narrow routed responses with operation schemas.
