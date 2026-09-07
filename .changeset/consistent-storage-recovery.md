---
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
---

Keep admission identities and applied input markers consistent across storage adapters, and reject checkpoints whose payload disagrees with stored metadata. Read SQLite recovery snapshots without acquiring a write lock.
