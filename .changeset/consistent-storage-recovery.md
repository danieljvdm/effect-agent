---
"@yielded/agent-storage-memory": patch
"@yielded/agent-storage-sqlite": patch
"@yielded/agent-storage-cloudflare": patch
---

Keep admission identities and applied input markers consistent across storage adapters, and reject checkpoints whose payload disagrees with stored metadata. Read SQLite recovery snapshots without acquiring a write lock.
