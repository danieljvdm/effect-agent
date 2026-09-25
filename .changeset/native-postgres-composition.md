---
"@effect-agent/storage-postgres": patch
"@effect-agent/storage-sql": patch
---

Compose PostgreSQL storage with an application-provided Effect SQL client and Crypto layer, preserving native connection pooling, codecs, and schema defaults.

BEHAVIOR CHANGE: Replace the `client` option and `PostgresStorageClient` with `PostgresStorage.make()` plus native Layers; call shared schema and index creation helpers as functions, optionally supplying a namespace.
