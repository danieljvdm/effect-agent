---
"@effect-agent/platform-node": patch
"@effect-agent/storage-sqlite": patch
---

Recover automatically managed Node hosts after process death without waiting for retained ownership leases. BEHAVIOR CHANGE: automatic hosts require existing databases to use WAL and exclusively own their SQLite database; use the host's services for live administration or stop it before opening a separate connection.
