---
"@effect-agent/platform-node": patch
"@effect-agent/storage-sqlite": patch
---

Recover automatically managed Node hosts after process death without waiting for retained ownership leases. BEHAVIOR CHANGE: automatic hosts exclusively own their SQLite connection; use the host's services for live administration or stop it before opening a separate connection.
