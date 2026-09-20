# @effect-agent/storage-sql

Shared SQL persistence for Effect Agent storage adapters. It owns the journal, submission
ledger, thread reads, schedules, subscriptions, message delivery, and activity transitions.

```text
effect-agent ports
       ↑
   storage-sql
       ↑
SQLite / Postgres adapters
```

Applications use `@effect-agent/storage-sqlite` or `@effect-agent/storage-postgres`. Adapters
supply a `SqlClient`, typed diagnostics, failpoints, and native transaction settings. Journal
format checks and supported upgrades remain adapter-owned; standalone activity initialization
is shared here.

Message delivery and subscriptions default to the client's transaction. Their factory options
accept a transaction callback for Postgres writer locking or Cloudflare's atomic alarm updates.
Cloudflare also reuses the native-read helpers. This package has no platform runtime dependency.
