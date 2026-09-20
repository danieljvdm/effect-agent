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

Applications use `@effect-agent/storage-sqlite` or `@effect-agent/storage-postgres`. Adapter
factories supply a `SqlClient`, typed errors, failpoints, and transaction settings. Database
connections, stored-format checks, and supported upgrades remain with each adapter.

Cloudflare reuses the subscription, message-delivery, and native-read helpers with its own
Durable Object transaction boundary. This package has no platform runtime dependency.
