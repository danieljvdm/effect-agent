# @effect-agent/storage-postgres

Postgres persistence adapters for the Effect Agent durable runtime.

```ts
import { PostgresThreadStore } from "@effect-agent/storage-postgres";
import { Redacted } from "effect";

const storage = PostgresThreadStore.layer({
  client: { url: Redacted.make(process.env.DATABASE_URL) },
  schema: "effect_agent",
});
```

## Requirements

- Postgres 16 or newer. The adapter uses the `IS JSON` predicate, which arrived in 16.
- A role permitted to `CREATE SCHEMA`, or a pre-created schema it may own.

## What differs from the SQLite adapter

**Schema, not file.** Tables live in a dedicated schema (`effect_agent` by default) rather than a
database of their own, so agent storage stays separable from application tables on one instance.

**Serializable write transactions.** SQLite serializes writers, so a read-then-write inside
`BEGIN IMMEDIATE` is safe by construction. Postgres does not, so canonical appends, fence checks,
and ledger compare-and-set run at `SERIALIZABLE` with a bounded `lock_timeout`. A transaction the
database rolls back for a serialization failure, a deadlock, or a lock timeout surfaces as the
retryable `PostgresWriteContention` — the same signal, and the same caller contract, as the
SQLite adapter's busy-timeout error.

**Snapshot reads.** Exports and paged reads run at `REPEATABLE READ READ ONLY`, preserving the
snapshot-with-concurrent-writer contract without taking a write lock.

**One storage version.** The SQLite adapter carries upgrade paths from its development formats.
This adapter starts at version 1: an absent schema is created, a current schema is used, and any
other version fails with `PostgresStorageCompatibilityError`.

**Integers.** Stored sequences, ordinals, and epoch milliseconds are `BIGINT`. The adapter's
client registers an `int8` codec that decodes to a JavaScript number and rejects anything outside
the safe-integer range, so the shared row schemas stay identical across adapters.
