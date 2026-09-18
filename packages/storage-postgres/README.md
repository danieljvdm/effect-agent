# @effect-agent/storage-postgres

Postgres persistence adapters for the Effect Agent durable runtime.

```ts
import { PostgresThreadStore } from "@effect-agent/storage-postgres";
import { Redacted } from "effect";

const storage = PostgresThreadStore.layer({
  client: { url: Redacted.make(process.env.DATABASE_URL) },
});
```

`PostgresSubmissionLedger.layer` serves durable accepted work from the same options. Each store
also exposes `layerWithServices`, which takes the config, failpoint, client and crypto services
from the composition root instead of owning them.

## Requirements

- Postgres 16 or newer. The adapter uses the `IS JSON` predicate, which arrived in 16.
- A role permitted to create the adapter's tables in the connection's schema.

## What differs from the SQLite adapter

**The schema must be the connection's.** Tables live in whatever schema the connection resolves,
`public` by default. `search_path` binds per connection while the client is a pool, and this
driver exposes no way to set one for the pool, so selecting another schema means making it the
connection default (`ALTER ROLE ... SET search_path`). Startup verifies the effective schema
across concurrent connections and refuses to run if they disagree, rather than writing some
statements to the wrong place.

**One writer at a time.** SQLite's `BEGIN IMMEDIATE` is a database-wide write lock, and the
stores' read-then-write invariants — tail comparison, batch idempotency, ledger admission — were
written against it. Write transactions take a transaction-scoped advisory lock to reproduce it.
A bounded `lock_timeout` turns a blocked writer into the retryable `PostgresWriteContention`
rather than an indefinitely held connection.

**Snapshot reads.** Exports and paged reads run at `REPEATABLE READ READ ONLY`, preserving the
snapshot-with-concurrent-writer contract without taking a write lock.

**One storage version.** The SQLite adapter carries upgrade paths from its development formats.
This adapter starts at version 1: an absent schema is created, a current schema is used, and any
other version fails with `PostgresStorageCompatibilityError`.

**Integers.** Stored sequences, ordinals, and epoch milliseconds are `BIGINT`, which the driver
reports as `BigInt`. `PostgresStorageClient.layer` registers an `int8` codec that decodes to a
JavaScript number and rejects anything outside the safe-integer range, so the shared row schemas
stay identical across adapters. Build the client with that Layer; one built elsewhere will not
decode stored values correctly.
