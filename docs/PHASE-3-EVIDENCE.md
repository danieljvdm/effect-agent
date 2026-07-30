# Phase 3 persistent Conversation foundation evidence

Status: **Implemented**

Phase 3 makes Conversation history persistent and replayable without claiming that accepted work
will finish after process loss. The deployment-class label for this slice is `P`, never `DN`.

## Delivered package surface

- `@effect-agent/session` owns the current-version canonical record envelope and payload union,
  atomic batch/read/export/checkpoint requests, pure reducers and projections, opaque observation
  offsets, definition digests, and separate Conversation Store and non-durable Submission Store
  ports.
- `@effect-agent/storage-memory` is the scoped deterministic reference adapter and conformance
  implementation.
- `@effect-agent/storage-sqlite` uses the pinned Effect SQL SQLite client, current-version
  migrations, transactions for atomic batch writes, validated reads, resumable observation, export,
  and checkpoints.
- The private-development reset command deletes only an explicitly selected incompatible local
  SQLite database. It is not migration tooling.

## Executable exit-gate evidence

| Phase 3 claim                                          | Deterministic evidence                                                                 |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| Full replay and checkpoint replay are equivalent       | session reducer tests and both adapter conformance suites                              |
| Canonical batch append is atomic and conflict checked  | memory and SQLite append/idempotency/conflict tests                                    |
| Opaque offsets resume without replay gaps              | shared observation conformance scenarios                                               |
| Stored values are Schema-validated on writes and reads | session codec fixtures plus corrupt-row SQLite tests                                   |
| Unsupported versions fail before mutation              | current-version compatibility cases in session and adapters                            |
| Definition/configuration changes are digest visible    | deterministic definition digest tests                                                  |
| Projections rebuild from canonical records             | pure replay tests and the persistent Travel Planner itinerary projection               |
| SQLite restart reconstructs the same Conversation      | persistent Travel Planner SQLite restart scenario                                      |
| Conversation export is portable and redacted           | current-version export fixture tests                                                   |
| Submission persistence makes no durable-work promise   | separate Submission Store contract exposes only its explicit non-durable Phase 3 claim |

## Stored-version policy

Only the current private-development version is supported. Records and checkpoints still carry
explicit versions, but incompatible local data is reset rather than migrated. Corrupt, unsupported,
or digest-mismatched data fails through typed errors before a caller-visible mutation.

To remove an incompatible local database, select one explicit SQLite file inside the repository
and acknowledge that it is disposable private-development data:

```sh
bun run reset:development-storage \
  --database .data/effect-agent.sqlite \
  --confirm-private-development
```

The database's parent directory must already exist. Add `--dry-run` to inspect the database, WAL,
and shared-memory files that would be removed. The command rejects directories, paths outside the
repository, symlinked parents outside the repository, and non-SQLite filename extensions.

## Non-claims

- A persisted Conversation is not durable accepted work.
- Phase 3 returns no durable Receipt and owns no lease, attempt, wake, recovery, or terminal
  settlement protocol.
- SQLite persistence does not make an ordinary external Tool side effect exactly-once.
- Checkpoints and projections are disposable derivatives; the canonical append-only record log is
  authoritative.
