# ADR-0011: Durable runtime placement, leases, storage versioning, and Turn-granular records

- Status: Accepted by default
- Status note (2026-08-13): adopted as the Phase 4 implementation default by the integrated P4
  plan (decisions D1, D5, D7, D8) and implemented as such
  ([Phase 4 evidence](../PHASE-4-EVIDENCE.md)); owner review may still amend it
- Date: 2026-08-13
- Decision owners: Project owner
- Related decisions: D-004, D-005, D-006, D-014, D-015, D-020, D-029

## Context

Phase 4 turns the persistent Conversation foundation into the durable Node/SQLite runtime (`DN`).
Four choices shaped its architecture and needed a durable record:

1. where the durable coordinator lives;
2. what role ownership leases play beside producer-epoch fencing;
3. how the SQLite storage version treats existing Phase 3 files;
4. at what granularity model output becomes canonical.

The engine remains one pull-based interpreter with producer suspension at semantic seams, so a
consumer that commits canonically on `TurnCompleted` before pulling further gets
turn-boundary commit without engine changes.

## Decision

### Coordinator placement (D1)

`DurableAgentRuntime`, the recovery classifier, and the run journal live in
`@effect-agent/session`, which now depends on `@effect-agent/engine`.
`@effect-agent/platform-node` stays a Layer-assembly, host-gate, and crash-harness package.

This adds one deliberate edge to the dependency graph:
`core ← engine ← session ← storage adapters ← platform packages`. The engine remains inward of
session and never imports it; session drives the interpreter through its public seams
(`AgentRuntime.stream`).

### Leases as liveness, epochs as correctness (D5)

Ledger claims mint an ownership token and atomically bump the Conversation's producer epoch. The
epoch is the correctness authority: every canonical append is fenced, and a stale owner cannot
append a late model token, Tool result, or Settlement, no matter what its clock says.

Ownership rows additionally carry a renewable lease (default 30 seconds, adapter-configurable).
The lease is liveness only: its expiry makes an abandoned lane claimable without operator action.
Post-expiry renewal succeeds if nobody claimed in between; losing the race is the typed
`OwnershipLost`.

### SQLite storage version 2, exact-match, no migration (D7)

Ledger tables live in the same database file as the canonical journal so claims fence the same
epochs the journal checks. The stored version becomes **2** and the open-time check is
exact-or-zero: fresh files initialize, version-2 files open, and version-1 files fail with typed
reset guidance before any mutation — exactly like any newer version. No migration tooling is
built (D-015). The canonical record envelope stays at `schemaVersion: 1` because the Phase 4
payloads are additive tags on the existing union.

### Turn-granular canonical model records (D8)

Each committed Turn appends one `ModelResponseRecorded` carrying the Schema-encoded Prompt
messages the Turn added to the model-visible transcript, plus a digest, in one canonical batch
with that Turn's `ToolCallSettled` records. Replay rebuilds the exact resume Prompt from these
records without re-invoking providers. Recovery therefore resumes at the last committed Turn
boundary; work inside an uncommitted Turn is re-executed and duplicate provider cost is possible
and observable.

## Consequences

Positive:

- one platform-neutral coordinator; the Cloudflare phase implements adapters, not a second
  coordinator;
- crashes never wedge a lane (lease expiry restores claimability) while fencing stays exact
  (epochs restore correctness);
- incompatible development data fails loudly and is reset, never half-decoded;
- canonical volume stays proportional to Turns, and replay is deterministic from the log alone.

Negative:

- the published dependency direction now includes `engine ← session`; documentation and package
  graph checks had to be updated in step;
- an evicted-but-alive worker keeps computing until its next fenced write fails; leases do not
  stop it, only fencing does;
- Phase 3 SQLite files must be reset even though their journal rows would still decode;
- streaming deltas are not canonical in Phase 4, so mid-Turn crashes lose the partial response
  and repeat model cost (bounded by the Turn).

## Alternatives considered

### Put the durable coordinator in `@effect-agent/engine`

Rejected. The engine is inward of session; the coordinator needs canonical records, digests, the
ledger, and the store — all session concepts. Moving them inward would invert the dependency
rules that keep the interpreter platform-free.

### Put the durable coordinator in `@effect-agent/platform-node`

Rejected. The coordinator is platform-neutral protocol logic; Cloudflare (P6) would duplicate it.
Platform packages assemble Layers, host gates, and process-level harnesses only.

### Epoch-only ownership without leases

Rejected. Fencing alone is safe but not live: a crashed owner would block its lane until an
operator intervened. A renewable lease bounds unavailability without being trusted for
correctness.

### Leases as the correctness mechanism

Rejected. Wall-clock leases cannot fence a paused-but-alive process that resumes after expiry;
only an atomically checked epoch/token on every write can. Leases remain a liveness hint.

### Migrate version-1 SQLite files to version 2

Rejected for private development (D-015). Migration tooling would be built against disposable
data; a clear typed failure plus the documented reset command is cheaper and safer.

### Keep the existing “reject only newer versions” check

Rejected. A version-1 file would open and then fail obscurely (or decode incorrectly) once ledger
tables were missing. The check must be exact-or-zero so old files fail before mutation.

### Per-delta canonical model records

Rejected for Phase 4 on volume: canonical rows per streamed token/delta multiply storage and
digest work for no recovery benefit at Turn granularity. Durability §9 still permits canonical
deltas; revisit in Phase 5 alongside interrupted-response convergence.

### Reconstruct resume prompts from provider transcripts instead of canonical records

Rejected. Replay must never depend on re-querying an external system; canonical records are the
only recovery truth.

## Validation

- Both ledger adapters pass one conformance suite covering admission idempotency, FIFO-head
  claims, epoch fencing, lease expiry/renewal races, settlement idempotency and conflicts, abort,
  scans, and recovery snapshots.
- The recovery classifier is pure and tested row-by-row against the crash matrix; the coordinator
  replays the same seams under failpoint-induced kills.
- The SQLite adapter proves reopen durability, atomic claim/epoch bumps, typed write contention,
  and exact version rejection without mutation.
- See [Phase 4 evidence](../PHASE-4-EVIDENCE.md) for the file-and-test map.
