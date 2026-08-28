# Persistence specification

Status: Draft

Persistence provides the mechanism used by sessions, canonical conversation
history, the Submission Ledger, checkpoints, durable steps, approvals, and
artifacts. The durable protocol is specified in
[durability.md](./durability.md); this document specifies storage contracts and
adapter behavior.

## 1. Design principles

- Persistence services are framework-owned Effect services. Model content uses Effect AI values at
  runtime and explicit framework record Schemas at rest.
- The canonical log and Submission Ledger are distinct logical records.
- Atomicity boundaries are explicit in port methods.
- Adapters expose their consistency and transaction capabilities.
- Schema versions are stored with every durable record family.
- Writes are idempotent and fenced where a producer is involved.
- Projections are disposable; canonical records are not.
- Large blobs are addressed by digest and stored outside hot transactional rows.

## 2. Logical stores

### 2.1 Conversation Log

Append-only canonical batches for each conversation. It is the source for replay,
context reconstruction, audit, and projections.

### 2.2 Submission Ledger

Tracks admission, scheduling, leases, attempts, commands, recovery classification,
and settlement. It is the source for operational obligations.

### 2.3 Session Store

Stores mutable, revisioned metadata and references to conversations.

### 2.4 Checkpoint Store

Stores derived interpreter snapshots and compaction artifacts keyed by canonical log
position and digest.

### 2.5 Step Store

Stores prepared and settled durable-step records under stable step identity.

### 2.6 Artifact Store

Stores large immutable content such as attachments, sandbox artifacts, and optional
encrypted raw provider payloads. Records use content digests and metadata; the
transactional stores contain references.

### 2.7 Projection Store

Stores rebuildable views for UI, search, metrics, and administration.

### 2.8 Schedule Store

Stores one authoritative, versioned Schedule record per owner and Schedule ID. Its atomic
transitions compare configuration revision and cursor, and conditionally create, retry, clear, or
refuse one immutable pending delivery. The [scheduling specification](./scheduling.md) defines the
record and transition rules. Schedule records are operational state, not canonical Conversation
records or a replacement Submission Ledger.

## 3. Port shape

The actual TypeScript API may split these services further, but it must preserve the
semantic operations below.

```ts
interface SubmissionLedger {
  readonly admit: (
    request: AdmissionRequest,
  ) => Effect.Effect<AdmissionResult, AdmissionConflict | LedgerError>;

  readonly markReady: (request: MarkReadyRequest) => Effect.Effect<void, LedgerError>;

  readonly claim: (request: ClaimRequest) => Effect.Effect<Option.Option<Claim>, LedgerError>;

  readonly renewOwnership: (
    request: RenewOwnershipRequest,
  ) => Effect.Effect<OwnershipRenewal, OwnershipLost | LedgerError>;

  readonly reserveSettlement: (
    request: SettlementReservation,
  ) => Effect.Effect<ReservedSettlement, SettlementConflict | OwnershipLost | LedgerError>;

  readonly finalizeSettlement: (
    request: SettlementFinalization,
  ) => Effect.Effect<Settlement, SettlementConflict | LedgerError>;

  readonly loadRecoverySnapshot: (
    request: RecoverySnapshotRequest,
  ) => Effect.Effect<RecoverySnapshot, LedgerError>;
}

interface ConversationStore {
  readonly materialize: (
    request: ConversationMaterialization,
  ) => Effect.Effect<void, ConversationStoreError>;

  readonly append: (
    request: FencedAppendRequest,
  ) => Effect.Effect<AppendResult, FenceRejected | AppendConflict | ConversationStoreError>;

  readonly read: (
    request: ConversationRead,
  ) => Stream.Stream<CanonicalRecord, ConversationStoreError>;

  readonly inspectTail: (
    request: ConversationTailRequest,
  ) => Effect.Effect<ConversationTail, ConversationStoreError>;
}
```

`inspectTail` returns the committed tail sequence, tail digest, and current producer epoch in
one cheap read inside the same consistency domain as `append`. A resuming producer composes
its next `FencedAppendRequest` from this value instead of exporting the whole log.

The shipped `@effect-agent/session` `SubmissionLedger` port implements exactly these semantic
operations and also exposes strongly consistent `lookup`, graceful `releaseOwnership`,
the idempotent `markInputApplied` canonical-input marker, idempotent `requestAbort`, the ordered
`scanNonterminal` stream, and a `capabilities` durability declaration.

An unknown head is claimable only when its durable abort intent exists, checked atomically with
the ordinary ownership claim. This exception preserves FIFO, lease checks, producer fencing, and
uncertainty evidence; it enables cleanup and aborted settlement, never uncertain Tool replay.
Memory, SQLite, and Durable Object SQLite adapters share this contract.

Each method has an explicit atomic and idempotent contract. Admission and settlement intentionally
span the two stores through recoverable states:

- admit ledger row → materialize Conversation → mark ready;
- reserve settlement → append exact canonical record → finalize ledger.

Canonical input and settlement records win over cached ledger markers during repair.

## 4. Record envelopes

Every record has:

```ts
interface RecordEnvelope<A> {
  readonly recordId: RecordId;
  readonly family: RecordFamily;
  readonly schemaVersion: number;
  readonly createdAt: Instant;
  readonly deploymentId: DeploymentId;
  readonly payload: A;
}
```

Producer-written records also include producer identity and epoch. User or
tenant identity is included where authorization and retention require it.

Timestamps support audit and operations but do not define conversation ordering.

## 5. Event families

The canonical log supports, at minimum:

- conversation created/closed;
- user, steering, and follow-up input;
- immutable Run start with its original duration allowance;
- model request metadata and per-call normalized provider/model/tier/token/cost usage;
- model text/reasoning deltas, completion, signature/redaction metadata, or structured item;
- tool call requested, prepared, approved/rejected, settled, unknown;
- compaction created/selected;
- subagent requested/started/joined and child Parent Link;
- run warning/failure and atomic terminal-Tool completion marker;
- abort requested;
- terminal outcome with aggregate Run usage;
- repair annotations.

Partial Tool-argument deltas, queue depth changes, heartbeats, and debug spans are
not canonical events.

`RunStarted` stores the Run ID and finite positive `maxDurationMillis`; its envelope `createdAt`
is the start timestamp. The deterministic `run-start:{runId}` record and batch identities admit
one start per logical Run. Input remains a separate fact because binding-free recovery may apply
it before execution, and joined input can later become a standalone Run. Conflicting starts,
changed duration policies, and execution history without start evidence fail recovery rather than
resetting the clock. No migration reconstructs missing timing from current code.

## 6. Optimistic and idempotent writes

Each append declares:

- expected conversation tail sequence and digest;
- unique batch ID;
- producer epoch;
- event IDs;
- intended sequence count.

On retry:

- the same batch ID and digest returns the original append result;
- the same batch ID with different content is a conflict (`batch-digest`);
- a reused canonical record ID, inside one batch or across batches, is a conflict
  (`record-identity`);
- a stale expected tail is a conflict (`tail`), and the conflict carries the actual
  committed tail sequence and digest as a diagnostic resume hint;
- a stale producer epoch is fenced;
- a partial batch is impossible.

## 7. Reads

The store supports:

- bounded forward reads by conversation sequence;
- tail reads (`inspectTail`: tail sequence, tail digest, and producer epoch, strongly
  consistent with `append`);
- batch lookup by ID;
- submission lookup by ID and idempotency key;
- nonterminal work scans;
- settlement lookup;
- point-in-time projection rebuild;
- checkpoint lookup at or before a sequence.

Reads that drive recovery must be strongly consistent with prior successful writes.
Eventually consistent replicas may serve UI/history views if their staleness is
visible.

Recovery-pass history reads first capture the strongly consistent tail and then request the exact
bounded prefix through that sequence: one tail inspection plus `ceil(T / 1024)` page requests.
Pagination is fail-closed: a short page or non-contiguous sequence is corruption, not
end-of-history. The runtime shares that verified prefix across the Conversation's nonterminal
recovery classifications, retains an `O(T)` working set for only one Conversation prefix at a
time, and discards it after the group; concurrent append-only suffixes remain canonical and are
read at an exact mutation seam or by the next pass.

## 8. Checkpoints

A checkpoint contains a versioned encoded interpreter snapshot plus:

- conversation and submission identity;
- canonical sequence covered;
- canonical tail digest;
- engine version;
- agent definition digest;
- active model/tool configuration digests;
- loaded skill versions;
- creation timestamp.

A checkpoint is a performance optimization. If its schema is unsupported, digest
does not match, or referenced configuration is unavailable, the engine rebuilds
from canonical events or returns a typed compatibility failure. It never silently
uses a mismatched checkpoint.

## 9. Storage adapters

### 9.1 In-memory

Purpose: unit tests and ephemeral development.

- deterministic;
- supports fault injection;
- no process durability claim;
- implements the same conflict and fencing rules.

### 9.2 SQLite

Purpose: first operational local runtime and single-node durable host.

- WAL mode;
- transactions for admission, append, claim, and terminalization;
- write transactions begin with `BEGIN IMMEDIATE`: producer epochs exist precisely because
  two owners transiently coexist, and a deferred `BEGIN` would let a read-then-write
  transaction fail with `SQLITE_BUSY_SNAPSHOT`, which the busy timeout never retries;
- single-writer behavior handled with bounded busy retry (configurable `busyTimeout`); a
  write-lock timeout is classified as the typed, retryable `SqliteWriteContention` and
  surfaces on the port as a `ConversationStoreError` carrying it as the cause;
- monotonically increasing epochs allocated transactionally;
- blob payload thresholds with artifact spillover;
- backup and integrity-check guidance: opening the store verifies the storage version and
  required tables; the full payload/digest-chain integrity scan is an explicit opt-in
  (`verifyOnOpen`, default off) because per-operation Schema decoding and the digest chain
  already fail clearly on corrupt rows;
- no multi-host scheduler claim unless deployment constraints prove safe.

### 9.3 Cloudflare

Purpose: first-class edge deployment alongside Node/SQLite.

- one SQLite-backed Durable Object owns each Conversation's queue, canonical log, and local
  operational ledger;
- Durable Object identity provides per-Conversation routing and serialized coordination;
- Durable Object storage transactions implement local atomic methods;
- important state is always written to storage rather than relying on in-memory object state;
- a durable dirty/processed maintenance generation and pre-armed alarm wake committed autonomous
  work without a new client request, while stable externally-driven waits quiesce until their
  next mutation;
- alarm handlers are idempotent because alarm delivery is at least once;
- R2 may hold large immutable attachments/artifacts;
- cross-Conversation indexes are optional projections, not recovery truth.

All Cloudflare bindings are wrapped in Effect services and provided through Layers. Core Agent and
engine packages do not import Cloudflare platform types.

## 10. Serialization

Effect Schema is the source of truth for domain codecs.

Rules:

- use tagged, versioned envelopes;
- avoid serialized class prototypes or closures;
- encode dates, durations, big integers, binary, and redacted values explicitly;
- preserve unknown future fields where forward compatibility requires it;
- cap nesting, array length, and byte size before decode;
- validate data read from storage, even if the application originally wrote it;
- store content digests over canonical encoding.

`SubmissionSettled` is family-discriminated at this boundary. A `failed` record must carry exactly
the bounded `{ errorTag, message }` diagnostic defined by durability §12; an `aborted` record must
carry no result, while a joined `completed` record may have no independent result. Current-version
legacy failed records that omit the diagnostic or attach additional cause/stack/application fields
are corrupt private-development data: adapters fail clearly while reading or finalizing them and
do not synthesize a replacement. Ledger finalization projects the failed diagnostic from the exact
reserved record, so process-style reopen and idempotent finalization replay return the same value.

Canonical encoding must be deterministic and locale-independent. Canonical JSON
serialization orders object keys by UTF-16 code units (RFC 8785 style). Locale-aware
collation is forbidden: it varies across hosts, and it treats canonically equivalent
but distinct key sequences as equal, leaking insertion order through a stable sort.
The project will choose and document the remaining details of the encoding profile
before the first persistent release.

## 11. Stored-version policy during private development

No data migration or backward-compatibility support is implemented or promised.

- records still carry versions;
- unsupported versions fail before mutation;
- local SQLite development data may be deleted and recreated;
- Cloudflare development namespaces may be replaced after breaking changes;
- fixtures cover only the current version unless needed for a specific regression.

Migration design is reopened before external users rely on stored data.

## 12. Retention and deletion

During private development, canonical records are retained indefinitely. Physical
deletion, legal retention, tenant export, attachment lifecycle, and backup erasure
are deferred until an actual internal requirement appears or external release work
begins.

## 13. Encryption and secrets

- transport encryption is required for remote adapters;
- production persistent stores require encryption at rest;
- application secrets are referenced by handle and never stored in canonical
  events;
- sensitive event fields support envelope encryption and key rotation;
- digest inputs must not make low-entropy secrets guessable;
- backup encryption and restore authorization are part of the adapter runbook.

## 14. Projection processing

Projection consumers checkpoint their own canonical sequence and are idempotent.
They may lag, fail, and rebuild without blocking the engine's canonical append,
unless a specific projection is declared part of admission or settlement.

Notifications are disposable liveness hints emitted only after the relevant durable mutation. A
dropped notification cannot cause lost work because schedulers also scan the ledger. An
interactive progress wait does not scan periodically: it installs a conversation-specific scoped
registration before a strongly consistent one-record canonical read, returns immediately when a
record after the caller's sequence already exists, and otherwise parks on that registration. A
wake is allowed to be a false positive and only asks the caller to reread canonical records.
Eviction discards registrations; reconnect/retry reconstructs the registration and repeats the
authoritative read.

## 15. Requirements

- **STORE-001**: The Conversation Log and Submission Ledger are distinct logical
  stores even if one database implements both.
- **STORE-002**: Each admission and settlement transition is atomic and idempotent; cross-store
  progress is recoverable rather than falsely described as one transaction.
- **STORE-003**: Recovery reads are strongly consistent.
- **STORE-004**: Batch append is atomic, idempotent, conflict-checked, and fenced.
- **STORE-005**: Every durable record carries a schema version.
- **STORE-006**: Effect Schema validates both writes and reads, including the exact bounded
  diagnostic required by every failed canonical settlement; malformed current-version legacy
  records fail clearly rather than being repaired from non-canonical state.
- **STORE-007**: Checkpoints are disposable and bound to a canonical digest.
- **STORE-008**: Projections are rebuildable from canonical records.
- **STORE-009**: Large immutable payloads use digest-addressed artifact storage.
- **STORE-010**: Every durable adapter passes the shared conformance and
  crash-consistency suite.
- **STORE-011**: Unsupported stored versions fail clearly; private development provides no
  migration promise.
- **STORE-012**: Canonical records are retained indefinitely during private development.
- **STORE-013**: Node/SQLite and Cloudflare Durable Object adapters implement the same Effect
  service contracts and conformance suite.
- **STORE-014**: Public progress waits close subscribe/check and check/park races without making
  in-memory notifications authoritative or adding a periodic polling/alarm loop.
- **STORE-015**: One recovery pass reads each Conversation's captured canonical prefix in bounded
  pages once, rejects incomplete pagination, and retains no canonical-history cache across
  Conversation groups or passes.
