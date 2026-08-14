# Phase 4 durable Node/SQLite runtime evidence

Status: **Implemented**

Phase 4 makes accepted work durable on the Node/SQLite assembly. The deployment-class label this
phase earns is `DN`, stated precisely: **durable Submissions with turn-boundary recovery, valid
for safe-to-repeat toolkits**. Recovery may re-invoke the model from the last committed Turn
boundary, so duplicate provider cost is possible and observable (D6). Supplier booking replay is
**not** claimed; prepared/settled ordinary Tool records, unknown outcomes, and durable Steps are
Phase 5 scope.

## Delivered package surface

- `@effect-agent/core` adds the `ReceiptId` and `SettlementId` branded identifiers
  (`packages/core/src/identifiers.ts`).
- `@effect-agent/session` owns the durable protocol:
  - the `SubmissionLedger` port with admission, readiness, strongly consistent lookup,
    FIFO-head claim with atomic producer-epoch fencing, lease renewal/release, canonical-input
    markers, settlement reservation/finalization, idempotent abort intent, nonterminal scans, and
    recovery snapshots (`packages/session/src/ledger.ts`);
  - the `WakeScheduler` liveness-hint port whose delivery may be dropped, coalesced, or
    duplicated without affecting correctness (`packages/session/src/wake.ts`);
  - the `DurableAgentRuntime` coordinator: `submit`, `awaitSettlement`, `observe`, `abort`,
    `processConversation`, `runWorker`, and `runRecovery`
    (`packages/session/src/durable-runtime.ts`);
  - the pure recovery classifier `classifyRecovery` and its `RecoveryDecision` union
    (`packages/session/src/recovery.ts`);
  - the run journal that rebuilds the model-visible Prompt from canonical records and folds each
    Turn into one deterministic canonical batch (`packages/session/src/run-journal.ts`);
  - coordinator failpoints around every durable seam (`packages/session/src/durable-failpoint.ts`);
  - the adapter-neutral `SubmissionLedger` conformance suite
    (`packages/session/src/ledger-conformance.ts`);
  - new canonical payloads `AbortRequested`, `SubmissionSettled`, and `ModelResponseRecorded`
    on the unchanged version-1 envelope, plus `settlements`/`abortRequests` projections
    (`packages/session/src/records.ts`, `packages/session/src/reducer.ts`).
- `@effect-agent/storage-memory` provides the deterministic in-memory `SubmissionLedger`
  reference adapter (`packages/storage-memory/src/memory-ledger.ts`). The Phase 3 non-durable
  `SubmissionStore` stub is deleted rather than kept beside the real ledger.
- `@effect-agent/storage-sqlite` provides the durable SQLite `SubmissionLedger` adapter in the
  same database file as the canonical journal, with `BEGIN IMMEDIATE` write transactions, atomic
  claim/epoch bumps, typed write contention, and before/after failpoints on every ledger mutation
  (`packages/storage-sqlite/src/sqlite-ledger.ts`, `packages/storage-sqlite/src/migrations.ts`).
- `@effect-agent/platform-node` is the new `DN` host assembly: one shared SQLite client behind
  the Conversation Store and Submission Ledger, validated configuration, the in-process
  wake scheduler with a ledger-scan fallback, graceful ownership drain, and the
  `NodeDurableHost` startup/shutdown gates (`packages/platform-node/src/layers.ts`,
  `packages/platform-node/src/wake-scheduler.ts`, `packages/platform-node/src/host.ts`).
- `@effect-agent/testing` extends the cumulative Travel Planner with the `DN` durable planning
  profile, whose fixture pins `deploymentClass: "DN"` and `supplierBookingReplaySafe: false`
  (`packages/testing/src/fixtures/travel-planner/phase4.ts`,
  `packages/testing/test/travel-planner-phase4.test.ts`).
- The real process-kill crash harness spawns child worker processes over one shared SQLite file,
  arms failpoints to hard-kill with `process.exit(137)` (no finalizers, no drain), restarts, and
  asserts each crash-matrix row's durable outcome (`packages/platform-node/test/crash/crash.test.ts`,
  `packages/platform-node/test/crash/worker-entry.ts`, `packages/platform-node/test/crash/fixtures.ts`).

## Executable exit-gate evidence

| Phase 4 claim                                                                            | Deterministic evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Receipt only after durable admission and readiness                                       | `packages/session/test/durable-runtime.test.ts` — “submit returns a durable Receipt once admission and readiness commit”, “a failpoint-interrupted submit resumes to the same Receipt”                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Conflicting idempotency retries fail                                                     | durable-runtime “same-key resubmission returns the original Receipt; different content conflicts”; ledger conformance “replays identical admissions and rejects conflicting input digests”                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Crash after admission completes readiness                                                | recovery classifier cases “kill submit:after-admit …” and “kill submit:after-materialize …” (`packages/session/test/recovery-classifier.test.ts`); platform-node “startup reconciliation settles an orphaned reserved settlement before admission”                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Input applied exactly once before model consumption                                      | durable-runtime “recovery re-applies input after a claim-boundary kill (exactly one record)”, “recovery repairs a lost input marker from canonical history (DUR-015)”                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| One ordered head per Conversation; FIFO preserved                                        | durable-runtime “keeps one Conversation lane FIFO by admitted queue sequence”; ledger conformance “claims only the lowest unsettled head and fences the lane epoch forward”, “allocates distinct FIFO queue sequences under concurrent admission and claims in order”                                                                                                                                                                                                                                                                                                                                                                                                   |
| Stale Attempts are fenced                                                                | durable-runtime “fences a superseded Attempt out of canonical history”; SQLite “bumps the conversation producer epoch atomically with a claim”; conformance “reclaims an expired lease with a higher epoch and fences the stale token”                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Exactly one canonical Settlement per accepted Submission                                 | durable-runtime “runs accepted work to settlement with an ordered canonical log”; conformance “reserves and finalizes exactly one settlement with idempotent replays”, “rejects conflicting settlement reservations and finalizations”                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Reserved settlement appends byte-identically                                             | durable-runtime “recovery appends a reserved-but-unappended settlement exactly as reserved”                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Canonical settlement repairs ledger finalization                                         | durable-runtime “recovery finalizes the ledger from history without rewriting the record”; classifier “canonical settlement without any reservation still finalizes from history (DUR-015)”                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Turn-boundary resume after mid-run loss                                                  | durable-runtime “a new Attempt resumes from the last committed Turn boundary” and “folds Turn seams into batches and replays them to the same prompt”                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Durable abort for ready, running, and settled work                                       | durable-runtime “aborts ready, unclaimed work through the recovery pass without an Attempt”, “aborts a running Submission canonically before interrupting its Run fiber”; conformance “records abort intent idempotently and refuses to abort settled work”                                                                                                                                                                                                                                                                                                                                                                                                             |
| Failpoints before/after every durable ledger mutation                                    | `packages/storage-sqlite/test/sqlite-ledger.test.ts` — “leaves a recovery-classifiable state at every ledger failpoint”                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Ledger survives process-style reopen                                                     | sqlite-ledger “persists admissions durably across process-style reopen”; “classifies cross-connection write contention as retryable typed contention”                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Host startup refuses incompatible storage                                                | `packages/platform-node/test/layers.test.ts` — “startup refuses an incompatible v1 storage file without mutating it”; sqlite-ledger “rejects a v1 file exactly with reset guidance and still rejects newer versions”                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Recovery runs before admission opens; shutdown drains                                    | platform-node “startup reconciliation settles an orphaned reserved settlement before admission”, “shutdown closes admission and releases ownership for the next host”                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Dropped wake notifications cannot lose work                                              | platform-node “wake-scan fallback claims ready work without any notify”                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Both ledger adapters obey one contract                                                   | shared suite in `packages/session/src/ledger-conformance.ts`, run by `packages/storage-memory/test/memory-ledger.test.ts` and `packages/storage-sqlite/test/sqlite-ledger.test.ts`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Recovery decisions are pure and auditable (DUR-013)                                      | `packages/session/test/recovery-classifier.test.ts` (full crash matrix as pure classification); `runRecovery` appends a deterministic `RepairAnnotated` audit record per executed repair                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `DN` Travel Planner: Receipt, FIFO trip lane, restart equivalence, abort, one Settlement | `packages/testing/test/travel-planner-phase4.test.ts` — “runs one durable planning Submission to Settlement with canonical input, per-Turn, and settlement records on SQLite …”, “restart-equivalence: a worker killed at terminalize:after-reserve recovers on the reopened SQLite file to the exact reserved settlement and an uninterrupted run's projection”, “serializes two Submissions on one trip lane FIFO: the second is not claimable until the first settles”, “durable abort of a ready Submission settles aborted through recovery without running an Attempt”, “pins the DN durability claim and explicitly does not claim replay-safe supplier booking” |

The recovery classifier suite covers the crash matrix row-by-row as pure decisions over persisted
snapshots, and the coordinator suite replays the same seams end-to-end with failpoint-induced
kills against the in-memory adapters. The SQLite adapter proves the same contract plus real
reopen, contention, and epoch atomicity.

The process-kill harness (`packages/platform-node/test/crash/crash.test.ts`) then proves the same
matrix under actual process loss — real child processes hard-killed at armed failpoints over one
SQLite file: “kill at submit:after-admit: recovery completes materialization; the same key
resumes”, “kill at ledger:mark-ready:after: the same key returns the original Receipt”, “kill at
claim:after-claim: recovery re-applies the input exactly once”, “kill at
input:after-canonical-append: the marker is repaired and FIFO holds for the queued Submission”,
“SIGKILL mid model Turn: a new Attempt resumes from the committed Turn boundary and the client
reattaches”, “a stale Attempt in a live process is fenced out of canonical history”, “kill before
settlement reservation: the outcome is recomputed and settles exactly once”, “kill at
terminalize:after-reserve: recovery appends the EXACT reserved record”, “kill at
terminalize:after-canonical-append: the ledger is finalized from history, the record never
rewritten”, “kill at abort:after-intent: ready work settles aborted without an Attempt”, and
“abort of an active Run in another process: canonical AbortRequested precedes interruption”.

## Stored-version policy

The SQLite storage version is now **2** (ledger tables added beside the journal). The check is
exact-or-zero: a fresh file initializes to version 2; a version-1 file — and any newer version —
fails with typed reset guidance before any mutation (D7). No migration tooling exists or is
promised; the canonical record envelope stays at `schemaVersion: 1` because the new payloads are
additive tags. Phase 3 checkpoints whose persisted projection lacks the new fields decode-fail and
are rebuilt from canonical records — checkpoints remain disposable derivatives.

The Phase 3 reset command remains the documented way to discard an incompatible
private-development database.

## Non-claims

- No exactly-once external side effects. Model inference and ordinary Tool handlers may execute
  more than once across Attempts; recovery resumes at the last committed Turn boundary and may
  re-invoke the model with observable duplicate cost (D6).
- The `DN` claim is valid for safe-to-repeat toolkits only. Consequential external mutations
  (supplier booking, payment) get prepared/settled records, unknown outcomes, reconciliation, and
  durable Steps in Phase 5; the `MarkUnknown` recovery branch exists but no Phase 4 flow triggers
  it (DUR-009 remains untriggered by construction).
- An unresolved ordinary Tool call is never automatically replayed.
- Leases are liveness only. Ownership correctness comes from producer-epoch fencing; an expired
  lease merely makes the lane claimable again (D5).
- One active scheduler node. The SQLite assembly makes no multi-host scheduling claim.
- Durable attachments are not part of admission; an `AttachmentStore` port is deferred until a
  real attachment requirement appears.
