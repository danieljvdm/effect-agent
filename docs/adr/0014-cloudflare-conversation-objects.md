# ADR-0014: Cloudflare Conversation Objects — serialized owner, multiplexed alarm, routed ports

- Status: Accepted by default
- Status note (2026-08-13): adopted as the Phase 6 implementation default by the adopted P6 plan
  (decisions D-P6-1…D-P6-8) and implemented as such
  ([Phase 6 evidence](../PHASE-6-EVIDENCE.md)); owner review may still amend it.
- Date: 2026-08-13
- Decision owners: Project owner
- Related decisions: D-014, D-020, D-029, D-030, D-031, D-032
- Builds on: [ADR-0011](0011-durable-runtime-placement-and-leases.md),
  [ADR-0012](0012-durable-tool-uncertainty-and-steps.md),
  [ADR-0013](0013-durable-subagent-establishment.md)

## Context

Phase 6 implements deployment class `DC`: the P4/P5/S2 durable protocol on Cloudflare Workers
with one SQLite-backed Durable Object per Conversation. ADR-0011 D1 placed the coordinator in
`@effect-agent/session` behind exactly three ports (`ConversationStore`, `SubmissionLedger`,
`WakeScheduler`), so P6 is adapters and host assembly only — nothing in the coordinator branches
on the platform. The platform constrains every choice: an Object has one single-threaded
execution context, one alarm slot, storage that forbids explicit `BEGIN`/`SAVEPOINT`, a 2 MB
per-value bound, a 10 GB database cap, and eviction as its normal failure mode. Eight
shape-level decisions recur in any re-implementation and need a durable record.

## Decision

### D-P6-1 — Worker-loop placement: inside the Conversation's Object, as bounded passes

Durability §6 names the Conversation's Durable Object the serialized owner. The Object does
**not** run `runResolvedWorker`'s infinite loop (that would pin the Object in memory only to be
killed by eviction); each ingress event or alarm runs one bounded pass —
`runRecovery` before any claim, then one `processConversationResolved` drain over this Object's
lane (the S2 entry point that exists for exactly this shape). Leases and epochs keep the same
rows and SQL, but their primary `DC` role shifts to fencing late asynchronous work and
cross-incarnation ownership. Startup reconciliation stays local in the constructor gate
(`blockConcurrencyWhile`: migration/version check, config decode, defensive ensure-alarm) and is
**never** a recovery pass there — parent recovery can require child-Object reads and vice versa,
and two Objects blocked in constructor gates awaiting each other's RPC is a distributed
deadlock. Reconcile-before-claim in every pass preserves the "reconciliation strictly precedes
new work" gate without holding the event gate across network calls.

**Rejected — loop in the stateless Worker:** violates §6; stateless Workers have no durable
execution context and cannot be woken by alarms; leases would become the primary mechanism
instead of the Object's own serialization.

**Rejected — Cloudflare Queues as the scheduler:** a new platform dependency for work alarms
already cover; alarms are transactional with the Object's storage, queues are not.

### D-P6-2 — Alarm semantics: one multiplexed slot, an idempotent maintenance pass, pre-armed invariant

A Durable Object has one alarm. `DurableAlarmService` wraps it; `ConversationMaintenance`
multiplexes every cadence the Node host got from fibers (wake scan, lease expiry,
settlement/unknown re-checks, retry backoff) into one idempotent pass:
`pre-arm → runRecovery → processConversationResolved → re-arm-or-clear`. Re-arm policy:
nonterminal work re-arms at `now + min(backoff-with-jitter, wakeScanInterval)` (backoff resets on
progress); all-settled deletes the alarm, with a recount closing the cancel/admission
interleaving window. The **alarm invariant** — committed nonterminal work implies a committed
alarm — is established by pre-arming before the first durable mutation of every mutating entry
point and pass: alarms live in the same storage as the rows, so a committed admission can never
be observed without the alarm that will finish it. This is precisely what makes "eviction at
every failpoint recovers without an incoming request" true, and idempotency under at-least-once
delivery is by construction — every pass step is a durability-protocol step that already
tolerates re-execution. `setAlarm(now)` doubles as the local wake (deferred while a pass runs so
workerd never cancels the running handler).

**Rejected — one alarm purpose per table row:** needless machinery given an idempotent pass; the
single slot cannot represent it anyway.

### D-P6-3 — Cross-Object transport: Durable Object JS RPC carrying Schema envelopes

The Conversation Object exposes `portCall(encoded)` plus `wake()` and the client entry points;
payloads are Schema-encoded request/response/failure envelopes (`port-protocol.ts`), and errors
re-decode to the same tagged types on the caller side. The protocol module is
transport-agnostic; fetch-with-JSON is the documented fallback if RPC proves awkward. A
transport fault on `resolveAdmission` maps to `AdmissionIndeterminate` — the S2 tri-state's
first genuinely unreachable authority (SUB-031).

**Rejected — fetch/JSON as primary:** RPC is the platform-native call path with typed stubs;
the envelopes keep the wire contract schema-first either way.

### D-P6-4 — Storage schema: mirror the v4 tables; independent adapter, no shared SQL core

The `DC` schema mirrors the Node v4 tables byte-for-byte (same names, same columns) in one
migration `1_current_cloudflare_conversation_object`, plus two `DC`-specific tables:
`effect_agent_meta` (`storage_version`, replacing `PRAGMA user_version`, exact-or-fresh with
typed reset guidance) and `effect_agent_child_settlements` (the durable cross-store
child-settlement notification marker the ledger `suspend` contract mandates, written in the
parent's Object by `recordChildSettled`). Transactions delegate to the sqlite-do client's
storage-backed `withTransaction`; the `BEGIN IMMEDIATE`/`busy_timeout`/WAL machinery serialized
multiple OS processes on one file and has no analogue with one writer per Object — it is absent,
not faked. The adapter is an independent implementation mirroring the SQLite adapters
structurally; no shared SQL-core package was extracted mid-phase.

**Rejected — extracting a shared SQL core:** would churn landed P4/P5 code and break
parallel-work disjointness; the shared conformance suites are the anti-drift guard. Revisit in
P7 if a third SQL adapter appears.

### D-P6-5 — Routing identity: adapter-minted routable Submission identities

Several route-capable operations are submissionId-addressed with no conversation in the request
(`lookup`, `requestAbort`, `recordChildSettled`). The ledger port mints identities at admission,
so the `DC` adapter mints `{uuidv7}:{conversationId}` and parses **its own** format for routing
(split at the first `:`); identities stay opaque to every other component — the same trick the
Node adapter uses with its `submission-{uuid}` prefix. Identities beyond the routable bound fail
typed before routing.

**Rejected — a global directory Object:** a bottleneck, an extra hop, and a new
correctness-critical store, violating "Durable Object storage is the only correctness-critical
store for that Conversation".

**Rejected — an eventually-consistent KV mirror:** deployment §12 forbids treating projection
absence as proof; admission resolution must be authoritative or `Indeterminate`.

**Rejected — widening the port schemas to carry conversation identity:** touches landed P4/P5
surface for a platform detail.

### D-P6-6 — Equivalence evidence: one cross-platform normal form, one committed golden

`normalizeCrossPlatformTravelPlannerEvidence` extends the P4 normalization: drop
`RepairAnnotated` audit records (recovery evidence is legally present in a recovered run and
absent from a control; on `DC` even a clean run reconciles before claiming) and renumber, then
scrub conversation/deployment/producer identities, `createdAt` timestamps, and content digests.
Both the `DN` suite (`packages/testing/test/travel-planner-phase6.test.ts`) and the `DC` suite
(`packages/platform-cloudflare/test/travel-planner-dc.test.ts`) assert byte-equality against the
one committed `phase6TravelPlannerGoldenEvidence`, so `DN` ≡ `DC` transitively — the durability
§5 sense of equivalence (same observable canonical ordering), without assembling both stacks in
one process.

**Rejected — running both stacks in one test process:** workerd and the Node SQLite runtime do
not share a process; a committed golden also pins the expected history against silent drift.

### D-P6-7 — Test harness: pool-workers primary, Miniflare restart lane mandatory, direct `vitest run`

`@cloudflare/vitest-pool-workers@0.21.3` executes the suites inside workerd against real
SQLite-backed Objects (`runInDurableObject`, `runDurableObjectAlarm` as the at-least-once
lever). The WP0 probe resolved the runner question to Fallback A: `vp test` boots workerd but
executes test files in-process, so both Cloudflare packages run `"test": "vitest run"` with the
catalog-pinned Vitest and package-local `vite.config.ts` files using the `cloudflareTest`
plugin; test files import from `vite-plus/test`. The programmatic Miniflare lane
(`durableObjectsPersist` + esbuild-bundled worker entry) is mandatory regardless, for the
full-runtime-restart form of the eviction gate; the WP0 probe proved persisted alarms re-deliver
after reopen without any request, so the restart lane needs no poke fallback. Pool 0.21.x has no
per-test isolated storage: suites mint unique Conversation names per case.

**Rejected — forcing everything through `vp test`:** the probe showed describe-level failures
under the bundled module runner; a wrapper cannot be the correctness boundary for an in-workerd
suite.

### D-P6-8 — R2: defer

Deployment §3.1 already defers the `AttachmentStore` port until a real attachment requirement,
and no scenario through S2 approaches the 2 MB value bound. Delivered instead: the enforced
typed value bound (`DoValueBoundExceeded` before any write) naming oversized values as the
designed overflow seam. "R2 artifact service if needed" is answered "not needed for the P6 exit
gates".

**Rejected — shipping an R2 service now:** it would invent the port P6 was told not to create,
without a requirement to shape it.

## Consequences

Positive:

- the coordinator is provably platform-neutral: `DC` shipped as adapters and assembly with zero
  coordinator changes, zero new canonical payloads, and the same conformance suites;
- eviction recovery is request-free by construction — the pre-armed alarm invariant makes the
  platform's normal failure mode the tested path;
- cross-Object delegation reuses the S2 protocol exactly; `Indeterminate` admission stopped
  being a degenerate branch and earned its contract;
- the crash matrix kept its Node row names, so `DN` and `DC` evidence stay comparable
  row-for-row;
- in-Object memory is demonstrably a cache: chaos-abort between every host operation preserves
  the normalized evidence.

Negative:

- alarm-paced recovery is coarser than the Node host's fiber cadences; a stuck lane waits out
  backoff/scan intervals rather than being immediately retried;
- the stored-value bound is ~8× tighter than Node's (platform limit), a documented behavioral
  difference in threshold;
- the route-capable subset is closed — genuinely new cross-Conversation operations require a
  protocol change, deliberately;
- two SQL adapters now mirror one schema by convention, guarded only by the shared conformance
  suites until a P7 extraction decision;
- the Cloudflare suites run outside `vp test` (direct `vitest run`), a permanent toolchain
  asymmetry recorded in TOOLCHAIN.md;
- evidence executes on workerd/Miniflare, not the hosted platform; production observability and
  live soak remain P7.

## Validation

- WP0 probe suites: `packages/storage-cloudflare/test/do-sql-probe.test.ts`,
  `effect-vitest-probe.test.ts`, `testclock-manual-probe.test.ts`,
  `packages/platform-cloudflare/test/restart/miniflare-restart-probe.test.ts`.
- Shared conformance in-workerd: `packages/storage-cloudflare/test/do-ledger.test.ts` (29
  ledger cases), `do-storage.test.ts` (8 store cases), `do-ledger-failpoints.test.ts`.
- Routed subset honesty and SUB-031: `packages/storage-cloudflare/test/routing.test.ts`.
- Eviction matrix, alarm idempotency/invariant, admission limits, chaos, reconciliation
  ordering: `packages/platform-cloudflare/test/eviction.test.ts`, `alarm.test.ts`,
  `limits.test.ts`, `chaos.test.ts`.
- Cross-Object subagent matrix: `packages/platform-cloudflare/test/subagents-cross-do.test.ts`.
- Travel Planner `DC` slice and DN/DC equivalence:
  `packages/platform-cloudflare/test/travel-planner-dc.test.ts`,
  `packages/testing/test/travel-planner-phase6.test.ts`.
- Restart lane: `packages/platform-cloudflare/test/restart/travel-planner-restart.test.ts`.
- Dependency-direction guard: `packages/testing/test/toolchain.test.ts`.
- See the [Phase 6 evidence](../PHASE-6-EVIDENCE.md) for the full gate-by-gate map.
