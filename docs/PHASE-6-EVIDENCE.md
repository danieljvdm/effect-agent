# Phase 6 Cloudflare Durable Object runtime evidence

Status: **Implemented**

Phase 6 delivers deployment class **`DC`**: the unchanged `DurableAgentRuntime` coordinator
running inside one SQLite-backed Cloudflare Durable Object per Conversation, with the Object as
the serialized owner (durability §6), a single multiplexed storage alarm as the liveness engine,
and cross-Object Subagent delegation over a closed, Schema-typed routed port subset. Stated
precisely: **the same ports, the same conformance suites, the same crash-matrix location names,
and byte-equal cross-platform normalized canonical Travel Planner evidence as the Node/SQLite
`DN` assembly — under eviction and alarm redelivery instead of process kill and restart.** No
code path claims exactly-once external side effects (DUR-003), eviction recovery converges
without any incoming client request, and the evidence harness is workerd/Miniflare — no claim is
made about the hosted Cloudflare production service.

## Stated assumptions — the WP0 probe outcomes

The phase rests on four toolchain facts, each recorded as a committed, executable probe suite
rather than prose:

1. **The pinned Effect version drives Durable Object SQLite.**
   `@effect/sql-sqlite-do@4.0.0-beta.102` (`SqliteClient.layer({ storage })`) provides the
   generic `SqlClient` inside a SQLite-backed Durable Object under
   `@cloudflare/vitest-pool-workers@0.21.3`/workerd: DDL, typed DML, and storage-backed
   `withTransaction` commit and roll back with typed failures; nested transactions are
   unsupported by that client (`packages/storage-cloudflare/test/do-sql-probe.test.ts` —
   “executes DDL/DML and storage-backed withTransaction with commit and rollback”).
2. **`vp test` cannot drive the workers pool** (decision D-P6-7, resolved to its Fallback A):
   pool-workers 0.21.x replaced `defineWorkersConfig` with the `cloudflareTest` Vite plugin, and
   under `vp` the bundled module runner executes test files in-process instead of inside
   workerd. Both Cloudflare packages therefore run `"test": "vitest run"` against the same
   catalog-pinned Vitest `4.1.10` the root override guarantees, with package-local
   `vite.config.ts` files importing `defineConfig` from `vite-plus` and test files importing the
   test API from `vite-plus/test`. Recorded in [TOOLCHAIN.md](TOOLCHAIN.md) and enforced by
   `packages/testing/test/toolchain.test.ts`.
3. **`@effect/vitest` works inside the pool**: `it.effect` runs with its test environment and
   `TestClock` virtual time inside workerd
   (`packages/storage-cloudflare/test/effect-vitest-probe.test.ts` — “provides a TestClock that
   drives virtual time inside workerd”), with the manual `TestClock.layer()` fallback also
   proven (`packages/storage-cloudflare/test/testclock-manual-probe.test.ts`) though not needed.
4. **Miniflare restart persistence re-delivers alarms**: with `durableObjectsPersist`, Durable
   Object SQLite storage **and** the scheduled alarm slot survive `dispose()`/reopen, and a
   persisted, not-yet-fired alarm re-delivers to the fresh runtime **without any incoming
   request** (`packages/platform-cloudflare/test/restart/miniflare-restart-probe.test.ts` —
   “persists DO storage across dispose/reopen and re-delivers the overdue alarm without an
   incoming request”). The eviction gate’s “recovers without an incoming request” therefore
   holds in its strongest form on the restart lane; no poke fallback was needed.

Harness facts the suites are written against: pool-workers 0.21.x dropped per-test isolated
storage, so Durable Object storage is **shared** across tests within a run
(`do-sql-probe.test.ts` — the “shares DO storage across tests in a run” pair proves it) and
every suite mints a unique Conversation name per case; `runDurableObjectAlarm` fires the
persisted alarm directly and returns `false` when nothing is scheduled (the alarm-idempotency
lever — “delivers Durable Object alarms through runDurableObjectAlarm (WP3 harness lever)”);
compatibility is pinned at `2025-05-01` with an explicit `nodejs_compat` flag; `wrangler` is
vendored-transitive inside the pool and is **not** a repository dependency. Local agent runs of
the two Cloudflare suites need sandbox-disabled shells (workerd binds `127.0.0.1` and writes
outside the repository); CI runs them through the ordinary `bun run test` gate.

## Delivered package surface

- `@effect-agent/storage-cloudflare` owns the Durable Object SQLite adapters and the
  cross-Object port protocol. It never imports the `cloudflare:workers` runtime module —
  Durable Object handles are injected as Layer construction values and
  `@cloudflare/workers-types` stays a types-only devDependency:
  - the typed error family and the failpoint-location union copied **verbatim** from
    `SqliteStorageFailpointLocation` — all 52 location strings, so every crash-matrix row keeps
    its Node name — plus the eviction handler that maps a failpoint hit to `ctx.abort()`
    (`packages/storage-cloudflare/src/errors.ts`,
    `packages/storage-cloudflare/src/do-storage-failpoint.ts`);
  - one migration `1_current_cloudflare_conversation_object` creating the v4-mirror tables plus
    the two DC-specific tables: `effect_agent_meta` (`storage_version = "1"`, the exact-or-fresh
    gate with typed `DoStorageCompatibilityError` reset guidance, DEPLOY-008, replacing
    `PRAGMA user_version`) and `effect_agent_child_settlements` (the durable cross-store
    child-settlement notification marker the ledger port contract mandates)
    (`packages/storage-cloudflare/src/migrations.ts`);
  - the journal over the sqlite-do client’s storage-backed transactions — no `BEGIN IMMEDIATE`,
    no contention machinery; ownership/epoch checks stay inside the transaction (DUR-006) — with
    the ~1.9 MB `DoStorageConfigValue.maxStoredValueBytes` bound enforced typed
    (`DoValueBoundExceeded`) before any write (`packages/storage-cloudflare/src/do-journal.ts`,
    `packages/storage-cloudflare/src/do-storage-config.ts`);
  - the full local port facets `conversationStoreLayer`/`submissionLedgerLayer`, structurally
    mirroring the Node adapters, including the S2 operations, routable minted Submission
    identities (`{uuidv7}:{conversationId}`, D-P6-5), and `recordChildSettled` writing the
    marker table in the parent’s Object
    (`packages/storage-cloudflare/src/do-conversation-store.ts`,
    `packages/storage-cloudflare/src/do-ledger.ts`);
  - the cross-Object seam: Schema request/response/failure envelopes for the **closed**
    route-capable subset (ledger `admit`/`markReady`/`lookup`/`resolveAdmission`/
    `requestAbort`/`recordChildSettled`; store `materialize`/`append`/`read`/`inspectTail`/
    `export`) and the routed decorator Layers over the local facets — this-conversation
    executes locally, route-capable foreign operations go through the
    `ConversationPortTransport` service, everything else foreign fails fast typed; a transport
    fault on `resolveAdmission` maps to `AdmissionIndeterminate`, never to absence (SUB-031)
    (`packages/storage-cloudflare/src/port-protocol.ts`,
    `packages/storage-cloudflare/src/routing.ts`).
- `@effect-agent/platform-cloudflare` is the `DC` Layer-assembly library (deployment §12) and
  the only workspace package that imports `cloudflare:workers`:
  - binding Layers (DEPLOY-010): `ConversationObjectNamespace`, `DurableObjectContext`, and
    `ConversationObjectIdentity` (`packages/platform-cloudflare/src/bindings.ts`);
  - schema-validated configuration (DEPLOY-003): `CloudflareDurableRuntimeConfigValue` and
    `CloudflareAdmissionLimitsValue` with defaults bounded under the platform caps
    (`packages/platform-cloudflare/src/config.ts`);
  - the single multiplexed alarm: `DurableAlarmService` over
    `ctx.storage.getAlarm/setAlarm/deleteAlarm`, and `ConversationMaintenance` — the idempotent
    pass `pre-arm → runRecovery → processConversationResolved → re-arm-or-clear`, re-arming at
    `now + min(backoff-with-jitter, wakeScanInterval)` while work is nonterminal and clearing
    the slot when settled. Pre-arming before the first durable mutation establishes the alarm
    invariant — _committed nonterminal work implies a committed alarm_ — which is what makes
    eviction recovery request-free (`packages/platform-cloudflare/src/alarm.ts`);
  - the wake scheduler: local notify = durable `setAlarm(now)` (deferred while a pass runs so
    workerd never cancels the running handler); remote notify = fire-and-forget transport
    `wake()` with failures swallowed and logged (hints are droppable by contract); `wakes` = an
    in-memory bounded sliding PubSub, a pure cache behind the coordinator’s poll fallback
    (`packages/platform-cloudflare/src/wake-scheduler.ts`);
  - the Durable Object JS RPC transport carrying the WP2 envelopes
    (`packages/platform-cloudflare/src/transport.ts`);
  - `CloudflareDurableRuntime.layer` assembling the unchanged coordinator over the routed
    facets, `BrowserCrypto.layer`, per-Object producer identity
    (`{prefix}:{conversationId}`), and `AgentBindingResolver`
    (`packages/platform-cloudflare/src/layers.ts`);
  - `makeConversationObjectClass(options, observability?)` — the `effect-cf`-backed class
    applications export from their Worker (one cached runtime and native RPC boundary):
    a **local-only** constructor gate (`blockConcurrencyWhile`: migration + exact-version
    check, config decode, defensive ensure-alarm; never a recovery pass, so two Objects can
    never deadlock in constructor gates), entry points (`submitEncoded`,
    `awaitSettlementEncoded`, `observePage`, `abortEncoded`, `resolveApprovalEncoded`,
    `resolveUnknownEncoded`, `portCall`, `wake`, `alarm()`), and the admission-limits gate
    checking queue depth, input bytes, and `ctx.storage.sql.databaseSize` **before**
    `runtime.submit`, refusing with typed `AdmissionLimitExceeded` (DEPLOY-007)
    (`packages/platform-cloudflare/src/conversation-object.ts`);
  - the Worker-side `CloudflareConversationClient` over the namespace binding
    (`packages/platform-cloudflare/src/client.ts`).
- `@effect-agent/testing` gains one additive fixture module: the pinned
  `TravelPlannerCloudflareProfile`/`phase6TravelPlannerProfile` (`deploymentClass: "DC"`,
  `cloudflareEquivalence: true`, `exactlyOnceExternalEffects: false`), the cross-platform
  normal form `normalizeCrossPlatformTravelPlannerEvidence` (D-P6-6: extends the P4
  normalization by dropping `RepairAnnotated` audit records and renumbering, then scrubbing
  conversation/deployment/producer identities, `createdAt` timestamps, and content digests),
  and the committed golden `phase6TravelPlannerGoldenEvidence` both platform halves assert
  (`packages/testing/src/fixtures/travel-planner/phase6.ts`).

## Executable exit-gate evidence

| ROADMAP P6 exit gate                                                                                                                                      | Deterministic evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Eviction at every failpoint recovers without an incoming client request                                                                                   | `packages/platform-cloudflare/test/eviction.test.ts` — the 59-row `ctx.abort()` matrix over every storage and coordinator failpoint location (same location names as the Node crash matrix), each row converging by persisted-alarm delivery with **no further client call on the lane** (submit path, maintenance pass, uncertainty/approval/steps, client mutation entry points, joined input and lease renewal, checkpoints/export); the 11 `subagent:*` coordinator rows re-run cross-Object in `packages/platform-cloudflare/test/subagents-cross-do.test.ts`; strongest form: `packages/platform-cloudflare/test/restart/travel-planner-restart.test.ts` — “a Miniflare runtime restart over persisted DO storage converges the armed failpoint scenario: …” (three representative recovery classes across a full `dispose()`/reopen, convergence proven alarm-delivery-only with zero client entries before the read)                                                                                                                                        |
| Alarm handlers are idempotent under at-least-once delivery                                                                                                | `packages/platform-cloudflare/test/alarm.test.ts` — “double-fired alarms are idempotent on a ready lane: one settlement, no duplicate records”, “… on a durably suspended lane”, “… on a lane blocked by an Unknown Outcome” (all via `runDurableObjectAlarm` twice); “a typed failure inside the pass rejects the delivery and redelivery converges the lane” (workerd’s own alarm retry); the alarm-invariant audit — “the alarm invariant holds while work is nonterminal and clears once everything settles”, “the alarm invariant survives an eviction mid-pass: the persisted alarm outlives the incarnation”                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Important state never depends on in-memory Durable Object fields                                                                                          | `packages/platform-cloudflare/test/chaos.test.ts` — “chaos-abort between every host operation preserves the normalized canonical evidence” (`ctx.abort()` between every pair of host operations, final evidence byte-equal to the unchaosed run); `packages/platform-cloudflare/test/travel-planner-dc.test.ts` — “chaos-abort between every host operation preserves the normalized evidence”; `packages/storage-cloudflare/test/do-ledger.test.ts` — “persists admissions across Durable Object re-instantiation (ctx.abort eviction + reread)”; the only in-memory fields are documented caches (alarm pass bookkeeping, stall counters, wake PubSub) whose loss changes promptness, never state                                                                                                                                                                                                                                                                                                                                                                 |
| Agent/core/engine packages import no Cloudflare platform types                                                                                            | `packages/testing/test/toolchain.test.ts` — “keeps Cloudflare platform dependencies out of inward framework manifests” (core, engine, capabilities, session, storage-sqlite, and storage-memory manifests carry no `@cloudflare/*` and no `@effect/sql-sqlite-do` dependency in any section) plus the pre-existing scaffold guard restricting `@cloudflare/*` to the two Cloudflare packages; `storage-cloudflare` itself never imports `cloudflare:workers` (types-only devDependency); `platform-cloudflare` is the single runtime importer                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| The same durability conformance suite passes for Node and Cloudflare                                                                                      | `packages/storage-cloudflare/test/do-ledger.test.ts` — all 29 exported `submissionLedgerConformanceCases` (the identical arrays run by `packages/storage-sqlite/test/sqlite-ledger.test.ts` and `packages/storage-memory/test/memory-ledger.test.ts`) against the local facet over a real Durable Object’s storage, plus the Layer-requirements type proof (“keeps configuration, failpoint, SQL, and Crypto authority in the named Layer input”); `packages/storage-cloudflare/test/do-storage.test.ts` — all 8 exported `conversationStoreConformanceCases`; `packages/storage-cloudflare/test/do-ledger-failpoints.test.ts` — “leaves a recovery-classifiable state at every ledger failpoint”, “… every Phase 5 ledger failpoint”, “… every S2 ledger failpoint”                                                                                                                                                                                                                                                                                                |
| Resource limits are checked before admission                                                                                                              | `packages/platform-cloudflare/test/limits.test.ts` — “refuses the over-depth admission typed and admits again once the lane drains”, “refuses over-limit input bytes typed before any ledger row exists”, “refuses admissions typed when the database exceeds its configured ceiling” (`ctx.storage.sql.databaseSize` against the configured ceiling under the 10 GB platform cap); `packages/platform-cloudflare/test/travel-planner-dc.test.ts` — “admission refuses over-limit input before any ledger row exists”, “admission refuses over-limit queue depth before any third ledger row exists”; `packages/storage-cloudflare/test/do-ledger.test.ts` — “refuses an over-bound admission input payload typed before any ledger row exists” (the storage-value bound beneath the host gate)                                                                                                                                                                                                                                                                     |
| Travel Planner produces equivalent canonical outcomes under `DN` and `DC` while eviction and alarm retries exercise the Cloudflare-specific recovery path | one committed golden, asserted from both platforms: `packages/testing/test/travel-planner-phase6.test.ts` — “the DN run’s cross-platform normalized canonical evidence equals the committed golden” and `packages/platform-cloudflare/test/travel-planner-dc.test.ts` — “DN/DC equivalence: the DC run’s cross-platform normalized canonical evidence equals the committed golden that the DN suite asserts” (DN ≡ DC transitively, D-P6-6); the Cloudflare-specific recovery path is exercised by “eviction-equivalence: a DO aborted at terminalize:after-reserve converges by alarm alone to the exact reserved settlement, with no further client request”, “approval-gated booking suspends durably across eviction and resumes from the recorded decision without re-invoking the model”, “unknown supplier outcome under eviction never fabricates a booking result and wakes only through resolveUnknown”, and “coordinator→researcher delegation joins across two Durable Objects after parent and child evictions; the completed child never re-executes” |

## Deliverable evidence

| ROADMAP P6 deliverable                                                                                                                                              | Where it is real                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@effect-agent/storage-cloudflare` and `platform-cloudflare` packages                                                                                               | `packages/storage-cloudflare`, `packages/platform-cloudflare`; both phase-gated, private, source export maps, `check`/`test`/`build` scripts; guarded by `packages/testing/test/toolchain.test.ts`                                                                                                                                                                                                                                                                                                       |
| Cloudflare platform package whose bindings are Effect services/Layers                                                                                               | `packages/platform-cloudflare/src/bindings.ts` (`ConversationObjectNamespace.layer`, `conversationNamespaceFromEnv`/`conversationNamespaceLayer`, `DurableObjectContext.layer`); no engine or core module touches a binding (DEPLOY-010)                                                                                                                                                                                                                                                                 |
| One SQLite-backed Durable Object per Conversation                                                                                                                   | `namespace.idFromName(conversationId)` in the client and transport; `makeConversationObjectClass` (`packages/platform-cloudflare/src/conversation-object.ts`); `packages/platform-cloudflare/test/travel-planner-dc.test.ts` — “runs one durable DC planning Submission to Settlement with canonical input, per-Turn, and settlement records in the Conversation’s Durable Object”                                                                                                                       |
| Local Conversation Log and Submission Ledger tables                                                                                                                 | the v4-mirror schema plus `effect_agent_meta` and `effect_agent_child_settlements` in one migration (`packages/storage-cloudflare/src/migrations.ts`); both shared conformance suites in-workerd (`do-ledger.test.ts`, `do-storage.test.ts`)                                                                                                                                                                                                                                                             |
| Durable Object alarm service for unsettled work                                                                                                                     | `DurableAlarmService` + `ConversationMaintenance` (`packages/platform-cloudflare/src/alarm.ts`): one slot multiplexing wake-scan, lease expiry, settlement/unknown re-checks, and retry backoff into the idempotent maintenance pass; `packages/platform-cloudflare/test/alarm.test.ts`                                                                                                                                                                                                                  |
| Startup reconciliation before new work                                                                                                                              | the local-only constructor gate plus `runRecovery` before any claim in **every** pass (`packages/platform-cloudflare/src/alarm.ts`, `conversation-object.ts`); `packages/platform-cloudflare/test/chaos.test.ts` — “startup reconciliation ordering: the armed repair executes before the pass claims new work”                                                                                                                                                                                          |
| R2 artifact service if needed                                                                                                                                       | **not needed for the P6 exit gates** — see the deferral rationale below (decision D-P6-8)                                                                                                                                                                                                                                                                                                                                                                                                                |
| Miniflare/Workers test harness and deployment configuration                                                                                                         | in-workerd suites under `@cloudflare/vitest-pool-workers` with package-local `vite.config.ts` projects (compatibility date `2025-05-01`, `nodejs_compat`, SQLite-backed Object bindings, dedicated worker entries in `packages/platform-cloudflare/test/worker.ts` and `test/travel-planner-worker.ts`); the programmatic Miniflare restart lane with an esbuild-bundled worker entry (`packages/platform-cloudflare/test/restart/`); runner amendment recorded in [TOOLCHAIN.md](TOOLCHAIN.md) (D-P6-7) |
| The same Travel Planner Agent Definition and scenario suite assembled with `DC` Cloudflare Layers, live model Layers still optional, no Worker application scaffold | `packages/platform-cloudflare/test/travel-planner-dc.test.ts` assembles the **same** `@effect-agent/testing` fixtures (P4 planner, P5 booking desk/reconciler, S2 coordinator→researcher delegation) through `makePhase6TravelPlannerBindings` with the scripted model only; the worker entries live in test files, no `apps/` workspace or `wrangler` configuration exists (toolchain guard)                                                                                                            |

## The cross-Object Subagent matrix (the S2 `DC` row)

The [S2 evidence](S2-EVIDENCE.md) explicitly deferred the “Node/SQLite and Cloudflare run the
same Subagent suite” row and pinned `cloudflareEquivalence: false`. Phase 6 discharges it with
parent and child in **different** Durable Objects
(`packages/platform-cloudflare/test/subagents-cross-do.test.ts`):

- every `subagent:*` coordinator failpoint re-run under `ctx.abort()` eviction with
  alarm-only convergence, plus “the chained establishment ladder (five evictions in order)
  converges on one child Receipt, Conversation, and join” — child researcher model invocation
  count stays exactly 1, proven from eviction-surviving state;
- “recordChildSettled redelivery answers not-waiting idempotently and mutates nothing”
  (at-least-once cross-Object delivery; byte-identical canonical log, unchanged marker table);
- “a child evicted between its settlement finalize and the parent wake is healed by the
  parent’s own alarm re-poll” — the lost-notification row: the parent’s persisted alarm
  re-polls unsettled children through the routed per-child lookup and writes the durable
  marker itself;
- “aborting the waiting parent propagates ONE durable abort to the child’s Object and settles
  only after the join” and “eviction at subagent:after-child-abort-intent: the replayed
  propagation is a no-op, never a second cross-Object command” (request-abort-and-join across
  Objects);
- “an unreachable child Object during establishment recovery yields Indeterminate, never a
  duplicate child, and converges when transport heals” — the first genuinely unreachable
  authority for the S2 tri-state `resolveAdmission` (SUB-031): no child Submission, no start
  link, no researcher invocation across many failing alarm passes, then exactly one child.

## What cannot be identical to Node — the honest divergence table

Equivalence is claimed at the layer durability §5 defines — same ports, same conformance
suites, same crash-row names, same canonical record semantics, byte-equal **normalized**
canonical evidence — never byte-equal SQL files, error `cause` chains, or timings. The
mechanical differences, each deliberate:

| Concern                | `DN` (Node/SQLite)                                                                      | `DC` (Cloudflare Durable Objects)                                                                                                                   | Why the claim survives                                                                                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Write transactions     | `BEGIN IMMEDIATE` + `busy_timeout` + WAL, serializing multiple OS processes on one file | `ctx.storage.transaction()` via `@effect/sql-sqlite-do`; one writer per Object, so `SqliteWriteContention` has no analogue and is absent, not faked | the port contract never mentions contention; atomicity and fencing-inside-the-transaction (DUR-006/DUR-007) are asserted by the same 29-case conformance suite on both |
| Crash lever            | real `process.exit(137)` kills                                                          | `ctx.abort()` eviction (the platform’s actual failure mode) plus the Miniflare full-runtime restart lane                                            | crash-matrix rows keep identical failpoint-location names; the pure recovery classifier is literally shared code                                                       |
| Serialized owner       | ledger claims + leases across worker processes                                          | the Object itself; leases/epochs (same rows, same SQL) fence late async work and cross-incarnation ownership                                        | ADR-0011 D5 semantics (“leases are liveness, epochs are correctness”) is platform-invariant; the TestClock-driven lease/fencing conformance cases run unchanged        |
| Durability boundary    | WAL + fsync before the Receipt returns                                                  | Durable Object output gates — the Receipt response cannot be observed before the write is durable                                                   | different mechanism, same DUR-001 observable                                                                                                                           |
| Stored-value bound     | 16 MB (`BoundedStoredText`)                                                             | ~1.9 MB default under the platform’s 2 MB per-value limit                                                                                           | both fail typed before any mutation; threshold is configuration, not semantics; Travel Planner payloads are kilobytes                                                  |
| `resolveAdmission`     | never answers `Indeterminate` (one strongly consistent store)                           | answers `AdmissionIndeterminate` on transport failure                                                                                               | the port was specified tri-state in S2 for exactly this adapter; `DN` is the degenerate case                                                                           |
| Wake transport         | in-process PubSub + scan loop                                                           | `setAlarm(now)` locally, fire-and-forget stub `wake()` remotely, + the alarm-pass scan                                                              | `WakeScheduler` is contractually a droppable hint; every liveness proof pairs hints with scans                                                                         |
| Startup reconciliation | Layer-construction recovery pass before admission                                       | local-only constructor gate + reconcile-before-claim in every pass                                                                                  | reconciliation still strictly precedes new work per lane, which is what the gate demands                                                                               |
| Graceful shutdown      | ownership-drain decorator on close                                                      | none — eviction is the normal case; lease expiry + the next alarm pass replace draining                                                             | DEPLOY-006: correctness never depended on graceful shutdown on either platform                                                                                         |
| Repair audit records   | a clean run carries no `RepairAnnotated`                                                | even a clean run carries one (every pass reconciles before claiming, so ready input is applied through the recovery path)                           | `RepairAnnotated` is DUR-013 evidence of recovery itself; the cross-platform normal form drops it and renumbers, comparing canonical order — the durability §5 claim   |

## R2 deferral rationale (decision D-P6-8)

The roadmap line “R2 artifact service if needed” is answered **not needed**. Deployment §3.1
already defers the `AttachmentStore` port “until a real attachment requirement”, and no Travel
Planner scenario through S2 produces a record within an order of magnitude of the 2 MB platform
value bound. Shipping an R2 service now would invent the port P6 was told not to create. What
ships instead is the enforced, validated size bound: `DoStorageConfigValue.maxStoredValueBytes`
(default 1,900,000 bytes) fails typed (`DoValueBoundExceeded`) before any write at both
admission and append, naming oversized values as the designed overflow seam
(`packages/storage-cloudflare/test/do-ledger.test.ts`, `do-storage.test.ts` bound rows;
`packages/platform-cloudflare/test/limits.test.ts` for the host gate above it). When a real
attachment requirement appears, the `AttachmentStore` port gets designed once, inward-first,
with R2 as one adapter.

## Disaster recovery documentation (DEPLOY-009)

Durable Object SQLite storage ships point-in-time recovery: `ctx.storage.getCurrentBookmark()`
and `getBookmarkForTime()` name restore points for the preceding 30 days, and
`onNextSessionRestartBookmark()` restarts an Object at a bookmark. A restore is a fork of that
Conversation’s single correctness-critical store, so the standing rules apply unchanged:
producer epochs minted after the bookmark are invalidated by the restored ledger state (a
resumed stale owner is fenced by the same epoch checks the conformance suite proves), and
external side effects newer than the restored database are exactly the Unknown-Outcome/
reconciliation regime — never assumed rolled back. Restore drills and operator tooling are P7
hardening scope; until then `DC` deployments own their restore procedure operationally.

## Stored-version policy

The Durable Object storage version is **1** (`CurrentDoStorageVersion`,
`packages/storage-cloudflare/src/migrations.ts`): a fresh platform starts at the v4-equivalent
schema in one migration, recorded in `effect_agent_meta` rather than `PRAGMA user_version`
(unverified on DO SQL storage; a meta table is portable regardless). The check is exact-or-fresh
with typed `DoStorageCompatibilityError` reset guidance (DEPLOY-008); Cloudflare development
namespaces are replaced, never migrated. The canonical record envelope stays at
`schemaVersion: 1` — Phase 6 adds **no** canonical payloads; the same records that settle on
`DN` settle on `DC`, which is the point.

## Allowed claim

**`DC` Cloudflare durable execution** on the tested workerd/Miniflare assembly (DEPLOY-001),
pinned executable by `phase6TravelPlannerProfile`: `deploymentClass: "DC"`,
`durableAcceptedWork: true`, `supplierBookingUncertaintyProtocol: true`,
`durableAttachedSubagents: true`, `cloudflareEquivalence: true` — with the explicit non-claim
`exactlyOnceExternalEffects: false`
(`packages/testing/src/fixtures/travel-planner/phase6.ts`). The deployment-spec §11 condition —
“experimental until the generic durability conformance suite passes eviction, alarm retry,
deployment, and fault-injection scenarios” — is discharged by the tables above: eviction
(the 59-row abort matrix + cross-Object subagent rows), alarm retry (double-fire and
throw-retry), deployment-shaped restart (the Miniflare dispose/reopen lane, the platform’s
code-replacement analogue), and fault injection (failpoints at every durable mutation plus
routed-transport faults).

## Non-claims

- **No exactly-once external side effects** (DUR-003) — unchanged from P5/S2. Alarm redelivery
  and eviction recovery may re-invoke the model from the last committed boundary; Steps stay
  at-least-once; an unresolved ordinary Tool stops at a visible Unknown Outcome.
- **No claim about the hosted Cloudflare production platform.** The evidence executes inside
  workerd via `@cloudflare/vitest-pool-workers` and programmatic Miniflare — the platform’s own
  local runtime, but not the deployed service. Production observability adapters, deploy
  pipelines, and live-platform soak are P7 scope.
- **No Worker application scaffold.** `platform-cloudflare` is a Layer-assembly library; the
  Worker entries in the repository are test fixtures. No `apps/` workspace, no `wrangler`
  dependency or configuration exists (enforced by `packages/testing/test/toolchain.test.ts`).
- **Cross-Object routing is a closed subset, not general distribution.** Any port operation on
  a foreign Conversation outside the documented route-capable set fails fast typed
  (`packages/storage-cloudflare/test/routing.test.ts` — “fails fast typed for foreign
  operations outside the closed route-capable subset”).
- **Equivalence is normalized-canonical, not byte-level.** Timestamps, producer/deployment
  identities, minted ID formats, repair-audit placement, and content digests legally differ;
  the committed golden compares canonical order and content after the documented normal form.
- **No R2 artifact service and no `AttachmentStore` port** (D-P6-8, deferral above); values
  beyond the bound fail typed rather than overflowing anywhere.
- **No graceful drain on `DC`** — eviction is the normal case and the durability protocol is
  the correctness story (DEPLOY-006); the Node ownership-drain decorator is deliberately not
  ported.
- **Restore drills are not automated** (DEPLOY-009 scoping above); PITR procedure and epoch
  invalidation are documented, drill automation is P7.
- **Operator surface, aging, and alerting for Unknown/suspended lanes remain P7** (DUR-017
  scoping carried forward from P5).
