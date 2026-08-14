# Operations guide

Status: P7 operator documentation. Everything here describes behavior that exists in the tree
today; anything that cannot be exercised in this repository's test environment is explicitly
labeled a manual runbook rather than silently claimed.

This guide covers the operator surface of the durable runtime: the administrative operations,
obligation monitoring, backup and restore for both deployment classes, and the chaos/soak
verification lanes that back these claims.

## 1. Administrative operations

The five administrative operations are members of `DurableAgentRuntime`
(`packages/session/src/durable-runtime.ts`) implemented over the `SubmissionLedger` and
`ConversationStore` ports only, so they behave identically on `DN` (Node/SQLite) and `DC`
(Cloudflare Durable Objects):

- `explain(submissionId)` / `explainConversation(conversationId)` — read-only recovery
  explanation: the classifier decision, its operator meaning, and the disposition a recovery
  pass would report. Performs zero writes (tested byte-identical durable state).
- `verify(conversationId)` — read-only integrity checks with typed per-check results; never a
  repair. The digest-chain check reports `skipped` with the honest reason unless per-batch
  producer identity is supplied out-of-band (adapter-level `verifyOnOpen` is the storage-side
  audit).
- `retry(submissionId, { author, reason })` — audited re-drive of exactly the classifier's one
  decision, with typed refusals for settled work and for lanes owned by the
  `resolveUnknown`/`resolveApproval` paths (SEC-011: author and reason are mandatory).
- `wake(conversationId)` — the droppable liveness nudge.
- `scanObligations(thresholds)` — the DUR-017/OPS-001 obligation report (next section).

On `DN`, `NodeDurableHost` re-exposes all five and `scripts/durable-admin.ts`
(`bun run admin:durable -- <explain|verify|retry|wake|obligations> --database <file>`) is the
CLI. On `DC`, the Conversation Object exposes the Schema-encoded entry points
(`explainEncoded`, `verifyEncoded`, `retryEncoded`, `obligationsEncoded`, `wake`); deployments
reach them through their own Worker. All of them (plus `observe`, `resolveUnknown`,
`resolveApproval`) consult the `OperationAuthorizer` fail-closed; the default Layer preserves
service-possession behavior, and a host-supplied authorizer turns denials into the typed
`OperationDenied` before any read or write.

## 2. Obligation monitoring (OPS-001, OPS-002)

`scanObligations` is scan-based, never a daemon: it folds the ledger's nonterminal scan into
rows `{submissionId, conversationId, state, blockedOn, ageSeconds, severity}` where `blockedOn`
is one of `unknown`, `approval`, `waitingForChild`, `ready-aged`, `running-aged` and severity is
classified against the host-supplied `{agingSeconds, overdueSeconds}` thresholds. Ages come
from timestamps that already exist (`createdAt`/`readyAt`, `suspendedAt`, the canonical
`ToolCallUnknown` record), so no storage-version bump is involved.

**Hosts own the alert loop.** The framework deliberately does not schedule the scan or deliver
alerts: run `scanObligations` periodically from your host (cron, alarm, monitoring agent),
export the rows as logs/metrics, and alert on:

- any row with `blockedOn: "unknown"` — an Unknown Outcome needs the authorized
  `resolveUnknown` decision (OPS-002 expects an immediate signal);
- any row with severity `overdue` — accepted work without timely settlement (OPS-001);
- a growing `approval` backlog.

## 3. DN backup and restore (executable drill)

`packages/platform-node/test/restore-drill.test.ts` is the executable DN restore drill: it
snapshots the SQLite file mid-run (an external supplier effect committed, its canonical outcome
not), keeps the original timeline running to settlement, restores the snapshot into a fresh
host, and asserts the semantics below. Restoring a backup means accepting them:

1. **Pre-backup history survives intact** — the restored store passes the same integrity
   checks (`verify`) as the original.
2. **Post-backup epochs are fenced** — an ownership token minted on the original timeline
   after the backup point is rejected typed by the restored store. Before serving traffic from
   a restore, fence or terminate every producer that ever ran against the original store
   (security-operations §14): the restored store's epochs will supersede them, but a divergent
   producer must never be left assuming it still owns anything.
3. **Post-backup external effects surface through the Unknown regime** — an external call
   whose outcome was only recorded after the backup re-enters recovery as an open call and is
   marked `ToolCallUnknown`. It is _never assumed rolled back_ and _never automatically
   replayed_: resolve each one through `resolveUnknown` from external truth (the supplier's
   records), exactly as the drill does.
4. **Post-backup admissions are gone** — Receipts issued after the backup point do not exist
   in the restored store. Reconcile them explicitly: clients holding such Receipts must
   resubmit (idempotency keys make the resubmission safe), and any external effects those lost
   Submissions performed must be reconciled through the same Unknown discipline.

Backup mechanics on DN: any file-consistent snapshot works (the drill copies the `.sqlite`,
`-wal`, and `-shm` files while no process holds the database; `VACUUM INTO` or SQLite's backup
API are equivalent online options).

## 4. DC point-in-time recovery (manual runbook — not executable here)

Cloudflare Durable Objects provide point-in-time recovery over the last 30 days through the
hosted platform (`ctx.storage.getCurrentBookmark()`, `getBookmarkForTime(...)`,
`onNextSessionRestoreBookmark(...)`). **Miniflare/workers-pool does not implement these APIs, so
this repository carries no executable DC restore evidence — this section is a manual runbook,
recorded as scoped (DEPLOY-009's DC half), not a discharged claim.**

Manual procedure for a deployed Conversation Object:

1. Stop new admission for the affected Conversations (route submissions away or deny at your
   Worker) and let in-flight alarms drain.
2. Obtain a bookmark: `getBookmarkForTime(timestamp)` for the desired restore point, or a
   bookmark captured earlier (e.g., logged by your deployment before risky operations).
3. Call `onNextSessionRestoreBookmark(bookmark)` inside the Object, then `ctx.abort()`; the
   next session starts from the restored state.
4. Apply the same four restore semantics as §3: the restored Object's startup reconciliation
   pass re-classifies in-flight work; treat every open external effect as Unknown and resolve
   it from supplier truth; treat Receipts issued after the bookmark as lost admissions to be
   reconciled; assume every pre-restore producer epoch is superseded (the Object's serialized
   ownership makes this automatic within one Object, but cross-Object children established
   after the bookmark must be reconciled through the parent's recovery ladder).
5. Run `verifyEncoded` and `obligationsEncoded` before reopening admission.

## 5. Chaos and soak lanes

These suites back the safety claims above with randomized and long-running evidence. All of
them end in the same shared checker (`verifyConversationInvariants`) plus a zero-entry
`scanObligations`, so admin `verify`, adapter certification, chaos, and soak all assert one set
of claims.

| Suite         | Location                                            | Shape                                                                                                                                                          |
| ------------- | --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Memory chaos  | `packages/testing/test/chaos-memory.test.ts`        | ~200 seeded plans over the memory adapters, TestClock, seconds                                                                                                 |
| SQLite chaos  | `packages/testing/test/chaos-sqlite.test.ts`        | reduced seeded sweep over real SQLite files, adapter failpoint arms included                                                                                   |
| DC chaos      | `packages/platform-cloudflare/test/chaos.test.ts`   | abort-between-every-operation equivalence plus a seeded random `ctx.abort()`/alarm-order variant, bounded rounds                                               |
| DN soak       | `packages/platform-node/test/soak/soak.test.ts`     | 500 Submissions across 20 lanes over one SQLite file, seeded real worker SIGKILLs, joins + delegations mixed, resource-stability assertions, ≤ 5 minute budget |
| Memory soak   | `packages/testing/test/soak-memory.test.ts`         | 5,000 Submissions under TestClock asserting the heap returns to baseline when each wave's scope closes                                                         |
| Restore drill | `packages/platform-node/test/restore-drill.test.ts` | the §3 drill                                                                                                                                                   |

**Seeds and replay.** The chaos generators (`packages/testing/src/chaos.ts`) derive every plan
from one root seed. Set `CHAOS_SEED=<seed>` to replay a failing run byte-for-byte; every chaos
failure message prints the root seed and the failing plan's own seed. The DC seeded variant and
the DN soak use the same convention (the soak's kill schedule is seeded with its documented
constant).

**Gating honesty.** Today the crash matrices, chaos suites, soak, and restore drill all run in
the ordinary per-package test lane (`bun run test`, and therefore `bun run ready` and CI) —
the repository currently has a single test lane, so "the crash-test lane" and "the PR gate" are
the same thing. testing.md §13 assigns process-kill crash and soak suites to the
release-candidate gate as CI matures; when a separate release lane appears, the soak and the
crash matrices move there together. No durability suite is skipped anywhere (TEST-012).

**Scope notes, stated honestly.**

- The chaos runner tolerates _typed_ failures while a fault is armed and re-drives; a defect
  fails the plan. Its convergence claim is: every accepted Submission settles, every touched
  Conversation passes the shared integrity checks (full digest chain included — the runner
  knows its single producer), no obligation remains visible, and no desk-backed Tool success
  exists that the deterministic desk did not produce (durability §10).
- The DN soak runs one worker process at a time (killed and replaced), matching the documented
  DN deployment shape of one process owner per database file (durability §6). Running multiple
  concurrent worker pools over one SQLite file is not a claimed deployment shape; exploratory
  runs in that topology showed that the _append order_ of two joined Submissions' settlement
  records can interleave between a host's joined-settle loop and a concurrent recovery pass
  (inputs stay FIFO, every settlement stays exactly-once) — recorded as a known observation for
  the multi-node scheduling work deferred to open-source preparation.
