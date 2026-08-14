# Phase 7 internal hardening evidence

Status: **Implemented**

Phase 7 is the final roadmap phase. It adds no deployment class and no new canonical payload
family; it hardens and _proves_ what P0–P6 and the S1/S2 slices built: a three-tier adapter
certification with committed certificates, model-checked TLA+ specifications of the durable
protocol, an operator surface that explains recovery without editing storage, a descriptive
threat model with a mechanically checked findings register, seeded chaos and bounded soak
evidence, three real internal Agents, and an API-simplification pass driven by their authoring
friction notes. With this phase, **every numbered roadmap phase (P0–P7) is Complete** and both
Subagent slices remain implemented as roadmap-assigned proposed defaults. Completion of the
roadmap is an engineering claim, not a governance one: the specifications stay **Draft**, the
project stays pre-1.0 and private, ADR-0010 remains formally Proposed, ADRs 0011–0015 (and
D-029…D-033) are accepted by default **awaiting owner review**, and no claim is made about the
hosted Cloudflare production platform. Open-source preparation remains the deferred next chapter
([ROADMAP](ROADMAP.md)).

## Deliverable evidence

| ROADMAP P7 deliverable                                                                                                                                                            | Where it is real                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Adapter certification suite                                                                                                                                                       | one entry point `certifyDurableAdapters` (`packages/testing/src/certification.ts`) over the report schemas, `certifyPorts`, and the shared invariant checker `verifyConversationInvariants` (`packages/session/src/certification.ts`); runners `packages/testing/test/certification-memory.test.ts`, `certification-sqlite.test.ts`, and — in-workerd against a real Durable Object, STORE-013 — `packages/storage-cloudflare/test/certification.test.ts`; three committed certificates under `docs/certification/`; the guide [Certify storage adapters](guide/certify-adapters.md); details below                                                                                                                                      |
| TLA+/PlusCal or Alloy model for ordering, joining, settlement, and uncertainty                                                                                                    | `formal/DurableSubmission.tla` and `formal/SubagentEstablishment.tla` (PlusCal with committed translations) over six committed `.cfg` instances, including two negative controls that must fail; TLC actually ran — recorded state counts in `formal/EVIDENCE.md`; action-by-action and invariant-by-invariant code correspondence plus the abstraction assumptions in `formal/CORRESPONDENCE.md`; `bun run formal:check` (`scripts/formal-check.ts`) and the scheduled/dispatchable `.github/workflows/formal.yml`, both deliberately outside `bun run ready`; details below                                                                                                                                                            |
| Administrative explain, verify, retry, and wake operations                                                                                                                        | five operations (`explain`/`explainConversation`, `verify`, `retry`, `wake`, plus `scanObligations`) as `DurableAgentRuntime` members over the two ports only (`packages/session/src/durable-runtime.ts`, report logic in `packages/session/src/admin.ts`), so DN and DC behave identically; `NodeDurableHost` re-exposes all of them (`packages/platform-node/src/host.ts`); the Conversation Object exposes Schema-encoded entry points (`packages/platform-cloudflare/src/conversation-object.ts`, `test/admin-encoded.test.ts`); the CLI is `bun run admin:durable` (`scripts/durable-admin.ts`); operator documentation in the [operations guide](guides/operations.md); details and the zero-writes proof below                    |
| Security review and threat model                                                                                                                                                  | descriptive STRIDE walk of all ten security-operations §1 trust boundaries in [THREAT-MODEL.md](THREAT-MODEL.md) (explicit exclusions; artifact-classification laundering recorded N/A per D-P6-8); triaged findings register with owner + status in [docs/security/FINDINGS.md](security/FINDINGS.md); three deterministic red-team suites under `packages/testing/test/security/` plus the `crossPrincipalAdmissionScoping` conformance case running on all three ledger adapters; details below                                                                                                                                                                                                                                       |
| Chaos and soak tests                                                                                                                                                              | seeded Schema-first chaos generator (`ChaosPlan`, `packages/testing/src/chaos.ts`, `effect/testing/FastCheck`, `CHAOS_SEED` replay): 200 memory plans (`packages/testing/test/chaos-memory.test.ts`), a 24-plan SQLite sweep with adapter failpoint arms (`chaos-sqlite.test.ts`), and a seeded `ctx.abort()`/alarm-order DC variant (`packages/platform-cloudflare/test/chaos.test.ts`); the DN soak — 500 Submissions across 20 lanes over one SQLite file under 6 seeded real worker SIGKILLs with heap/handle/orphan/clean-close assertions (`packages/platform-node/test/soak/soak.test.ts`); a 5,000-Submission pure-memory soak (`soak-memory.test.ts`); the executable DN restore drill (`restore-drill.test.ts`); details below |
| Internal usage feedback and API simplification                                                                                                                                    | three real authoring-friction notes (quoted below): `examples/repo-ops/FRICTION.md`, the note in `packages/testing/src/fixtures/travel-planner/phase7.ts`, and the note in `packages/testing/src/fixtures/docs-researcher/index.ts`; the simplification pass shipped items (a)–(h) of the adopted plan — the establishment-race fix, the `durable-cloudflare` capability label, immediate settling of aborted non-head work, the generated requirements-coverage gate, the P5 stale-non-claim fix, the recorded shared-SQL-core non-extraction, the `schemaSyncInEffect` lint fixes, and the `withTerminalDefectEvent` boundary helper; details below                                                                                    |
| Travel Planner internal use with live model and selected supplier Layers, red-team cases for untrusted supplier content and traveler data, and evidence across mocked, DN, and DC | the P7 dual-profile pin `phase7TravelPlannerProfile` (`packages/testing/src/fixtures/travel-planner/phase7.ts`) and the opt-in live-model smoke suite (`examples/providers/test/live-smoke.test.ts`); “selected supplier Layers” is **honestly scoped to live-model-only** — no real supplier exists to integrate, the deterministic supplier desk is retained deliberately, and the profile pins `liveSupplierLayers: false` (plan decision 9); red-team supplier/traveler cases in `redteam-supplier-injection.test.ts` and `redteam-child-exfiltration.test.ts`; mocked evidence is the cumulative deterministic suites, DN evidence the crash matrices and soak, DC evidence the eviction/cross-DO/restart lanes; details below      |

## Executable exit-gate evidence

| ROADMAP P7 exit gate                                                                                                               | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ---------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Safety properties hold under the documented assumptions                                                                            | layered, with each layer’s assumptions stated where it is defined: (1) TLC model checking of the protocol design on bounded instances — all six committed instances produce the expected verdict, negative controls included (`formal/EVIDENCE.md`; assumptions enumerated in `formal/CORRESPONDENCE.md` §3); (2) certification Tier 2 — 168 fault cells per adapter × 3 adapters converge through public levers into `verifyConversationInvariants` with `requireAllSettled` and a fully recomputed digest chain (`docs/certification/*.json`, all `ok: true`); (3) seeded chaos and soak convergence over the same shared checker; (4) both real-loss matrices stay green — Node process-kill (`packages/platform-node/test/crash/crash.test.ts`, `crash-subagents.test.ts`) and DC eviction/cross-DO/restart (`packages/platform-cloudflare/test/eviction.test.ts`, `subagents-cross-do.test.ts`, `restart/travel-planner-restart.test.ts`); no durability suite anywhere is skipped (TEST-012) |
| Security review has no unowned critical finding                                                                                    | mechanical check over [docs/security/FINDINGS.md](security/FINDINGS.md): every row names an owner (zero unowned rows), and no row is severity `critical` or `high` — the highest of the seven findings (SEC-P7-001…007) is `medium`, so zero critical findings are unfixed; the register also records what was probed and found sound, so the review is reproducible                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Operators can explain recovery state without editing storage                                                                       | `packages/testing/test/admin-operations.test.ts` — “explain performs zero writes: the canonical log and ledger rows are byte-identical before and after”; each of the 29 `RecoveryDecision` tags renders an operator meaning from the static table in `packages/session/src/admin.ts` (`RECOVERY_DECISION_MEANINGS`) plus the disposition a recovery pass would earn (`predictRecoveryDisposition`), and `renderRecoveryExplanation` produces the operator text; reachable from a shell via `bun run admin:durable -- explain` and from a DC Worker via `explainEncoded` (`packages/platform-cloudflare/test/admin-encoded.test.ts`); `explainConversation` names the `AwaitParentEstablishment` deferral on a live cross-Object lane (`subagents-cross-do.test.ts`)                                                                                                                                                                                                                               |
| At least three real internal Agents validate the authoring model                                                                   | (1) the **Travel Planner** — the cumulative reference application through every phase plus the P7 dual-profile pin; (2) the **repo-ops evidence auditor** (`examples/repo-ops`) — reads the repository’s evidence documents, extracts cited test titles, and verifies them against the tree through sandbox-local read-only command tools, named Durable Steps per document on DN, and an approval-gated report write (`examples/repo-ops/test/evidence-auditor.test.ts`); (3) the **docs-researcher** (`packages/testing/src/fixtures/docs-researcher/`, `packages/testing/test/docs-researcher.test.ts`) — S2 durable delegation to a `doc-summarizer` child on DN with MCP-discovered content tools, discovery bounds, redaction, and `waitingForChild`; each shipped a real authoring-friction note (quoted below); the demo general-chat bench remains supplementary `E`-class evidence                                                                                                       |
| Travel Planner is one of those internal Agents and retains a deterministic offline conformance profile alongside its live profiles | `phase7TravelPlannerProfile` pins the claim as a decodable Schema value: the offline cumulative suites stay deterministic and credential-free, the live profile is opt-in (`EFFECT_AGENT_LIVE=1` + `OPENAI_API_KEY`, `describe.skipIf` on `phase7LiveProfileEnabled`), transcripts are structurally redacted, `liveSupplierLayers: false`, `exactlyOnceExternalEffects: false`; pinned every run by `examples/providers/test/live-smoke.test.ts` — “pins the deterministic offline conformance profile alongside the opt-in live-model profile” and “enables the live profile only for EFFECT_AGENT_LIVE=1 plus a present credential”                                                                                                                                                                                                                                                                                                                                                              |

## Adapter certification (TEST-004, STORE-010, STORE-013)

Certification is one entry point over a candidate `SubmissionLedger`/`ConversationStore` Layer
pair, producing one Schema-encoded `CertificationReport`
(`packages/session/src/certification.ts`):

- **Tier 1 — port contract:** the two shared conformance case arrays run verbatim — all 32
  `submissionLedgerConformanceCases` and 8 `conversationStoreConformanceCases`, with per-case
  typed results.
- **Tier 2 — coordinator convergence:** every `DurableRuntimeFailpointLocation` (28) armed
  one-shot across six scenario shapes (plain, uncertain-tool, durable-steps, approval, join,
  delegation) — 168 cells; after the fault the state must classify and re-drives through
  **public levers only** (`runRecovery`, worker re-drives, `resolveUnknown`/`resolveApproval`
  selected from `explainConversation` evidence) must converge into
  `verifyConversationInvariants` with `requireAllSettled`. The digest-chain check is **fully
  recomputed** from `EMPTY_TAIL_DIGEST` via append-time per-batch producer-identity capture —
  never skipped in a certificate. Exactly three locations are unreachable by the six shapes
  (`abort:after-intent`, `resolve:after-intent`, `subagent:after-child-abort-intent` — operator
  and abort paths the shapes do not take); the runners assert the observed never-fired set
  **equals** this pinned list (`TIER2_UNREACHED_LOCATIONS`), so scoped coverage is stated and
  cannot silently grow. Those locations are pinned by the P5/S2 in-process suites and the crash
  matrices.
- **Tier 3 — real loss:** honest by construction — `exercised` (adapter-supplied crash lever run
  now), `recorded-evidence` (committed real-loss suite citations), `not-exercised` (visible), or
  `not-applicable` (the non-durable reference). `report.ok` covers executed checks only.

Committed certificates (deterministic — scripted model, virtual clock, fixed lane names; the
runner tests carry the semantic assertions per testing.md §12, the JSON is evidence, never the
assertion source):

| Certificate                                  | Tier 1 | Tier 2                                 | Tier 3                                                                                                  |
| -------------------------------------------- | ------ | -------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `docs/certification/storage-memory.json`     | 40/40  | 168 cells, 0 failed, digest chain full | `not-applicable` — declared `non-durable` reference                                                     |
| `docs/certification/storage-sqlite.json`     | 40/40  | 168 cells, 0 failed, digest chain full | `recorded-evidence` — the process-kill crash matrices (TEST-005)                                        |
| `docs/certification/storage-cloudflare.json` | 40/40  | 168 cells, 0 failed, digest chain full | `recorded-evidence` — the eviction matrix, the cross-DO subagent matrix, and the Miniflare restart lane |

The memory and SQLite runners live in `packages/testing/test` because vp’s task graph rejects
the `storage-* → testing` dev-edge cycle (the c106b53 precedent); the Cloudflare runner executes
the identical entry point inside workerd against real Durable Object storage under
`vitest run` (D-P6-7). Every Tier-2 re-drive round advances the TestClock past the D5 ownership
lease, so certification never depends on the memory reference’s same-producer-reclaim fast path.

## Formal model (TLC outcomes and correspondence honesty)

**TLC actually ran on every committed instance**; `formal/EVIDENCE.md` records the results
(Temurin OpenJDK 21, TLC2 2.19):

| Instance                         | Result                                             | States generated / distinct |
| -------------------------------- | -------------------------------------------------- | --------------------------: |
| `DurableSubmission.cfg`          | pass (8 invariants, 2 workers)                     |    144,036,108 / 37,736,499 |
| `DurableSubmissionLiveness.cfg`  | pass (+ `EventuallySettled`, 1 worker)             |       4,541,406 / 1,333,275 |
| `DurableSubmissionNoFencing.cfg` | **fails as required** (`FencingSafety` violated)   |               2,611 / 1,161 |
| `SubagentEstablishment.cfg`      | pass (6 invariants + 2 liveness)                   |             43,136 / 14,238 |
| `SubagentEstablishmentRace.cfg`  | **fails as required** (`ChildTurnRequiresLineage`) |               3,130 / 1,231 |
| `SubagentEstablishmentFix.cfg`   | pass (`AwaitParentEstablishment` discipline)       |              25,166 / 8,434 |

**Scope of the claim, stated precisely:** these are bounded-instance checks of the _protocol
design_ under the abstraction assumptions enumerated in `formal/CORRESPONDENCE.md` §3 (atomic
ledger operations, strongly consistent recovery reads, abstracted digests/time, quiet-lane
recovery, fault/route budgets conditioning liveness — the durability §1/DUR-017
resolution-dependency assumption made formal). They are **not** a proof about the code; the code
claim remains the executable test corpus, which CORRESPONDENCE links action-by-action and
invariant-by-invariant. Liveness on the 2-worker instance was **not** discharged (it exceeded
the TLC budget) and is honestly scoped to the committed 1-worker liveness instance, with
stale-Attempt interleavings still exercised via lease expiry and recovery claims. The §7(a)
establishment race was model-checked both ways — demonstrated reachable under the pre-P7
discipline and eliminated under `AwaitParentEstablishment` — **before** the fix was implemented.
`formal:check` and the workflow are never part of `bun run ready` or the PR gate (Bun-only CI
carries no JVM).

## Administrative operations (DUR-017, SEC-011, OPS-001/OPS-002)

All five operations are coordinator members over `SubmissionLedger` + `ConversationStore` only,
so they behave identically on DN and DC:

- **`explain` / `explainConversation`** are provably read-only — tested byte-identical durable
  state before and after — rendering each of the 29 `RecoveryDecision` tags via the static
  meaning table matching the `recovery.ts` docstrings, plus the disposition it would earn.
- **`verify`** never repairs and returns typed per-check results (`IntegrityReport`): digest
  chain, envelope decode, record-identity uniqueness, FIFO order, ledger-terminal vs canonical
  agreement (DUR-015), checkpoint binding. Through the port-only runtime member the digest-chain
  check reports `skipped` with the explicit reason (the port deliberately does not export
  producer identity); it is fully discharged wherever per-batch producer identity is available —
  certification, chaos, and soak. Stated as scoped, never silently claimed.
  `verifyConversationInvariants` is the **single shared checker** for admin verify,
  certification Tier 2, chaos, and soak.
- **`retry`** re-drives exactly the classifier’s one decision with the existing deterministic
  `RepairAnnotated` audit (no new record type), mandatory `author`/`reason` (SEC-011), and typed
  `RetryRefused` for settled work and for lanes owned by `resolveUnknown`/`resolveApproval`.
- **`wake`** is the documented droppable liveness nudge.
- **`scanObligations`** is scan-based, never a daemon: ages from timestamps that already exist
  (`createdAt`/`readyAt`, `suspendedAt`, the canonical `ToolCallUnknown`), no storage-version
  bump; hosts own the alert loop (OPS-001/OPS-002, [operations guide](guides/operations.md) §2).

**Authorization stance:** the additive `OperationAuthorizer` port
(`packages/session/src/operation-authorizer.ts`) defaults to the pre-P7 service-possession
behavior; a non-default Layer is enforced **fail-closed** — typed `OperationDenied` before any
read or write — on `observe`, all admin operations, `resolveUnknown`, and `resolveApproval`
(`admin-operations.test.ts` — “a non-default authorizer denies every consulted surface
fail-closed”; adversarially swept per operation in `redteam-idor-sweep.test.ts`). The framework
deliberately ships no identity system; hosts supply the authenticator. The DC admin entry points
run the possession default (no DC authorizer config lever yet) — recorded as FINDINGS
SEC-P7-003, not silently assumed.

## Security review (threat model, findings register, red-team suites)

[THREAT-MODEL.md](THREAT-MODEL.md) is descriptive (security-operations.md stays normative) and
walks all ten §1 trust boundaries with STRIDE tables, explicit scope exclusions (hosted
Cloudflare service, real supplier APIs, application-owned authentication), and the
artifact-classification-laundering row recorded **N/A** (no artifact port exists, D-P6-8).
[docs/security/FINDINGS.md](security/FINDINGS.md) is the triage register: seven findings
(SEC-P7-001…007), every row owned, highest severity `medium`, each `deferred` row carrying an
explicit gate and each `accepted-risk` row its reason — plus the analyzed-safe list so the
review’s negative results are reproducible. The five named surfaces (session admin operations,
cross-DO transport, sandbox-local, MCP connector, demo/live profiles) were adversarially
reviewed with fresh eyes; the one probe-confirmed finding (SEC-P7-001, MCP discovery
canonicalization depth) is `medium`/`deferred` with a named gate.

The executable red-team evidence (deterministic — scripted model, TestClock, temp SQLite, no
network):

- `packages/testing/test/security/redteam-supplier-injection.test.ts` — prompt-injected supplier
  content cannot escalate capability: un-granted tools stay denied, the approval gate fires
  before any handler start, a denial settles failed with zero supplier calls, and the injected
  credential is stripped by the structural Redactor (SEC-007).
- `packages/testing/test/security/redteam-child-exfiltration.test.ts` — a durable child’s
  secrets never cross the delegation boundary: only the bounded declared summary joins; the
  `SubagentExecutionFailure` projection has no Cause/stack channel and rejects over-length
  payloads (the bounded-projection proof); the Redactor strips secret scalars from child
  failure/progress previews (SUB-015, SEC-008).
- `packages/testing/test/security/redteam-idor-sweep.test.ts` — under a tenant-scoped
  `OperationAuthorizer`, every operation (`observe`, `explain`, `explainConversation`, `verify`,
  `retry`, `wake`, `resolveUnknown`, `resolveApproval`) denies foreign targets fail-closed and
  permits the caller’s own (SEC-002, SEC-003).
- the `crossPrincipalAdmissionScoping` conformance case (`packages/session/src/ledger-conformance.ts`)
  closes the cross-principal idempotency-key gap on **all three** ledger adapters: a second
  principal reusing another’s key mints a distinct Submission — never a replay, collision, or
  discovery.

## Chaos, soak, and the restore drill

Every chaos plan and soak run ends in the same shared `verifyConversationInvariants`
(convergence mode, full digest chain — each runner knows its single producer) plus a zero-entry
`scanObligations` and, where the supplier desk is in play, the non-fabrication check:

- **Memory chaos:** “CHAOS: 200 seeded failpoint/abort interleavings over memory adapters
  converge to verified invariants” (`packages/testing/test/chaos-memory.test.ts`), seconds under
  TestClock. **SQLite chaos:** a 24-plan sweep including the adapter SQLite failpoint arms
  (`chaos-sqlite.test.ts`; placed in `packages/testing` for the vp dev-cycle constraint).
  **DC chaos:** “CHAOS: seeded random ctx.abort()/alarm-order interleaving across two lanes
  converges within bounded rounds and preserves normalized evidence”
  (`packages/platform-cloudflare/test/chaos.test.ts`). `CHAOS_SEED` replays any failing run;
  failure output prints the seed.
- **DN soak:** 500 Submissions (480 plain + 20 delegations → 20 child Conversations) across 20
  lanes over one SQLite file under 6 seeded **real** worker SIGKILLs, joins and delegations
  mixed, asserting heap stability after forced GC, `getActiveResourcesInfo()` handle return to
  baseline, no orphan children, and a clean close within the ≤5-minute budget
  (`packages/platform-node/test/soak/soak.test.ts`). **Memory soak:** 5,000 Submissions across
  500 join-heavy lanes under TestClock asserting the heap returns to baseline as each wave’s
  scope closes (`soak-memory.test.ts`).
- **Gating honesty:** the repository currently has a single test lane, so the crash matrices,
  chaos, soak, and drill all run in the ordinary per-package `vp test` gate — that _is_ how the
  crash tests are gated today. testing.md §13 assigns them to a release-candidate lane as CI
  matures; when one exists they move together. No durability suite is skipped (TEST-012).
- **Restore drill (DEPLOY-009):** the DN half is executable —
  `packages/platform-node/test/restore-drill.test.ts` snapshots the SQLite file mid-run,
  restores into a fresh host, and asserts: pre-backup history verifies intact, post-backup
  epochs are fenced typed, post-backup external effects surface through the **Unknown regime**
  (never assumed rolled back, never auto-replayed), and post-backup admissions are honestly
  lost. The DC half — Durable Object point-in-time recovery — is a hosted-platform API
  unavailable under Miniflare and is documented as a **manual runbook**
  ([operations guide](guides/operations.md) §4): scoped, never silently claimed.

## The three internal Agents and their friction notes

Each Agent consumed the public framework surface and filed a real authoring-friction note; the
notes drove the WP7 simplification backlog and are quoted here as the “internal usage feedback”
deliverable:

1. **Travel Planner** (live-profile wiring, `packages/testing/src/fixtures/travel-planner/phase7.ts`):
   “There was no framework-owned place to put a test-side live gate … the split (fixture exports
   the predicate, the example test applies it to `process.env`) is a convention a future author
   has to discover by reading this file rather than a typed seam.” And: “Capturing a structurally
   redacted transcript from a live Run takes manual assembly … a `redactedTranscript(events)`
   helper … would make the safe path the short path.” Also: binding a live model to the existing
   definition was “pleasantly trivial”.
2. **Repo-ops evidence auditor** (`examples/repo-ops/FRICTION.md`): “Sandbox command tools need a
   lot of ceremony for ‘run one read-only command’ … a small framework helper …
   `Sandbox.exec(command, args, { cwd, limits })` … would remove ~60 lines of boilerplate per
   agent.” And: “Nothing composes ‘validate a model-supplied path’ fail-closed.” Also: “Durable
   Steps were the smoothest part of the whole surface … worked first try. No change requested.”
3. **Docs-researcher** (`packages/testing/src/fixtures/docs-researcher/index.ts`): “MCP discovery
   and Agent authoring do not meet in the type system … a framework helper that checks a
   `McpConnection` against a static Toolkit value … would remove a whole class of
   look-alike-toolkit mistakes.” And: “Prompt-aware scripted models are boilerplate-heavy …
   a shared `makePromptAwareCountingModel` in the testing package is an easy WP7
   simplification.”

The unshipped helper suggestions above are recorded backlog for open-source preparation; the
notes’ protocol-level items were absorbed by the WP7 pass below.

## The simplification pass (what shipped)

- **(a) Cross-Object establishment race — closed.** The model-vindicated
  `AwaitParentEstablishment` discipline (`formal/SubagentEstablishmentFix.cfg`): a parent-linked
  `admitted` Submission without a canonical `SubagentLineageRecorded` defers — through the
  classifier **and** through the worker claim path, since the claim head rule legally grants an
  admitted head — and the parent’s idempotent establishment (lineage strictly before readiness)
  completes it. Pinned cross-Object by `packages/platform-cloudflare/test/subagents-cross-do.test.ts`
  — “a child never runs a Turn before its lineage record is canonical” — with `explainConversation`
  naming the deferral on the live lane, plus the classifier rows in
  `packages/session/test/recovery-classifier.test.ts`. The `RecoveryDecision` union grew to 29
  tags; the meaning table, disposition predictor, and certification count pins moved in the same
  change set.
- **(b) `LedgerCapabilities.durability`** gained `"durable-cloudflare"`
  (`packages/session/src/ledger.ts`); the DO ledger stopped reporting `"durable-node"`; no
  caller branches on the literal (conformance/type tests updated). The Cloudflare certificate
  now names its own durability class.
- **(c) Aborted non-head ready Submissions settle immediately** via the recovery/maintenance
  pass — settlement order of never-run work is not execution order, so FIFO (DUR-004) is
  untouched; the contiguous-joining-prefix gap rule treats aborted-settled rows as non-gaps in
  all three adapters. Pinned by the shared conformance cases (`queuedAbortSettlementAuthority`,
  `abortedSettledRowIsNotAJoiningGap` — the suite grew to 32), the same-named DN and DC tests
  (“an aborted non-head ready submission settles without waiting for the head” in
  `packages/testing/test/durable-runtime.test.ts` and
  `packages/platform-cloudflare/test/eviction.test.ts`), and a new process-kill crash row
  (`packages/platform-node/test/crash/crash.test.ts`).
- **(d) Requirements truthing + generated coverage:** `bun run requirements:coverage`
  (`scripts/requirements-coverage.ts`) extracts every requirement ID from `docs/spec/*.md`,
  collects test-title references, and fails on undocumented gaps and on stale exception rows —
  TEST-011 discharged mechanically; the current tree passes with honest documented exceptions
  ([REQUIREMENTS](REQUIREMENTS.md)).
- **(e)** the stale `PHASE-5-EVIDENCE` S2 non-claim was replaced with evidence pointers, along
  with the other named stale surfaces (docs hero, architecture tree, README milestone).
- **(f) Shared SQL core — deliberately not extracted** (ADR-0014 revisit): no third SQL adapter
  appeared; extraction would churn two landed adapters for zero behavior; the shared conformance
  suites remain the anti-drift guard. Recorded in ADR-0014’s status note and D-033.
- **(g)** the three `schemaSyncInEffect` lint sites in `durable-runtime.ts` were hoisted or
  converted to `decodeUnknownEffect`, matching the file’s existing pattern.
- **(h) Defects stay defects by default.** The engine does not convert defects to typed events;
  `withTerminalDefectEvent` (`packages/engine/src/index.ts`) is the exported, documented,
  bounded opt-in that appends a `RunFailed { errorTag: "Defect" }` before rethrowing the cause —
  specified in [runtime.md §10](spec/runtime.md) and adopted by the demo.

## Allowed claim

Every numbered roadmap phase, P0 through P7, is implemented with committed executable evidence,
and the P7 exit gates are discharged as scoped above: safety properties hold under documented,
enumerated assumptions (bounded-instance model checking + certification + chaos/soak + both
real-loss matrices over one shared invariant checker); the security review has zero unowned and
zero critical findings; operators can explain recovery state without editing storage on both
deployment classes; three real internal Agents validate the authoring model; and the Travel
Planner retains its deterministic offline conformance profile alongside opt-in live-model
profiles.

## Non-claims and open governance

- **Roadmap-complete is not 1.0.** The specifications remain **Draft**, the project remains a
  private pre-1.0 internal project, and Effect v4 stays pinned to one exact beta.
- **Open governance items, stated plainly:** ADR-0010 (declared attached Subagents) remains
  formally **Proposed** — both slices are implemented as roadmap-assigned proposed defaults.
  ADRs 0011–0015 and decisions D-029…D-033 are **accepted by default, awaiting owner review**;
  P7 cannot self-accept them, and resolving them is part of roadmap completion that only the
  owner can perform.
- **The live suites were not executed against live credentials in this environment.** They are
  implemented, typechecked, and verified to gate-skip; the exit-gate claim they support is the
  gating pattern plus offline determinism. Live execution remains a release-lane action
  (testing.md §13). “Selected supplier Layers” is scoped to live-model-only — the deterministic
  supplier desk is retained and `liveSupplierLayers` is pinned `false`.
- **No hosted-Cloudflare evidence is claimed.** DC evidence executes on workerd/Miniflare; the
  DC point-in-time-recovery restore remains a manual runbook; the DC admin entry points run the
  possession-default authorizer (FINDINGS SEC-P7-003).
- **The formal claim is bounded.** TLC checked the committed `.cfg` instances of the protocol
  design; nothing is proven about the code beyond the linked test corpus, and 2-worker liveness
  is explicitly not discharged.
- **No exactly-once external side effects** (DUR-003) — unchanged through every phase and pinned
  `false` in every profile.
- **Deferred findings stay visible:** SEC-P7-001 (MCP canonicalization depth bound) and
  SEC-P7-004 (conversationId on submission-scoped authorization requests) carry named gates in
  the findings register.
- **Open-source preparation remains the deferred next chapter** ([ROADMAP](ROADMAP.md)):
  migrations and compatibility windows, retention/deletion, public naming/licensing/publication,
  PostgreSQL and multi-node scheduling, Subagent extensions beyond S1/S2, and channels/UI/hosted
  control plane.
