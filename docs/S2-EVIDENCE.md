# S2 durable attached Subagent evidence

Status: **Implemented** (as the roadmap-assigned proposed default; ADR-0010 remains **Proposed**;
the S2 shape-level defaults are recorded in
[ADR-0013](adr/0013-durable-subagent-establishment.md))

S2 turns the S1 delegation surface into accepted work: a durable parent's delegation Tool Call
establishes a separately admitted child Submission in a distinct child Conversation — reservation,
canonical request, idempotent admission, lineage, Receipt, start link — then suspends
`waitingForChild` without holding a worker permit, wakes durably from the child's Settlement,
verifies and joins the child's canonical outcome atomically with its own `ToolCallSettled`, and
releases the parent-owned budget reservation exactly once. Parent and child fence independently;
recovery reattaches the one existing child from canonical records and never respawns it from
prose (SUB-016/SUB-018). The deployment-class label for this slice is **`DN` durable attached
Subagents** on the tested Node/SQLite assembly; `DC` requires the same suite under Durable Object
eviction and alarms and is deferred to P6. No code path claims exactly-once child external
effects: an unresolved ordinary child Tool stops at a visible Unknown Outcome that blocks the
parent (SUB-021), and the tests assert child model invocation counts rather than assuming them.

## Delivered package surface

- `@effect-agent/core` owns the delegation Tool naming rule (`delegationToolPrefix`,
  `isDelegationToolName`) hoisted from capabilities so the session classifier can recognize a
  delegation call without importing outward (`packages/core/src/subagent.ts`); capabilities
  re-exports it.
- `@effect-agent/engine` owns the durable seam (D1):
  - the `RunSubagentHook` contract on `RunOptions` — `establish`/`join` operations in
    core/engine vocabulary only (`packages/engine/src/run-options.ts`);
  - the per-batch `SubagentDurability` service with an explicit ephemeral default, added to
    `EngineProvidedToolServices` so it never appears in public requirements, plus the
    `ToolCallWaiting` signal, the `AgentChildPending` typed suspension error, and
    `SubagentDurabilityError` (`packages/engine/src/index.ts`);
  - the batch-executor rule that a waiting delegation call does not trigger the batch failure
    policy: siblings run to completion, settled sibling results commit as per-call late-settle
    batches, and the run suspends (`packages/engine/test/subagent-durable-seam.test.ts`);
  - `resume.leadingMessages` so a pending batch resume replays a response record's pre-assistant
    messages (`packages/engine/src/run-options.ts`).
- `@effect-agent/session` owns the durable protocol:
  - four new canonical payloads on the unchanged version-1 envelope — `SubagentRequested`
    (carrying the encoded child input, digests, reservation identity, and intended child
    identity), `SubagentStarted`, `SubagentJoined` (carrying the final accounting decision), and
    the child-log `SubagentLineageRecorded` (`packages/session/src/records.ts`);
  - deterministic identities derived from the parent Run and Tool Call pair —
    `subagent-requested:`/`subagent-started:`/`subagent-join:`/`subagent-lineage:` record and
    batch families plus `childConversationIdFor`/`childIdempotencyKeyFor`
    (`packages/session/src/run-journal.ts`) and `childReservationIdFor`
    (`packages/session/src/durable-runtime.ts`); all four tags are prompt-transparent;
  - reducer projections: the parent-side `subagentInvocations` requested/started/joined fold and
    the child-side `parentLink` (`packages/session/src/reducer.ts`);
  - ledger port extensions (D2/D6/D8) — `ParentLinkage` on admission, the tri-state
    `AdmissionResolution` returned by `resolveAdmission`, the `WaitingForChildSuspension` member
    of the additive `SuspensionReason` union, the cross-lane `recordChildSettled` wake, and the
    generic child budget reservation state machine `reserveChildBudget` →
    `attachChildToReservation` → `beginChildBudgetRelease` → `releaseChildBudget`
    (`packages/session/src/ledger.ts`), with nine new adapter-neutral conformance cases
    (twenty-nine total) (`packages/session/src/ledger-conformance.ts`);
  - the extended recovery classifier: `openDelegationCalls` evidence separated from
    `openToolCalls` (delegation calls never mark Unknown), nine new decisions
    (`CompleteChildAdmission`, `AwaitChildAdmissionResolution`, `RepairSubagentStartLink`,
    `EnsureWaitingForChild`, `AwaitChildSettlement`, `ResumeWaitingParent`,
    `ApplyJoinAccounting`, `PropagateChildAbort`, `ReleaseOrphanChildReservation`), and the
    changed precedence rows for abort-with-attached-children and suspended `WaitingForChild`
    (`packages/session/src/recovery.ts`);
  - the coordinator (D3/D4/D5/D11): subagent hook closures in `runModel`, the
    `waitingForChild` suspension seam in `runAttempt`, `recordChildSettled` drive-forward on
    every settlement-finalization path, the atomic `[SubagentJoined, ToolCallSettled]` join
    batch, request-abort-and-join propagation (one canonical `AbortRequested` authored
    `subagent-parent-abort`; the delegate settles `{errorTag: "SubagentParentAborted"}` and the
    parent settles aborted strictly after all joins), binding-free recovery executors, and the
    conservative accounting bases (`reserved-conservative`, `aborted-conservative`,
    `orphan-zero-consumed`) (`packages/session/src/durable-runtime.ts`);
  - the host-supplied binding resolution port (D7): `AgentBindingResolver`,
    `DurableWorkerBinding.make(binding, digests)` and `makeDigestTransparent`, `ResolvedBinding`,
    `definitionDigestsEqual`, and the typed fail-closed `DurableBindingFailure` family
    (`BindingUnavailable` | `BindingDigestMismatch`)
    (`packages/session/src/binding-resolver.ts`), consumed by the new
    `runResolvedWorker`/`processConversationResolved` coordinator operations — a parent-linked
    head whose Binding is unavailable or digest-mismatched settles the framework
    `ChildCompatibilityFailure` without running application code, while a root head is refused
    typed (`packages/session/src/durable-runtime.ts`);
  - eleven new coordinator failpoints, `subagent:after-reserve` through
    `subagent:after-release` (`packages/session/src/durable-failpoint.ts`).
- `@effect-agent/storage-memory` and `@effect-agent/storage-sqlite` implement the extended
  ledger port; SQLite migration `4_durable_subagents` adds the parent-linkage columns and the
  `effect_agent_child_reservations` table with before/after failpoints on every new mutation
  (`ledger:child-reservation`, `ledger:child-attach`, `ledger:child-release-pending`,
  `ledger:child-release`, `ledger:child-settled`)
  (`packages/storage-sqlite/src/migrations.ts`, `packages/storage-sqlite/src/errors.ts`,
  `packages/storage-sqlite/src/sqlite-ledger.ts`, `packages/storage-memory/src/memory-ledger.ts`).
- `@effect-agent/capabilities` owns the durable handler branch of `SubagentRuntime.layer`
  (D1/D5): `SubagentRuntimeOptions.durable.targetDigests` fixes the child Binding digest
  authority at Layer construction, durable mode establishes through the engine-provided service
  and never spawns an in-process child fiber, join runs `projectResult` over the verified child
  output with conservative `SubagentDurableAccounting`, failed/aborted/compatibility-failed
  children join as the bounded `SubagentExecutionFailure` (no raw Cause), and absence of the
  durable coordinator falls back honestly to the S1 ephemeral spawn
  (`packages/capabilities/src/subagent.ts`).
- `@effect-agent/platform-node` wires `NodeDurableRuntimeOptions.bindings` into
  `AgentBindingResolver.layer` (empty default: every claim fails closed) and exposes
  `NodeDurableHost.runResolvedWorkers` over the resolver-backed worker loop
  (`packages/platform-node/src/layers.ts`, `packages/platform-node/src/host.ts`).
- `@effect-agent/testing` extends the cumulative Travel Planner with the S2 fixture: the S1
  coordinator → `destination-researcher` delegation re-run as accepted work on SQLite, with the
  pinned `TravelPlannerSubagentDurabilityProfile`/`s2TravelPlannerProfile`, byte-for-byte digest
  pairing (`s2CoordinatorDigests`, `s2ResearcherDigestStrings`), and the invocation-counting
  child model Layers behind `makeDurableResearchHarness`
  (`packages/testing/src/fixtures/travel-planner/subagents-durable.ts`,
  `packages/testing/test/travel-planner-subagents-durable.test.ts`).

## Executable exit-gate evidence (spec §17 S2 deliverables)

| Spec §17 S2 deliverable                                     | Deterministic evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Child accepted-work admission and Receipt                   | `packages/testing/test/durable-subagents.test.ts` — “establishes the child, suspends waitingForChild without a worker permit, and joins the settled child”; conformance — “admit records and replays parent linkage”; `packages/testing/test/travel-planner-subagents-durable.test.ts` — “re-runs the S1 delegation as accepted work on SQLite: establish, waitingForChild without a permit, durable wake, verified join, conserved reservation”                                                                                                                                        |
| Parent Link and exact binding resolution                    | `SubagentLineageRecorded` + `AgentBindingResolver` exact-digest resolution; session — “folds the parent-side invocation view and the child-side lineage into the projection” (`packages/session/test/session.test.ts`); durable-subagents — “a resolver digest mismatch writes ChildCompatibilityFailure without running child code”; travel-planner-subagents-durable — “a fabricated child admission at the derived child identity fails Parent Link verification fail-closed (IDOR, D10)”                                                                                            |
| Requested/started/joined canonical records                  | `packages/session/test/session.test.ts` — “round-trips the four new payload tags through the version-1 record envelope”, “rejects malformed subagent payloads (invalid identity, digest, size, outcome, and depth)”; `packages/session/test/run-journal.test.ts` — “subagent lifecycle records are prompt-transparent (S2, spec §5/§11)”, “derives the record, batch, and child identities from the parent Run and Tool Call pair”                                                                                                                                                      |
| Parent-owned reservation state machine and recovery repair  | conformance — “replays an identical child reservation and rejects a divergent allocation digest”, “beginRelease freezes the accounting decision exactly once”, “release returns unused allocation exactly once”, “attachChildToReservation is idempotent and rejects a divergent child”; durable-subagents — “a reservation without a request under abort releases exactly once”; crash — “a reservation orphaned before its request releases exactly once under parent abort”                                                                                                          |
| Authoritative cross-Conversation admission lookup           | conformance — “resolveAdmission distinguishes notAdmitted from admitted authoritatively”; `packages/storage-memory/test/memory-ledger.test.ts` — “answers Indeterminate from the fault seam and never treats it as absence”; durable-subagents — “an indeterminate admission resolution never admits a second child”; classifier — “requested with an indeterminate admission waits and never admits a second child (row 4, SUB-031)”                                                                                                                                                   |
| Parent `waitingForChild` suspension, permit release, wakeup | engine — “a waiting delegation call does not trigger the batch failure policy”, “siblings run to completion before the run suspends”; conformance — “recordChildSettled wakes only when every listed child settled”, “suspend returns resume-immediately when children already settled”; durable-subagents — “a dropped child-settlement wake is replayed by ResumeWaitingParent”; travel-planner-subagents-durable — “workerConcurrency=1: the suspended parent frees the single worker, which runs the child to Settlement and the woken parent joins (spec §12 smallest-pool proof)” |
| Child Settlement join and parent Tool settlement            | the atomic `subagent-join:` batch (`SubagentJoined` + `ToolCallSettled`); capabilities — “joins the settled child output through projectResult with conservative accounting”, “fails closed when the settled child output escapes the target schema”; travel-planner-subagents-durable happy path asserts per-dimension conservation (`consumed + released == allocation`), basis `reserved-conservative`, and `usageSummary {turns: 2, toolCalls: 1}` from canonical child evidence                                                                                                    |
| Independent parent/child ownership and fencing              | conformance — “a stale parent token cannot transition a child reservation”; classifier — “a child Submission with parentLinkage classifies through the ordinary rows under its own fence (SUB-020)”; crash — “a stale parent resumed past its replacement is fenced out of the join and the child stays untouched”, “fencing the child's stale Attempt leaves the waiting parent's lane and epoch untouched”                                                                                                                                                                            |
| Durable abort propagation (request-abort-and-join, §13.1)   | durable-subagents — “request-abort-and-join settles the parent aborted only after every join”; travel-planner-subagents-durable — “request-abort-and-join: the aborted child joins as the framework failure and the parent settles aborted strictly after the join (spec §13.1)”; crash — “kill at abort:after-intent with a waiting child: one idempotent abort command, joins before the aborted settlement”, “kill at subagent:after-child-abort-intent: the replayed propagation is a no-op, never a second command”                                                                |
| Establishment/join failpoints and process-kill tests        | eleven coordinator failpoints (`packages/session/src/durable-failpoint.ts`) + five before/after SQLite mutation pairs; sqlite — “leaves a recovery-classifiable state at every S2 ledger failpoint”; durable-subagents — “every establishment failpoint converges on one child Receipt and Conversation”; the ten-row real-process-kill suite `packages/platform-node/test/crash/crash-subagents.test.ts` (“S2 durable Subagent crash matrix (real process kills)”)                                                                                                                     |
| Node/SQLite conformance before Cloudflare                   | both adapters run the same twenty-nine-case conformance suite (`packages/storage-memory/test/memory-ledger.test.ts`, `packages/storage-sqlite/test/sqlite-ledger.test.ts`); sqlite additionally proves “persists child reservations and parent linkage across reopen”; the fixture pins `cloudflareEquivalence: false` — the `DC` row is P6 scope                                                                                                                                                                                                                                       |

## Durability gate evidence (spec §16.4)

| §16.4 gate                                                    | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Crash matrices run against the executable recovery model      | pure classifier rows for every §13 state (`packages/session/test/recovery-classifier.test.ts`, describes “S2 subagent establishment rows (spec §13)”, “S2 changed-precedence pins (plan §4.3)”, “S2 abort rows (spec §13.1)”); the in-process failpoint sweep (`packages/testing/test/durable-subagents.test.ts`); the real-process-kill matrix (`packages/platform-node/test/crash/crash-subagents.test.ts`)                                               |
| Duplicate admission → one child Receipt and Conversation      | durable-subagents — “every establishment failpoint converges on one child Receipt and Conversation”; travel-planner-subagents-durable — “a kill at subagent:after-request-append resumes to the same child Receipt and one child Conversation (SUB-016/SUB-017)”; crash — “every establishment failpoint kill converges on one child Receipt, Conversation, and join”                                                                                       |
| Independent fencing of stale writers                          | crash — “a stale parent resumed past its replacement is fenced out of the join and the child stays untouched” and “fencing the child's stale Attempt leaves the waiting parent's lane and epoch untouched” (the stale child's answer never becomes canonical); conformance — “a stale parent token cannot transition a child reservation”                                                                                                                   |
| Completed child never re-executed on lost join acknowledgment | durable-subagents — “a kill at subagent:after-join-append replays the accounting and never re-executes the child”; travel-planner-subagents-durable — “a lost join acknowledgment replays the accounting and never re-executes the completed child” (three failpoints); crash — “join and release failpoint kills replay the canonical accounting and never re-execute the child” (file-backed child model count stays exactly 1)                           |
| `notAdmitted`/`admitted`/`indeterminate` admission lookup     | conformance — “resolveAdmission distinguishes notAdmitted from admitted authoritatively”; memory adapter fault seam — “answers Indeterminate from the fault seam and never treats it as absence”; durable-subagents — “an indeterminate admission resolution never admits a second child”; classifier — “requested without an admission answer stays fail-closed at the authoritative wait (SUB-031)”                                                       |
| Waiting parents release permits and wake durably              | travel-planner-subagents-durable — “workerConcurrency=1: the suspended parent frees the single worker, which runs the child to Settlement and the woken parent joins (spec §12 smallest-pool proof)”; crash — “workerConcurrency=1: the suspension frees the single worker, the child runs, and the woken parent joins” over `NodeDurableHost.runResolvedWorkers`; durable-subagents — “a dropped child-settlement wake is replayed by ResumeWaitingParent” |
| Reservation/request/admission/abort/join/release failpoints   | the durable-subagents failpoint sweep plus “a reservation without a request under abort releases exactly once”; crash — “kill between the child settlement finalize and the parent wake: recovery replays the wake” and the join/release rows; classifier rows for `subagent:after-reserve` through `subagent:after-release`                                                                                                                                |
| Unknown child Tool outcome blocks the parent, visible         | travel-planner-subagents-durable — “an unknown child Tool outcome blocks the parent; resolveUnknown from a second runtime handle converges the child, wakes the parent, and joins”; classifier — “a child blocked at an unknown ordinary outcome keeps the parent waiting with a visible obligation (row 8, SUB-021)” (aging and alerting remain P7 — see the scoping below)                                                                                |
| Simultaneous parent/child process loss converges              | crash — “simultaneous SIGKILL of the parent and child workers converges on one link, one Settlement, one join”                                                                                                                                                                                                                                                                                                                                              |
| Node/SQLite and Cloudflare run the same Subagent suite        | scoped honestly: the shared adapter-neutral conformance suite runs on both current adapters (memory reference and SQLite); no Cloudflare adapter exists yet, so `DC` is not claimed and the P6 phase owns running the same suite under eviction/alarms                                                                                                                                                                                                      |
| No durable-Subagent claim while a required crash test skips   | no S2 suite contains a skipped test; the crash matrix runs real `process.exit(137)` kills over one SQLite file per row                                                                                                                                                                                                                                                                                                                                      |

The recovery classifier covers every new §13 row as a pure decision over persisted snapshots,
including the three deliberate precedence changes: “abort with a nonterminal attached child
propagates instead of settling (SUB-022, §13.1)”, “an open delegation call never marks Unknown
while an ordinary sibling still does”, and “suspended WaitingForChild with all children settled
resumes the waiting parent” (`packages/session/test/recovery-classifier.test.ts`).

## D10 authorization scoping (plan decision point 10)

Spec §6.3 and §16.3 require authenticated, per-read observation authorization. S2 scopes its
security claim honestly, exactly like Phase 5's DUR-017 precedent:

- **Authorization in S2 is service possession.** `submit`, `observe`, `abort`,
  `resolveUnknown`, and `resolveApproval` are operations on the `DurableAgentRuntime` service;
  whoever a deployment hands the service to is the trust boundary. There is no authenticated
  Principal check inside the runtime.
- **The S2 additions are structural, and they are tested.** An identifier is never a capability:
  the join verifies the child's canonical Settlement against the recorded Parent Link, target
  identity, and stored digests, so a fabricated child or parent reference fails closed
  (“a fabricated child admission at the derived child identity fails Parent Link verification
  fail-closed (IDOR, D10)”); admission linkage is immutable on replay (conformance — “admit
  records and replays parent linkage”); and observation stays Conversation-scoped — a Receipt
  for an unrelated Conversation observes nothing of the child log (“a Receipt for an unrelated
  Conversation observes nothing of the child log (D10 honest scope)”, both in
  `packages/testing/test/travel-planner-subagents-durable.test.ts`).
- **Authenticated per-read authorization, redaction policy, obligation aging, and alerting are
  Phase 7 deliverables.** Until then, a deployment claiming durable Subagent liveness must supply
  its own operational path to the resolution operations, as with DUR-017.

## Stored-version policy

The SQLite storage version is now **4** (migration `4_durable_subagents`: submission
parent-linkage columns and the `effect_agent_child_reservations` table). The check remains
exact-or-zero: a fresh file initializes to version 4; v1–v3 development files — and any newer
version — fail with typed reset guidance before any mutation (D7/D-015;
`packages/storage-sqlite/test/sqlite-ledger.test.ts` — “rejects v1-v3 files exactly with reset
guidance and still rejects newer versions”). No migration tooling exists or is promised. The
canonical record envelope stays at `schemaVersion: 1` because the four new payloads are additive
tags on the existing union. Checkpoints whose persisted projection lacks the new subagent fields
decode-fail and are rebuilt from canonical records (`packages/session/test/session.test.ts` —
“rejects a Phase 5 checkpoint projection state so callers rebuild from canonical records”).

## Allowed claim

**`DN` durable attached Subagents** (spec §17 S2), pinned executable by
`s2TravelPlannerProfile`: `deploymentClass: "DN"`, `durableAttachedSubagents: true`,
`subagentReplaySafe: true` — with the explicit non-claims
`childExternalEffectsExactlyOnce: false` and `cloudflareEquivalence: false`
(`packages/testing/src/fixtures/travel-planner/subagents-durable.ts`). `DC` durable Subagents
require the same suite under Durable Object eviction and alarms and remain P6 scope.

## Governance status

[ADR-0010](adr/0010-declared-attached-subagents.md) is formally unresolved: the owner has not
accepted it. Per `AGENTS.md`, implementation proceeded against the proposed default because the
roadmap recommendation assigned the S2 slice between Phase 5 and Phase 6 and the surface remains
locally reversible. The S2 shape-level decisions (D1–D11) are recorded in
[ADR-0013](adr/0013-durable-subagent-establishment.md) with status **Accepted by default**,
pending owner review, and in the register as
[D-031](DECISIONS.md#d-031--durable-subagent-establishment-waiting-suspension-and-binding-resolution).

## Non-claims

- **No exactly-once child external effects** (DUR-003, spec §1). Child ordinary Tools follow the
  P5 uncertainty protocol; an unprovable child effect stops at `UnknownToolOutcome` and blocks
  the parent rather than replaying. The never-re-executed claim is specifically about the child
  _Submission_ after a lost join acknowledgment, and it is asserted with invocation counters.
- **`DC` is not claimed.** No Cloudflare adapter exists; the P6 phase owns running the same
  Subagent conformance and crash suites under eviction and alarm retries.
- **Durable failed children join as the bounded framework failure** (D5, spec §4.2 option 2):
  the child's typed failure union does not survive its Settlement, so `mapChildFailure` remains
  the ephemeral-path contract and durable joins carry `SubagentExecutionFailure`
  (classification, child references, bounded tag/message, no raw Cause). Schema-declared durable
  domain-failure mapping is a recorded later extension.
- **Token and cost accounting is conservative** (D11, spec §7): structural dimensions (turns,
  tool calls) come from canonical child evidence; dimensions without reported usage consume
  their full reservation at join (`basis: "reserved-conservative"`), and abort/orphan paths use
  the `aborted-conservative`/`orphan-zero-consumed` bases. Unreported usage never creates budget.
- **No authenticated Principal/Tenant authorization, obligation aging, or alerting** — see the
  D10 scoping above; P7 scope.
- **Depth stays 1**: every nested delegation is rejected at preflight (SUB-029); detachment,
  child Conversation reuse, follow-ups to a child, and handoff remain later proposals.
- **The durable attachment store stays deferred**: child readiness uses the existing
  `markReady` path; the `AttachmentStore` port non-claim from Phase 4 carries forward.
