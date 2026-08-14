# Formal model correspondence

Status: Descriptive (P7 deliverable). The specifications model the _protocol
design_; the claim they support is "the protocol satisfies these properties on
bounded instances under the stated abstractions". The claim about the _code_
remains the executable test corpus, which this document links per action and
per invariant.

Both specifications are PlusCal algorithms (the `(* --algorithm ... *)`
blocks) with their mechanical TLA+ translation committed in the same file.
Regenerate the translation with `pcal.trans` after editing the algorithm; see
[README.md](./README.md).

## 1. `DurableSubmission.tla`

One Conversation lane, `Subs` FIFO Submissions, `Workers` durable workers, a
recovery pass, the DUR-017 resolution dependency, a client, an aborter, and
lease expiry. Crash is a nondeterministic branch at every PlusCal label —
i.e. between any two durable mutations — bounded by `MaxFaults`.

### 1.1 Action → coordinator function / ledger operation

All code references are `packages/session/src/durable-runtime.ts` (coordinator)
and `packages/session/src/ledger.ts` (port contract) unless noted.

| Model action                 | Corresponds to                                                                                                                                                                           |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CAdmit`                     | `DurableAgentRuntime.submit` → `SubmissionLedger.admit` (atomic admission row, queue sequence = `Subs` index)                                                                            |
| `CMat`                       | `submit` → `ConversationStore.materialize` + `ensureConversationCreated`                                                                                                                 |
| `CReady`                     | `submit` → `SubmissionLedger.markReady` (idempotent `admitted → ready`; no-op on later states)                                                                                           |
| `WIdle` (claim)              | `SubmissionLedger.claim`: FIFO-head rule, atomic producer-epoch bump (DUR-006), fresh ownership + lease                                                                                  |
| `WResume` dispatch           | `drainConversation` resume dispatch over canonical evidence + ledger row                                                                                                                 |
| `WInput`                     | `applyCanonicalInput`: fenced append of the deterministic `input:{sid}` record (batch idempotency, DUR-007)                                                                              |
| `WMarkInput`                 | `SubmissionLedger.markInputApplied` (ownership-token-guarded marker)                                                                                                                     |
| `JoinOrRespond` (respond)    | one Turn's model invocation; a complete response commits atomically (durability §9; partial streams are re-invoked, so the model lets an uncommitted response be re-chosen)              |
| `JoinOrRespond` (join)       | `SubmissionLedger.claimJoining`: contiguous strictly-later ready prefix under the host token                                                                                             |
| `WJoinDeliver`               | joined-input append (`durable-runtime.ts` join delivery) or `revertJoining` when the abort intent precedes consumption (revert-then-abort)                                               |
| `WJoinMark`                  | `SubmissionLedger.markJoined`: strictly `joining → joined` under the recorded host linkage; fails on a reverted row (`storage-sqlite` `markJoined` linkage/state checks)                 |
| `WPrepare`                   | fenced `ToolCallPrepared` append (durability §10)                                                                                                                                        |
| `WExec`                      | the ordinary tool's EXTERNAL effect — deliberately unfenced: a superseded Attempt can still fire it; that is why Unknown exists (DUR-009)                                                |
| `WSettleCall`                | fenced `ToolCallSettled` append after output validation                                                                                                                                  |
| `WApprReq`                   | fenced `ToolApprovalRequested` append (ADR-0012: the request record is the suspension's entire canonical footprint)                                                                      |
| `WSuspend`                   | `SubmissionLedger.suspend(ApprovalPending)`, including the `resume-immediately` covered-reason outcome                                                                                   |
| `WMarkUnknown`               | worker-side `reconcileOpenCalls` → `markCallsUnknown` (no reconciler proof available in the model)                                                                                       |
| `WAbortAudit`                | `settleAborted` → `appendUnknownRecords`: `ToolCallUnknown` audit for open calls; abort never asserts rollback (durability §13)                                                          |
| `WAbortReserve` / `WReserve` | `SubmissionLedger.reserveSettlement` (one exact outcome, DUR-011)                                                                                                                        |
| `WSettApp`                   | fenced append of the EXACT reserved `SubmissionSettled` record (DUR-011 step 2)                                                                                                          |
| `WFin`                       | `SubmissionLedger.finalizeSettlement` (token-free; canonical history authorizes, DUR-015)                                                                                                |
| `WJoinedSettle`/`WJoinedApp` | `settleJoinedSubmissions`/`settleOneJoined`: each joined Submission owed its own settlement with the host outcome                                                                        |
| `RScan` decisions            | `runRecovery` → `classifyRecovery` (the `Classify` operator mirrors `recovery.ts` rows 1–8 and 10–12 verbatim) → `executeRecoveryDecision`, one idempotent durable step per iteration    |
| `ResLoop` (approve)          | `resolveApproval` → `SubmissionLedger.recordApprovalDecision` (+ wake)                                                                                                                   |
| `ResLoop` (resolve)          | `resolveUnknown` → `SubmissionLedger.recordUnknownResolution`; the resolver consults the external truth (`extEffect`), so `NeverHappened` is only issued for a provably-unstarted effect |
| `AbLoop`                     | `abort` → `SubmissionLedger.requestAbort` (idempotent; refused for `joined` targets — `JoinedToHost`)                                                                                    |
| `ExpLoop`                    | lease expiry (D5): a liveness hint that creates the stale-Attempt scenario fencing exists for                                                                                            |
| crash branch at any label    | process loss between two durable mutations — the same boundary set as `DurableRuntimeFailpointLocation` plus the storage adapters' `*:before` failpoints                                 |

### 1.2 Invariant / property → executable evidence

| Model property                 | Requirement                                                              | Executable evidence                                                                                                                                                                                                                                       |
| ------------------------------ | ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ExactlyOneSettlement`         | DUR-002/DUR-011                                                          | ledger conformance (`packages/session/src/ledger-conformance.ts`: settlement reservation/finalization/conflict cases); crash matrices `packages/platform-node/test/crash/crash.test.ts`; `verifyConversationInvariants` single-canonical-settlement check |
| `FIFOPerLane`                  | DUR-004                                                                  | conformance FIFO claim cases; `verifyConversationInvariants` FIFO input/settlement order check; `packages/testing/test/durable-*.test.ts` ordering assertions                                                                                             |
| `NoLostAcceptedWork`           | DUR-014                                                                  | classifier totality tests (`packages/session/test/recovery.test.ts`); `scanNonterminal` conformance; `scanObligations` (admin) visibility                                                                                                                 |
| `UnknownBlocksContinuation`    | DUR-009/DUR-017                                                          | `durable-tools.test.ts` unknown-outcome suites; failpoint sweep rows `tools:after-prepared-append`; resolution-path tests                                                                                                                                 |
| `FencingSafety`                | DUR-006                                                                  | conformance stale-fence cases (`stale producer fence`, `ownership loss and reclaim`); crash-matrix stale-epoch rows; DC eviction tests                                                                                                                    |
| `JoinConservation`             | DUR-016                                                                  | `durable-join.test.ts` joining/joined failpoint sweep (`join:after-claim`, `join:after-canonical-append`); conformance revert/mark-joined cases                                                                                                           |
| `NoFabricatedToolResult`       | DUR-009 (“the engine must not manufacture an error result and continue”) | unknown-outcome + reconciliation tests; red-team supplier suites (WP5)                                                                                                                                                                                    |
| `EventuallySettled` (liveness) | durability §1 conditional promise                                        | crash-matrix convergence assertions (“terminalizing work eventually settles”); chaos convergence runs (WP4)                                                                                                                                               |

### 1.3 Findings confirmed by TLC (kept in the model deliberately)

1. **Aborted-without-execution FIFO exemption.** An accepted-but-inactive
   Submission may settle aborted with no canonical input (durability §13), so
   a later Submission's input can exist while an earlier aborted one's never
   does. `FIFOPerLane`'s input clause carries exactly that exemption; the
   settlement clause is strict — TLC confirms recovery defers every non-head
   repair (`claimFor`'s FIFO gate).
2. **Reverted-join `markJoined` rejection.** A lease-expired-but-alive host
   racing recovery's `RevertJoining` must NOT be able to mark the reverted
   row joined. The model initially allowed it and TLC produced the linkage
   corruption; the committed model mirrors the adapter's strict
   `joining → joined` + host-linkage check (`storage-sqlite` `markJoined`),
   under which the property holds.
3. **Benign stale re-mark of a resolved lane.** A superseded Attempt can
   reach the ownership-free `ledger.markUnknown` after recovery already
   applied a covering resolution (its canonical audit append dedupes as an
   identity conflict — `markCallsUnknown` swallows `AppendConflict`). The
   lane transiently returns to `unknown` with no open call and classifier
   row 7 wakes it immediately. Safety holds; the wobble is bounded. Candidate
   WP7 hygiene: `markUnknown` could re-validate open-ness, but ledger
   idempotence + the classifier already absorb it.

## 2. `SubagentEstablishment.tla`

One parent Submission with one delegation Tool Call and one child lane:
the S2 establishment ladder, `waitingForChild`, at-least-once
`recordChildSettled`, canonical-settlement joins, the budget-reservation
lifecycle, and request-abort-and-join (spec/subagents.md §12–§14).

### 2.1 Action → coordinator function / ledger operation

| Model action                                  | Corresponds to                                                                                                                                                                                                         |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PRespond`                                    | the parent Turn committing the delegation-declaring response                                                                                                                                                           |
| `PReserveBudget`                              | `SubmissionLedger.reserveChildBudget` (idempotent get-or-create under the parent claim)                                                                                                                                |
| `PReqApp`                                     | canonical `SubagentRequested` append (deterministic identity)                                                                                                                                                          |
| `PResolveAdm`                                 | `SubmissionLedger.resolveAdmission` (SUB-031 tri-state; `Indeterminate`/transport failure bounded by `MaxRouteFaults`, then the authoritative owner answers)                                                           |
| `PAdmit`                                      | `establishChildFromRequest` → child-lane `SubmissionLedger.admit` with immutable `ParentLinkage`                                                                                                                       |
| `PMat`                                        | child `ConversationStore.materialize` + `ensureConversationCreated`                                                                                                                                                    |
| `PLineage`                                    | `ensureChildLineage`: deterministic `SubagentLineageRecorded` append (the stale-tail refresh + record-identity dedupe of `durable-runtime.ts` `appendBatch`/`ensureChildLineage` is folded into one idempotent action) |
| `PReady`                                      | child `SubmissionLedger.markReady` + `wake.notify`                                                                                                                                                                     |
| `PStart`                                      | canonical `SubagentStarted` append (SUB-017 exact deterministic link)                                                                                                                                                  |
| `PSuspend`                                    | `SubmissionLedger.suspend(WaitingForChild)` incl. `resume-immediately`                                                                                                                                                 |
| `PJoin`                                       | `verifySettledChild` + the atomic `SubagentJoined` + parent `ToolCallSettled` batch (child canonical Settlement is the only cross-lane authority)                                                                      |
| `PRelBegin` / `PRelease`                      | `beginChildBudgetRelease` (freeze from the canonical join) / `releaseChildBudget`                                                                                                                                      |
| `PAbortOrphan`                                | `beginChildBudgetRelease` with the deterministic zero-consumed decision (`releaseOrphanReservation`)                                                                                                                   |
| `PAbortChild`                                 | `abortAttachedChildren`: the one idempotent durable child abort command                                                                                                                                                |
| `PAbortWait`                                  | the §13.1 wait: the parent never settles past an unjoined child                                                                                                                                                        |
| `CInput`/`CTurn`/`CReserve`/`CSettApp`/`CFin` | the child as a normal Submission on its own lane (SUB-020); windows inside these are proven in `DurableSubmission.tla`                                                                                                 |
| `CNotify`                                     | `SubmissionLedger.recordChildSettled` routed to the parent lane: at-least-once (lost deliveries bounded by `MaxRouteFaults`; recovery's `ResumeWaitingParent` replays the wake)                                        |
| `RScan` parent rows                           | `classifyDelegationRepairs` / `classifyDelegationAbort` (`recovery.ts`), executed one idempotent step per iteration                                                                                                    |
| `RScan` child rows                            | `classifyRecovery` rows for the child lane, including row 11 (`CompleteMaterialization`/`RepairReadiness`) — the §7(a) race point                                                                                      |

### 2.2 Invariant / property → executable evidence

| Model property                                       | Requirement              | Executable evidence                                                                                                                                                                                                                                                                                                                                              |
| ---------------------------------------------------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OneChildPerToolCall`                                | SUB-016/SUB-031          | `durable-subagents.test.ts` establishment idempotency + admission-resolution rows; `subagents-cross-do.test.ts`; crash rows `subagent:after-request-append`/`subagent:after-admit`                                                                                                                                                                               |
| `JoinRequiresChildSettlement`                        | SUB-019/SUB-023          | join verification fail-closed tests (fabricated-child IDOR pin in `travel-planner-subagents-durable.test.ts`); `crash-subagents.test.ts` join rows                                                                                                                                                                                                               |
| `ReservationConservation`                            | spec §12 step 6, §14     | reservation-lifecycle conformance (`ChildReservationConflict` cases); failpoints `subagent:after-release-pending`/`subagent:after-release`                                                                                                                                                                                                                       |
| `AbortJoinsBeforeParentSettles`                      | SUB-022, §13.1           | request-abort-and-join suites in `durable-subagents.test.ts`; crash rows `subagent:after-child-abort-intent`                                                                                                                                                                                                                                                     |
| `EstablishmentOrder`                                 | spec §12 ladder          | establishment failpoint sweep (`subagent:after-reserve` → `subagent:after-start-append`)                                                                                                                                                                                                                                                                         |
| `ChildTurnRequiresLineage`                           | plan §7(a)               | implemented by WP7: the `AwaitParentEstablishment` classifier decision plus the worker-claim lineage gate (`recovery.ts` row 11, `durable-runtime.ts` `drainConversation`), pinned cross-Object by `subagents-cross-do.test.ts` `"a child never runs a Turn before its lineage record is canonical"` and by the classifier rows in `recovery-classifier.test.ts` |
| `ParentEventuallySettles` / `ChildEventuallySettles` | durability §1 + spec §13 | S2 crash-matrix convergence; DC subagent matrix convergence                                                                                                                                                                                                                                                                                                      |

### 2.3 The §7(a) establishment race, model-checked

Current discipline (`AwaitParentEstablishment = FALSE`): the child lane's own
recovery classifies its `admitted` Submission through `recovery.ts` rows 11
(`CompleteMaterialization` → `RepairReadiness`) without consulting lineage.
TLC (`SubagentEstablishmentRace.cfg`) finds the 17-state interleaving: parent
commits `SubagentRequested` and admits the child, crashes before
`ensureChildLineage`; child recovery materializes and marks ready; the child
claims, applies input, and runs a Turn with no canonical lineage record.
This is ordering/liveness hygiene, not a safety hole — the coordinator's
stale-tail refresh tolerates the concurrent lineage append and the join fails
closed without lineage — and the four protocol invariants plus both liveness
properties hold under the current discipline (`SubagentEstablishment.cfg`).

Fix discipline (`AwaitParentEstablishment = TRUE`, `SubagentEstablishmentFix.cfg`):
a parent-linked `admitted` child without canonical lineage defers its own
materialization/readiness repair (the `AwaitParentEstablishment` classifier
decision); the parent's idempotent establishment completes it. TLC proves the
race is eliminated and liveness is preserved. WP7 implemented exactly this
discipline after the model check (plan §7(a)): the classifier row plus the
worker-claim lineage gate in `drainConversation` (the claim head rule legally
grants an `admitted` head, so the worker path must enforce the same deferral
the classifier names — the implementation closes both entrances to the race).

## 3. Abstraction assumptions (both specifications)

1. **Ledger operations are atomic actions.** Every `SubmissionLedger`
   operation is one transactional write in the adapters (SQLite transactions,
   DO storage); the model gives each its own label so crash falls between,
   not inside, operations. The adapters' own `*:before`/`*:after` failpoint
   suites cover intra-operation atomicity.
2. **Recovery reads are strongly consistent** (STORE-003): `Classify` reads
   the current state directly.
3. **Digests, byte-identity, and schema validation are abstracted.** Batch
   and record identity model the digest chain's dedupe effect; corrupt-store
   behavior (Byzantine storage) is out of scope — `verify` (admin) and the
   conformance corruption cases own that.
4. **Time is abstracted.** Lease expiry is a nondeterministic action; a
   crashed worker's lease is cleared with the crash (its expiry is only a
   delay). Premature expiry of a live owner is modeled explicitly — that is
   the stale-Attempt scenario.
5. **Fault budgets bound the state space and condition liveness.** Crashes
   and premature expiries (`MaxFaults`), and Indeterminate answers/lost wakes
   (`MaxRouteFaults`), eventually cease. This is the documented durability §1
   conditional promise — liveness is claimed only under weak fairness of
   worker/recovery/resolver actions plus these budgets, exactly the DUR-017
   resolution-dependency assumption.
6. **One model turn per Run; outcomes `completed`/`aborted`.** `failed` is
   protocol-identical to `completed` for ordering/settlement purposes;
   multi-turn Runs repeat the proven Turn window.
7. **Approval decisions always approve** in `DurableSubmission.tla` (denial
   is a settled-without-execution variant covered by engine tests); the
   abort path denies via `settleAbortedForRecovery`'s deny-and-wake, which
   IS modeled.
8. **Recovery acts only on quiet lanes** (no live lease). Deployed recovery
   triggers on worker loss/lease expiry; recovery racing a live owner is
   covered by ledger idempotence + fencing in the executable suites. Fenced
   recovery repairs fold claim+mutation+release into one action (rotating
   ownership like `claimFor`).
9. **`SubagentEstablishment.tla` does not re-model epoch fencing.** Each lane
   has one claim at a time and crash clears it; ownership rotation and
   stale-Attempt fencing are proven in `DurableSubmission.tla`. Cross-lane
   appends (lineage) are modeled as idempotent single actions per the
   stale-tail refresh + record-identity dedupe.
10. **Resolution types.** `CompletedWithResult` and `NeverHappened` are
    modeled (the resolver consults the external truth); `SafeToRetry` is
    behaviorally `NeverHappened`-shaped for the model's single call, and
    `AbortSubmission` is covered by the abort path.
11. **In `DurableSubmission.tla` the joining claim is not re-attempted against
    an abort-intended row** (the coordinator claims joining input once per
    Turn boundary and reverts an aborted claim; unbounded re-claiming without
    Turn progress is an unfair schedule, not a real drive loop). Abort
    arriving between claim and delivery still exercises the revert path.

## 4. What the model does NOT claim

- Nothing about the engine's Turn internals (streaming, tool batches wider
  than one call, compaction) — testing.md §3 owns those.
- Nothing about storage adapters' internal transactions — conformance owns
  those (TEST-004).
- No exactly-once external execution claim anywhere (`WExec` is deliberately
  unfenced; DUR-003).
- No unbounded-instance proof: TLC checks the committed `.cfg` bounds only.
