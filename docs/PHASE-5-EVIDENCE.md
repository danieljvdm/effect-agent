# Phase 5 durable Tools and joined input evidence

Status: **Implemented**

Phase 5 extends the `DN` claim from safe-to-repeat toolkits to consequential external mutation.
Stated precisely: **ordinary Tool Calls that may mutate external state are
exactly-once-recorded and at-least-once-executed under an explicit uncertainty protocol** —
prepared before any handler starts, settled after validated completion, reconciled or stopped at
a visible Unknown Outcome after loss, and never replayed automatically without proof. Durable
Steps record exactly one accepted result per `(toolCallId, stepName)` while their external side
effects honestly remain at least once. Queued input can join an active Run at safe seams and
settles with its host. Approval waits are durable, worker-permit-free suspensions. No code path
claims exactly-once external side effects (DUR-003); the tests assert observable duplicate
execution rather than hiding it.

## Delivered package surface

- `@effect-agent/engine` owns the runtime-neutral seams:
  - the `ToolExecutionClass` annotation (`readonly` | `idempotent` | `uncertain`, fail-closed
    `uncertain` default) and `getToolExecutionClass`
    (`packages/engine/src/durable-step.ts`);
  - the `DurableStep` service — `step.do(name, OutputSchema, effect)` — provided locally per
    Tool Call, with an ephemeral pass-through when no durability hook exists, plus
    `RunStepHook`, `DurableStepError` (`packages/engine/src/durable-step.ts`);
  - the `RunDurabilityHook` (`commitResponse`/`prepareToolCalls`/`step`) and `RunTurnResume`
    batch-resume seam on `RunOptions`, ordered finish-part validations → `commitResponse` →
    approval preflight → `prepareToolCalls` → handler permits
    (`packages/engine/src/run-options.ts`, `packages/engine/src/index.ts`);
  - the carry-in fix: official history and `commitResponse` carry Schema-ENCODED tool-call
    parameters while handlers still receive decoded values
    (`packages/engine/src/index.ts`).
- `@effect-agent/session` owns the durable protocol:
  - seven new canonical payloads on the unchanged version-1 envelope — `ToolCallPrepared`,
    `ToolCallUnknown`, `ToolCallResolved`, `ToolStepSettled`, `ToolApprovalRequested`,
    `ToolApprovalDecided`, `ModelResponseInterrupted` (`packages/session/src/records.ts`);
  - the split Turn commit for tool-declaring Turns (`turn-response:` / `turn-prepared:` /
    `turn-results:` batch identities; no-tool Turns keep the P4 single batch) and
    prompt-transparent projection of every new tag (`packages/session/src/run-journal.ts`);
  - the `ToolReconciler` port with the fail-closed `ToolReconciler.uncertain` default and the
    `PreparedToolCallEvidence`/`ReconciliationDecision` contract
    (`packages/session/src/reconciler.ts`);
  - the extended recovery classifier: structured `openToolCalls`/`declaredPendingBatch`/
    `approvalsPending`/`joinedInputCovered` evidence, nine new decisions, and the revised
    precedence in which a recorded terminal outcome beats open tool calls
    (`packages/session/src/recovery.ts`);
  - the coordinator: durability-hook implementation, reconcile-then-mark-unknown recovery
    execution, `resolveUnknown` and `resolveApproval` operations, durable approval suspension,
    the joining/joined input drain with the prompt-coverage rule, joined settlement with the
    host, and `ModelResponseInterrupted` supersession audits
    (`packages/session/src/durable-runtime.ts`);
  - ledger port extensions — `joining`/`joined`/`suspended`/`unknown` states, `claimJoining`,
    `markJoined`, `revertJoining`, `suspend`, `recordApprovalDecision`, `markUnknown`,
    `recordUnknownResolution` — with eight new adapter-neutral conformance cases (twenty total)
    (`packages/session/src/ledger.ts`, `packages/session/src/ledger-conformance.ts`);
  - new coordinator failpoints at every new durable seam
    (`packages/session/src/durable-failpoint.ts`).
- `@effect-agent/storage-memory` and `@effect-agent/storage-sqlite` implement the extended
  ledger port; SQLite migration `3_durable_tools_and_joined_input` adds the approval-decision and
  unknown-resolution tables plus join/suspension/unknown columns, with before/after failpoints on
  every new mutation (`packages/storage-memory/src/memory-ledger.ts`,
  `packages/storage-sqlite/src/sqlite-ledger.ts`, `packages/storage-sqlite/src/migrations.ts`).
- `@effect-agent/capabilities` adapts the P2 approval stack to the durable path:
  `toDurableRunApprovalHook` fails closed to `unresolved` on any adapter fault
  (`packages/capabilities/src/engine-adapters.ts`).
- `@effect-agent/platform-node` wires `ToolReconciler.uncertain` as the default, accepts a
  deployment reconciler via `NodeDurableRuntimeOptions.toolReconciler`, forwards the new ledger
  operations through the ownership drain, and surfaces `"unknown"` recovery dispositions
  (`packages/platform-node/src/layers.ts`, `packages/platform-node/src/host.ts`).
- `@effect-agent/testing` extends the cumulative Travel Planner with the P5 booking slice:
  the idempotency-keyed `SupplierBookingDesk` with per-key call counters and injectable crash
  windows (`packages/testing/src/fixtures/travel-planner/deterministic-layers.ts`), the
  `book_flight` (uncertain, approval-gated), `cancel_booking` (idempotent by `bookingRef`,
  approval-gated), and `book_itinerary` (Durable Tool with Steps `reserve-flight`,
  `reserve-lodging`, `issue-confirmation`, each deriving its supplier idempotency key from
  `(toolCallId, stepName)`) Tools, and the `TravelSupplierReconcilerLayer` that queries the desk
  by those same derivations (`packages/testing/src/fixtures/travel-planner/phase5.ts`,
  `packages/testing/test/travel-planner-phase5.test.ts`).

## Executable exit-gate evidence

| ROADMAP P5 exit gate                                                      | Deterministic evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Recorded Tool outcomes do not rerun                                       | `packages/testing/test/durable-tools.test.ts` — “recorded Tool outcomes do not rerun and supersession is audited”; `packages/testing/test/travel-planner-phase5.test.ts` — “a recovered Attempt replays the settled booking result without a second supplier call”; crash harness — “kill at turn:after-response-append: the declared batch resumes without model re-invocation”                                                                                                                                                                                                                                                                                                                                                                                     |
| Uncertain ordinary effects do not replay automatically                    | durable-tools — “a prepared call without a settled record marks unknown under the default reconciler and frees the worker permit”; travel-planner-phase5 — “a kill during book_flight stops at UnknownToolOutcome under the default reconciler and the lane blocks”; classifier — “kill tools:after-prepared-append — a prepared call without an outcome marks unknown (DUR-009)”; crash harness — “kill at tools:after-prepared-append under the default reconciler: Unknown blocks the lane until resolveUnknown from a second process”                                                                                                                                                                                                                            |
| Completed Step results replay without executing                           | engine `packages/engine/test/durable-tool-seam.test.ts` — “DurableStep replays a hook-recorded result without executing”; durable-tools — “completed Step results replay without executing; the handler re-enters honestly”; travel-planner-phase5 — “re-entering book_itinerary replays reserve-flight from its ToolStepSettled record (supplier count 1)”; crash harness — “kill at step:after-step-append: step 1 replays from its record while step 2 executes once”                                                                                                                                                                                                                                                                                             |
| External Step side effects remain honestly at least once                  | travel-planner-phase5 — “a kill inside reserve-lodging re-executes the step and the supplier count of 2 is observable” (two supplier calls, one booking under the supplier's key); durable-tools Step test asserts handler re-entry counts                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| Joining input reverts before canonical append and reattaches after it     | `packages/testing/test/durable-join.test.ts` — “a kill at join:after-claim reverts the claim and delivers the input exactly once”, “a kill at join:after-canonical-append repairs the marker and reattaches without duplication”; travel-planner-phase5 — “a traveler follow-up killed at join:after-claim reverts to ready and joins exactly once on resume”, “a traveler follow-up killed at join:after-canonical-append reattaches without duplicate delivery”; crash harness — “kill at join:after-claim: RevertJoining returns the queued Submission and it joins exactly once”, “kill at join:after-canonical-append: RepairJoinMarker reattaches the input without duplication”; conformance — “revertJoining returns exactly the pre-append claims to ready” |
| Truncated Tool arguments never execute after recovery                     | engine — “truncated tool arguments never execute after resume” (RUN-004 re-validation on the batch-resume seam: nothing executes, no hook fires); DUR-008 holds structurally — only complete declared responses ever commit `turn-response:`/`turn-prepared:` batches, so no prepared record can exist for a length-truncated response (crash harness “SIGKILL mid model Turn …” pins the mid-stream case)                                                                                                                                                                                                                                                                                                                                                           |
| Never fabricate a booking result: recover / safe-repeat / stop at Unknown | travel-planner-phase5 — “the reconciler recovers the confirmed supplier booking canonically”, “an idempotent-annotated cancel repeats safely under its bookingRef contract”, “an unprovable booking stops at UnknownToolOutcome and resolveUnknown with supplier truth converges to one settlement”, plus the `assertSettledBookingsExistAtSupplier` fixture assertion on every settled log; crash harness — “SIGKILL mid-handler: the reconciler recovers the supplier booking without a second call”, “kill after the handler returns, before the results append: the idempotent contract re-executes honestly”, and its convergence assertion fails any settled booking result the supplier store cannot produce                                                  |

## Deliverable evidence

| ROADMAP P5 deliverable                                           | Where it is real                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Interrupted Effect AI response convergence                       | `ModelResponseInterrupted` audit on supersession (`packages/session/src/durable-runtime.ts`); durable-tools — “recorded Tool outcomes do not rerun and supersession is audited”; crash harness — “SIGKILL mid model Turn …”                                                                                                                                                                                                                                                                                                                                                                |
| Prepared and settled ordinary Tool records                       | `ToolCallPrepared`/`ToolCallSettled` split commits (`packages/session/src/records.ts`, `run-journal.ts`); durable-tools — “splits a tool Turn into response, prepared, and results commits”, “readonly toolkits never produce prepared records (P4 parity)”, “a mixed batch prepares only the non-readonly calls”; run-journal — “splits a tool Turn into response and results batches with stable identities”, “replays split-batch commits to the same prompt as P4 single-batch commits”                                                                                                |
| Unknown outcomes for unresolved external effects                 | `ToolCallUnknown`/`ToolCallResolved` + ledger `unknown` state; durable-tools unknown/resolution suite; classifier — “state unknown without covering resolutions awaits the authorized DUR-017 path”, “state unknown is never re-marked — the resolution regime owns the lane”                                                                                                                                                                                                                                                                                                              |
| Named durable Steps                                              | `DurableStep`/`RunStepHook`/`ToolStepSettled` (`packages/engine/src/durable-step.ts`, `packages/session/src/records.ts`); engine seam suite (pass-through, replay, duplicate-name conflict, success-only recording); durable-tools and travel-planner-phase5 Step tests                                                                                                                                                                                                                                                                                                                    |
| Application reconciliation hook                                  | `ToolReconciler` port + fail-closed default (`packages/session/src/reconciler.ts`); durable-tools — “a reconciler-recovered result settles canonically without executing the handler”; travel fixture `TravelSupplierReconcilerLayer` queries the desk by the handlers' own key derivations                                                                                                                                                                                                                                                                                                |
| Claimed `joining` and `joined` queued input                      | ledger `claimJoining`/`markJoined`/`revertJoining` + coordinator drain and joined settlement (`packages/session/src/durable-runtime.ts`); durable-join suite (seam join, coverage rule, drain policy, revert/reattach, host-linkage abort conflict, admitted-gap FIFO); conformance — “claims a contiguous joining prefix and stops at a gap”, “a joined Submission's settlement reservation is authorized by host linkage”; crash harness — “SIGKILL of the host after the join …”, “kill between host finalization and joined settlement: SettleJoinedWithHost completes the obligation” |
| Durable approval suspension                                      | ledger `suspend`/`recordApprovalDecision` + coordinator approval hook and `resolveApproval` (`packages/session/src/durable-runtime.ts`); durable-approval suite (suspension without settlement, resume without model re-invocation, denial-terminal, idempotent decisions, repair-lost-suspension, immediate-resume race, policy-auto atomic batch); crash harness — both “kill at approval:after-suspend …” rows and “kill at approval:after-request-append …”                                                                                                                            |
| Approval-gated Travel Planner booking and cancellation scenarios | `packages/testing/test/travel-planner-phase5.test.ts` — “an unapproved booking suspends durably without a settlement and resumes on resolveApproval”, “a denied booking settles failed with canonical request and decision records”, plus the SQLite end-to-end itinerary test proving class-shaped encoded parameters, prepared records, and canonical Step records persist (“books an itinerary on SQLite: class-shaped Tool parameters, prepared records, and canonical Step records persist end-to-end”)                                                                               |

The recovery classifier suite (`packages/session/test/recovery-classifier.test.ts`) covers every
new crash-matrix row as a pure decision over persisted snapshots, including the deliberate P5
precedence inversion (“a canonical settlement beats open tool calls (P5 precedence inversion,
DUR-002/DUR-015)”). Both ledger adapters run the same twenty-case conformance suite
(`packages/storage-memory/test/memory-ledger.test.ts`,
`packages/storage-sqlite/test/sqlite-ledger.test.ts`), and the SQLite adapter additionally proves
“persists joins, suspensions, approvals, and unknown resolutions across reopen” and “leaves a
recovery-classifiable state at every Phase 5 ledger failpoint”. The process-kill harness
(`packages/platform-node/test/crash/crash.test.ts`) replays the matrix under real child-process
loss, including `resolveUnknown`/`resolveApproval` driven from a second process.

## DUR-017 authorization scoping (plan decision point 8)

DUR-017 requires that Unknown Outcomes route to an **authorized, alerted resolution dependency**
before a deployment may claim durable liveness. Phase 5 scopes that claim honestly:

- **Authorization in P5 is service possession.** `resolveUnknown` and `resolveApproval` are
  operations on the `DurableAgentRuntime` service, exactly like `abort`: whoever a deployment
  hands the service to is the trust boundary. There is no authenticated principal check inside
  the runtime.
- **Every resolution is a canonical audit record.** Both operations require `author`/`resolver`
  and `reason`; the durable intent is idempotent per `(submissionId, toolCallId)` and a divergent
  re-resolution fails typed (`UnknownResolutionConflict`/`ApprovalConflict`). The canonical
  `ToolCallResolved`/`ToolApprovalDecided` records preserve who resolved what and why.
- **Visibility exists; alerting does not.** Unknown lanes are visible accepted-work obligations:
  the ledger state is `unknown`, recovery reports the `"unknown"` disposition, and
  `NodeDurableHost.startupRecovery` surfaces it. Aging, alerting, and the authenticated
  operator-facing admin surface (administrative explain/verify/retry/wake) are **Phase 7
  deliverables** — until then, a deployment claiming durable liveness must supply its own
  operational path to these two runtime operations.

## Stored-version policy

The SQLite storage version is now **3** (approval-decision and unknown-resolution tables plus
join/suspension/unknown submission columns). The check remains exact-or-zero: a fresh file
initializes to version 3; version-1 and version-2 development files — and any newer version —
fail with typed reset guidance before any mutation (D7/D-015;
`packages/storage-sqlite/test/sqlite-ledger.test.ts` — “rejects v1 and v2 files exactly with
reset guidance and still rejects newer versions”). No migration tooling exists or is promised.
The canonical record envelope stays at `schemaVersion: 1` because the seven new payloads are
additive tags on the existing union. Phase 4 checkpoints whose persisted projection lacks the new
`openToolCalls`/`unknownToolCalls`/`approvals` fields decode-fail and are rebuilt from canonical
records — checkpoints remain disposable derivatives.

## Non-claims

- **No exactly-once external side effects** (DUR-003). Ordinary Tool handlers and Step bodies may
  execute more than once across Attempts; the tests assert the duplicate supplier call counts
  instead of hiding them. Deduplication, where it happens, is the external system's idempotency
  key — never the framework.
- **The `idempotent` annotation is a declared application contract**, not a verified property.
  Recovery honors the declaration without proof; a false declaration is an application bug the
  framework cannot detect.
- **Reconciliation decisions are application claims about external truth.** The framework only
  guarantees fail-closed behavior when no policy is registered (`ToolReconciler.uncertain`) and
  that a reconciler fault leaves the call open rather than fabricating an outcome.
- **No operator surface, aging, or alerting for Unknown Outcomes yet** — see the DUR-017 scoping
  above; P7 scope.
- **Recoverable (non-terminal) approval denial is not implemented**: denial remains terminal per
  the P2 policy default.
- **Durable attached Subagents (S2) remain unimplemented**; the S2 slice builds on this phase's
  ledger and uncertainty machinery next.
