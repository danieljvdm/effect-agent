# ADR-0013: Durable Subagent establishment, waiting suspension, and binding resolution

- Status: Accepted by default
- Status note (2026-08-13): adopted as the S2 implementation default by the adopted S2 plan
  (decisions D1–D11) and implemented as such ([S2 evidence](../S2-EVIDENCE.md)); owner review may
  still amend it. The Subagent capability decision itself ([ADR-0010](0010-declared-attached-subagents.md))
  remains **Proposed** — this record fixes only the shape-level implementation defaults of the S2
  slice.
- Date: 2026-08-13
- Decision owners: Project owner
- Related decisions: D-013, D-020, D-029, D-030, D-031
- Builds on: [ADR-0010](0010-declared-attached-subagents.md),
  [ADR-0011](0011-durable-runtime-placement-and-leases.md),
  [ADR-0012](0012-durable-tool-uncertainty-and-steps.md)

## Context

S2 turns ADR-0010's durable attached delegation into executable protocol on the P4/P5 coordinator:
child accepted-work admission, requested/started/joined canonical records, a parent-owned budget
reservation, `waitingForChild` suspension with durable wakeup, verified Settlement join, durable
abort propagation, and exact Binding resolution during recovery.

The dependency graph constrains every option: `capabilities` (which owns `Subagent.define`,
`prepareInput`/`projectResult`, and policy math) does not depend on `session` (which owns records,
the ledger, and the coordinator), and `engine` imports neither. Eleven shape-level choices recur in
any re-implementation and need a durable record.

## Decision

### D1 — Durable seam: engine-owned hook, session implementation, capabilities consumption

`@effect-agent/engine` owns a `RunSubagentHook` contract on `RunOptions` (`establish`/`join`, in
core/engine vocabulary only) and provides a per-batch `SubagentDurability` service — added to
`EngineProvidedToolServices` so it never enters public requirements — with an explicit ephemeral
default when no hook is present. `@effect-agent/session` implements the hook as closures inside
the coordinator, exactly like the P5 `durability`/`approval`/`input` hooks.
`@effect-agent/capabilities`' delegation handler branches on the service: durable mode
establishes/waits/joins; absence falls back to the S1 in-process spawn, which is honest because
only the durable coordinator supplies the hook.

A genuinely new engine mechanism accompanies the seam: a waiting delegation call surfaces from
_inside_ a handler while siblings run, so the engine treats the `ToolCallWaiting` signal as “this
call stays open” rather than a batch failure — siblings run to completion, their results commit as
per-call late-settle batches, and the run suspends with the typed `AgentChildPending`.

**Rejected — coordinator-recognized delegation calls:** the coordinator cannot run
`prepareInput`/`projectResult` or grant preflight; session would re-own application-typed Subagent
policy, violating the spec §4.3 ownership table.

**Rejected — a durable `SubagentRuntime.layer` variant in session:** session would need the
capabilities delegation types (a forbidden dependency edge) or duplicate them as a competing
authoring surface.

### D2 — `waitingForChild`: extend `SuspensionReason`; one new wake operation

The suspended state carries every invariant `waitingForChild` needs (not worker-claimable, no
permit, obligation still owed, visible in recovery snapshots), so S2 adds
`WaitingForChildSuspension` to the deliberately additive `SuspensionReason` union rather than a
new submission state or a second suspend operation. `suspend`'s `resume-immediately` return
already covers the race where every child settled before the suspension committed.

The wake is a new explicit ledger operation, `recordChildSettled`: approval wake is
adapter-atomic inside `recordApprovalDecision`, but child settlement happens on a different lane
(potentially a different store on Cloudflare), so the transition
`suspended(WaitingForChild) → input-applied` — only when every listed child settled — must be an
idempotent cross-lane op the coordinator drives from every settlement-finalization path.

**Rejected — a parallel suspension subsystem:** it would duplicate the conformance surface of the
suspended state for no new invariant; the union was built for additive reasons.

**Rejected — wake as an adapter-internal side effect of `finalizeSettlement`:** cross-lane and
cross-store wakes cannot be an adapter transaction detail; a dropped wake must be repairable by
the classifier (`ResumeWaitingParent`) from durable evidence.

### D3 — Recovery style: idempotent handler re-entry first, binding-free executors for repairs

Every establishment step is a fenced, deterministic get-or-create keyed by
`(parentRunId, parentToolCallId)`, so the primary recovery mechanism is re-entering the delegation
handler through the existing P5 batch-resume seam. Recovery-pass executors exist only for repairs
that need no Agent Binding: `CompleteChildAdmission`, `RepairSubagentStartLink`,
`ResumeWaitingParent`, `ApplyJoinAccounting`, `PropagateChildAbort`,
`ReleaseOrphanChildReservation`. To make admission completable without a live handler, the
Schema-encoded child input rides the canonical `SubagentRequested` record.

Consequently delegation calls are excluded from the P5 `openToolCalls → MarkUnknown` path: their
prepared-without-outcome state is provably replay-safe, and the classifier separates
`openDelegationCalls` using durable evidence (a reservation row or `SubagentRequested`) plus the
core-hoisted delegation Tool naming rule.

**Rejected — a dedicated recovery executor per crash row:** executors that need `projectResult`
or the child Binding would drag application code into the recovery pass; batch resume already
re-enters the one code path that owns those capabilities.

### D4 — Deterministic child identity

Child `ConversationId` = `subagent:{parentSubmissionId}:{toolCallId}`; admission
`IdempotencyKey` = `subagent:{parentRunId}:{toolCallId}`; the parent's `Principal` carries into
the child for tenant/audit lineage. `SubagentRequested` records the intended identity as the
authoritative admission input (SUB-016).

**Rejected — generated child identity:** random identity would need a separate durable
pre-allocation step to stay idempotent; derivation makes duplicate establishment structurally
impossible.

### D5 — Durable child failure projection: bounded framework failure

A failed durable child's `SubmissionSettled.result` carries only the bounded
`{errorTag, message}` shape — the typed child failure union does not survive settlement — so
`mapChildFailure` cannot run at durable join. Failed, aborted, and compatibility-failed children
join as the Schema-backed `SubagentExecutionFailure` (classification + child references, no raw
Cause), spec §4.2 option 2. `mapChildFailure` remains the ephemeral-path contract.

**Rejected — persisting the typed child failure for durable joins:** it would make application
error unions part of the canonical wire contract across restarts and code changes;
Schema-declared durable domain-failure mapping is recorded as a later extension instead.

### D6 — Admission lookup: a tri-state `resolveAdmission` port operation

`resolveAdmission(key)` returns `NotAdmitted | Admitted | Indeterminate`. SQLite derives it from
`lookup` (a single strongly consistent store can always answer); the tri-state exists so the P6
Cloudflare adapter can answer `Indeterminate` honestly when the child-owner Durable Object is
unreachable. Only `NotAdmitted` permits an admission attempt (SUB-031).

**Rejected — a coordinator-level convention over `lookup`:** “absence from a projection is never
proof” must be a typed, conformance-tested contract, not a comment.

### D7 — Binding resolution: session-owned resolver, exact digests, fail-closed

`AgentBindingResolver` is a host-supplied session port resolving `(agentId, digests)` to a
`ResolvedBinding` built by `DurableWorkerBinding.make`, which captures the Binding's worker
requirements Context at Layer construction. Resolution is stable identity plus exact stored
digests; `BindingUnavailable`/`BindingDigestMismatch` fail closed (SUB-023). The coordinator
gains `runResolvedWorker`/`processConversationResolved`; a parent-linked head with an unresolvable
Binding settles the framework `ChildCompatibilityFailure` without application code, while a root
head is refused typed. The legacy single-binding `runWorker` is reimplemented over the resolver
with the identity check applied, closing the latent P4 gap where a worker ran any claimed head
against its one binding.

**Rejected — resolving “the latest” Binding:** recovery must never silently substitute newer
code; a digest mismatch is a typed compatibility failure, not a resolution strategy.

### D8 — Reservation placement: generic rows in the one `SubmissionLedger` port

Child budget reservations live behind the existing `SubmissionLedger` port as opaque-payload
state-machine rows (`reserved → releasePending → released`) fenced by the parent ownership token.
Adapters implement transitions only; the accounting arithmetic stays in session (join) and
capabilities (policy math), honoring “adapters MUST NOT invent Subagent policy”.

**Rejected — a separate budget store/port:** a second durable store splits recovery truth and
duplicates fencing, the same reasoning that rejected a physical Step Store in ADR-0012.

### D9 — Delegation calls remain prepared Tool Calls

Delegation calls keep `ToolCallPrepared` records and the uniform “no handler starts unprepared”
invariant, so batch resume works unchanged; recovery separates them via durable subagent evidence
plus the core-owned naming rule rather than a new `ToolExecutionClass` value.

**Rejected — a `delegation` execution class:** the execution-class annotation is a contract about
external side effects (ADR-0012); delegation replay safety comes from establishment idempotency,
not from a declared class.

### D10 — Authorization scoping: service possession plus structural verification

S2's trust boundary is service possession, exactly like P5's DUR-017 scoping. The S2 additions
are structural and tested: join verifies Parent Link identity and stored digests (an identifier
is never a capability; fabricated references fail closed), admission linkage is immutable on
replay, and child observation stays Conversation-scoped. Authenticated Principal/Tenant per-read
authorization, obligation aging, and alerting remain P7 deliverables, stated as non-claims.

### D11 — Usage accounting honesty

Structural dimensions (turns, tool calls) are computed from canonical child evidence at join;
token/cost dimensions conservatively consume the reserved amount when unreported
(`reserved-conservative`); abort joins freeze `aborted-conservative` and orphaned reservations
freeze `orphan-zero-consumed`. Overruns are recorded, never clipped (spec §7). Unreported usage
never creates budget.

## Consequences

Positive:

- durable delegation is idempotent by construction: every establishment step is a fenced
  read-or-perform keyed by parent Run and Tool Call identity, so crash recovery is mostly the
  ordinary batch resume;
- the parent waits without consuming any permit, and the smallest worker pool
  (`workerConcurrency: 1`) provably runs child and woken parent through one freed worker;
- parent and child fence independently — a stale writer on either side cannot touch the other
  lane;
- the engine, session, and capabilities ownership boundaries survive: no package gained a
  forbidden dependency edge, and adapters received only generic rows and transitions;
- the Cloudflare phase implements adapters against already-typed contracts (`Indeterminate`
  admission, cross-lane wake), not new protocol.

Negative:

- the delegation Tool handler is now re-entered across process boundaries and must stay
  deterministic over canonical inputs (projection recomputation is part of the contract);
- durable failed children lose their typed failure union at the join boundary until a
  Schema-declared durable domain-failure mapping is proposed;
- hosts must register child Bindings under byte-identical digest strings to the declared
  `targetDigests`, or every child claim fails closed — deliberate, but operationally unforgiving;
- token/cost accounting is conservative rather than exact when providers under-report;
- an unresolved ordinary child Tool blocks the parent abort path honestly (ADR-0010's “attached
  abort may block” consequence is now executable behavior).

## Validation

- Engine seam ordering, waiting-not-failure, sibling completion, ephemeral default, and public
  requirement exclusion: `packages/engine/test/subagent-durable-seam.test.ts`.
- Pure classifier coverage of every §13 row and the three changed-precedence pins:
  `packages/session/test/recovery-classifier.test.ts`.
- Ledger conformance for reservation idempotency/fencing/freeze/release, linkage replay,
  tri-state admission, and the cross-lane wake on both adapters:
  `packages/session/src/ledger-conformance.ts` via
  `packages/storage-memory/test/memory-ledger.test.ts` and
  `packages/storage-sqlite/test/sqlite-ledger.test.ts`.
- Coordinator failpoint sweep, duplicate establishment, abort-and-join, resolver fail-closed:
  `packages/testing/test/durable-subagents.test.ts`.
- Durable handler branch, projection fail-closed, bounded failure joins, grant recheck on resume:
  `packages/capabilities/test/subagent.test.ts` (“SubagentRuntime S2 durable delegation”).
- Reference-application slice with conservation, IDOR, and observation-scope evidence:
  `packages/testing/test/travel-planner-subagents-durable.test.ts`.
- Real process-kill matrix including simultaneous parent/child loss and both fencing directions:
  `packages/platform-node/test/crash/crash-subagents.test.ts`.
- See the [S2 evidence](../S2-EVIDENCE.md) for the full file-and-test map.
